// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/cgapi"
)

const workerScope = "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod1.slice/cri-containerd-a.scope"

// writeCgroup writes a fake cgroup directory with the files the reader reads.
func writeCgroup(t *testing.T, dir string, usage, current, inactive int64) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"cpu.stat":       fmt.Sprintf("usage_usec %d\nuser_usec 1\nsystem_usec 1\nnr_periods 100\nnr_throttled 4\nthrottled_usec 900\n", usage),
		"cpu.pressure":   "some avg10=0.00 avg60=0.00 avg300=0.00 total=777\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=555\n",
		"memory.current": fmt.Sprintf("%d\n", current),
		"memory.stat":    fmt.Sprintf("anon 1\ninactive_file %d\n", inactive),
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func newTestReader(t *testing.T, ringSize int) (*reader, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "cgroup.controllers"), []byte("cpu memory\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	uptime := filepath.Join(root, "uptime")
	if err := os.WriteFile(uptime, []byte("20429.96 1000.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return newReader(root, uptime, ringSize), root
}

func TestReaderSample(t *testing.T) {
	t.Parallel()
	r, root := newTestReader(t, 100)
	writeCgroup(t, filepath.Join(root, workerScope), 3000, 5000, 1000)
	writeCgroup(t, filepath.Join(root, workerScope, "uid1-_pause"), 2000, 3000, 0)
	writeCgroup(t, filepath.Join(root, workerScope, "ateom"), 1000, 2000, 500)
	if err := r.addTargets([]string{workerScope, workerScope}); err != nil {
		t.Fatal(err)
	}
	r.sample()

	resp := r.since(0, false)
	got := map[string]cgapi.Row{}
	for _, row := range resp.Rows {
		got[row.Leaf] = row
	}
	if len(resp.Rows) != 3 || resp.Oldest != 1 || resp.UptimeSeconds != 20429.96 {
		t.Fatalf("response = %+v, want 3 rows from seq 1 and the uptime", resp)
	}
	for _, tc := range []struct {
		leaf string
		want cgapi.Row
	}{
		{"", cgapi.Row{UsageUsec: 3000, MemoryCurrent: 5000, WorkingSetBytes: 4000}},
		{"uid1-_pause", cgapi.Row{UsageUsec: 2000, MemoryCurrent: 3000, WorkingSetBytes: 3000}},
		{"ateom", cgapi.Row{UsageUsec: 1000, MemoryCurrent: 2000, WorkingSetBytes: 1500}},
	} {
		t.Run("leaf "+tc.leaf, func(t *testing.T) {
			g := got[tc.leaf]
			if g.UsageUsec != tc.want.UsageUsec || g.MemoryCurrent != tc.want.MemoryCurrent || g.WorkingSetBytes != tc.want.WorkingSetBytes {
				t.Errorf("got %+v, want %+v", g, tc.want)
			}
			if g.NrPeriods != 100 || g.NrThrottled != 4 || g.ThrottledUsec != 900 || g.CPUSomeUsec != 777 || g.Target != workerScope {
				t.Errorf("got %+v, want the cpu.stat and pressure fields of the fixture", g)
			}
		})
	}
}

func TestReaderGoneLeafAndRing(t *testing.T) {
	t.Parallel()
	r, root := newTestReader(t, 5)
	writeCgroup(t, filepath.Join(root, workerScope), 3000, 0, 0)
	writeCgroup(t, filepath.Join(root, workerScope, "uid1-_pause"), 2000, 0, 0)
	if err := r.addTargets([]string{workerScope}); err != nil {
		t.Fatal(err)
	}
	r.sample() // seq 1-2
	if err := os.RemoveAll(filepath.Join(root, workerScope, "uid1-_pause")); err != nil {
		t.Fatal(err)
	}
	r.sample() // seq 3 target, seq 4 gone leaf
	r.sample() // seq 5 target only: the gone row is written once

	resp := r.since(2, false)
	var gone []cgapi.Row
	for _, row := range resp.Rows {
		if row.Gone {
			gone = append(gone, row)
		}
	}
	if len(resp.Rows) != 3 || len(gone) != 1 || gone[0].Leaf != "uid1-_pause" || gone[0].UsageUsec != 2000 {
		t.Errorf("rows after seq 2 = %+v, want 3 rows with one gone row carrying the last usage", resp.Rows)
	}
	r.sample() // seq 6: the ring of 5 drops seq 1
	if resp := r.since(0, false); resp.Oldest != 2 || resp.Rows[0].Seq != 2 {
		t.Errorf("oldest = %d, first row %d, want 2 after the ring dropped seq 1", resp.Oldest, resp.Rows[0].Seq)
	}
}

func TestAddTargetsRejectsEscapes(t *testing.T) {
	t.Parallel()
	r, _ := newTestReader(t, 10)
	for _, p := range []string{"../etc", "/abs", ".", "a/../../b", "etc/x", "kubepods.slice/../x", "kubepods.slicex/y"} {
		if err := r.addTargets([]string{p}); err == nil {
			t.Errorf("addTargets(%q) accepted a path outside %s", p, kubepodsRoot)
		}
	}
	many := make([]string, 0, maxTargets+1)
	for i := range maxTargets + 1 {
		many = append(many, fmt.Sprintf("%s/t%d", kubepodsRoot, i))
	}
	if err := r.addTargets(many); err == nil {
		t.Errorf("addTargets accepted %d targets, want the request rejected", len(many))
	}
	if err := r.addTargets([]string{workerScope}); err != nil {
		t.Fatal(err)
	}
	if len(r.requested) != len(r.targets) || len(r.targets) != 1 {
		t.Errorf("after a rejected request: %d requested, %d targets, want 1 and 1; a rejected request must leave no trace", len(r.requested), len(r.targets))
	}
}

func TestHandler(t *testing.T) {
	t.Parallel()
	r, root := newTestReader(t, 100)
	writeCgroup(t, filepath.Join(root, workerScope), 3000, 0, 0)
	srv := httptest.NewServer(newHandler(r))
	t.Cleanup(srv.Close)

	get := func(query string) (int, cgapi.Response) {
		t.Helper()
		resp, err := http.Get(srv.URL + cgapi.SamplesRoute + query)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out cgapi.Response
		if resp.StatusCode == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				t.Fatal(err)
			}
		}
		return resp.StatusCode, out
	}
	if code, out := get("?target=" + workerScope); code != http.StatusOK || len(out.Rows) != 0 {
		t.Fatalf("first pull = %d %+v, want 200 and no rows before a sample", code, out)
	}
	r.now = func() time.Time { return time.Unix(1700000000, 0) }
	r.sample()
	if code, out := get("?since=0"); code != http.StatusOK || len(out.Rows) != 1 || out.Rows[0].WallNanos != 1700000000e9 {
		t.Errorf("pull after a sample = %d %+v, want the target's row stamped by the reader clock", code, out)
	}
	if code, out := get("?since=latest"); code != http.StatusOK || len(out.Rows) != 0 || out.Latest != 1 {
		t.Errorf("since=latest = %d %+v, want no rows and latest 1", code, out)
	}
	if code, _ := get("?since=x"); code != http.StatusBadRequest {
		t.Errorf("bad since answered %d, want 400", code)
	}
	if code, _ := get("?target=../x"); code != http.StatusBadRequest {
		t.Errorf("escaping target answered %d, want 400", code)
	}
}

func TestCheckRoot(t *testing.T) {
	t.Parallel()
	if err := checkRoot(t.TempDir()); err == nil {
		t.Errorf("checkRoot accepted a directory with no cgroup.controllers")
	}
	r, _ := newTestReader(t, 1)
	if err := checkRoot(r.root); err != nil {
		t.Errorf("checkRoot = %v on a cgroup v2 root", err)
	}
}

func TestReaderPagingAndLatest(t *testing.T) {
	t.Parallel()
	r, root := newTestReader(t, 3*cgapi.MaxRows)
	for i := range 4 {
		writeCgroup(t, filepath.Join(root, workerScope, fmt.Sprintf("leaf%d", i)), 1, 0, 0)
	}
	writeCgroup(t, filepath.Join(root, workerScope), 1, 0, 0)
	if err := r.addTargets([]string{workerScope}); err != nil {
		t.Fatal(err)
	}
	for range cgapi.MaxRows/5 + 10 { // 5 rows a round
		r.sample()
	}
	total := uint64(5 * (cgapi.MaxRows/5 + 10))

	if resp := r.since(0, true); len(resp.Rows) != 0 || resp.Latest != total || resp.Boot == 0 {
		t.Errorf("since=latest = %d rows, latest %d, boot %d; want no rows, latest %d and the boot time", len(resp.Rows), resp.Latest, resp.Boot, total)
	}
	first := r.since(0, false)
	if len(first.Rows) != cgapi.MaxRows || !first.More {
		t.Fatalf("first page = %d rows, more %v; want %d and more", len(first.Rows), first.More, cgapi.MaxRows)
	}
	second := r.since(first.Rows[len(first.Rows)-1].Seq, false)
	if uint64(len(first.Rows)+len(second.Rows)) != total || second.More || second.Rows[0].Seq != cgapi.MaxRows+1 {
		t.Errorf("second page = %d rows from seq %d, more %v; want the remaining %d from %d", len(second.Rows), second.Rows[0].Seq, second.More, total-cgapi.MaxRows, cgapi.MaxRows+1)
	}
}

func TestReaderExpiresTargets(t *testing.T) {
	t.Parallel()
	r, root := newTestReader(t, 100)
	writeCgroup(t, filepath.Join(root, workerScope), 1, 0, 0)
	now := time.Unix(1000, 0)
	r.now = func() time.Time { return now }
	if err := r.addTargets([]string{workerScope}); err != nil {
		t.Fatal(err)
	}
	r.sample()
	now = now.Add(targetTTL - time.Second)
	r.sample()
	now = now.Add(2 * time.Second)
	r.sample()
	if resp := r.since(0, false); len(resp.Rows) != 2 || len(r.targets) != 0 {
		t.Errorf("%d rows and targets %v, want 2 rows and the target dropped after %v without a request", len(resp.Rows), r.targets, targetTTL)
	}
	if err := r.addTargets([]string{workerScope}); err != nil {
		t.Fatal(err)
	}
	r.sample()
	if resp := r.since(2, false); len(resp.Rows) != 1 || resp.Rows[0].Gone {
		t.Errorf("rows after a new request = %+v, want the target sampled again with no gone row", resp.Rows)
	}
}
