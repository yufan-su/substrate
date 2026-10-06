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
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/cgapi"
)

// maxTargets bounds what one reader samples.
const maxTargets = 512

// targetTTL drops a target no request has named for this long, so a reader
// left behind by a finished run stops filling its ring.
const targetTTL = 2 * time.Minute

// reader samples cgroup v2 files of the registered targets and their child
// cgroups into a ring of rows.
type reader struct {
	root      string // the host's cgroup root
	uptime    string // path of /proc/uptime
	now       func() time.Time
	ringSize  int
	mu        sync.Mutex
	targets   []string
	requested map[string]time.Time // target -> last request naming it
	ttl       time.Duration
	boot      int64
	ring      []cgapi.Row
	nextSeq   uint64
	last      map[string]cgapi.Row // target|leaf -> last row read
	announced map[string]bool      // target|leaf -> gone row already written
}

func newReader(root, uptime string, ringSize int) *reader {
	return &reader{root: root, uptime: uptime, now: time.Now, ringSize: ringSize, nextSeq: 1,
		last: map[string]cgapi.Row{}, announced: map[string]bool{}, requested: map[string]time.Time{},
		ttl: targetTTL, boot: time.Now().UnixNano()}
}

// addTargets registers cgroup paths, relative to the root, to sample.
func (r *reader) addTargets(paths []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range paths {
		clean := filepath.Clean(p)
		if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
			return fmt.Errorf("target %q is not a path below the cgroup root", p)
		}
		r.requested[clean] = r.now()
		if slices.Contains(r.targets, clean) {
			continue
		}
		if len(r.targets) >= maxTargets {
			return fmt.Errorf("more than %d targets", maxTargets)
		}
		r.targets = append(r.targets, clean)
	}
	return nil
}

// sample reads every target and each of its child cgroups once, after
// dropping the targets that expired.
func (r *reader) sample() {
	r.mu.Lock()
	now := r.now()
	r.targets = slices.DeleteFunc(r.targets, func(t string) bool {
		if now.Sub(r.requested[t]) <= r.ttl {
			return false
		}
		delete(r.requested, t)
		for k := range r.last {
			if strings.HasPrefix(k, t+"|") {
				delete(r.last, k)
				delete(r.announced, k)
			}
		}
		return true
	})
	targets := slices.Clone(r.targets)
	r.mu.Unlock()
	up, _ := readUptime(r.uptime)
	wall := r.now().UnixNano()
	var rows []cgapi.Row
	seen := map[string]bool{}
	for _, t := range targets {
		dir := filepath.Join(r.root, t)
		leaves := []string{""}
		if entries, err := os.ReadDir(dir); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					leaves = append(leaves, e.Name())
				}
			}
		}
		for _, leaf := range leaves {
			key := t + "|" + leaf
			row, err := readCgroup(filepath.Join(dir, leaf))
			if err != nil {
				continue
			}
			row.WallNanos, row.UptimeSeconds, row.Target, row.Leaf = wall, up, t, leaf
			rows = append(rows, row)
			seen[key] = true
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// A cgroup read before and missing now is reported gone once, with its
	// last counters, so sums over leaves keep what it used.
	for _, key := range slices.Sorted(maps.Keys(r.last)) {
		if seen[key] || r.announced[key] {
			continue
		}
		gone := r.last[key]
		gone.WallNanos, gone.UptimeSeconds, gone.Gone = wall, up, true
		rows = append(rows, gone)
		r.announced[key] = true
	}
	for _, row := range rows {
		row.Seq = r.nextSeq
		r.nextSeq++
		if !row.Gone {
			r.last[row.Target+"|"+row.Leaf] = row
			delete(r.announced, row.Target+"|"+row.Leaf)
		}
		r.ring = append(r.ring, row)
	}
	if over := len(r.ring) - r.ringSize; over > 0 {
		r.ring = slices.Delete(r.ring, 0, over)
	}
}

// since returns up to cgapi.MaxRows rows after seq, or none when latest is
// set, and the reader's clocks now.
func (r *reader) since(seq uint64, latest bool) cgapi.Response {
	up, _ := readUptime(r.uptime)
	r.mu.Lock()
	defer r.mu.Unlock()
	resp := cgapi.Response{WallNanos: r.now().UnixNano(), UptimeSeconds: up, Oldest: r.nextSeq,
		Latest: r.nextSeq - 1, Boot: r.boot, Rows: []cgapi.Row{}}
	if len(r.ring) > 0 {
		resp.Oldest = r.ring[0].Seq
	}
	if latest {
		return resp
	}
	i, _ := slices.BinarySearchFunc(r.ring, seq+1, func(row cgapi.Row, s uint64) int {
		switch {
		case row.Seq < s:
			return -1
		case row.Seq > s:
			return 1
		}
		return 0
	})
	end := min(len(r.ring), i+cgapi.MaxRows)
	resp.Rows = append(resp.Rows, r.ring[i:end]...)
	resp.More = end < len(r.ring)
	return resp
}

// readCgroup reads one cgroup's CPU, pressure and memory files. Only
// cpu.stat is required; the others are absent for some cgroups.
func readCgroup(dir string) (cgapi.Row, error) {
	var row cgapi.Row
	stat, err := readKeyed(filepath.Join(dir, "cpu.stat"))
	if err != nil {
		return row, err
	}
	row.UsageUsec = stat["usage_usec"]
	row.NrPeriods = stat["nr_periods"]
	row.NrThrottled = stat["nr_throttled"]
	row.ThrottledUsec = stat["throttled_usec"]
	if b, err := os.ReadFile(filepath.Join(dir, "cpu.pressure")); err == nil {
		row.CPUSomeUsec = pressureTotal(string(b), "some")
	}
	if b, err := os.ReadFile(filepath.Join(dir, "memory.current")); err == nil {
		row.MemoryCurrent, _ = strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		row.WorkingSetBytes = row.MemoryCurrent
		if ms, err := readKeyed(filepath.Join(dir, "memory.stat")); err == nil {
			row.WorkingSetBytes = max(0, row.MemoryCurrent-ms["inactive_file"])
		}
	}
	return row, nil
}

// readKeyed parses "key value" lines.
func readKeyed(path string) (map[string]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]int64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), " ")
		if !ok {
			continue
		}
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			out[k] = n
		}
	}
	return out, sc.Err()
}

// pressureTotal returns total= of the kind ("some" or "full") line.
func pressureTotal(text, kind string) int64 {
	for line := range strings.Lines(text) {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != kind {
			continue
		}
		for _, f := range fields[1:] {
			if v, ok := strings.CutPrefix(f, "total="); ok {
				n, _ := strconv.ParseInt(v, 10, 64)
				return n
			}
		}
	}
	return 0
}

func readUptime(path string) (float64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	first, _, _ := strings.Cut(strings.TrimSpace(string(b)), " ")
	return strconv.ParseFloat(first, 64)
}

var errNoCgroupV2 = errors.New("no cgroup v2 hierarchy")

// checkRoot fails unless root is a cgroup v2 hierarchy.
func checkRoot(root string) error {
	if _, err := os.Stat(filepath.Join(root, "cgroup.controllers")); errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%w at %s", errNoCgroupV2, root)
	} else if err != nil {
		return err
	}
	return nil
}
