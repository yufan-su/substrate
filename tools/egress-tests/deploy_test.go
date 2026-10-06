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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// logShim logs its name and arguments to $SHIM_LOG and drains stdin.
const logShim = `#!/bin/bash
echo "$(basename "$0") $*" >>"${SHIM_LOG}"
cat >/dev/null
`

// goShim stands in for go build: it writes a logging kubectl-ate to -o.
const goShim = `#!/bin/bash
echo "go $*" >>"${SHIM_LOG}"
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-o" ]]; then
    printf '#!/bin/bash\necho "kubectl-ate $*" >>"${SHIM_LOG}"\n' >"$2"
    chmod +x "$2"
  fi
  shift
done
`

// TestDeployDelete runs deploy.sh --delete in a scratch repository with
// kubectl, go and hack/run-tool.sh replaced by logging shims, no
// .ate-dev-env.sh, and a TMPDIR with a space in it.
func TestDeployDelete(t *testing.T) {
	t.Parallel()
	for _, tool := range []string{"bash", "git"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found: %v", tool, err)
		}
	}
	tmp := t.TempDir()
	repo := filepath.Join(tmp, "repo")
	shims := filepath.Join(tmp, "shims")
	tmpdir := filepath.Join(tmp, "with space")
	log := filepath.Join(tmp, "shim.log")

	writeFile(t, filepath.Join(repo, "tools/egress-tests/deploy.sh"), readFile(t, "deploy.sh"), 0o755)
	manifests, err := filepath.Glob("manifests/*")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range manifests {
		writeFile(t, filepath.Join(repo, "tools/egress-tests", m), readFile(t, m), 0o644)
	}
	writeFile(t, filepath.Join(repo, "hack/run-tool.sh"), logShim, 0o755)
	writeFile(t, filepath.Join(shims, "kubectl"), logShim, 0o755)
	writeFile(t, filepath.Join(shims, "go"), goShim, 0o755)
	if err := os.MkdirAll(tmpdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "init", "-q", repo).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	gitDir := filepath.Dir(mustLookPath(t, "git"))

	cmd := exec.Command("bash", "tools/egress-tests/deploy.sh", "--delete")
	cmd.Dir = repo
	cmd.Env = []string{
		"PATH=" + shims + ":" + gitDir + ":/usr/bin:/bin",
		"HOME=" + tmp,
		"TMPDIR=" + tmpdir,
		"SHIM_LOG=" + log,
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("deploy.sh --delete: %v\n%s", err, out)
	}

	logged := readFile(t, log)
	for _, want := range []string{
		"kubectl-ate delete actor-template egress-tests-actor -a egress-tests",
		"kubectl delete namespace egress-tests-targets --ignore-not-found",
		"run-tool.sh ko delete --ignore-not-found -f -",
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("deploy.sh --delete did not run %q; ran:\n%s", want, logged)
		}
	}
	left, err := os.ReadDir(tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Errorf("TMPDIR %q holds %d entries after exit, want the kubectl-ate build dir removed", tmpdir, len(left))
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func mustLookPath(t *testing.T, name string) string {
	t.Helper()
	p, err := exec.LookPath(name)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
