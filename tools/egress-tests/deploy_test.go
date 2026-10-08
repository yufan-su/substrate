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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
)

// logShim logs its name and arguments to $SHIM_LOG and drains stdin. No
// campaign Pod exists.
const logShim = `#!/bin/bash
echo "$(basename "$0") $*" >>"${SHIM_LOG}"
cat >/dev/null
if [[ "$*" == *"get pod egress-campaign"* ]]; then
  exit 1
fi
`

// goShim stands in for go build: it writes a logging kubectl-ate to -o,
// which reports a ready golden snapshot when asked for the template, after
// any --kubeconfig and --context.
const goShim = `#!/bin/bash
echo "go $*" >>"${SHIM_LOG}"
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-o" ]]; then
    cat >"$2" <<'SHIM'
#!/bin/bash
echo "kubectl-ate $*" >>"${SHIM_LOG}"
while [[ "$1" == "--kubeconfig" || "$1" == "--context" ]]; do
  shift 2
done
if [[ "$1 $2" == "get actor-template" ]]; then
  echo '{"status":{"goldenSnapshotStatus":{"goldenTag":{"name":"golden"}}}}'
fi
SHIM
    chmod +x "$2"
  fi
  shift
done
`

// deployToolsRequired says whether a missing tool fails TestDeployScript
// rather than skipping it. A skip and a pass share an exit code, so in CI
// the test must fail, or the script would go untested unnoticed.
func deployToolsRequired() bool {
	return os.Getenv("CI") == "true" || os.Getenv("REQUIRE_DEPLOY_TOOLS") == "true"
}

// TestDeployScript runs deploy.sh in a scratch repository with kubectl, go
// and hack/run-tool.sh replaced by logging shims, no .ate-dev-env.sh, and a
// TMPDIR with a space in it.
func TestDeployScript(t *testing.T) {
	t.Parallel()
	for _, tool := range []string{"bash", "git", "jq"} {
		if _, err := exec.LookPath(tool); err != nil {
			if deployToolsRequired() {
				t.Fatalf("%s not found: %v", tool, err)
			}
			t.Skipf("%s not found: %v", tool, err)
		}
	}
	for _, tc := range []struct {
		name    string
		args    []string
		env     []string
		want    []string
		notWant []string
		// dir, when set, is the directory under the repository the script
		// runs from.
		dir string
		// cluster, when set, must reach every kubectl, kubectl-ate and ko
		// call. "{repo}" stands for the scratch repository's path.
		cluster string
	}{
		{
			name: "delete without the env file",
			args: []string{"--delete"},
			want: []string{
				"kubectl-ate delete actor-template egress-tests-actor -a egress-tests",
				"kubectl delete namespace egress-tests-targets --ignore-not-found",
				"kubectl delete daemonset egress-tests-cgreader --namespace=egress-tests --ignore-not-found",
				"run-tool.sh ko delete --ignore-not-found -f -",
			},
		},
		{
			name: "deploy without the cgroup reader",
			args: []string{"--deploy", "--endpoints", "2"},
			env:  []string{"BUCKET_NAME=b", "KO_DOCKER_REPO=r"},
			want: []string{
				"kubectl-ate create actor-template -f -",
				"kubectl rollout status deployment/egress-target --namespace=egress-tests-targets --timeout=300s",
			},
			notWant: []string{"daemonset/egress-tests-cgreader"},
		},
		{
			name: "deploy with the cgroup reader",
			args: []string{"--deploy", "--endpoints", "2", "--cgreader"},
			env:  []string{"BUCKET_NAME=b", "KO_DOCKER_REPO=r"},
			want: []string{"kubectl rollout status daemonset/egress-tests-cgreader --namespace=egress-tests --timeout=300s"},
		},
		{
			name:    "deploy to a named cluster",
			args:    []string{"--deploy", "--endpoints", "2", "--cgreader", "--kubeconfig", "/k/c 2", "--context", "ctx"},
			env:     []string{"BUCKET_NAME=b", "KO_DOCKER_REPO=r"},
			want:    []string{"run-tool.sh ko apply -f - -- --kubeconfig /k/c 2 --context ctx"},
			cluster: "--kubeconfig /k/c 2 --context ctx",
		},
		{
			name:    "delete from a named context",
			args:    []string{"--delete", "--context=ctx"},
			want:    []string{"run-tool.sh ko delete --ignore-not-found -f - --context ctx"},
			cluster: "--context ctx",
		},
		{
			name:    "unpatch the gateway in a named context",
			args:    []string{"--unpatch-gateway", "--context", "ctx"},
			want:    []string{"kubectl --context ctx -n ate-system delete configmap egress-tests-target-ca --ignore-not-found"},
			cluster: "--context ctx",
		},
		{
			// The script changes to the repository root, so a relative
			// kubeconfig must be resolved against the caller's directory.
			name:    "relative kubeconfig from a subdirectory",
			args:    []string{"--delete", "--kubeconfig", "kc.yaml"},
			dir:     "tools/egress-tests",
			cluster: "--kubeconfig {repo}/tools/egress-tests/kc.yaml",
		},
		{
			name: "campaign",
			args: []string{"--campaign", "--kubeconfig", "/k/c", "--context", "ctx", "--image", "img"},
			want: []string{
				"kubectl --kubeconfig /k/c --context ctx apply -f tools/egress-tests/manifests/campaign.yaml.tmpl -f tools/egress-tests/manifests/campaign-pvc.yaml.tmpl",
				"kubectl --kubeconfig /k/c --context ctx -n egress-tests get pod egress-campaign-rehearsal",
				"kubectl --kubeconfig /k/c --context ctx create -f -",
			},
			cluster: "--kubeconfig /k/c --context ctx",
		},
		{
			name:    "campaign without the Pod",
			args:    []string{"--campaign", "--context", "ctx", "--image", "img", "--no-start"},
			want:    []string{"kubectl --context ctx apply -f tools/egress-tests/manifests/campaign.yaml.tmpl"},
			notWant: []string{"create -f -"},
		},
		{
			name: "rehearsal",
			args: []string{"--campaign", "--rehearsal", "--context", "ctx", "--image", "img"},
			want: []string{
				"kubectl --context ctx -n egress-tests get pod egress-campaign\n",
				"kubectl --context ctx create -f -",
			},
		},
		{
			name: "delete the campaign",
			args: []string{"--delete-campaign", "--context", "ctx"},
			want: []string{
				"kubectl --context ctx -n egress-tests delete pod egress-campaign egress-campaign-rehearsal --ignore-not-found",
				"kubectl --context ctx delete -f tools/egress-tests/manifests/campaign.yaml.tmpl --ignore-not-found",
			},
			notWant: []string{"kubectl-ate", "ko delete", "campaign-pvc.yaml.tmpl"},
		},
		{
			name: "purge the campaign",
			args: []string{"--delete-campaign", "--purge", "--context", "ctx"},
			want: []string{
				"kubectl --context ctx delete -f tools/egress-tests/manifests/campaign.yaml.tmpl --ignore-not-found",
				"kubectl --context ctx delete -f tools/egress-tests/manifests/campaign-pvc.yaml.tmpl --ignore-not-found",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logged, tmpdir, repo := runDeployScript(t, tc.dir, tc.args, tc.env)
			tc.cluster = strings.ReplaceAll(tc.cluster, "{repo}", repo)
			for _, want := range tc.want {
				if !strings.Contains(logged, want) {
					t.Errorf("deploy.sh did not run %q; ran:\n%s", want, logged)
				}
			}
			for _, nw := range tc.notWant {
				if strings.Contains(logged, nw) {
					t.Errorf("deploy.sh ran %q; ran:\n%s", nw, logged)
				}
			}
			if tc.cluster != "" {
				for _, line := range strings.Split(strings.TrimSpace(logged), "\n") {
					tool, rest, _ := strings.Cut(line, " ")
					switch {
					case tool == "kubectl" || tool == "kubectl-ate":
						if !strings.HasPrefix(rest, tc.cluster+" ") {
							t.Errorf("%s call without %q first: %s", tool, tc.cluster, line)
						}
					case strings.HasPrefix(line, "run-tool.sh ko resolve"):
					case strings.HasPrefix(line, "run-tool.sh ko"):
						if !strings.HasSuffix(line, " "+tc.cluster) {
							t.Errorf("ko call without %q last: %s", tc.cluster, line)
						}
					}
				}
			}
			left, err := os.ReadDir(tmpdir)
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != 0 {
				t.Errorf("TMPDIR %q holds %d entries after exit, want the kubectl-ate build dir removed", tmpdir, len(left))
			}
		})
	}
}

// runDeployScript runs deploy.sh with args from dir under a scratch
// repository and returns the shims' log, the TMPDIR it used and the
// repository's path.
func runDeployScript(t *testing.T, dir string, args, env []string) (logged, tmpdir, repo string) {
	t.Helper()
	tmp := t.TempDir()
	// The script's $PWD has symlinks resolved, as on macOS's /var.
	tmp, err := filepath.EvalSymlinks(tmp)
	if err != nil {
		t.Fatal(err)
	}
	repo = filepath.Join(tmp, "repo")
	shims := filepath.Join(tmp, "shims")
	tmpdir = filepath.Join(tmp, "with space")
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
	cmd := exec.Command("bash", append([]string{filepath.Join(repo, "tools/egress-tests/deploy.sh")}, args...)...)
	cmd.Dir = filepath.Join(repo, dir)
	cmd.Env = append([]string{
		"PATH=" + shims + ":" + filepath.Dir(mustLookPath(t, "git")) + ":" + filepath.Dir(mustLookPath(t, "jq")) + ":/usr/bin:/bin",
		"HOME=" + tmp,
		"TMPDIR=" + tmpdir,
		"SHIM_LOG=" + log,
	}, env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("deploy.sh %v: %v\n%s", args, err, out)
	}
	return readFile(t, log), tmpdir, repo
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

// TestDeployMaxEndpointsMatchesTheDriver pins deploy.sh's MAX_ENDPOINTS to
// egressapi.MaxEndpoints: the script and the driver must refuse the same
// --endpoints, or a deploy the driver accepts fails at preflight.
func TestDeployMaxEndpointsMatchesTheDriver(t *testing.T) {
	t.Parallel()
	m := regexp.MustCompile(`(?m)^MAX_ENDPOINTS=(\d+)$`).FindStringSubmatch(readFile(t, "deploy.sh"))
	if m == nil {
		t.Fatal("deploy.sh has no MAX_ENDPOINTS=<n> line")
	}
	if got, _ := strconv.Atoi(m[1]); got != egressapi.MaxEndpoints {
		t.Errorf("deploy.sh MAX_ENDPOINTS = %d, want egressapi.MaxEndpoints = %d", got, egressapi.MaxEndpoints)
	}
}

// TestPrintTargets renders the target Deployment and Services with
// --print-targets, which must not need a cluster: kubectl is not on PATH.
func TestPrintTargets(t *testing.T) {
	t.Parallel()
	// Every Service gets its own target ports, and the target listens on
	// the whole MAX_ENDPOINTS range whatever --endpoints is, so a Service
	// left from an earlier, larger deploy still reaches an open port.
	wantArgs := []string{
		fmt.Sprintf("--listen-ports=10000-%d", 10000+egressapi.MaxEndpoints-1),
		fmt.Sprintf("--tls-listen-ports=12000-%d", 12000+egressapi.MaxEndpoints-1),
	}
	for _, tc := range []struct {
		name      string
		args      []string
		wantImage string
	}{
		{
			name:      "the ko image",
			args:      []string{"--endpoints", "3"},
			wantImage: "ko://github.com/agent-substrate/substrate/tools/egress-tests/target",
		},
		{
			name:      "another image",
			args:      []string{"--endpoints", "3", "--target-image", "gcr.io/p/target@sha256:0"},
			wantImage: "gcr.io/p/target@sha256:0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := filepath.Join(t.TempDir(), "repo")
			writeFile(t, filepath.Join(repo, "tools/egress-tests/deploy.sh"), readFile(t, "deploy.sh"), 0o755)
			for _, m := range []string{"targets.yaml.tmpl", "target-service.yaml.tmpl"} {
				writeFile(t, filepath.Join(repo, "tools/egress-tests/manifests", m), readFile(t, filepath.Join("manifests", m)), 0o644)
			}
			if out, err := exec.Command("git", "init", "-q", repo).CombinedOutput(); err != nil {
				t.Fatalf("git init: %v\n%s", err, out)
			}
			cmd := exec.Command("bash", append([]string{"tools/egress-tests/deploy.sh", "--print-targets"}, tc.args...)...)
			cmd.Dir = repo
			cmd.Env = []string{"PATH=" + filepath.Dir(mustLookPath(t, "git")) + ":/usr/bin:/bin", "HOME=" + repo}
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("deploy.sh --print-targets: %v\n%s", err, out)
			}
			var services []corev1.Service
			var dep *appsv1.Deployment
			for _, doc := range strings.Split(string(out), "\n---\n") {
				var meta struct{ Kind string }
				if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
					t.Fatalf("decoding %q: %v", doc, err)
				}
				switch meta.Kind {
				case "Deployment":
					dep = &appsv1.Deployment{}
					if err := yaml.UnmarshalStrict([]byte(doc), dep); err != nil {
						t.Fatal(err)
					}
				case "Service":
					var svc corev1.Service
					if err := yaml.UnmarshalStrict([]byte(doc), &svc); err != nil {
						t.Fatal(err)
					}
					services = append(services, svc)
				}
			}
			if dep == nil || len(services) != 3 {
				t.Fatalf("got a Deployment %v and %d Services, want one and 3", dep != nil, len(services))
			}
			c := dep.Spec.Template.Spec.Containers[0]
			if c.Image != tc.wantImage {
				t.Errorf("target image = %q, want %q", c.Image, tc.wantImage)
			}
			for _, a := range wantArgs {
				if !slices.Contains(c.Args, a) {
					t.Errorf("target args %v lack %q", c.Args, a)
				}
			}
			for i, svc := range services {
				wantHTTP, wantHTTPS := intstr.FromInt32(int32(10000+i)), intstr.FromInt32(int32(12000+i))
				if p := svc.Spec.Ports; len(p) != 2 || p[0].TargetPort != wantHTTP || p[1].TargetPort != wantHTTPS {
					t.Errorf("Service %s ports %v, want targetPorts %v and %v", svc.Name, p, wantHTTP.String(), wantHTTPS.String())
				}
			}
		})
	}
}
