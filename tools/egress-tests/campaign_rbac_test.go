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
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
)

const campaignSA = "egress-campaign"

// access is one API request: namespace "" is cluster-wide.
type access struct {
	namespace, group, resource, verb, name string
}

// campaignRBAC is what the rendered campaign manifest grants the
// ServiceAccount.
type campaignRBAC struct {
	cluster    []rbacv1.PolicyRule
	namespaced map[string][]rbacv1.PolicyRule
}

func (r campaignRBAC) allows(a access) bool {
	rules := r.cluster
	if a.namespace != "" {
		rules = append(slices.Clone(rules), r.namespaced[a.namespace]...)
	}
	return slices.ContainsFunc(rules, func(rule rbacv1.PolicyRule) bool {
		return slices.Contains(rule.APIGroups, a.group) && slices.Contains(rule.Resources, a.resource) &&
			slices.Contains(rule.Verbs, a.verb) && (len(rule.ResourceNames) == 0 || slices.Contains(rule.ResourceNames, a.name))
	})
}

// renderCampaign runs deploy.sh --campaign --dry-run and returns its output.
func renderCampaign(t *testing.T, args ...string) string {
	t.Helper()
	for _, tool := range []string{"bash", "git"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found: %v", tool, err)
		}
	}
	repo := filepath.Join(t.TempDir(), "repo")
	writeFile(t, filepath.Join(repo, "tools/egress-tests/deploy.sh"), readFile(t, "deploy.sh"), 0o755)
	manifests, err := filepath.Glob("manifests/*")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range manifests {
		writeFile(t, filepath.Join(repo, "tools/egress-tests", m), readFile(t, m), 0o644)
	}
	if out, err := exec.Command("git", "init", "-q", repo).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	cmd := exec.Command("bash", append([]string{"tools/egress-tests/deploy.sh", "--campaign", "--dry-run"}, args...)...)
	cmd.Dir = repo
	// No kubectl on PATH: a dry run must not reach a cluster.
	cmd.Env = []string{"PATH=" + filepath.Dir(mustLookPath(t, "git")) + ":/usr/bin:/bin", "HOME=" + t.TempDir()}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("deploy.sh --campaign --dry-run %v: %v\n%s", args, err, stderr.String())
	}
	return string(out)
}

// parseCampaign decodes the rendered documents strictly and collects the
// rules bound to the ServiceAccount, and the Pods.
func parseCampaign(t *testing.T, rendered string) (campaignRBAC, map[string]*corev1.Pod) {
	t.Helper()
	clusterRoles := map[string][]rbacv1.PolicyRule{}
	roles := map[string][]rbacv1.PolicyRule{} // by namespace/name
	var crbs []rbacv1.ClusterRoleBinding
	var rbs []rbacv1.RoleBinding
	pods := map[string]*corev1.Pod{}
	for _, doc := range strings.Split(rendered, "\n---\n") {
		var meta struct{ Kind string }
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatalf("decoding %q: %v", doc, err)
		}
		var obj any
		switch meta.Kind {
		case "":
			continue
		case "ClusterRole":
			obj = &rbacv1.ClusterRole{}
		case "Role":
			obj = &rbacv1.Role{}
		case "ClusterRoleBinding":
			obj = &rbacv1.ClusterRoleBinding{}
		case "RoleBinding":
			obj = &rbacv1.RoleBinding{}
		case "Pod":
			obj = &corev1.Pod{}
		case "ServiceAccount":
			obj = &corev1.ServiceAccount{}
		case "PersistentVolumeClaim":
			obj = &corev1.PersistentVolumeClaim{}
		default:
			t.Fatalf("unexpected kind %q in the campaign manifest", meta.Kind)
		}
		if err := yaml.UnmarshalStrict([]byte(doc), obj); err != nil {
			t.Fatalf("decoding a %s strictly: %v\n%s", meta.Kind, err, doc)
		}
		switch o := obj.(type) {
		case *rbacv1.ClusterRole:
			clusterRoles[o.Name] = o.Rules
		case *rbacv1.Role:
			roles[o.Namespace+"/"+o.Name] = o.Rules
		case *rbacv1.ClusterRoleBinding:
			crbs = append(crbs, *o)
		case *rbacv1.RoleBinding:
			rbs = append(rbs, *o)
		case *corev1.Pod:
			pods[o.Name] = o
		}
	}
	bindsSA := func(subjects []rbacv1.Subject) bool {
		return slices.ContainsFunc(subjects, func(s rbacv1.Subject) bool {
			return s.Kind == "ServiceAccount" && s.Name == campaignSA && s.Namespace == poolNamespace
		})
	}
	rbac := campaignRBAC{namespaced: map[string][]rbacv1.PolicyRule{}}
	for _, b := range crbs {
		if bindsSA(b.Subjects) && b.RoleRef.Kind == "ClusterRole" {
			rbac.cluster = append(rbac.cluster, clusterRoles[b.RoleRef.Name]...)
		}
	}
	for _, b := range rbs {
		if bindsSA(b.Subjects) && b.RoleRef.Kind == "Role" {
			rbac.namespaced[b.Namespace] = append(rbac.namespaced[b.Namespace], roles[b.Namespace+"/"+b.RoleRef.Name]...)
		}
	}
	return rbac, pods
}

const poolNamespace = "egress-tests"

// driverAccess lists every request the driver and campaign.py make, built
// from the same tables the samplers use.
func driverAccess() []access {
	sys := installdefaults.SystemNamespace
	acc := []access{
		{group: "certificates.k8s.io", resource: "clustertrustbundles", verb: "list"}, // ateclient's server roots
		{group: "", resource: "nodes/proxy", verb: "get"},                             // cAdvisor
		{namespace: egressapi.TargetNamespace, resource: "services", verb: "list"},
		{namespace: egressapi.TargetNamespace, resource: "secrets", verb: "get", name: targetTLSSecret},
		{namespace: sys, resource: "configmaps", verb: "get", name: gatewayCAConfigMap},
		{namespace: sys, group: "apps", resource: "deployments", verb: "get", name: gatewayDeployment},
		{namespace: cgreaderNamespace, resource: "pods", verb: "list"},
		{namespace: cgreaderNamespace, resource: "pods", verb: "get"},
		{namespace: cgreaderNamespace, resource: "pods/proxy", verb: "get"},
		{namespace: envoyTarget.namespace, resource: "pods", verb: "list"},
		{namespace: envoyTarget.namespace, resource: "pods/proxy", verb: "get"},
		// campaign.py: the gateway pod, and its /stats.
		{namespace: sys, resource: "pods", verb: "list"},
		{namespace: sys, resource: "pods/proxy", verb: "get"},
	}
	for _, t := range liveTargets {
		acc = append(acc, access{namespace: t.namespace, resource: "pods", verb: "list"},
			access{namespace: t.namespace, resource: "pods/proxy", verb: "get"})
	}
	for _, t := range cadvisorTargets {
		acc = append(acc, access{namespace: t.namespace, resource: "pods", verb: "list"},
			access{namespace: t.namespace, group: "metrics.k8s.io", resource: "pods", verb: "list"})
	}
	return acc
}

// knownCalls are the Kubernetes calls driverAccess covers, as the source
// scan below names them.
var knownCalls = []string{
	"Pods.List", "Pods.Get", "Services.List", "Secrets.Get", "ConfigMaps.Get", "Deployments.Get",
	"GetRaw pods/proxy", "GetRaw nodes/proxy", "GetRaw metrics.k8s.io",
	"kubectl get pod", "kubectl get --raw pods/proxy",
}

var (
	typedCall = regexp.MustCompile(`\.(?:CoreV1|AppsV1|[A-Z]\w*V\d\w*)\(\)\.(\w+)\([^)]*\)\.(\w+)\(`)
	rawCall   = regexp.MustCompile(`\.GetRaw\(ctx, ([^,]+)`)
	kubectlPy = regexp.MustCompile(`\[\*kube, ([^\]]*)`)
)

// scanCalls names every Kubernetes call in the driver's sources and in
// campaign.py.
func scanCalls(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src := readFile(t, f)
		for _, m := range typedCall.FindAllStringSubmatch(src, -1) {
			calls = append(calls, m[1]+"."+m[2])
		}
		for _, m := range rawCall.FindAllStringSubmatch(src, -1) {
			switch path := m[1]; {
			case strings.HasPrefix(path, "podProxyPath("):
				calls = append(calls, "GetRaw pods/proxy")
			case strings.HasPrefix(path, `"/api/v1/nodes/"`):
				calls = append(calls, "GetRaw nodes/proxy")
			case strings.HasPrefix(path, `"/apis/metrics.k8s.io/`):
				calls = append(calls, "GetRaw metrics.k8s.io")
			default:
				calls = append(calls, "GetRaw "+path)
			}
		}
	}
	for _, m := range kubectlPy.FindAllStringSubmatch(readFile(t, "campaign/campaign.py"), -1) {
		switch args := m[1]; {
		case strings.HasPrefix(args, `"-n", "ate-system", "get", "pod",`):
			calls = append(calls, "kubectl get pod")
		case strings.HasPrefix(args, `"get", "--raw", path`):
			calls = append(calls, "kubectl get --raw pods/proxy")
		default:
			calls = append(calls, "kubectl "+args)
		}
	}
	return calls
}

// TestCampaignRBACCoversTheDriver fails when the driver or campaign.py
// makes a Kubernetes call the campaign manifest does not grant, or when the
// manifest grants more than reads.
func TestCampaignRBACCoversTheDriver(t *testing.T) {
	t.Parallel()
	calls := scanCalls(t)
	for _, c := range calls {
		if !slices.Contains(knownCalls, c) {
			t.Errorf("Kubernetes call %q is new: grant it in manifests/campaign.yaml.tmpl and add it to driverAccess and knownCalls", c)
		}
	}
	for _, c := range knownCalls {
		if !slices.Contains(calls, c) {
			t.Errorf("knownCalls lists %q, but the scan found no such call; drop it, and its grant if nothing else needs it", c)
		}
	}

	rbac, _ := parseCampaign(t, renderCampaign(t, "--context", "ctx", "--image", "img", "--no-start"))
	for _, a := range driverAccess() {
		if !rbac.allows(a) {
			t.Errorf("the campaign ServiceAccount cannot %+v", a)
		}
	}
	for _, a := range []access{
		{namespace: installdefaults.SystemNamespace, group: "apps", resource: "deployments", verb: "patch", name: gatewayDeployment},
		{namespace: poolNamespace, resource: "pods", verb: "create"},
		// Nothing reads Node objects; cAdvisor goes through nodes/proxy.
		{resource: "nodes", verb: "get"},
		{resource: "nodes", verb: "list"},
		{namespace: egressapi.TargetNamespace, resource: "secrets", verb: "get", name: "other"},
		{namespace: installdefaults.SystemNamespace, resource: "serviceaccounts/token", verb: "create", name: "ate-client"},
	} {
		if rbac.allows(a) {
			t.Errorf("the campaign ServiceAccount can %+v, want denied", a)
		}
	}
	all := slices.Clone(rbac.cluster)
	for _, rules := range rbac.namespaced {
		all = append(all, rules...)
	}
	for _, rule := range all {
		for _, v := range rule.Verbs {
			if v != "get" && v != "list" {
				t.Errorf("rule %+v grants %q, want get and list only", rule, v)
			}
		}
		if slices.Contains(rule.Resources, "*") || slices.Contains(rule.APIGroups, "*") {
			t.Errorf("rule %+v uses a wildcard", rule)
		}
	}
}

func TestCampaignPods(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		args       []string
		wantPod    string
		wantPool   string
		wantPolls  string
		wantScript []string
	}{
		{
			name:      "campaign",
			args:      []string{"--context", "gke_p_z_c", "--image", "gcr.io/p/egress-campaign:abc"},
			wantPod:   "egress-campaign",
			wantPool:  "campaign",
			wantPolls: "5s",
			wantScript: []string{
				"python3 /work/campaign/campaign.py --in-cluster --context gke_p_z_c \\\n    --driver /work/egress-tests --out /out --ref-dir /work/ref \\\n" +
					`    --base-check-ref "/work/ref/$(cat /work/ref/base-check-ref)"` + "\n",
				"} 2>&1 | tee -a /out/campaign.log\n",
				"exec sleep infinity",
			},
		},
		{
			name: "campaign with the owner's kubeconfig",
			args: []string{"--kubeconfig", "/home/o/.kube/et2.kubeconfig", "--context", "gke_p_z_c",
				"--image", "gcr.io/p/egress-campaign:abc"},
			wantPod:   "egress-campaign",
			wantPool:  "campaign",
			wantPolls: "5s",
			wantScript: []string{
				"python3 /work/campaign/campaign.py --in-cluster --kubeconfig /home/o/.kube/et2.kubeconfig --context gke_p_z_c \\\n",
			},
		},
		{
			name: "campaign with an id and a scripts dir",
			args: []string{"--context", "gke_p_z_c", "--image", "gcr.io/p/egress-campaign:abc",
				"--campaign-id", "20261008-et1", "--scripts-dir", "~/s"},
			wantPod:   "egress-campaign",
			wantPool:  "campaign",
			wantPolls: "5s",
			wantScript: []string{
				"mkdir -p /out/20261008-et1\n",
				"--context gke_p_z_c --scripts-dir '~/s' \\\n",
				"--out /out/20261008-et1 --ref-dir /work/ref",
				"} 2>&1 | tee -a /out/20261008-et1/campaign.log\n",
			},
		},
		{
			name: "rehearsal on another pool",
			args: []string{"--context", "gke_p_z_c", "--image", "gcr.io/p/egress-campaign:abc", "--rehearsal",
				"--campaign-pool", "bench", "--progress-interval", "2s"},
			wantPod:   "egress-campaign-rehearsal",
			wantPool:  "bench",
			wantPolls: "2s",
			wantScript: []string{
				"--api-token-file /var/run/ateapi/token",
				`--output "/out/rehearsal/${r}.json"`,
				`--progress-interval "2s"`,
				"} 2>&1 | tee -a /out/rehearsal/rehearsal.log\n",
				"exec sleep infinity",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, pods := parseCampaign(t, renderCampaign(t, tc.args...))
			if len(pods) != 1 || pods[tc.wantPod] == nil {
				t.Fatalf("got Pods %v, want only %s", slices.Collect(maps.Keys(pods)), tc.wantPod)
			}
			p := pods[tc.wantPod]
			if p.Spec.ServiceAccountName != campaignSA || p.Spec.RestartPolicy != corev1.RestartPolicyNever {
				t.Errorf("got serviceAccountName %q restartPolicy %q, want %q Never", p.Spec.ServiceAccountName, p.Spec.RestartPolicy, campaignSA)
			}
			c := p.Spec.Containers[0]
			if c.Image != "gcr.io/p/egress-campaign:abc" {
				t.Errorf("got image %q", c.Image)
			}
			script := c.Command[len(c.Command)-1]
			for _, want := range tc.wantScript {
				if !strings.Contains(script, want) {
					t.Errorf("script does not contain %q:\n%s", want, script)
				}
			}
			sc := c.SecurityContext
			if sc == nil || sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem ||
				sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation ||
				sc.Capabilities == nil || !slices.Equal(sc.Capabilities.Drop, []corev1.Capability{"ALL"}) {
				t.Errorf("got container securityContext %+v, want read-only root, no escalation, all capabilities dropped", sc)
			}
			if ps := p.Spec.SecurityContext; ps == nil || ps.RunAsNonRoot == nil || !*ps.RunAsNonRoot {
				t.Errorf("got Pod securityContext %+v, want runAsNonRoot", ps)
			}
			var token *corev1.ServiceAccountTokenProjection
			for _, v := range p.Spec.Volumes {
				if v.Projected != nil {
					token = v.Projected.Sources[0].ServiceAccountToken
				}
			}
			if token == nil || token.Audience != "api.ate-system.svc" || token.Path != "token" {
				t.Errorf("got projected token %+v, want audience api.ate-system.svc at token", token)
			}
			if !slices.ContainsFunc(c.VolumeMounts, func(m corev1.VolumeMount) bool { return m.MountPath == "/var/run/ateapi" }) {
				t.Errorf("the token is not mounted at /var/run/ateapi: %+v", c.VolumeMounts)
			}
			req := p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			if len(req) != 1 || req[0].LabelSelector.MatchLabels["app"] != "atenet-egress" {
				t.Errorf("got required anti-affinity %+v, want off the gateway's node", req)
			}
			if got := p.Spec.NodeSelector["cloud.google.com/gke-nodepool"]; got != tc.wantPool || len(p.Spec.NodeSelector) != 1 {
				t.Errorf("got nodeSelector %v, want only node pool %q", p.Spec.NodeSelector, tc.wantPool)
			}
			if i := slices.IndexFunc(c.Env, func(e corev1.EnvVar) bool { return e.Name == "PROGRESS_INTERVAL" }); i < 0 || c.Env[i].Value != tc.wantPolls {
				t.Errorf("got env %+v, want PROGRESS_INTERVAL=%s", c.Env, tc.wantPolls)
			}
			wantToleration := corev1.Toleration{Key: "ate.dev/campaign", Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule}
			if !slices.Equal(p.Spec.Tolerations, []corev1.Toleration{wantToleration}) {
				t.Errorf("got tolerations %+v, want only %+v", p.Spec.Tolerations, wantToleration)
			}
		})
	}
}

func TestCampaignNeedsAContext(t *testing.T) {
	t.Parallel()
	repo := filepath.Join(t.TempDir(), "repo")
	writeFile(t, filepath.Join(repo, "tools/egress-tests/deploy.sh"), readFile(t, "deploy.sh"), 0o755)
	if out, err := exec.Command("git", "init", "-q", repo).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	cmd := exec.Command("bash", "tools/egress-tests/deploy.sh", "--campaign", "--dry-run", "--image", "img")
	cmd.Dir = repo
	cmd.Env = append(os.Environ(), "PATH=/usr/bin:/bin:"+filepath.Dir(mustLookPath(t, "git")))
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "need --context") {
		t.Errorf("deploy.sh --campaign without --context: got %v\n%s, want a refusal", err, out)
	}
}

// buildShims stand in for go build, which writes an empty -o, and docker,
// which logs the reference JSONs in the build context.
const buildGoShim = `#!/bin/bash
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "-o" ]]; then : >"$2"; fi
  shift
done
`

const buildDockerShim = `#!/bin/bash
echo "docker $*" >>"${SHIM_LOG}"
if [[ "$1" == "build" ]]; then
  ls "${@: -1}/ref" | sed 's/^/context ref /' >>"${SHIM_LOG}"
  sed 's/^/context base check /' "${@: -1}/ref/base-check-ref" >>"${SHIM_LOG}"
fi
if [[ "$1" == "inspect" ]]; then
  echo "sha256:0"
fi
`

func TestCampaignImageRefs(t *testing.T) {
	t.Parallel()
	for _, tool := range []string{"bash", "git"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found: %v", tool, err)
		}
	}
	for _, tc := range []struct {
		name string
		refs []string
		// baseCheckRef is BASE_CHECK_REF, a file name in the ref dir.
		baseCheckRef  string
		wantErr       string
		wantRefs      []string
		wantBaseCheck string
	}{
		{
			name:          "all four",
			refs:          []string{"t3-b100-c10.json", "t1a-b10-c100.json", "t1b-b10-c100.json", "t2-b11-c100.json"},
			wantRefs:      []string{"base-check-ref", "t1a-b10-c100.json", "t1b-b10-c100.json", "t2-b11-c100.json", "t3-b100-c10.json"},
			wantBaseCheck: "t3-b100-c10.json",
		},
		{
			name:          "only the base check",
			refs:          []string{"t3-b100-c10.json"},
			wantRefs:      []string{"base-check-ref", "t3-b100-c10.json"},
			wantBaseCheck: "t3-b100-c10.json",
		},
		{
			name:          "another base check ref",
			refs:          []string{"A-c010-b100.json", "t1a-b10-c100.json"},
			baseCheckRef:  "A-c010-b100.json",
			wantRefs:      []string{"A-c010-b100.json", "base-check-ref", "t1a-b10-c100.json"},
			wantBaseCheck: "A-c010-b100.json",
		},
		{
			name:    "no base check",
			refs:    []string{"t1a-b10-c100.json"},
			wantErr: "t3-b100-c10.json does not exist",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmp := t.TempDir()
			repo, shims, refDir := filepath.Join(tmp, "repo"), filepath.Join(tmp, "shims"), filepath.Join(tmp, "ref")
			log := filepath.Join(tmp, "shim.log")
			for _, f := range []string{"campaign/image/build.sh", "campaign/image/Dockerfile", "campaign/campaign.py",
				"plot/fit.py", "plot/runs.py", "plot/predictor.py", "plot/predictor.html.tmpl", "plot/requirements.txt"} {
				writeFile(t, filepath.Join(repo, "tools/egress-tests", f), readFile(t, f), 0o755)
			}
			for _, r := range tc.refs {
				writeFile(t, filepath.Join(refDir, r), "{}", 0o644)
			}
			writeFile(t, filepath.Join(shims, "go"), buildGoShim, 0o755)
			writeFile(t, filepath.Join(shims, "docker"), buildDockerShim, 0o755)
			writeFile(t, log, "", 0o644)
			// build.sh tags the image with the short HEAD.
			for _, args := range [][]string{{"init", "-q", repo},
				{"-C", repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init"}} {
				if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
					t.Fatalf("git %v: %v\n%s", args, err, out)
				}
			}
			cmd := exec.Command("bash", "tools/egress-tests/campaign/image/build.sh", "--ref-dir", refDir)
			cmd.Dir = repo
			cmd.Env = []string{
				"PATH=" + shims + ":" + filepath.Dir(mustLookPath(t, "git")) + ":/usr/bin:/bin",
				"HOME=" + tmp, "TMPDIR=" + tmp, "SHIM_LOG=" + log, "KO_DOCKER_REPO=r",
			}
			if tc.baseCheckRef != "" {
				cmd.Env = append(cmd.Env, "BASE_CHECK_REF="+filepath.Join(refDir, tc.baseCheckRef))
			}
			out, err := cmd.CombinedOutput()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(string(out), tc.wantErr) {
					t.Errorf("build.sh: got %v\n%s, want an error containing %q", err, out, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("build.sh: %v\n%s", err, out)
			}
			var got []string
			var baseCheck string
			for _, line := range strings.Split(readFile(t, log), "\n") {
				if r, ok := strings.CutPrefix(line, "context ref "); ok {
					got = append(got, r)
				}
				if b, ok := strings.CutPrefix(line, "context base check "); ok {
					baseCheck = b
				}
			}
			if !slices.Equal(got, tc.wantRefs) {
				t.Errorf("got reference files %v in the build context, want %v", got, tc.wantRefs)
			}
			if baseCheck != tc.wantBaseCheck {
				t.Errorf("got base-check-ref %q, want %q", baseCheck, tc.wantBaseCheck)
			}
		})
	}
}
