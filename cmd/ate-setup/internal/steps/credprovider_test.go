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

package steps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// The gateway is pointed at the provider ate-setup installs, at the k8s
// provider by default, and at whatever the explicit overrides name.
func TestCredentialProviderEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cfg         config.Config
		wantName    string
		wantAddress string
	}{
		{
			name:        "default",
			wantName:    "ate-secret://k8s.io",
			wantAddress: "k8s-credential-provider.ate-system.svc:50051",
		},
		{
			name:        "k8s",
			cfg:         config.Config{CredentialProvider: config.CredentialProviderK8s},
			wantName:    "ate-secret://k8s.io",
			wantAddress: "k8s-credential-provider.ate-system.svc:50051",
		},
		{
			name:        "gsm",
			cfg:         config.Config{CredentialProvider: config.CredentialProviderGSM},
			wantName:    "ate-secret://secretmanager.googleapis.com",
			wantAddress: "gsm-credential-provider.ate-system.svc:50051",
		},
		{
			name:        "overrides",
			cfg:         config.Config{CredentialProviderName: "ate-secret://vault.example.com", CredentialProviderAddress: "vault.ate-system.svc:50051"},
			wantName:    "ate-secret://vault.example.com",
			wantAddress: "vault.ate-system.svc:50051",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &Env{Cfg: &tc.cfg}
			name, address := env.credentialProviderEndpoint()
			if name != tc.wantName || address != tc.wantAddress {
				t.Errorf("credentialProviderEndpoint() = %q, %q; want %q, %q", name, address, tc.wantName, tc.wantAddress)
			}
		})
	}
}

// Each provider's entry restates facts its own manifests own: the Deployment,
// the policy ConfigMap it mounts and the file it reads from it, and the
// Service the gateway dials. A rename on either side would leave the provider
// waiting on a ConfigMap nobody creates, or the gateway dialing nothing, so
// the two are checked against each other.
func TestCredentialProvidersMatchTheirManifests(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	env := &Env{Cfg: &config.Config{Root: root}}

	for key, p := range credentialProviders {
		t.Run(key, func(t *testing.T) {
			manifest, err := env.render(env.Cfg.Path(p.manifest))
			if err != nil {
				t.Fatalf("rendering %s: %v", p.manifest, err)
			}
			objs, err := kube.DecodeManifestBytes(manifest)
			if err != nil {
				t.Fatalf("decoding %s: %v", p.manifest, err)
			}

			dep := findInAteSystem(objs, "Deployment", p.deployment)
			if dep == nil {
				t.Fatalf("%s has no deployment/%s -n %s", p.manifest, p.deployment, NamespaceAteSystem)
			}
			if got := configMapVolumes(t, dep); !slices.Contains(got, p.policyConfigMap) {
				t.Errorf("deployment/%s mounts ConfigMaps %v, want %s", p.deployment, got, p.policyConfigMap)
			}
			if args := containerArgs(t, dep); !slices.ContainsFunc(args, func(a string) bool { return strings.HasSuffix(a, "/"+p.policyKey) }) {
				t.Errorf("deployment/%s args %v read no file named %s", p.deployment, args, p.policyKey)
			}
			if image := containerImage(t, dep); p.module != "" && !strings.HasPrefix(image, images.KoReference(p.module)+"/") {
				t.Errorf("deployment/%s image %q is not built from its module %s", p.deployment, image, p.module)
			}

			host, port, _ := strings.Cut(p.address, ":")
			svcName, rest, _ := strings.Cut(host, ".")
			if rest != NamespaceAteSystem+".svc" {
				t.Fatalf("address %s is not a Service in %s", p.address, NamespaceAteSystem)
			}
			svc := findInAteSystem(objs, "Service", svcName)
			if svc == nil {
				t.Fatalf("%s has no service/%s -n %s, which address %s names", p.manifest, svcName, NamespaceAteSystem, p.address)
			}
			if ports := servicePorts(t, svc); !slices.Contains(ports, port) {
				t.Errorf("service/%s serves ports %v, want %s", svcName, ports, port)
			}

			// Teardown keeps the operator's grants, so the policy must not be
			// part of what the installer applies and deletes.
			if cm := findObject(objs, "ConfigMap", p.policyConfigMap); cm != nil {
				t.Errorf("%s carries %s itself; the installer would overwrite an operator's policy on every deploy", p.manifest, p.policyConfigMap)
			}
		})
	}
}

// The default the installer creates must parse the way both providers read
// their policies, strictly, and grant nothing.
func TestDefaultDenyPolicyGrantsNothing(t *testing.T) {
	var policy struct {
		Policies []json.RawMessage `json:"policies"`
	}
	if err := yaml.UnmarshalStrict([]byte(defaultDenyPolicy), &policy); err != nil {
		t.Fatalf("defaultDenyPolicy does not parse strictly: %v", err)
	}
	if policy.Policies == nil || len(policy.Policies) != 0 {
		t.Errorf("defaultDenyPolicy policies = %v, want an empty list", policy.Policies)
	}
}

// Without --credential-provider the gateway is pointed at a provider ate-setup
// does not manage, so nothing touches the cluster.
func TestDeployCredentialProviderWithoutTheFlag(t *testing.T) {
	env := &Env{Cfg: &config.Config{}}
	if err := env.deployCredentialProvider(context.Background()); err != nil {
		t.Fatalf("deployCredentialProvider() = %v, want nil", err)
	}
}

// errStopResolve ends a deploy at image resolution, the first step that needs
// more than the fake clientset.
var errStopResolve = errors.New("stop at image resolution")

type stoppingResolver struct{}

func (stoppingResolver) ResolvePath(context.Context, string) ([]byte, error) {
	return nil, errStopResolve
}

func (stoppingResolver) ResolveBytes(context.Context, []byte) ([]byte, error) {
	return nil, errStopResolve
}

// The provider pod mounts its policy ConfigMap and does not start without it,
// so the policy is in place before the provider is rendered: default-deny when
// there was none, and an operator's edited policy left exactly as it was.
func TestDeployCredentialProviderCreatesPolicyFirst(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	p := credentialProviders[config.CredentialProviderK8s]
	edited := map[string]string{p.policyKey: "policies:\n- atespace: team-a\n  allowedNamespaces: [ns1]\n"}

	for _, tc := range []struct {
		name     string
		existing map[string]string
		want     map[string]string
	}{
		{name: "absent", want: map[string]string{p.policyKey: defaultDenyPolicy}},
		{name: "edited", existing: edited, want: edited},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &Env{
				Cfg:      &config.Config{Root: root, CredentialProvider: config.CredentialProviderK8s},
				Kube:     fakeKube(t, policyConfigMap(p, tc.existing)),
				resolver: stoppingResolver{},
			}

			if err := env.deployCredentialProvider(context.Background()); !errors.Is(err, errStopResolve) {
				t.Fatalf("deployCredentialProvider() = %v, want it to reach image resolution", err)
			}
			cm, err := env.Kube.GetConfigMap(context.Background(), NamespaceAteSystem, p.policyConfigMap)
			if err != nil || cm == nil {
				t.Fatalf("GetConfigMap(%s) = %v, %v; want the policy", p.policyConfigMap, cm, err)
			}
			if !maps.Equal(cm.Data, tc.want) {
				t.Errorf("policy data = %v, want %v", cm.Data, tc.want)
			}
		})
	}
}

// policyConfigMap seeds a provider's policy ConfigMap. Nil data means it does
// not exist.
func policyConfigMap(p credentialProvider, data map[string]string) runtime.Object {
	if data == nil {
		return nil
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: p.policyConfigMap},
		Data:       data,
	}
}

// The provider manifests name ate-system literally, so a relocated install is
// refused before anything is created.
func TestDeployCredentialProviderRequiresTheCanonicalNamespace(t *testing.T) {
	env := &Env{
		Cfg:  &config.Config{CredentialProvider: config.CredentialProviderK8s, Namespace: "elsewhere"},
		Kube: fakeKube(t),
	}
	if err := env.deployCredentialProvider(context.Background()); err == nil {
		t.Fatal("deployCredentialProvider() succeeded with ATE_NAMESPACE=elsewhere, want an error")
	}
	p := credentialProviders[config.CredentialProviderK8s]
	if cm, _ := env.Kube.GetConfigMap(context.Background(), "elsewhere", p.policyConfigMap); cm != nil {
		t.Errorf("created %s in a namespace the provider does not run in", p.policyConfigMap)
	}
}

// findInAteSystem is findObject restricted to the system namespace, the one
// the gateway dials the provider in.
func findInAteSystem(objs []*unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	if obj := findObject(objs, kind, name); obj != nil && obj.GetNamespace() == NamespaceAteSystem {
		return obj
	}
	return nil
}

func podSpec(t *testing.T, dep *unstructured.Unstructured) map[string]any {
	t.Helper()
	spec, found, err := unstructured.NestedMap(dep.Object, "spec", "template", "spec")
	if err != nil || !found {
		t.Fatalf("%s has no pod spec: %v", kube.Describe(dep), err)
	}
	return spec
}

func firstContainer(t *testing.T, dep *unstructured.Unstructured) map[string]any {
	t.Helper()
	containers, _, _ := unstructured.NestedSlice(podSpec(t, dep), "containers")
	if len(containers) == 0 {
		t.Fatalf("%s has no containers", kube.Describe(dep))
	}
	c, ok := containers[0].(map[string]any)
	if !ok {
		t.Fatalf("%s: container 0 is not an object", kube.Describe(dep))
	}
	return c
}

func configMapVolumes(t *testing.T, dep *unstructured.Unstructured) []string {
	t.Helper()
	volumes, _, _ := unstructured.NestedSlice(podSpec(t, dep), "volumes")
	var names []string
	for _, v := range volumes {
		if vol, ok := v.(map[string]any); ok {
			if name, found, _ := unstructured.NestedString(vol, "configMap", "name"); found {
				names = append(names, name)
			}
		}
	}
	return names
}

func containerArgs(t *testing.T, dep *unstructured.Unstructured) []string {
	t.Helper()
	args, _, _ := unstructured.NestedStringSlice(firstContainer(t, dep), "args")
	return args
}

func containerImage(t *testing.T, dep *unstructured.Unstructured) string {
	t.Helper()
	image, _, _ := unstructured.NestedString(firstContainer(t, dep), "image")
	return image
}

func servicePorts(t *testing.T, svc *unstructured.Unstructured) []string {
	t.Helper()
	ports, _, _ := unstructured.NestedSlice(svc.Object, "spec", "ports")
	var out []string
	for _, p := range ports {
		// The decoder may hand the number back as int64 or float64.
		if port, ok := p.(map[string]any); ok && port["port"] != nil {
			out = append(out, fmt.Sprint(port["port"]))
		}
	}
	return out
}
