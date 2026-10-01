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
	"fmt"
	"maps"
	"slices"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// credentialProvider is a provider --credential-provider installs: the
// endpoint the egress gateway is pointed at, and what deploys the provider.
type credentialProvider struct {
	// name is the ate-secret:// prefix the gateway serves.
	name string
	// address is the gateway's dial target. Its host is the provider Service's
	// DNS name, which the provider's serving certificate carries.
	address string
	// deployment is the provider's Deployment.
	deployment string
	// manifest is the provider's manifest file or kustomization directory,
	// relative to the repository root.
	manifest string
	// module, when set, is the provider's own Go module, relative to the
	// repository root. ko has to build the provider's image from inside it.
	module string
	// policyConfigMap holds, under policyKey, the authorization policy the
	// provider mounts and cannot start without. The provider's manifests leave
	// it out so that a redeploy never overwrites an operator's grants.
	policyConfigMap string
	policyKey       string
	// guide documents the setup ate-setup cannot do, such as granting access.
	guide string
}

// credentialProviders are the providers --credential-provider installs, by
// flag value. A gateway installed without the flag is pointed at the k8s
// provider's endpoint.
//
// The gsm entry is the installer's only dependency on the plugin's layout; see
// internal/plugins/README.md.
var credentialProviders = map[string]credentialProvider{
	config.CredentialProviderK8s: {
		name:            "ate-secret://k8s.io",
		address:         "k8s-credential-provider.ate-system.svc:50051",
		deployment:      "k8s-credential-provider",
		manifest:        "manifests/egress-credential-injection/k8s-credential-provider.yaml",
		policyConfigMap: "k8s-credential-provider-namespace-policy",
		policyKey:       "namespace-policy.yaml",
		guide:           "docs/egress-credential-injection.md",
	},
	config.CredentialProviderGSM: {
		name:            "ate-secret://secretmanager.googleapis.com",
		address:         "gsm-credential-provider.ate-system.svc:50051",
		deployment:      "gsm-credential-provider",
		manifest:        "internal/plugins/gcp-secret-manager/config",
		module:          "internal/plugins/gcp-secret-manager",
		policyConfigMap: "gsm-credential-provider-project-policy",
		policyKey:       "project-policy.yaml",
		guide:           "internal/plugins/gcp-secret-manager/README.md",
	},
}

// defaultDenyPolicy is the policy installed for a provider that has none. Both
// providers read the same shape, and an empty list grants nothing: the provider
// starts, and refuses every credential until an operator grants access.
const defaultDenyPolicy = `# Installed by ate-setup --credential-provider. Default-deny: no atespace may
# resolve any credential until it is granted here. The provider reads this only
# at startup, so restart it after editing.
policies: []
`

// credentialProviderEndpoint returns the provider name and address the egress
// gateway's injector is pointed at: those of the provider --credential-provider
// installs, else the k8s provider's, unless --credential-provider-name or
// --credential-provider-address name a provider ate-setup does not install.
func (e *Env) credentialProviderEndpoint() (name, address string) {
	p, ok := credentialProviders[e.Cfg.CredentialProvider]
	if !ok {
		p = credentialProviders[config.CredentialProviderK8s]
	}
	name, address = p.name, p.address
	if e.Cfg.CredentialProviderName != "" {
		name = e.Cfg.CredentialProviderName
	}
	if e.Cfg.CredentialProviderAddress != "" {
		address = e.Cfg.CredentialProviderAddress
	}
	return name, address
}

// deployCredentialProvider installs the provider --credential-provider names
// and waits for it to roll out. Without the flag the gateway is pointed at a
// provider ate-setup does not install, and this does nothing.
//
// The provider's authorization policy is created default-deny when it is
// missing, and otherwise left alone.
func (e *Env) deployCredentialProvider(ctx context.Context) error {
	if e.Cfg.CredentialProvider == "" {
		return nil
	}
	p, ok := credentialProviders[e.Cfg.CredentialProvider]
	if !ok {
		return fmt.Errorf("unknown credential provider %q", e.Cfg.CredentialProvider)
	}
	log.Stepf("deploy_credential_provider %s", e.Cfg.CredentialProvider)

	// The provider's manifests name ate-system literally.
	if err := e.RequireCanonicalNamespace("--credential-provider"); err != nil {
		return err
	}
	created, err := e.Kube.CreateConfigMapIfAbsent(ctx, e.Namespace(), p.policyConfigMap,
		map[string]string{p.policyKey: defaultDenyPolicy})
	if err != nil {
		return err
	}
	if created {
		log.Infof("Created %s default-deny: no atespace can resolve a credential until it is granted access (see %s)",
			p.policyConfigMap, p.guide)
	}

	manifest, err := e.renderCredentialProvider(ctx, p)
	if err != nil {
		return err
	}
	if err := e.Kube.ApplyBytes(ctx, manifest); err != nil {
		return err
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, e.Namespace(), p.deployment, e.Cfg.RolloutTimeout); err != nil {
		return fmt.Errorf("%w (see %s for what %s needs to start)", err, p.guide, p.deployment)
	}
	return nil
}

// renderCredentialProvider renders a provider's manifests with their images
// resolved. A provider in a Go module of its own is built from inside it.
func (e *Env) renderCredentialProvider(ctx context.Context, p credentialProvider) ([]byte, error) {
	if p.module == "" {
		return e.renderResolve(ctx, e.Cfg.Path(p.manifest))
	}
	manifest, err := e.render(e.Cfg.Path(p.manifest))
	if err != nil {
		return nil, err
	}
	runner, err := e.koRunner()
	if err != nil {
		return nil, err
	}
	return runner.InModule(e.Cfg.Path(p.module)).ResolveBytes(ctx, manifest)
}

// deleteCredentialProviders removes every provider --credential-provider can
// install, not just the one this invocation names, for the same reason teardown
// covers both egress variants. The policy ConfigMaps stay: they hold an
// operator's grants, and the namespace delete takes them with it.
func (e *Env) deleteCredentialProviders(ctx context.Context) error {
	for _, key := range slices.Sorted(maps.Keys(credentialProviders)) {
		manifest, err := e.render(e.Cfg.Path(credentialProviders[key].manifest))
		if err != nil {
			return err
		}
		if err := e.Kube.DeleteBytes(ctx, manifest); err != nil {
			return err
		}
	}
	return nil
}
