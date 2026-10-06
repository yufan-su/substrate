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
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
)

// buildPolicy returns the egress policy every actor gets: HTTP on port 80 and
// HTTPS on port 443 to exactly the hosts of endpoints 0 through n-1, the ones
// the actor's loop calls. Both schemes are always allowed, so a run can switch
// between them without updating every actor's policy.
func buildPolicy(atespace string, n int) *ateapipb.EgressPolicy {
	hosts := make([]string, n)
	for i := range n {
		hosts[i] = egressapi.EndpointHost(i)
	}
	return &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: "default"},
		Rules: []*ateapipb.EgressRule{
			{Http: &ateapipb.HTTPRule{Hostnames: hosts}},
			{Https: &ateapipb.HTTPSRule{Hostnames: slices.Clone(hosts)}},
		},
	}
}

// samePolicyRules reports whether existing allows what want does: the same
// kinds of rule, each with the same hostnames. Ports are not compared; the
// server fills in each kind's default, and the driver sets none.
func samePolicyRules(existing, want *ateapipb.EgressPolicy) bool {
	return slices.Equal(ruleKeys(existing), ruleKeys(want))
}

// ruleKeys describes each rule by its kind and sorted hostnames, sorted, so
// policies compare regardless of order.
func ruleKeys(p *ateapipb.EgressPolicy) []string {
	var keys []string
	for _, r := range p.GetRules() {
		kind, hosts := "other", []string(nil)
		switch {
		case r.GetHttp() != nil:
			kind, hosts = "http", r.GetHttp().GetHostnames()
		case r.GetHttps() != nil:
			kind, hosts = "https", r.GetHttps().GetHostnames()
		}
		keys = append(keys, kind+" "+strings.Join(slices.Sorted(slices.Values(hosts)), ","))
	}
	slices.Sort(keys)
	return keys
}

// httpsPort is the endpoint Services' HTTPS port, the default the policy's
// https rule covers.
const httpsPort = 443

// preflight fails unless the Services of endpoints 0 through n-1 exist, so a
// run never measures requests to names that do not resolve. Over HTTPS it also
// needs their HTTPS port and a gateway that trusts the target's certificate.
func preflight(ctx context.Context, k8s kubernetes.Interface, n int, scheme string) error {
	list, err := k8s.CoreV1().Services(egressapi.TargetNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing endpoint Services in %s: %w", egressapi.TargetNamespace, err)
	}
	services := make(map[string]corev1.Service, len(list.Items))
	for _, svc := range list.Items {
		services[svc.Name] = svc
	}
	var missing, noHTTPS []string
	for i := range n {
		svc, ok := services[egressapi.ServiceName(i)]
		switch {
		case !ok:
			missing = append(missing, egressapi.ServiceName(i))
		case scheme == egressapi.SchemeHTTPS && !slices.ContainsFunc(svc.Spec.Ports, func(p corev1.ServicePort) bool { return p.Port == httpsPort }):
			noHTTPS = append(noHTTPS, egressapi.ServiceName(i))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%d of %d endpoint Services are missing in %s (%s); deploy them with tools/egress-tests/deploy.sh --deploy --endpoints %d",
			len(missing), n, egressapi.TargetNamespace, firstFew(missing), n)
	}
	if len(noHTTPS) > 0 {
		return fmt.Errorf("%d of %d endpoint Services have no port %d (%s); redeploy them with tools/egress-tests/deploy.sh --deploy --https --endpoints %d",
			len(noHTTPS), n, httpsPort, firstFew(noHTTPS), n)
	}
	if scheme == egressapi.SchemeHTTPS {
		return checkGatewayTrust(ctx, k8s)
	}
	return nil
}

// firstFew lists up to five names, and how many more there are.
func firstFew(names []string) string {
	shown := names[:min(len(names), 5)]
	more := ""
	if len(names) > len(shown) {
		more = fmt.Sprintf(" and %d more", len(names)-len(shown))
	}
	return strings.Join(shown, ", ") + more
}
