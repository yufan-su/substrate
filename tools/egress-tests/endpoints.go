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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
)

// buildPolicy returns the egress policy every actor gets: cleartext HTTP on
// port 80 to exactly the hosts of endpoints 0 through n-1, the ones the
// actor's loop calls.
func buildPolicy(atespace string, n int) *ateapipb.EgressPolicy {
	hosts := make([]string, n)
	for i := range n {
		hosts[i] = egressapi.EndpointHost(i)
	}
	return &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: "default"},
		Rules:    []*ateapipb.EgressRule{{Http: &ateapipb.HTTPRule{Hostnames: hosts}}},
	}
}

// samePolicyHosts reports whether existing allows exactly the hostnames of
// want, the only thing that differs between runs.
func samePolicyHosts(existing, want *ateapipb.EgressPolicy) bool {
	if len(existing.GetRules()) != 1 || existing.GetRules()[0].GetHttp() == nil {
		return false
	}
	got := slices.Sorted(slices.Values(existing.GetRules()[0].GetHttp().GetHostnames()))
	exp := slices.Sorted(slices.Values(want.GetRules()[0].GetHttp().GetHostnames()))
	return slices.Equal(got, exp)
}

// preflight fails unless the Services of endpoints 0 through n-1 exist, so a
// run never measures requests to names that do not resolve.
func preflight(ctx context.Context, k8s kubernetes.Interface, n int) error {
	list, err := k8s.CoreV1().Services(egressapi.TargetNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing endpoint Services in %s: %w", egressapi.TargetNamespace, err)
	}
	have := make(map[string]bool, len(list.Items))
	for _, svc := range list.Items {
		have[svc.Name] = true
	}
	var missing []string
	for i := range n {
		if !have[egressapi.ServiceName(i)] {
			missing = append(missing, egressapi.ServiceName(i))
		}
	}
	if len(missing) == 0 {
		return nil
	}
	shown := missing[:min(len(missing), 5)]
	more := ""
	if len(missing) > len(shown) {
		more = fmt.Sprintf(" and %d more", len(missing)-len(shown))
	}
	return fmt.Errorf("%d of %d endpoint Services are missing in %s (%s%s); deploy them with tools/egress-tests/deploy.sh --deploy --endpoints %d",
		len(missing), n, egressapi.TargetNamespace, strings.Join(shown, ", "), more, n)
}
