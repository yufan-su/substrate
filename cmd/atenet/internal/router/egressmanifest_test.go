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

package router

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

// egressManifests is the envoy egress gateway ate-setup installs, which
// terminates and re-originates the tunneled TLS.
var egressManifests = []string{
	"../../../../manifests/ate-install/atenet-egress.yaml",
}

// TestEgressManifestsDisableTheConnectTimeout is the static-config half of
// TestBuildConnectRoutes_DisablesTimeout: Envoy applies a route's timeout to a
// CONNECT tunnel's whole lifetime rather than to its headers, so a route left
// at Envoy's 15s default caps how long any actor's outbound connection may
// exist -- streaming responses, long downloads and SSH sessions all die
// mid-stream at 15s. These manifests are what the gateway actually runs, and
// nothing else in the tree notices when one of them loses the line.
func TestEgressManifestsDisableTheConnectTimeout(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			routes := connectRoutes(t, envoyConfig(t, path))
			if len(routes) == 0 {
				t.Fatal("found no connect_matcher route; the manifest changed shape and this test is checking nothing")
			}
			for _, r := range routes {
				if r.Route.Timeout == nil {
					t.Errorf("connect_matcher route to cluster %q sets no timeout, so it falls back to Envoy's 15s default and cuts every tunnel after 15s", r.Route.Cluster)
					continue
				}
				d, err := time.ParseDuration(*r.Route.Timeout)
				if err != nil {
					t.Errorf("connect_matcher route to cluster %q has timeout %q, which is not a duration: %v", r.Route.Cluster, *r.Route.Timeout, err)
					continue
				}
				if d != 0 {
					t.Errorf("connect_matcher route to cluster %q has timeout %s; it must be 0 to disable it", r.Route.Cluster, d)
				}
			}
		})
	}
}

// envoyDefaultCircuitBreakerLimit is what Envoy applies to a threshold the
// cluster leaves unset.
const envoyDefaultCircuitBreakerLimit = 1024

// TestEgressManifestsSizeTheTunnelCircuitBreaker pins mitm_internal's
// circuit breakers. Every tunnel holds one of its connections and one of its
// requests while open, so these thresholds are how many tunnels a gateway
// replica carries; left unset they fall back to Envoy's 1024 and the gateway
// refuses every tunnel past that with a 503. Every new tunnel also passes
// through the pending queue while its internal connection is set up, so a
// pending limit below the connection limit refuses bursts of new tunnels that
// would otherwise fit.
func TestEgressManifestsSizeTheTunnelCircuitBreaker(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			var cluster *envoyCluster
			for _, c := range staticClusters(t, envoyConfig(t, path)) {
				if c.Name == "mitm_internal" {
					cluster = &c
					break
				}
			}
			if cluster == nil {
				t.Fatal("found no mitm_internal cluster; the manifest changed shape and this test is checking nothing")
			}
			var th *circuitBreakerThresholds
			for i := range cluster.CircuitBreakers.Thresholds {
				if p := cluster.CircuitBreakers.Thresholds[i].Priority; p == "" || p == "DEFAULT" {
					th = &cluster.CircuitBreakers.Thresholds[i]
				}
			}
			if th == nil {
				t.Fatalf("mitm_internal sets no DEFAULT circuit breaker thresholds, so it carries at most %d tunnels", envoyDefaultCircuitBreakerLimit)
			}
			for name, v := range map[string]*int{
				"max_connections":      th.MaxConnections,
				"max_requests":         th.MaxRequests,
				"max_pending_requests": th.MaxPendingRequests,
			} {
				if v == nil || *v <= envoyDefaultCircuitBreakerLimit {
					t.Errorf("mitm_internal %s = %s, want it set above Envoy's default of %d", name, show(v), envoyDefaultCircuitBreakerLimit)
				}
			}
			if th.MaxPendingRequests != nil && th.MaxConnections != nil && *th.MaxPendingRequests < *th.MaxConnections {
				t.Errorf("mitm_internal max_pending_requests = %d is below max_connections = %d, so a burst of new tunnels is refused before the connection limit is reached",
					*th.MaxPendingRequests, *th.MaxConnections)
			}
		})
	}
}

func show(v *int) string {
	if v == nil {
		return "unset"
	}
	return strconv.Itoa(*v)
}

type circuitBreakerThresholds struct {
	Priority           string `json:"priority"`
	MaxConnections     *int   `json:"max_connections"`
	MaxRequests        *int   `json:"max_requests"`
	MaxPendingRequests *int   `json:"max_pending_requests"`
}

type envoyCluster struct {
	Name            string `json:"name"`
	CircuitBreakers struct {
		Thresholds []circuitBreakerThresholds `json:"thresholds"`
	} `json:"circuit_breakers"`
}

// staticClusters is every static cluster in the bootstrap.
func staticClusters(t *testing.T, raw string) []envoyCluster {
	t.Helper()
	var bootstrap struct {
		StaticResources struct {
			Clusters []envoyCluster `json:"clusters"`
		} `json:"static_resources"`
	}
	if err := yaml.Unmarshal([]byte(raw), &bootstrap); err != nil {
		t.Fatalf("parsing envoy.yaml: %v", err)
	}
	return bootstrap.StaticResources.Clusters
}

// TestEgressManifestsDisableTheConnectStreamIdleTimeout is the other half of
// the route timeout above. A CONNECT tunnel carries nothing while the actor's
// connection inside it is idle, and the connection manager's
// stream_idle_timeout, 5 minutes unless set, resets the whole tunnel when
// that happens: every keep-alive connection an actor leaves unused that long
// dies and is reopened. The MITM connection managers already disable it; the
// CONNECT one is the one that fronts every tunnel.
func TestEgressManifestsDisableTheConnectStreamIdleTimeout(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			var bootstrap envoyBootstrap
			if err := yaml.Unmarshal([]byte(envoyConfig(t, path)), &bootstrap); err != nil {
				t.Fatalf("parsing envoy.yaml: %v", err)
			}
			found := 0
			for _, l := range bootstrap.StaticResources.Listeners {
				for _, fc := range l.FilterChains {
					for _, f := range fc.Filters {
						if f.TypedConfig.StatPrefix != "egress_connect" {
							continue
						}
						found++
						if f.TypedConfig.StreamIdleTimeout == nil {
							t.Error("the egress_connect connection manager sets no stream_idle_timeout, so Envoy's 5 minute default resets every tunnel idle that long")
							continue
						}
						d, err := time.ParseDuration(*f.TypedConfig.StreamIdleTimeout)
						if err != nil {
							t.Errorf("egress_connect stream_idle_timeout %q is not a duration: %v", *f.TypedConfig.StreamIdleTimeout, err)
							continue
						}
						if d != 0 {
							t.Errorf("egress_connect stream_idle_timeout is %s; it must be 0 to disable it", d)
						}
					}
				}
			}
			if found == 0 {
				t.Fatal("found no egress_connect connection manager; the manifest changed shape and this test is checking nothing")
			}
		})
	}
}

// envoyRoute is the sliver of an Envoy bootstrap this test reads. Unnamed
// fields are dropped by the decoder, so the manifests stay free to grow.
type envoyRoute struct {
	Match struct {
		ConnectMatcher *struct{} `json:"connect_matcher"`
	} `json:"match"`
	Route struct {
		Cluster string  `json:"cluster"`
		Timeout *string `json:"timeout"`
	} `json:"route"`
}

type envoyBootstrap struct {
	StaticResources struct {
		Listeners []struct {
			FilterChains []struct {
				Filters []struct {
					TypedConfig struct {
						StatPrefix        string  `json:"stat_prefix"`
						StreamIdleTimeout *string `json:"stream_idle_timeout"`
						RouteConfig       struct {
							VirtualHosts []struct {
								Routes []envoyRoute `json:"routes"`
							} `json:"virtual_hosts"`
						} `json:"route_config"`
					} `json:"typed_config"`
				} `json:"filters"`
			} `json:"filter_chains"`
		} `json:"listeners"`
	} `json:"static_resources"`
}

// connectRoutes is every route in the bootstrap matched by connect_matcher.
func connectRoutes(t *testing.T, raw string) []envoyRoute {
	t.Helper()
	var bootstrap envoyBootstrap
	if err := yaml.Unmarshal([]byte(raw), &bootstrap); err != nil {
		t.Fatalf("parsing envoy.yaml: %v", err)
	}
	var out []envoyRoute
	for _, l := range bootstrap.StaticResources.Listeners {
		for _, fc := range l.FilterChains {
			for _, f := range fc.Filters {
				for _, vh := range f.TypedConfig.RouteConfig.VirtualHosts {
					for _, r := range vh.Routes {
						if r.Match.ConnectMatcher != nil {
							out = append(out, r)
						}
					}
				}
			}
		}
	}
	return out
}

// envoyConfig is the envoy.yaml the atenet-egress ConfigMap in path ships.
func envoyConfig(t *testing.T, path string) string {
	t.Helper()
	return envoyConfigFrom(t, path, "atenet-egress")
}

// envoyConfigFrom is the envoy.yaml the ConfigMap named configMap in path ships.
func envoyConfigFrom(t *testing.T, path, configMap string) string {
	t.Helper()
	manifest, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	for _, doc := range strings.Split(string(manifest), "\n---\n") {
		var obj struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Data map[string]string `json:"data"`
		}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("parsing a document of %s: %v", path, err)
		}
		if obj.Kind != "ConfigMap" || obj.Metadata.Name != configMap {
			continue
		}
		envoyYaml, ok := obj.Data["envoy.yaml"]
		if !ok {
			t.Fatalf("the %s ConfigMap in %s has no envoy.yaml key", configMap, path)
		}
		return envoyYaml
	}
	t.Fatalf("%s has no ConfigMap named %s", path, configMap)
	return ""
}
