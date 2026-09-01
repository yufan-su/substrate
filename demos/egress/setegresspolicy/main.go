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

// Command setegresspolicy is a small dev helper for the egress credential-
// injection demo: it creates (or replaces) an Actor's `default` egress policy
// with a single hostname rule pointing at a credential set, via the ateapi
// Control API. It reuses kubectl-ate's client (auto port-forward + a Kubernetes
// ServiceAccount bearer token), so it needs no client certificate.
//
// In the credential-set model the injector reads the headers to inject from the
// secret the URI resolves to (a JSON map of host->headers), so the policy's
// header field is an ignored placeholder set here.
//
// This exists only because kubectl-ate has no egress-policy subcommand yet; it
// is the programmatic stand-in for that command in the POC.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func main() {
	var (
		kubeContext = flag.String("context", "", "kube context to use (default: current context)")
		endpoint    = flag.String("endpoint", "", "ateapi gRPC endpoint override; empty auto-port-forwards to the ate-api pods")
		tokenFile   = flag.String("token-file", "", "bearer token file, or '-' for stdin; empty uses a Kubernetes ServiceAccount token")
		atespace    = flag.String("atespace", "", "actor's atespace (required)")
		actor       = flag.String("actor", "", "actor name (required)")
		host        = flag.String("host", "", "hostname pattern to match, e.g. httpbin.org or *.example.com (required)")
		uri         = flag.String("uri", "", "credential-set substrate-secret:// URI (required)")
		replace     = flag.Bool("replace", false, "delete an existing policy first, then create (for iterating)")
	)
	flag.Parse()

	// The header field is required by the API but ignored by the credential-set
	// injector, which takes header names from the resolved set.
	const placeholderHeader = "X-Substrate-Credential-Set"

	missing := false
	for name, v := range map[string]string{"atespace": *atespace, "actor": *actor, "host": *host, "uri": *uri} {
		if v == "" {
			fmt.Fprintf(os.Stderr, "error: --%s is required\n", name)
			missing = true
		}
	}
	if missing {
		flag.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	client, err := ateclient.NewClient(ctx, "", *kubeContext, *endpoint, *tokenFile, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to ateapi: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	actorRef := &ateapipb.ObjectRef{Atespace: *atespace, Name: *actor}

	if *replace {
		// Best-effort delete; a missing policy is fine.
		if _, err := client.DeleteActorEgressPolicy(ctx, &ateapipb.DeleteActorEgressPolicyRequest{Actor: actorRef}); err != nil && status.Code(err) != codes.NotFound {
			fmt.Fprintf(os.Stderr, "delete existing policy: %v\n", err)
			os.Exit(1)
		}
	}

	policy, err := client.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
		Actor: actorRef,
		EgressPolicy: &ateapipb.EgressPolicy{
			// name must be "default"; atespace must match the actor's.
			Metadata: &ateapipb.ResourceMetadata{Atespace: *atespace, Name: "default"},
			Rules: []*ateapipb.EgressRule{{
				Hostnames: &ateapipb.HostnameRule{
					Patterns: []string{*host},
					Effects: &ateapipb.EgressRuleEffects{
						InjectStaticHeaders: []*ateapipb.CredentialHeaderInjection{{
							Header:        placeholderHeader,
							CredentialUri: *uri,
						}},
					},
				},
			}},
		},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			fmt.Fprintf(os.Stderr, "policy already exists; re-run with --replace to overwrite it\n")
		}
		fmt.Fprintf(os.Stderr, "create egress policy: %v\n", err)
		os.Exit(1)
	}

	out, _ := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(policy)
	fmt.Printf("created egress policy for %s/%s:\n%s\n", *atespace, *actor, out)
}
