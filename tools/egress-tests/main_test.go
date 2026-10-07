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
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateclient"
)

const testKubeconfig = `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster: {server: "https://127.0.0.1:1"}
users:
- name: u
  user: {token: t}
contexts:
- name: ctx
  context: {cluster: c, user: u}
current-context: ctx
`

// TestConnectPassesTheEndpointAndTokenFile runs both commands up to the
// ateapi dial. It replaces newATEClient, so it does not run in parallel.
func TestConnectPassesTheEndpointAndTokenFile(t *testing.T) {
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	writeFile(t, kubeconfig, testKubeconfig, 0o600)

	type dial struct{ context, endpoint, tokenFile string }
	var got *dial
	errStub := errors.New("stub dial")
	saved := newATEClient
	t.Cleanup(func() { newATEClient = saved })
	newATEClient = func(_ context.Context, _, k8sContext, endpoint, tokenFile string, _ bool) (*ateclient.Client, error) {
		got = &dial{k8sContext, endpoint, tokenFile}
		return nil, errStub
	}

	commands := map[string]func(context.Context, []string) error{"run": runCmd, "cleanup": cleanupCmd}
	for _, cmd := range []string{"run", "cleanup"} {
		for _, tc := range []struct {
			name    string
			args    []string
			want    *dial
			wantErr string
		}{
			{
				name: "port-forward",
				args: []string{"--context", "ctx"},
				want: &dial{context: "ctx"},
			},
			{
				name: "endpoint, minted token",
				args: []string{"--api-endpoint", "api.ate-system.svc:443"},
				want: &dial{endpoint: "api.ate-system.svc:443"},
			},
			{
				name: "endpoint and token file",
				args: []string{"--api-endpoint", "api.ate-system.svc:443", "--api-token-file", "/var/run/ateapi/token"},
				want: &dial{endpoint: "api.ate-system.svc:443", tokenFile: "/var/run/ateapi/token"},
			},
			{
				name:    "token file without an endpoint",
				args:    []string{"--api-token-file", "/var/run/ateapi/token"},
				wantErr: "--api-token-file needs --api-endpoint",
			},
		} {
			t.Run(cmd+"/"+tc.name, func(t *testing.T) {
				got = nil
				err := commands[cmd](t.Context(), append([]string{"--kubeconfig", kubeconfig}, tc.args...))
				if tc.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Errorf("%s %v: got error %v, want %q", cmd, tc.args, err, tc.wantErr)
					}
					if got != nil {
						t.Errorf("%s %v dialed ateapi with %+v, want no dial", cmd, tc.args, *got)
					}
					return
				}
				if !errors.Is(err, errStub) {
					t.Fatalf("%s %v: got error %v, want the stub's", cmd, tc.args, err)
				}
				if *got != *tc.want {
					t.Errorf("%s %v dialed %+v, want %+v", cmd, tc.args, *got, *tc.want)
				}
			})
		}
	}
}
