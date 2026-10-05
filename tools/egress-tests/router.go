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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/atenet"
)

// routerClient sends HTTP requests to actors through the atenet router, which
// picks the actor from the ate-target-actor header.
type routerClient struct {
	baseURL  string
	atespace string
	http     *http.Client
}

func newRouterClient(baseURL, atespace string, conns int) *routerClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = max(conns, 2)
	return &routerClient{
		baseURL:  strings.TrimSuffix(baseURL, "/"),
		atespace: atespace,
		http:     &http.Client{Timeout: 30 * time.Second, Transport: transport},
	}
}

// statusError is a non-2xx answer from the router or the actor.
type statusError struct {
	code int
	body string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.code, e.body)
}

// do sends body (if non-nil) as JSON to the actor's path and, on a 2xx answer,
// decodes the response into out (if non-nil).
func (c *routerClient) do(ctx context.Context, method, actor, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set(atenet.TargetActorHeader, c.atespace+"/"+actor)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &statusError{code: resp.StatusCode, body: strings.TrimSpace(string(msg))}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
