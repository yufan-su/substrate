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
	"io"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// cleanupResult counts what cleanup did.
type cleanupResult struct {
	phaseResult
	NotFound int `json:"notFound"`
}

// cleanup deletes actors 0 through n-1 in whatever state they are in. An
// actor's egress policy goes with it. Actors that do not exist count as done.
func cleanup(ctx context.Context, api ateapipb.ControlClient, b backoff, atespace string, n, concurrency int, out io.Writer) cleanupResult {
	res := newPhase()
	var notFound, done atomic.Int64
	start := time.Now()

	stop := make(chan struct{})
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				fmt.Fprintf(out, "%s  cleanup: %d/%d\n", time.Now().Format("15:04:05"), done.Load(), n)
			}
		}
	}()

	forEach(n, concurrency, func(i int) {
		defer done.Add(1)
		if ctx.Err() != nil {
			return
		}
		err := b.retry(ctx, func(ctx context.Context) error {
			_, err := api.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
				Actor:    &ateapipb.ObjectRef{Atespace: atespace, Name: actorName(i)},
				AnyState: true,
			})
			return err
		})
		switch {
		case err == nil:
			res.succeed()
		case status.Code(err) == codes.NotFound:
			notFound.Add(1)
			res.succeed()
		default:
			res.fail(fmt.Errorf("delete %s: %w", actorName(i), err))
		}
	})
	close(stop)

	return cleanupResult{phaseResult: res.result(n, time.Since(start)), NotFound: int(notFound.Load())}
}
