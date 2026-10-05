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
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// backoff retries control-plane calls that failed for a reason that can pass:
// a full worker pool, an unavailable or slow server, or a concurrent update.
type backoff struct {
	initial, max time.Duration
	// rpcTimeout bounds each attempt.
	rpcTimeout time.Duration
}

var defaultBackoff = backoff{initial: 50 * time.Millisecond, max: 2 * time.Second, rpcTimeout: 30 * time.Second}

// retry runs call until it succeeds, fails for good, or ctx ends.
func (b backoff) retry(ctx context.Context, call func(context.Context) error) error {
	delay := b.initial
	for {
		callCtx, cancel := context.WithTimeout(ctx, b.rpcTimeout)
		err := call(callCtx)
		cancel()
		if err == nil || !retryable(err) {
			return err
		}
		jitter := time.Duration(rand.Int64N(int64(delay)/2 + 1))
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (last error: %w)", ctx.Err(), err)
		case <-time.After(delay + jitter):
		}
		delay = min(2*delay, b.max)
	}
}

func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
		return true
	case codes.Aborted:
		// A concurrent update conflict passes; a crashed actor does not.
		return !strings.Contains(strings.ToLower(status.Convert(err).Message()), "crashed")
	default:
		return false
	}
}

// forEach calls fn for every i in [0, n), at most limit at a time.
func forEach(n, limit int, fn func(i int)) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(limit, 1))
	for i := range n {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			fn(i)
		})
	}
	wg.Wait()
}
