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

// Command cgreader samples the cgroup v2 CPU and memory files of the cgroups
// the egress-tests driver names, once per interval, and serves the rows over
// HTTP. One runs on every node as a DaemonSet with the host's cgroup tree
// mounted read-only; the driver reaches it through the API server's pod
// proxy. It needs no Kubernetes API access.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/tools/egress-tests/internal/cgapi"
)

func newHandler(r *reader) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET "+cgapi.SamplesRoute, func(w http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		var since uint64
		latest := q.Get("since") == cgapi.SinceLatest
		if s := q.Get("since"); s != "" && !latest {
			var err error
			if since, err = strconv.ParseUint(s, 10, 64); err != nil {
				http.Error(w, "bad since: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
		if err := r.addTargets(q["target"]); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(r.since(since, latest))
	})
	return mux
}

func main() {
	listen := flag.String("listen", fmt.Sprintf(":%d", cgapi.Port), "Address to serve on.")
	root := flag.String("cgroup-root", "/host-cgroup", "Where the host's cgroup v2 tree is mounted.")
	interval := flag.Duration("interval", time.Second, "How often to read every target.")
	ring := flag.Int("ring", 50000, "Rows kept for the driver to pull; it pulls every few seconds.")
	flag.Parse()
	if err := checkRoot(*root); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	r := newReader(*root, "/proc/uptime", *ring)
	go func() {
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.sample()
			}
		}
	}()

	srv := &http.Server{Addr: *listen, Handler: newHandler(r), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Printf("cgreader serving %s on %s every %v", *root, *listen, *interval)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serving: %v", err)
	}
}
