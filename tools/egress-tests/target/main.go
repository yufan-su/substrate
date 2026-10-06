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

// Command target is the endpoint the egress-tests actors call: every request
// gets a 200 with a small fixed body, over plain HTTP and, when given a
// certificate, HTTPS. One Deployment of it sits behind all of the test's
// Services.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func newHandler(responseBytes int) http.Handler {
	body := bytes.Repeat([]byte("x"), responseBytes)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write(body)
	})
	return mux
}

// newTLSConfig serves the certificate in certFile with the key in keyFile.
func newTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading TLS certificate: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

func main() {
	listen := flag.String("listen", ":8080", "Address to serve plain HTTP on.")
	tlsListen := flag.String("tls-listen", "", "Address to serve HTTPS on; empty serves no HTTPS.")
	tlsCertFile := flag.String("tls-cert-file", "", "PEM certificate to serve HTTPS with.")
	tlsKeyFile := flag.String("tls-key-file", "", "PEM private key of --tls-cert-file.")
	responseBytes := flag.Int("response-bytes", 16, "Size of the body every request gets back.")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	handler := newHandler(*responseBytes)
	servers := []*http.Server{{Addr: *listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second}}
	if *tlsListen != "" {
		cfg, err := newTLSConfig(*tlsCertFile, *tlsKeyFile)
		if err != nil {
			log.Fatal(err)
		}
		servers = append(servers, &http.Server{Addr: *tlsListen, Handler: handler, TLSConfig: cfg, ReadHeaderTimeout: 10 * time.Second})
	}

	// The first server to fail stops them all, as the signal does.
	failed := make(chan error, len(servers))
	var wg sync.WaitGroup
	for _, srv := range servers {
		wg.Go(func() {
			var err error
			if srv.TLSConfig != nil {
				log.Printf("egress-tests target serving HTTPS on %s", srv.Addr)
				err = srv.ListenAndServeTLS("", "")
			} else {
				log.Printf("egress-tests target serving HTTP on %s", srv.Addr)
				err = srv.ListenAndServe()
			}
			if !errors.Is(err, http.ErrServerClosed) {
				failed <- fmt.Errorf("serving on %s: %w", srv.Addr, err)
			}
		})
	}

	var err error
	select {
	case <-ctx.Done():
	case err = <-failed:
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	for _, srv := range servers {
		_ = srv.Shutdown(shutdownCtx)
	}
	wg.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
