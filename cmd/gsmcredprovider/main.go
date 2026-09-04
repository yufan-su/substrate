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

// Command gsmcredprovider is a POC credential-provider plugin: a gRPC service
// that resolves substrate-secret:// URIs of the secretmanager.googleapis.com
// class to Google Cloud Secret Manager secret payloads. It is a drop-in
// alternative to the Kubernetes-Secret-backed credprovider: point the egress
// injector's --credential-provider-address at whichever backend a deployment
// wants. It is the only component in the egress credential-injection path with
// Secret Manager access; the egress gateway and its injector never access
// secrets directly.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"

	"github.com/agent-substrate/substrate/cmd/gsmcredprovider/internal/gsmprovider"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
)

const serviceName = "gsmcredprovider"

var (
	listenAddr   = pflag.String("listen-address", ":50051", "gRPC listen address")
	metricsAddr  = pflag.String("metrics-address", ":9090", "Prometheus/health HTTP listen address")
	serverBundle = pflag.String("server-cred-bundle", "", "credential bundle (PEM key+chain) presented for serving TLS; empty serves plaintext (dev only)")
	clientCAFile = pflag.String("client-ca-file", "", "CA bundle that caller (injector) client certificates must chain to; empty accepts any client when TLS is on")
	logLevel     = pflag.String("log-level", "info", "one of debug, info, warn, error")
	drainGrace   = pflag.Duration("drain-grace", 5*time.Second, "how long to wait for in-flight RPCs on shutdown before a hard stop")
)

func main() {
	pflag.Parse()

	ctx := context.Background()
	serverboot.InitLogger()
	if err := serverboot.SetLogLevel(*logLevel); err != nil {
		serverboot.Fatal(ctx, "invalid --log-level", err)
	}

	slog.InfoContext(ctx, "starting gsmcredprovider", slog.String("version", version.String()))

	if err := run(ctx); err != nil {
		serverboot.Fatal(ctx, "gsmcredprovider exited with error", err)
	}
}

func run(ctx context.Context) error {
	mp, err := serverboot.InitMetrics(ctx, serviceName)
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	readiness := &serverboot.Readiness{}
	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{
		Addr:          *metricsAddr,
		Readiness:     readiness,
		EnableHealthz: true,
	})

	// Application Default Credentials: in-cluster this is a GKE Workload Identity
	// binding on the Pod's ServiceAccount, granting Secret Manager access.
	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("secret manager client: %w", err)
	}
	defer smClient.Close()

	creds, err := buildServerCreds(ctx)
	if err != nil {
		return fmt.Errorf("server credentials: %w", err)
	}

	opts := []grpc.ServerOption{grpc.StatsHandler(otelgrpc.NewServerHandler())}
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}
	srv := grpc.NewServer(opts...)
	reflection.Register(srv)
	credproviderpb.RegisterCredentialProviderServer(srv, gsmprovider.NewServer(smClient))

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listenAddr, err)
	}

	shutdownCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-shutdownCtx.Done()
		slog.Info("shutting down")
		readiness.MarkNotReady()
		done := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(*drainGrace):
			slog.Warn("graceful shutdown timed out; forcing stop", slog.Duration("grace", *drainGrace))
			srv.Stop()
		}
	}()

	slog.InfoContext(ctx, "gsmcredprovider listening", slog.String("address", lis.Addr().String()), slog.Bool("tls", creds != nil))
	if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
		return fmt.Errorf("serving: %w", err)
	}
	return nil
}

// buildServerCreds composes serving TLS from the credential bundle, verifying
// caller client certificates against the configured CA. Returns nil (plaintext)
// when no bundle is configured — a dev-only path; in-cluster the injector dials
// with mTLS and SAN pinning.
func buildServerCreds(ctx context.Context) (credentials.TransportCredentials, error) {
	if *serverBundle == "" {
		slog.WarnContext(ctx, "no --server-cred-bundle set; serving plaintext (dev only)")
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS13,
		GetCertificate: credbundle.Loader(*serverBundle),
		ClientAuth:     tls.RequireAnyClientCert,
	}
	if *clientCAFile != "" {
		ca, err := os.ReadFile(*clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read --client-ca-file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("no certificates in --client-ca-file %q", *clientCAFile)
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
		slog.InfoContext(ctx, "verifying caller client certificates", slog.String("ca", *clientCAFile))
	}
	return credentials.NewTLS(cfg), nil
}
