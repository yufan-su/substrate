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

// Command egress-tests drives the egress scale test: it creates A actors,
// resumes B of them, and has each resumed actor loop over HTTP requests to C
// in-cluster endpoints through the egress gateway. See README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/portforward"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/egressapi"
	"github.com/agent-substrate/substrate/tools/egress-tests/internal/targetcert"
)

const usage = `Usage: go run ./tools/egress-tests <command> [flags]

Commands:
  run       create the actors, run the egress loops, and print a report
  cleanup   delete the actors a run created
  certs     write a CA and the target's HTTPS certificate (deploy.sh uses this)

Run "go run ./tools/egress-tests <command> -h" for the flags of a command.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "run":
		err = runCmd(ctx, os.Args[2:])
	case "cleanup":
		err = cleanupCmd(ctx, os.Args[2:])
	case "certs":
		err = certsCmd(os.Args[2:])
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "egress-tests %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

// clusterFlags select the cluster and how to reach ateapi.
type clusterFlags struct {
	kubeconfig  string
	kubeContext string
	apiEndpoint string
	atespace    string
}

func (f *clusterFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.kubeconfig, "kubeconfig", "", "Path to the kubeconfig. Empty uses the default loading rules.")
	fs.StringVar(&f.kubeContext, "context", "", "Kubeconfig context. Empty uses the current context.")
	fs.StringVar(&f.apiEndpoint, "api-endpoint", "", "ateapi address. Empty port-forwards to the api Service.")
	fs.StringVar(&f.atespace, "atespace", "egress-tests", "Atespace of the actor template and the actors.")
}

// connection is what both commands talk to.
type connection struct {
	api *ateclient.Client
	k8s kubernetes.Interface
}

func (f *clusterFlags) connect(ctx context.Context) (*connection, error) {
	cfg, err := ateclient.LoadKubeConfig(f.kubeconfig, f.kubeContext)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating Kubernetes client: %w", err)
	}
	api, err := ateclient.NewClient(ctx, f.kubeconfig, f.kubeContext, f.apiEndpoint, "", false)
	if err != nil {
		return nil, fmt.Errorf("connecting to ateapi: %w", err)
	}
	return &connection{api: api, k8s: k8s}, nil
}

func runCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var cf clusterFlags
	cf.register(fs)
	cfg := runConfig{}
	fs.IntVar(&cfg.Actors, "actors", 1000, "A: actors to create. Reruns reuse the actors that exist.")
	fs.IntVar(&cfg.Parallel, "parallel", 1, "B: actors to resume and run the loop in, at the same time.")
	fs.IntVar(&cfg.Endpoints, "endpoints", 10, fmt.Sprintf("C: endpoints each loop calls, egress-target-0 up to egress-target-<C-1> (at most %d).", egressapi.MaxEndpoints))
	fs.DurationVar(&cfg.Duration, "duration", 5*time.Minute, "How long the loops run once all of them have started.")
	fs.StringVar(&cfg.ConnMode, "conn-mode", connModeKeepAlive, "How the loops connect: keepalive (one connection per endpoint) or new-conn (one per request).")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", 5*time.Second, "Timeout of one request.")
	fs.DurationVar(&cfg.RequestInterval, "request-interval", 100*time.Millisecond, "Pause after each request in a loop, which sets each actor's rate; 0 sends back to back at full speed.")
	fs.IntVar(&cfg.CreateConcurrency, "create-concurrency", 32, "Actors created at the same time.")
	fs.DurationVar(&cfg.ResumeTimeout, "resume-timeout", 5*time.Minute, "Per actor: how long to keep resuming (a full pool is retried) and waiting for it to answer.")
	fs.DurationVar(&cfg.ProgressInterval, "progress-interval", 30*time.Second, "How often to print progress; 0 turns it off.")
	fs.StringVar(&cfg.Template, "template", "egress-tests-actor", "Actor template the actors are created from.")
	fs.StringVar(&cfg.Scheme, "scheme", egressapi.SchemeHTTP, "Scheme the loops request endpoints over: http, or https through the gateway's TLS interception (needs deploy.sh --https).")
	routerURL := fs.String("router-url", "", "atenet router base URL. Empty port-forwards to the atenet-router Service.")
	output := fs.String("output", "", "If set, write the report as JSON to this file.")
	resources := fs.Bool("resources", false, "Sample the CPU and memory of the components on the egress path during the run.")
	resourcesInterval := fs.Duration("resources-interval", time.Second, "How often --resources reads the live sources (Go process counters, Envoy stats).")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg.Atespace = cf.atespace
	if err := cfg.validate(); err != nil {
		return err
	}
	if *resources && *resourcesInterval <= 0 {
		return fmt.Errorf("--resources-interval must be positive, got %v", *resourcesInterval)
	}

	conn, err := cf.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.api.Close()

	if *routerURL == "" {
		restCfg, err := ateclient.LoadKubeConfig(cf.kubeconfig, cf.kubeContext)
		if err != nil {
			return fmt.Errorf("loading kubeconfig: %w", err)
		}
		port, stopForward, err := portforward.ServicePortForward(ctx, restCfg, conn.k8s,
			installdefaults.SystemNamespace, installdefaults.RouterServiceName, 80)
		if err != nil {
			return fmt.Errorf("port-forwarding to %s: %w", installdefaults.RouterServiceName, err)
		}
		defer stopForward()
		*routerURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	}

	r := newRunner(cfg, conn.api, conn.k8s, newRouterClient(*routerURL, cfg.Atespace, cfg.Parallel), os.Stdout)
	if *resources {
		r.res = newResourceSampler(restRawGetter{conn.k8s.CoreV1().RESTClient()}, conn.k8s, *resourcesInterval)
	}
	rep, runErr := r.run(ctx)
	if rep != nil {
		rep.print(os.Stdout)
		if *output != "" {
			if err := writeReport(*output, rep); err != nil {
				return errors.Join(runErr, err)
			}
			fmt.Fprintf(os.Stdout, "report written to %s\n", *output)
		}
	}
	return runErr
}

func writeReport(path string, rep *report) error {
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func cleanupCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cleanup", flag.ExitOnError)
	var cf clusterFlags
	cf.register(fs)
	actors := fs.Int("actors", 1000, "Delete actors egress-0 up to egress-<actors-1>. Use the largest --actors any run used.")
	concurrency := fs.Int("concurrency", 64, "Actors deleted at the same time.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *actors < 1 || *concurrency < 1 {
		return errors.New("--actors and --concurrency must be at least 1")
	}

	conn, err := cf.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.api.Close()

	res := cleanup(ctx, conn.api, defaultBackoff, cf.atespace, *actors, *concurrency, os.Stdout)
	printPhase(os.Stdout, "cleanup", res.phaseResult, fmt.Sprintf(" (%d did not exist)", res.NotFound))
	if res.Failed > 0 {
		return fmt.Errorf("%d actors were not deleted", res.Failed)
	}
	return ctx.Err()
}

func certsCmd(args []string) error {
	fs := flag.NewFlagSet("certs", flag.ExitOnError)
	out := fs.String("out", "", "Directory to write ca.crt, tls.crt and tls.key to. Required.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("--out is required")
	}
	bundle, err := targetcert.Generate(time.Now())
	if err != nil {
		return err
	}
	return bundle.Write(*out)
}
