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

package egress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// tunnelDialer opens an authenticated tunnel to an original destination.
type tunnelDialer interface {
	DialContext(context.Context, string) (net.Conn, error)
	Close() error
}

type actorCertificateSource interface {
	MintWorkerCertificate(context.Context) (time.Time, error)
}

// originalDestinationFunc returns the address that a transparently intercepted
// connection originally targeted.
type originalDestinationFunc func(net.Conn) (string, error)

// Proxy proxies actor TCP connections through an egress CONNECT dialer. It is
// long-lived across actor activations, but only carries traffic while an actor
// is assigned to its worker.
type Proxy struct {
	originalDestination originalDestinationFunc
	// tunnelOpenTimeout bounds the wait for the gateway's CONNECT answer.
	tunnelOpenTimeout time.Duration

	mu sync.Mutex
	// Keyed by actor UID, supplied by the namespace-specific listener.
	active map[string]*activation
}

type activation struct {
	actorUID          string
	dialer            tunnelDialer
	certificateSource actorCertificateSource
	expiresAt         time.Time

	// ctx scopes certificate renewal and every tunnel opened by this activation. wg
	// lets Deactivate wait until both renewal and tunnel forwarding have exited.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates an activation-aware egress proxy.
func New(originalDestination originalDestinationFunc) (*Proxy, error) {
	if originalDestination == nil {
		return nil, fmt.Errorf("egress: original destination resolver is required")
	}
	// The gateway answers within its 5s authorization timeout; a longer wait
	// means it has queued the CONNECT behind its connection cap, and closing
	// the connection is what frees that queue slot.
	const tunnelOpenTimeout = 10 * time.Second
	return &Proxy{
		originalDestination: originalDestination,
		tunnelOpenTimeout:   tunnelOpenTimeout,
		active:              map[string]*activation{},
	}, nil
}

// Bind captures the actor's activation before its listeners start serving.
func (e *Proxy) Bind(actorUID string) (func(context.Context, net.Listener) error, error) {
	if actorUID == "" {
		return nil, fmt.Errorf("egress: actor UID is required")
	}
	e.mu.Lock()
	active := e.activationLocked(actorUID)
	e.mu.Unlock()
	return func(ctx context.Context, listener net.Listener) error {
		defer func() {
			e.mu.Lock()
			defer e.mu.Unlock()
			if e.active[actorUID] == active && active.dialer == nil {
				delete(e.active, actorUID)
				active.cancel()
			}
		}()
		return e.serve(ctx, listener, active)
	}, nil
}

func (e *Proxy) activationLocked(actorUID string) *activation {
	active := e.active[actorUID]
	if active == nil {
		ctx, cancel := context.WithCancel(context.Background())
		active = &activation{actorUID: actorUID, ctx: ctx, cancel: cancel}
		e.active[actorUID] = active
	}
	return active
}

func (e *Proxy) serve(ctx context.Context, listener net.Listener, active *activation) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-active.ctx.Done():
			_ = listener.Close()
		case <-done:
		}
	}()
	defer close(done)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("egress: accepting actor egress connection: %w", err)
		}
		e.handle(conn, active)
	}
}

// Activate enables the actor's egress and certificate renewal until deactivation.
func (e *Proxy) Activate(actorUID string, dialer tunnelDialer, certificateSource actorCertificateSource, expiresAt time.Time) error {
	if actorUID == "" {
		return fmt.Errorf("egress: actor UID is required")
	}
	if dialer == nil {
		return fmt.Errorf("egress: dialer is required")
	}
	if certificateSource == nil {
		return fmt.Errorf("egress: actor certificate source is required")
	}
	if !expiresAt.After(time.Now()) {
		return fmt.Errorf("egress: valid actor certificate is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	active := e.activationLocked(actorUID)
	if active.dialer != nil {
		return fmt.Errorf("egress: actor %s already has active egress", actorUID)
	}
	active.dialer = dialer
	active.certificateSource = certificateSource
	active.expiresAt = expiresAt
	active.wg.Add(1)
	go e.renew(active, expiresAt)
	return nil
}

func (e *Proxy) renew(active *activation, expiresAt time.Time) {
	defer active.wg.Done()
	// Schedule from the credential's remaining lifetime: renew at 90%, then
	// keep retrying after expiry so egress can recover without reactivation.
	delay := renewAfter(expiresAt)
	expired := false
	for waitForRenewal(active.ctx, delay) {
		if !expiresAt.After(time.Now()) && !expired {
			slog.WarnContext(active.ctx, "Atunnel actor certificate expired; blocking new egress connections",
				slog.Time("expiredAt", expiresAt))
			expired = true
		}
		nextExpiry, err := active.certificateSource.MintWorkerCertificate(active.ctx)
		if err != nil {
			code := status.Code(err)
			// Don't retry on these codes.  (No retries in worker will fix
			// Aborted, it indicates that worker is running an out-of-date
			// actor.)
			if code == codes.Aborted || code == codes.FailedPrecondition || code == codes.PermissionDenied {
				e.mu.Lock()
				active.expiresAt = time.Time{}
				e.mu.Unlock()
				slog.WarnContext(active.ctx, "Atunnel actor certificate renewal was denied; blocking new egress connections",
					slog.Any("err", err))
				return
			}
			delay = retryAfter(expiresAt)
			continue
		}
		if !nextExpiry.After(time.Now()) {
			delay = retryAfter(expiresAt)
			continue
		}
		e.mu.Lock()
		// Check cancellation under the same lock as Deactivate. Whichever wins
		// the lock last either installs a live expiry or leaves the activation empty;
		// renewal can never restore a credential after deactivation cleared it.
		if active.ctx.Err() != nil {
			e.mu.Unlock()
			return
		}
		active.expiresAt = nextExpiry
		e.mu.Unlock()
		if expired {
			slog.InfoContext(active.ctx, "Atunnel actor certificate renewed; allowing new egress connections",
				slog.Time("expiresAt", nextExpiry))
			expired = false
		}
		expiresAt = nextExpiry
		delay = renewAfter(expiresAt)
	}
}

func renewAfter(expiresAt time.Time) time.Duration {
	remaining := time.Until(expiresAt)
	return remaining - remaining/10
}

func retryAfter(expiresAt time.Time) time.Duration {
	remaining := time.Until(expiresAt)
	if remaining <= 0 {
		return 25*time.Second + rand.N(10*time.Second)
	}
	return min(30*time.Second, max(time.Second, remaining/10), remaining)
}

func waitForRenewal(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Deactivate disables the actor's egress and closes and drains its streams.
func (e *Proxy) Deactivate(ctx context.Context, actorUID string) error {
	e.mu.Lock()
	active := e.active[actorUID]
	delete(e.active, actorUID)
	if active != nil {
		active.expiresAt = time.Time{}
		active.cancel()
	}
	e.mu.Unlock()
	if active == nil {
		return nil
	}

	done := make(chan struct{})
	go func() {
		active.wg.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = fmt.Errorf("egress: waiting for active egress streams to stop: %w", ctx.Err())
	}
	if active.dialer != nil {
		err = errors.Join(err, active.dialer.Close())
	}
	return err
}

func (e *Proxy) handle(downstream net.Conn, active *activation) {
	e.mu.Lock()
	if active == nil || active.ctx.Err() != nil {
		e.mu.Unlock()
		_ = downstream.Close()
		return
	}
	if active.dialer == nil {
		e.mu.Unlock()
		// The actor sees this as a connection that closes right away, like a
		// network failure. This logs at Debug because an actor without an
		// egress gateway hits this on every outbound connection.
		slog.DebugContext(active.ctx, "atunnel dropped an egress connection: actor egress is not active",
			slog.String("actorUID", active.actorUID))
		_ = downstream.Close()
		return
	}
	if time.Now().Compare(active.expiresAt) >= 0 {
		// Expiry blocks only new tunnels. Connections admitted with a valid
		// certificate have completed mTLS and are allowed to drain normally.
		e.mu.Unlock()
		// renew logs the expiry or the denied renewal once at Warn.
		slog.DebugContext(active.ctx, "atunnel dropped an egress connection: actor has no valid certificate",
			slog.String("actorUID", active.actorUID))
		_ = downstream.Close()
		return
	}
	active.wg.Add(1)
	e.mu.Unlock()

	accepted := time.Now()
	go func() {
		defer active.wg.Done()
		defer downstream.Close()

		destination, err := e.originalDestination(downstream)
		if err != nil {
			slog.WarnContext(active.ctx, "atunnel failed to resolve original egress destination", slog.Any("err", err))
			return
		}
		dialCtx, cancelDial := context.WithTimeout(active.ctx, e.tunnelOpenTimeout)
		upstream, err := active.dialer.DialContext(dialCtx, destination)
		cancelDial()
		tunnelOpen := time.Now()
		if err != nil {
			slog.WarnContext(active.ctx, "atunnel failed to open egress tunnel", slog.String("destination", destination), slog.Any("err", err))
			return
		}
		defer upstream.Close()

		stop := context.AfterFunc(active.ctx, func() {
			_ = downstream.Close()
			_ = upstream.Close()
		})
		defer stop()

		t := copyBothWays(downstream, upstream)
		var connectSent time.Time
		if stream, ok := upstream.(*h2StreamConn); ok {
			if ns := stream.reqBody.headersWritten.Load(); ns != 0 {
				connectSent = time.Unix(0, ns)
			}
		}
		slog.InfoContext(active.ctx, "atunnel egress connection timing",
			slog.String("actorAddr", downstream.RemoteAddr().String()),
			slog.String("destination", destination),
			slog.String("accepted", accepted.UTC().Format(time.RFC3339Nano)),
			slog.Float64("connectSentMs", msAfter(accepted, connectSent)),
			slog.Float64("tunnelOpenMs", msAfter(accepted, tunnelOpen)),
			slog.Float64("firstActorReadMs", msAfter(accepted, t.firstActorRead)),
			slog.Float64("firstUpstreamWriteMs", msAfter(accepted, t.firstUpstreamWrite)),
			slog.Float64("firstActorWriteMs", msAfter(accepted, t.firstActorWrite)),
			slog.Float64("upDoneMs", msAfter(accepted, t.upDone)),
			slog.Float64("downDoneMs", msAfter(accepted, t.downDone)),
			slog.Int64("actorBytes", t.actorBytes),
			slog.Int64("originBytes", t.originBytes),
			slog.Any("upErr", t.upErr),
			slog.Any("downErr", t.downErr))
	}()
}

// msAfter returns the milliseconds from start to t, or -1 if t was never reached.
func msAfter(start, t time.Time) float64 {
	if t.IsZero() {
		return -1
	}
	return float64(t.Sub(start).Microseconds()) / 1000
}

// copyTiming records when each direction of a proxied connection first moved
// data and when it ended.
type copyTiming struct {
	firstActorRead, firstUpstreamWrite, firstActorWrite time.Time
	upDone, downDone                                    time.Time
	actorBytes, originBytes                             int64
	upErr, downErr                                      error
}

// copyBothWays copies actor connection a to and from upstream b. Unlike
// io.Copy, it doesn't hold a 32 KiB buffer per direction while the tunnel is
// idle.
func copyBothWays(a, b net.Conn) copyTiming {
	var t copyTiming
	done := make(chan struct{}, 2)
	toActor := &timedWriter{w: a}
	go func() {
		if stream, ok := b.(*h2StreamConn); ok {
			t.originBytes, t.downErr = stream.writeTo(toActor)
		} else {
			t.originBytes, t.downErr = (&streamCopy{dest: toActor, src: b}).do()
		}
		t.downDone = time.Now()
		closeWrite(a)
		done <- struct{}{}
	}()
	fromActor := &timedReader{r: a}
	toUpstream := &timedWriter{w: b}
	go func() {
		t.actorBytes, t.upErr = (&streamCopy{dest: toUpstream, src: fromActor}).do()
		t.upDone = time.Now()
		closeWrite(b)
		done <- struct{}{}
	}()
	<-done
	<-done
	t.firstActorRead = fromActor.at
	t.firstUpstreamWrite = toUpstream.at
	t.firstActorWrite = toActor.at
	return t
}

// timedReader records when its first non-empty read returned.
type timedReader struct {
	r  io.Reader
	at time.Time
}

func (t *timedReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 && t.at.IsZero() {
		t.at = time.Now()
	}
	return n, err
}

// timedWriter records when its first non-empty write returned.
type timedWriter struct {
	w  io.Writer
	at time.Time
}

func (t *timedWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if n > 0 && t.at.IsZero() {
		t.at = time.Now()
	}
	return n, err
}

func closeWrite(conn net.Conn) {
	if conn, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = conn.CloseWrite()
	}
}

// PortFromListenAddr is the port from a listen address, which each sandbox's
// redirect aims at. The address itself is never bound: egress is served from
// inside the sandbox namespaces.
func PortFromListenAddr(listenAddress string) (uint16, error) {
	_, port, err := net.SplitHostPort(listenAddress)
	if err != nil {
		return 0, fmt.Errorf("egress: listen address %q: %w", listenAddress, err)
	}
	p, ok := parsePort(port)
	if !ok {
		return 0, fmt.Errorf("egress: listen address %q has no usable port", listenAddress)
	}
	return uint16(p), nil
}

// parsePort parses s as a TCP port number, returning ok=false for anything
// outside the valid 1-65535 range (including non-numeric input).
func parsePort(s string) (port int, ok bool) {
	p, err := strconv.Atoi(s)
	if err != nil || p < 1 || p > 65535 {
		return 0, false
	}
	return p, true
}
