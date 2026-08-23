package tunnels

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// WorkerResolver is the slice of IWorkerRegistry the middleware needs
// to validate that the tunnel's bound worker is still online.
type WorkerResolver interface {
	// FindByWorkerID returns the connection id currently mapped to workerID.
	FindByWorkerID(workerID string) (string, bool)
}

// WorkerCommandDispatcher is the slice of IWorkerCommandDispatcher the
// middleware uses to forward HTTP requests to the bound worker.
type WorkerCommandDispatcher interface {
	SendTunnelHTTPRequest(ctx context.Context, workerConnectionID string, req TunnelHttpRequest) (TunnelHttpResponse, error)
}

// Middleware is the Go port of CortexTerminal.Gateway.Tunnels.TunnelMiddleware.
// Intercepts /t/<key>/... or <key>.<RootDomain>/... paths, validates the
// secret, and forwards the request to the bound worker via the SignalR
// tunnel frame.
type Middleware struct {
	options    Options
	registry   *Registry
	quota      *Quota
	workers    WorkerResolver
	dispatcher WorkerCommandDispatcher
	logger     *slog.Logger
	clock      func() time.Time
}

func NewMiddleware(
	options Options,
	registry *Registry,
	quota *Quota,
	workers WorkerResolver,
	dispatcher WorkerCommandDispatcher,
	logger *slog.Logger,
) *Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return &Middleware{
		options:    options,
		registry:   registry,
		quota:      quota,
		workers:    workers,
		dispatcher: dispatcher,
		logger:     logger,
		clock:      func() time.Time { return time.Now().UTC() },
	}
}

// Wrap returns an http.Handler that runs the tunnel claim before the
// next handler. Routes not matching either the path prefix or the
// subdomain fall through unchanged.
func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, subPath, ok := m.extractKey(r)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		m.serve(w, r, key, subPath)
	})
}

// extractKey returns (tunnelKey, subPath, true) when the request matches
// either the subdomain or path layout. (false) means the request falls
// through to the next handler.
func (m *Middleware) extractKey(r *http.Request) (string, string, bool) {
	host := r.Host
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	root := m.options.RootDomain
	if root != "" && host != "" && strings.HasSuffix(strings.ToLower(host), "."+strings.ToLower(root)) {
		sub := host[:len(host)-len(root)-1]
		if sub != "" && !strings.Contains(sub, ".") {
			subPath := r.URL.Path
			if subPath == "" {
				subPath = "/"
			}
			return sub, subPath, true
		}
	}
	prefix := m.options.RoutePrefix
	if !strings.HasPrefix(r.URL.Path, prefix) {
		return "", "", false
	}
	rest := r.URL.Path[len(prefix):]
	slash := strings.IndexByte(rest, '/')
	var key, sub string
	if slash < 0 {
		key = rest
		sub = "/"
	} else {
		key = rest[:slash]
		sub = rest[slash:]
	}
	if key == "" {
		return "", "", false
	}
	return key, sub, true
}

func (m *Middleware) serve(w http.ResponseWriter, r *http.Request, key, subPath string) {
	ctx := r.Context()

	if !m.options.Enabled {
		writeError(w, "Port forwarding is disabled.", http.StatusServiceUnavailable)
		return
	}
	tunnel, err := m.registry.FindByKey(ctx, key)
	if err != nil {
		m.logger.Warn("tunnel lookup failed", "key", key, "err", err)
		writeError(w, "Tunnel lookup failed.", http.StatusBadGateway)
		return
	}
	if tunnel == nil {
		writeError(w, "Tunnel does not exist or has been revoked.", http.StatusGone)
		return
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, tunnel.ExpiresAtUTC)
	if err != nil {
		writeError(w, "Tunnel has invalid expiry.", http.StatusGone)
		return
	}
	if !expiresAt.After(m.clock()) {
		writeError(w, "Tunnel has expired.", http.StatusGone)
		return
	}

	querySecret := r.URL.Query().Get("k")
	cookie, _ := r.Cookie("k")
	cookieSecret := ""
	if cookie != nil {
		cookieSecret = cookie.Value
	}
	secret := querySecret
	if secret == "" {
		secret = cookieSecret
	}
	if !Verify(secret, tunnel.SecretHash) {
		writeError(w, "Invalid or missing tunnel secret.", http.StatusUnauthorized)
		return
	}

	// First request that validated via ?k= seeds the cookie so subsequent
	// sub-resource fetches (no query) succeed without re-entering the
	// secret.
	if cookieSecret == "" && querySecret != "" {
		http.SetCookie(w, &http.Cookie{
			Name:     "k",
			Value:    secret,
			HttpOnly: true,
			Secure:   true,
			SameSite: http.SameSiteLaxMode,
			Path:     "/",
			Expires:  expiresAt,
		})
	}

	workerConn, ok := m.workers.FindByWorkerID(tunnel.WorkerID)
	if !ok || workerConn != tunnel.WorkerConnectionID {
		writeError(w, "Worker is offline. Start the worker and reattach the session.", http.StatusBadGateway)
		return
	}

	if !m.quota.TryAcquire(tunnel.ID) {
		writeError(w, "Rate limit exceeded.", http.StatusTooManyRequests)
		return
	}

	body, err := readBody(r)
	if err != nil {
		writeError(w, "Failed to read request body.", http.StatusBadRequest)
		return
	}
	headers := collectHeaders(r.Header)

	tunnelReq := TunnelHttpRequest{
		TunnelID: tunnel.ID,
		Port:     tunnel.Port,
		Method:   r.Method,
		Path:     subPath,
		Query:    r.URL.RawQuery,
		Headers:  headers,
		Body:     body,
	}

	dispatchCtx, cancel := context.WithTimeout(ctx, m.options.ForwardTimeout)
	defer cancel()

	resp, err := m.dispatcher.SendTunnelHTTPRequest(dispatchCtx, workerConn, tunnelReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			writeError(w, "Upstream did not respond in time.", http.StatusGatewayTimeout)
			return
		}
		m.logger.Warn("tunnel forward failed", "tunnel", tunnel.ID, "err", err)
		writeError(w, "Tunnel forward failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	if resp.ErrorMessage != "" {
		writeError(w, resp.ErrorMessage, http.StatusBadGateway)
		return
	}

	w.WriteHeader(resp.StatusCode)
	for name, values := range resp.Headers {
		for _, v := range values {
			w.Header().Add(name, v)
		}
	}
	w.Header().Del("Transfer-Encoding")
	if len(resp.Body) > 0 {
		_, _ = w.Write(resp.Body)
	}
}

// --- helpers ---

var hopByHop = map[string]struct{}{
	"Connection":          {},
	"Keep-Alive":          {},
	"Proxy-Authenticate":  {},
	"Proxy-Authorization": {},
	"TE":                  {},
	"Trailer":             {},
	"Transfer-Encoding":   {},
	"Upgrade":             {},
	"Host":                {},
}

func collectHeaders(h http.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, v := range h {
		if strings.HasPrefix(strings.ToLower(k), "authorization") {
			continue
		}
		if _, skip := hopByHop[k]; skip {
			continue
		}
		out[k] = append([]string(nil), v...)
	}
	return out
}

func readBody(r *http.Request) ([]byte, error) {
	if r.ContentLength == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(msg))
}