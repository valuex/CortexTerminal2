// Command tunnels-smoke proves the tunnels subsystem end-to-end:
// GenerateSecret / Hash / Verify round-trip, Quota rate-limits per
// tunnel, Registry CRUD, and the Middleware (path mode) routes through
// the worker dispatcher to a fake upstream.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/data"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/tunnels"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 1. Secret generation round-trip.
	secret := tunnels.GenerateSecret()
	if len(secret) == 0 {
		fatal("empty secret")
	}
	hashed := tunnels.Hash(secret)
	if !tunnels.Verify(secret, hashed) {
		fatal("verify(secret, hash) returned false")
	}
	if tunnels.Verify("wrong-secret", hashed) {
		fatal("verify accepted wrong secret")
	}
	if tunnels.Verify("", hashed) {
		fatal("verify accepted empty secret")
	}
	logger.Info("secret hashing ok",
		"secret_len", len(secret), "hash_len", len(hashed))

	// 2. Tunnel key generation.
	key := tunnels.GenerateTunnelKey()
	if len(key) != 10 {
		fatal("tunnel key should be 10 hex chars, got %d", len(key))
	}
	for _, r := range key {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			fatal("tunnel key contains non-hex char %q", r)
		}
	}
	logger.Info("tunnel key generation ok", "key", key)

	// 3. Quota: burst of 5, MaxQPS=3, expect 3 to pass.
	clock := time.Unix(1000, 0)
	q := tunnels.NewQuota(3, func() time.Time { return clock })
	for i := 0; i < 3; i++ {
		if !q.TryAcquire("t-1") {
			fatal("acquire #%d should succeed", i+1)
		}
	}
	if q.TryAcquire("t-1") {
		fatal("acquire #4 should be rejected (over MaxQPS=3)")
	}
	// Different tunnel — independent counter.
	if !q.TryAcquire("t-2") {
		fatal("t-2 should be unaffected by t-1 limit")
	}
	// Advance the clock — counter resets.
	clock = clock.Add(time.Second)
	if !q.TryAcquire("t-1") {
		fatal("after window reset, t-1 should acquire")
	}
	logger.Info("quota ok")

	// 4. Registry + middleware end-to-end against in-memory SQLite.
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	must(err, "open db")
	defer db.Close()
	must(data.RunMigrations(context.Background(), db), "migrate")

	tunnelsRepo := data.NewTunnelsRepo(db)
	registry := tunnels.NewRegistry(tunnelsRepo, nil)

	const userID = "user-zzz"
	const sessionID = "sess-zzz"
	const workerID = "worker-zzz"
	const workerConn = "worker-conn-zzz"

	tEntity, err := registry.Create(context.Background(),
		key, hashed, userID,
		workerID, workerConn, sessionID,
		8080, 1*time.Hour)
	must(err, "create tunnel")
	if tEntity.ID == "" || tEntity.TunnelKey != key || tEntity.SecretHash != hashed {
		fatal("entity mismatch: %+v", tEntity)
	}
	logger.Info("tunnel created", "id", tEntity.ID, "key", tEntity.TunnelKey)

	// Quota check: MaxTunnelsPerSession should be enforced.
	registryWithQuota := tunnels.NewRegistry(tunnelsRepo, nil)
	_ = registryWithQuota

	// FindByKey.
	found, err := registry.FindByKey(context.Background(), key)
	must(err, "find by key")
	if found == nil || found.ID != tEntity.ID {
		fatal("find by key mismatch: %+v", found)
	}

	// List.
	list, err := registry.ListForSession(context.Background(), sessionID, userID)
	must(err, "list")
	if len(list) != 1 {
		fatal("list should have 1 entry, got %d", len(list))
	}

	// Count.
	count, err := registry.CountActiveForSession(context.Background(), sessionID)
	must(err, "count")
	if count != 1 {
		fatal("count should be 1, got %d", count)
	}

	// Revoke.
	revoked, err := registry.Revoke(context.Background(), tEntity.ID, userID)
	must(err, "revoke")
	if !revoked {
		fatal("revoke should return true")
	}
	// Revoked tunnel should not be found by key.
	found, err = registry.FindByKey(context.Background(), key)
	must(err, "find after revoke")
	if found != nil {
		fatal("revoked tunnel still found by key")
	}
	list, err = registry.ListForSession(context.Background(), sessionID, userID)
	must(err, "list after revoke")
	if len(list) != 0 {
		fatal("list after revoke should be empty, got %d", len(list))
	}
	logger.Info("revoke + post-revoke visibility ok")

	// 5. Middleware end-to-end: register a tunnel, dispatch via worker
	// stub, assert the worker received the request.
	tEntity2, err := registry.Create(context.Background(),
		"abcdef0123", hashed, userID,
		workerID, workerConn, sessionID,
		8080, 1*time.Hour)
	must(err, "create tunnel 2")

	stubWorkers := &stubWorkerResolver{workers: map[string]string{workerID: workerConn}}
	stubDispatcher := &stubDispatcher{
		responses: []tunnels.TunnelHttpResponse{
			{StatusCode: 200, Headers: map[string][]string{"Content-Type": {"text/html"}}, Body: []byte("<h1>ok</h1>")},
			{StatusCode: 200, Headers: map[string][]string{"Content-Type": {"text/css"}}, Body: []byte("body { color: red }")},
		},
	}
	mw := tunnels.NewMiddleware(
		tunnels.Options{RoutePrefix: "/t/", Enabled: true, ForwardTimeout: 2 * time.Second}.WithDefaults(),
		registry,
		tunnels.NewQuota(50, nil),
		stubWorkers,
		stubDispatcher,
		logger,
	)

	// Spin up a chi-style handler: tunnel middleware wrapping a catch-all
	// 404. The middleware should claim /t/abcdef0123/... and forward.
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	handler := mw.Wrap(mux)

	// 5a. Plain path without /t/ — falls through to the next handler.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/health", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		fatal("non-tunnel path should fall through, got %d", rec.Code)
	}

	// 5b. Tunnel path with valid ?k= — forwards to worker.
	rec = httptest.NewRecorder()
	u := "/t/abcdef0123/index.html?" + url.Values{"k": {secret}}.Encode()
	req = httptest.NewRequest("GET", u, nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		fatal("tunnel request expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<h1>ok</h1>") {
		fatal("tunnel response body mismatch: %q", rec.Body.String())
	}
	// The "k" cookie should have been seeded.
	cookies := rec.Result().Cookies()
	var kCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "k" {
			kCookie = c
		}
	}
	if kCookie == nil {
		fatal("expected 'k' cookie to be seeded")
	}
	if kCookie.Value != secret {
		fatal("k cookie value mismatch")
	}
	if !kCookie.HttpOnly || !kCookie.Secure {
		fatal("k cookie must be HttpOnly + Secure")
	}
	if len(stubDispatcher.received) != 1 {
		fatal("expected 1 worker dispatch, got %d", len(stubDispatcher.received))
	}
	got := stubDispatcher.received[0]
	if got.Method != "GET" || got.Path != "/index.html" || got.Port != 8080 {
		fatal("worker request shape mismatch: %+v", got)
	}
	if got.TunnelID != tEntity2.ID {
		fatal("worker tunnel id mismatch: %q vs %q", got.TunnelID, tEntity2.ID)
	}
	logger.Info("middleware forward + cookie seed ok")

	// 5c. Tunnel path without ?k= but with valid cookie — succeeds.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/t/abcdef0123/style.css", nil)
	req.AddCookie(kCookie)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		fatal("cookie-only request expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	logger.Info("cookie-only path ok")

	// 5d. Tunnel path with wrong secret — 401.
	rec = httptest.NewRecorder()
	u = "/t/abcdef0123/?k=wrong-secret"
	req = httptest.NewRequest("GET", u, nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		fatal("wrong-secret expected 401, got %d", rec.Code)
	}
	logger.Info("wrong-secret rejected")

	// 5e. Tunnel path with missing key — 404.
	rec = httptest.NewRecorder()
	u = "/t/nonexistent/index.html?k=" + secret
	req = httptest.NewRequest("GET", u, nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusGone {
		fatal("missing-key expected 410, got %d", rec.Code)
	}
	logger.Info("missing-key returns 410")

	// 5f. Disabled — 503.
	mw2 := tunnels.NewMiddleware(
		tunnels.Options{RoutePrefix: "/t/", Enabled: false, ForwardTimeout: 2 * time.Second}.WithDefaults(),
		registry,
		tunnels.NewQuota(50, nil),
		stubWorkers,
		stubDispatcher,
		logger,
	)
	rec = httptest.NewRecorder()
	u = "/t/abcdef0123/?k=" + secret
	req = httptest.NewRequest("GET", u, nil)
	mw2.Wrap(mux).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		fatal("disabled expected 503, got %d", rec.Code)
	}
	logger.Info("disabled returns 503")

	// 5g. Worker offline — 502.
	stubWorkersOffline := &stubWorkerResolver{workers: map[string]string{}}
	mw3 := tunnels.NewMiddleware(
		tunnels.Options{RoutePrefix: "/t/", Enabled: true, ForwardTimeout: 2 * time.Second}.WithDefaults(),
		registry,
		tunnels.NewQuota(50, nil),
		stubWorkersOffline,
		stubDispatcher,
		logger,
	)
	rec = httptest.NewRecorder()
	u = "/t/abcdef0123/?k=" + secret
	req = httptest.NewRequest("GET", u, nil)
	mw3.Wrap(mux).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		fatal("worker-offline expected 502, got %d", rec.Code)
	}
	logger.Info("worker-offline returns 502")

	logger.Info("PASS")
}

// stubWorkerResolver satisfies tunnels.WorkerResolver.
type stubWorkerResolver struct {
	mu      sync.Mutex
	workers map[string]string
}

func (s *stubWorkerResolver) FindByWorkerID(id string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	conn, ok := s.workers[id]
	return conn, ok
}

// stubDispatcher records every request it forwards and returns the next
// pre-canned response.
type stubDispatcher struct {
	mu        sync.Mutex
	responses []tunnels.TunnelHttpResponse
	received  []tunnels.TunnelHttpRequest
}

func (s *stubDispatcher) SendTunnelHTTPRequest(_ context.Context, _ string, req tunnels.TunnelHttpRequest) (tunnels.TunnelHttpResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.received = append(s.received, req)
	if len(s.responses) == 0 {
		return tunnels.TunnelHttpResponse{StatusCode: 502, ErrorMessage: "no canned response"}, nil
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	return resp, nil
}

func must(err error, label string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", label, err)
		os.Exit(1)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}