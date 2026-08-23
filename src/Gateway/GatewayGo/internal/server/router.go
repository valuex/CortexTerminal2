// Package server wires the chi router with the same middleware order as the
// C# Gateway: ForwardedHeaders → TunnelMiddleware → migrate/seed → static
// files → authn → authz → WebSockets upgrade → controllers/hubs.
package server

import (
	"database/sql"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/auth"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/signalr"
)

// New builds the chi router with every route registered. Handlers are
// intentionally stubs (501 Not Implemented) until each subsystem is wired up
// in subsequent porting slices. See
// C:\Users\wei_x\.claude\plans\purrfect-dazzling-tower.md for the route map.
//
// hubServer is the SignalR server. Pass nil to disable hubs (e.g. for tests).
// signingKey is the HMAC key used by the JWT middleware. db may be nil for
// routes that don't need it (e.g. SignalR upgrade + REST stubs).
func New(hubServer *signalr.Server, signingKey string, db *sql.DB) http.Handler {
	r := chi.NewRouter()

	// JWT middleware runs on all paths. It accepts the token from
	// Authorization header OR ?access_token= / ?token= (for SignalR/WS).
	// Routes that require auth wrap themselves in auth.RequireAuth.
	r.Use(auth.Middleware(signingKey))

	// --- Auth + identity ---
	r.Post("/api/auth/device-flow", notImplemented)
	r.Post("/api/auth/device-flow/token", notImplemented)
	r.Post("/api/auth/device-flow/verify", authRequired(notImplemented))
	r.Post("/api/auth/refresh", authRequired(notImplemented))
	if db != nil {
		r.Get("/api/me/profile", authRequired(handleMeProfile(db)))
	} else {
		r.Get("/api/me/profile", authRequired(notImplemented))
	}
	r.Post("/api/me/profile", authRequired(notImplemented))
	r.Get("/api/me/preferences", authRequired(notImplemented))
	r.Put("/api/me/preferences", authRequired(notImplemented))
	r.Get("/api/me/identities", authRequired(notImplemented))
	r.Delete("/api/me/identities/{identityId}", authRequired(notImplemented))
	r.Post("/api/me/identities/phone", authRequired(notImplemented))
	r.Post("/api/me/identities/phone/send-code", authRequired(notImplemented))
	r.Get("/api/me/identities/{provider}/link", authRequired(notImplemented))
	r.Put("/api/me/password", authRequired(notImplemented))
	r.Post("/api/me/avatar", authRequired(notImplemented))
	r.Get("/api/users/{id}/avatar", notImplemented) // anon, binary
	r.Get("/api/auth/github", notImplemented)
	r.Get("/api/auth/callback/github", notImplemented)
	r.Get("/api/auth/google", notImplemented)
	r.Get("/api/auth/callback/google", notImplemented)
	if db != nil {
		r.Post("/api/auth/password/login", handlePasswordLogin(db, signingKey))
	} else {
		r.Post("/api/auth/password/login", notImplemented)
	}
	r.Post("/api/auth/password/register", authRequired(notImplemented))
	r.Get("/api/auth/methods", notImplemented)
	r.Post("/api/auth/phone/send-code", notImplemented)
	r.Post("/api/auth/phone/verify", notImplemented)
	r.Post("/api/auth/huawei/quick-login", notImplemented)
	r.Get("/api/auth/apple", notImplemented)
	r.Post("/api/auth/callback/apple", notImplemented) // POST, not GET
	r.Get("/api/auth/captcha/challenge", notImplemented)
	r.Post("/api/auth/captcha/verify", notImplemented)

	// --- Sessions / workers / tunnels ---
	r.Post("/api/sessions", authRequired(notImplemented))
	r.Get("/api/me/sessions", authRequired(notImplemented))
	r.Get("/api/me/stats", authRequired(notImplemented))
	r.Get("/api/me/sessions/{sessionId}", authRequired(notImplemented))
	r.Post("/api/me/sessions/{sessionId}/terminate", authRequired(notImplemented))
	r.Post("/api/me/sessions/{sessionId}/tunnels", authRequired(notImplemented))
	r.Get("/api/me/sessions/{sessionId}/tunnels", authRequired(notImplemented))
	r.Delete("/api/me/tunnels/{tunnelId}", authRequired(notImplemented))
	r.Delete("/api/me/sessions/{sessionId}", authRequired(notImplemented))
	r.Put("/api/me/sessions/{sessionId}", authRequired(notImplemented))
	r.Patch("/api/me/sessions/{sessionId}", authRequired(notImplemented))
	r.Get("/api/me/workers", authRequired(notImplemented))
	r.Get("/api/me/workers/{workerId}", authRequired(notImplemented))
	r.Post("/api/me/workers/{workerId}/upgrade", authRequired(notImplemented))

	// --- Admin ---
	r.Get("/api/admin/stats", authRequired(notImplemented))
	r.Get("/api/admin/user-activity", authRequired(notImplemented))
	r.Get("/api/admin/audit-stats", authRequired(notImplemented))
	r.Get("/api/admin/sessions", authRequired(notImplemented))
	r.Get("/api/admin/workers", authRequired(notImplemented))

	// --- User management ---
	r.Get("/api/users", authRequired(notImplemented))
	r.Post("/api/users/invite", authRequired(notImplemented))
	r.Patch("/api/users/{userId}", authRequired(notImplemented))
	r.Delete("/api/users/{userId}", authRequired(notImplemented))
	r.Delete("/api/me/account", authRequired(notImplemented))

	// --- Artifacts / agent activity ---
	r.Get("/api/sessions/{sessionId}/artifacts", authRequired(notImplemented))
	r.Post("/api/sessions/{sessionId}/artifacts", authRequired(notImplemented))
	r.Post("/api/sessions/{sessionId}/artifacts/{artifactId}/complete", authRequired(notImplemented))
	r.Get("/api/sessions/{sessionId}/artifacts/{artifactId}/download", authRequired(notImplemented))
	r.Delete("/api/sessions/{sessionId}/artifacts/{artifactId}", authRequired(notImplemented))
	r.Get("/api/sessions/{sessionId}/agent-events", authRequired(notImplemented))

	// --- Misc ---
	r.Get("/api/gateway/info", authRequired(notImplemented))
	r.Get("/api/support/info", notImplemented)
	r.Post("/api/me/feedback/uploads", authRequired(notImplemented))
	r.Get("/api/feedback/files/{objectName:.*}", notImplemented)
	// /api/tts/synthesize is registered only when Cloudflare or Aliyun is
	// configured (matches C# TtsEndpoints.Map guard at line 15).
	r.Post("/api/tts/synthesize", authRequired(notImplemented))
	// /api/audit-log lives on AuditLogController (matches Controllers/AuditLogController.cs).
	r.Get("/api/audit-log", authRequired(notImplemented))

	// --- Hubs ---
	if hubServer != nil {
		hubServer.RegisterOn(r)
	} else {
		// /hubs/terminal and /hubs/worker are SignalR endpoints. The /negotiate
		// sub-path serves a JSON {connectionToken, connectionId, availableTransports}.
		r.Get("/hubs/terminal/negotiate", notImplemented)
		r.Get("/hubs/worker/negotiate", notImplemented)
		// The WebSocket upgrade itself is handled by the SignalR package.
	}

	// --- WebSocket (native) ---
	// /ws/terminal?sessionId=...&access_token=... — upgraded by
	// TerminalWebSocketMiddleware (matches C# WebSockets/TerminalWebSocketMiddleware.cs).
	r.HandleFunc("/ws/terminal", notImplemented)

	// --- Static SPA fallback (last) ---
	r.HandleFunc("/*", notImplemented)

	return r
}

func notImplemented(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_, _ = w.Write([]byte(`{"error":"not implemented","path":"` + r.URL.Path + `"}`))
}

// authRequired is a placeholder middleware that, once JWT is implemented,
// will extract the bearer token, verify it, and stash the userId in context.
// For the thin-slice scaffold it just passes through (the handler is still
// 501, so this is invisible).
// authRequired aliases auth.RequireAuth for readability inside this file.
// The actual JWT parsing already happened in the global middleware; this
// only enforces that the request context has an authenticated user.
func authRequired(next http.HandlerFunc) http.HandlerFunc {
	return auth.RequireAuth(next)
}
