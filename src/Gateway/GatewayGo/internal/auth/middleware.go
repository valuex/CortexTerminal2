package auth

import (
	"context"
	"net/http"
	"strings"
)

type contextKey string

const (
	ctxUserID    contextKey = "user_id"
	ctxUsername  contextKey = "username"
	ctxRole      contextKey = "role"
)

// UserIDFromContext returns the user id stashed by Middleware. Empty string
// if the request was not authenticated.
func UserIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxUserID).(string)
	return v
}

// UsernameFromContext returns the username stashed by Middleware.
func UsernameFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxUsername).(string)
	return v
}

// RoleFromContext returns the role stashed by Middleware.
func RoleFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxRole).(string)
	return v
}

// Middleware verifies a bearer JWT on the request and stashes user_id,
// username, and role into the request context. Tokens may be supplied via
// the Authorization header (Authorization: Bearer <token>) or via the
// query string (?access_token= or ?token=) — the latter is required for
// SignalR / WebSocket endpoints that can't set custom headers.
//
// Paths under /hubs/terminal, /hubs/worker, and /ws/terminal accept the
// query-string variant without requiring the Authorization header (mirrors
// C# JwtBearerEvents.OnMessageReceived lines 226-244). All other paths
// require Authorization.
func Middleware(signingKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := extractToken(r)
			if raw == "" {
				// No token at all → unauthenticated.
				next.ServeHTTP(w, r)
				return
			}
			claims, err := ParseToken(signingKey, raw)
			if err != nil {
				http.Error(w, `{"error":"invalid_token"}`, http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), ctxUserID, claims.UserID())
			ctx = context.WithValue(ctx, ctxUsername, claims.Username)
			ctx = context.WithValue(ctx, ctxRole, claims.Role)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// extractToken picks the token off the request, preferring Authorization
// header (Bearer scheme) and falling back to ?access_token= or ?token=.
func extractToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		const prefix = "Bearer "
		if strings.HasPrefix(h, prefix) {
			return strings.TrimSpace(h[len(prefix):])
		}
	}
	if q := r.URL.Query().Get("access_token"); q != "" {
		return q
	}
	if q := r.URL.Query().Get("token"); q != "" {
		return q
	}
	return ""
}

// RequireAuth wraps a handler so it 401s when no authenticated user is in
// the context. Use this around handlers that must have a JWT — the bare
// Middleware without RequireAuth is a no-op for unauthenticated calls
// (matches the C# UseAuthentication() without RequireAuthorization()).
func RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if UserIDFromContext(r.Context()) == "" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}