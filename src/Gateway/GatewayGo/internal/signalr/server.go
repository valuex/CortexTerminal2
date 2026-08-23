package signalr

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/auth"
)

// chi is unused here; the chiWrapper interface keeps Server reusable from
// non-chi callers (e.g. tests using net/http directly).
var _ = chiWrapper(nil)

// Server hosts a set of hubs and exposes them at /hubs/<name>. One Server is
// intended to be created at startup with all hubs known up-front; adding a
// hub after Listen is not supported (C# doesn't support it either, since
// MapHub runs at startup).
type Server struct {
	upgrader websocket.Upgrader
	hubs     map[string]Hub
	opts     ConnectionOptions
	logger   *slog.Logger
}

// NewServer creates a Server. The supplied hubs are looked up by Name() at
// request time. JWT verification is intentionally not done here — it lives in
// a separate middleware that the chi router calls before upgrade so that we
// can reuse the same auth path across SignalR, /ws/terminal, and REST.
func NewServer(hubs []Hub, opts ConnectionOptions) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	m := make(map[string]Hub, len(hubs))
	for _, h := range hubs {
		m[h.Name()] = h
	}
	return &Server{
		upgrader: websocket.Upgrader{
			// The Worker connects from a desktop. Sub-Origin is "null" when
			// invoked from the .NET HubConnection. Browser Console sends
			// the real origin. Both must be accepted.
			CheckOrigin:     func(r *http.Request) bool { return true },
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			Subprotocols:    []string{"json", "messagepack"},
		},
		hubs:   m,
		opts:   opts,
		logger: opts.Logger,
	}
}

// NegotiateResponse is the JSON returned by GET /hubs/<name>/negotiate. The
// .NET SignalR client strictly validates each field, so the shape and casing
// must match.
type NegotiateResponse struct {
	ConnectionToken     string      `json:"connectionToken"`
	ConnectionID        string      `json:"connectionId"`
	AvailableTransports []Transport `json:"availableTransports"`
}

// Transport mirrors the .NET NegotiateResponse.AvailableTransports shape.
type Transport struct {
	Transport       string   `json:"transport"`
	TransferFormats []string `json:"transferFormats"`
}

// Negotiate handles GET /hubs/<name>/negotiate. We return WebSockets only,
// matching the C# Gateway's pipeline.AddSignalR(c => c.MapHub(...)) which does
// not register SSE or Long Polling.
func (s *Server) Negotiate(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Hub existence is implicit — unknown hub returns 404 from the
		// router. We accept the JWT from ?access_token= here because the
		// .NET client appends it to the negotiate URL.
		connID := uuid.New().String()
		token := uuid.New().String()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(NegotiateResponse{
			ConnectionToken: token,
			ConnectionID:    connID,
			AvailableTransports: []Transport{
				{Transport: "WebSockets", TransferFormats: []string{"Text", "Binary"}},
			},
		})
	}
}

// HandleWebSocket upgrades the request and runs the read/write loops against
// the named hub. Called from the chi router as
// r.Get("/hubs/{name}", srv.HandleWebSocket).
func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		// Fall back to parsing from URL — chi only fills PathValue for
		// `{name}` patterns, and we register static paths.
		const prefix = "/hubs/"
		if len(r.URL.Path) > len(prefix) {
			rest := r.URL.Path[len(prefix):]
			for i := 0; i < len(rest); i++ {
				if rest[i] == '/' {
					name = rest[:i]
					break
				}
			}
			if name == "" {
				name = rest
			}
		}
	}
	hub, ok := s.hubs[name]
	if !ok {
		http.NotFound(w, r)
		return
	}

	// Choose codec from the Sec-WebSocket-Protocol header. The .NET client
	// sends "messagepack" or "json" depending on the configured HubProtocol.
	codecName := "json"
	if sp := r.Header.Get("Sec-WebSocket-Protocol"); sp == "messagepack" {
		codecName = "messagepack"
	}
	r.Header.Set("X-SignalR-Codec", codecName)

	ws, err := s.upgrader.Upgrade(w, r, http.Header{
		// Echo the requested sub-protocol back — gorilla requires us to
		// set the header on the upgrade response or the handshake is rejected.
		"Sec-WebSocket-Protocol": []string{codecName},
	})
	if err != nil {
		s.logger.Debug("upgrade failed", "err", err)
		return
	}

	conn := newConnection(hub, r)
	go writePumpLoop(ws, conn, s.opts)
	runConnection(r.Context(), ws, conn, s.opts)
}

// newConnection allocates a Connection. The read loop is run synchronously by
// runConnection; the write loop runs in its own goroutine so a slow client
// can't block reads.
func newConnection(hub Hub, r *http.Request) *Connection {
	return &Connection{
		ID:        uuid.New().String(),
		Hub:       hub,
		writePump: make(chan []byte, 32),
		Request:   r,
		UserID:    userIDFromRequest(r),
	}
}

// userIDFromRequest extracts the user id the auth middleware stashed on the
// request context. Empty string means the connection was opened without a
// valid JWT — hubs should reject any state-changing method in that case.
func userIDFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	return auth.UserIDFromContext(r.Context())
}

// RegisterOn wires each hub's negotiate + WebSocket route onto a chi router.
// We register directly on chi (rather than via Mount) because chi's Mount
// wildcard (`/hubs/*`) interacts awkwardly with stdlib mux path matching and
// produced a 404 on the bare `/hubs/<name>` upgrade URL.
func (s *Server) RegisterOn(r chiWrapper) {
	for name := range s.hubs {
		r.Get("/hubs/"+name+"/negotiate", s.Negotiate(name))
		r.Get("/hubs/"+name, s.HandleWebSocket)
	}
}

// chiWrapper is the minimal chi.Router surface Server needs. Defining a local
// interface avoids importing chi in this file just for the type signature.
type chiWrapper interface {
	Get(pattern string, handler http.HandlerFunc)
}

// ConnectionCount returns the number of live connections across all hubs.
// Used by /api/admin/stats and the version-info endpoint. Returns 0 in the
// JSON-only slice; the Connection registry will be added once MessagePack
// lands.
func (s *Server) ConnectionCount() int { return 0 }

// Shutdown is a no-op for the JSON-only slice; the Connection registry will
// be added once MessagePack lands.
func (s *Server) Shutdown(timeout time.Duration) error {
	_ = timeout
	return nil
}