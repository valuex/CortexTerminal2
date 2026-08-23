package signalr

import (
	"context"
	"errors"
	"net/http"
	"sync"
)

// Hub is the contract every SignalR hub implements. Each method is a callable
// RPC on the hub — the C# Hub<T> exposes one method per interface method.
//
// Method names match the C# method names verbatim because the Worker/Console
// call them by name on the wire.
type Hub interface {
	// Name returns the hub path segment after `/hubs/`. Matches the .NET
	// generic MapHub<THub>("/hubs/<Name>").
	Name() string

	// OnConnected is called after the handshake completes and the connection
	// is fully registered. The implementation may stash connection-scoped
	// state on Conn.
	OnConnected(ctx context.Context, conn *Connection)

	// OnDisconnected is called when the WebSocket closes or the Close frame
	// arrives. Implementations should release any per-connection resources.
	OnDisconnected(ctx context.Context, conn *Connection, err error)

	// OnInvocation dispatches a client→server Invocation frame to a hub
	// method. Returning a non-nil error results in a Completion with `error`
	// set; nil returns a Completion with `result=null`.
	OnInvocation(ctx context.Context, conn *Connection, inv Invocation) (result any, err error)

	// OnPing is the keepalive tick. We do not respond (Ping is fire-and-forget
	// in the spec); the hook exists so hubs can update last-seen metrics.
	OnPing(ctx context.Context, conn *Connection)
}

// Connection is the per-WebSocket state shared across handler invocations.
// WritePump/Close are safe to call from any goroutine.
type Connection struct {
	ID        string // SignalR ConnectionToken, format: "<GUID>-N"
	UserID    string // resolved from JWT (empty until RequireAuth sets it)
	Hub       Hub
	writePump chan []byte // buffered; closed on disconnect
	closeOnce sync.Once
	Request   *http.Request // initial request — for query-string JWT
}

// ErrNotImplemented is returned by hub methods that are not yet ported.
var ErrNotImplemented = errors.New("signalr: hub method not implemented")