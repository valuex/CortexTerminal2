package signalr

import (
	"context"
	"log/slog"
)

// EchoHub is the verification hub used by the thin-slice phase. Its sole
// job is to confirm the JSON codec round-trips intact: every invocation it
// receives is mirrored back to the caller with the target name prefixed
// `echoed:`. This proves that the handshake, ping, and invocation dispatch
// produce identical wire bytes to what the C# client emits.
//
// This hub is also wired into /hubs/terminal in the router for the duration
// of the thin-slice; once the real TerminalHub lands, the echo hub is
// deleted.
type EchoHub struct {
	logger *slog.Logger
}

// NewEchoHub returns a Hub that logs every invocation. The slog default is
// fine for the thin-slice — production will wire the structured logger from
// main.
func NewEchoHub() *EchoHub {
	return &EchoHub{logger: slog.Default()}
}

func (*EchoHub) Name() string { return "terminal" }

func (*EchoHub) OnConnected(_ context.Context, _ *Connection) {}

func (*EchoHub) OnDisconnected(_ context.Context, _ *Connection, _ error) {}

func (*EchoHub) OnPing(_ context.Context, _ *Connection) {}

// OnInvocation echoes arguments back unchanged. The client invokes
// `Echo("hello")` and expects `["hello"]` in the Completion result. This
// is the minimum round-trip needed to prove the JSON codec is byte-stable.
func (*EchoHub) OnInvocation(_ context.Context, _ *Connection, inv Invocation) (any, error) {
	return inv.Arguments, nil
}