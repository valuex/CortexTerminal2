package signalr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// TerminalHub is the Console-side hub. Method names match the C#
// TerminalHub verbatim because the .NET Console client calls them by name
// on the wire:
//
//	CreateSession, DetachSession, ReattachSession,
//	WriteInput, ProbeLatency, ResizeSession, CloseSession
//
// Each method body delegates to a subsystem interface. Those interfaces
// are intentionally narrow so we can wire stubs (returning
// ErrNotImplemented) until the underlying subsystems land in their own
// porting slices — see the plan file at
// C:\Users\wei_x\.claude\plans\purrfect-dazzling-tower.md.
type TerminalHub struct {
	logger *slog.Logger

	// SessionLaunch / coordinator / replay / agent activity hooks.
	// Nil means the subsystem hasn't been ported yet; the matching method
	// returns ErrNotImplemented.
	LaunchCoordinator SessionLaunchCoordinator
	Sessions          SessionCoordinator
	Replay            ReplayCoordinator
	AgentActivity     AgentActivity
	WorkerCommands    WorkerCommandDispatcher

	// StatsLifecycle receives ClientConnected/ClientDisconnected callbacks.
	StatsLifecycle StatsLifecycle
}

// SessionLaunchCoordinator, SessionCoordinator, ReplayCoordinator,
// AgentActivity, WorkerCommandDispatcher, StatsLifecycle are the minimum
// interfaces TerminalHub needs from the underlying subsystems. Each one
// mirrors the C# interface so we can swap real implementations in
// without touching hub code.
type SessionLaunchCoordinator interface {
	CreateSession(ctx context.Context, userID string, request CreateSessionRequest, connectionID string) (CreateSessionResult, error)
}

type SessionCoordinator interface {
	DetachSession(ctx context.Context, userID, sessionID string, now timeNow) error
	ReattachSession(ctx context.Context, userID string, request ReattachSessionRequest, connectionID string, now timeNow) (ReattachSessionResult, error)
	TryGetSession(sessionID string) (SessionRecord, bool)
	MarkReplayCompleted(ctx context.Context, sessionID, connectionID string) error
}

type ReplayCoordinator interface {
	BeginReplay(sessionID, connectionID string)
	AbortReplay(sessionID string)
	FlushPending(ctx context.Context, sessionID, connectionID string, send func(any) error) error
}

type AgentActivity interface {
	// reserved for ForwardAgentStarted et al — not on TerminalHub.
}

type WorkerCommandDispatcher interface {
	WriteInput(ctx context.Context, workerConnectionID string, sessionID string, payload []byte) error
	ProbeLatency(ctx context.Context, workerConnectionID string, sessionID string, probe ProbeLatencyFrame) error
	ResizeSession(ctx context.Context, workerConnectionID string, req ResizePtyRequest) error
	CloseSession(ctx context.Context, workerConnectionID string, req CloseSessionRequest) error
	RequestScrollback(ctx context.Context, workerConnectionID, sessionID string) ([]TerminalChunkWire, error)
}

type StatsLifecycle interface {
	ClientConnected()
	ClientDisconnected()
}

// CreateSessionRequest mirrors CortexTerminal.Contracts.Sessions.CreateSessionRequest.
// Field names use msgpack-tagged JSON-friendly names so the msgpack codec
// round-trips them through the Connection's read loop.
type CreateSessionRequest struct {
	WorkerID         string `json:"workerId" msgpack:"workerId"`
	AgentKind        string `json:"agentKind" msgpack:"agentKind"`
	AgentSessionID   string `json:"agentSessionId,omitempty" msgpack:"agentSessionId,omitempty"`
	InitialMessage   string `json:"initialMessage,omitempty" msgpack:"initialMessage,omitempty"`
	WorkingDirectory string `json:"workingDirectory,omitempty" msgpack:"workingDirectory,omitempty"`
	ClientRequestID  string `json:"clientRequestId,omitempty" msgpack:"clientRequestId,omitempty"`
}

// CreateSessionResult mirrors CortexTerminal.Contracts.Sessions.CreateSessionResult.
type CreateSessionResult struct {
	Success   bool   `json:"success" msgpack:"success"`
	SessionID string `json:"sessionId,omitempty" msgpack:"sessionId,omitempty"`
	Error     string `json:"error,omitempty" msgpack:"error,omitempty"`
}

// ReattachSessionRequest mirrors CortexTerminal.Contracts.Sessions.ReattachSessionRequest.
type ReattachSessionRequest struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
}

// ReattachSessionResult mirrors CortexTerminal.Contracts.Sessions.ReattachSessionResult.
type ReattachSessionResult struct {
	Success bool `json:"success" msgpack:"success"`
	Error   string `json:"error,omitempty" msgpack:"error,omitempty"`
}

// ProbeLatencyFrame matches CortexTerminal.Contracts.Streaming.LatencyProbeFrame.
type ProbeLatencyFrame struct {
	SessionID  string `json:"sessionId" msgpack:"sessionId"`
	ProbeID    string `json:"probeId" msgpack:"probeId"`
	ClientTime int64  `json:"clientTime" msgpack:"clientTime"`
}

// ResizePtyRequest matches CortexTerminal.Contracts.Sessions.ResizePtyRequest.
type ResizePtyRequest struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	Columns   int    `json:"columns" msgpack:"columns"`
	Rows      int    `json:"rows" msgpack:"rows"`
}

// CloseSessionRequest matches CortexTerminal.Contracts.Sessions.CloseSessionRequest.
type CloseSessionRequest struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	Reason    string `json:"reason,omitempty" msgpack:"reason,omitempty"`
}

// TerminalChunkWire is the wire-friendly shape the gateway sends to the
// Console client when replaying scrollback. Mirrors
// CortexTerminal.Contracts.Streaming.TerminalChunk.
type TerminalChunkWire struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	Stream    string `json:"stream" msgpack:"stream"` // "stdout" / "stderr"
	Payload   []byte `json:"payload" msgpack:"payload"`
	Timestamp int64  `json:"timestamp" msgpack:"timestamp"`
}

// SessionRecord is the in-memory view TerminalHub looks up via
// SessionCoordinator.TryGetSession. Mirrors
// CortexTerminal.Gateway.Sessions.SessionRecord fields used here.
type SessionRecord struct {
	SessionID                  string
	UserID                     string
	WorkerConnectionID         string
	AttachedClientConnectionID string
}

// timeNow is an alias the hub uses to pass a clock to subsystem calls. The
// real implementation injects a time.Time; this minimal interface keeps the
// hub decoupled from the time package's clock source.
type timeNow = struct{ UnixNano int64 }

// NewTerminalHub builds a TerminalHub. Each subsystem interface may be nil
// — methods that depend on it will return ErrNotImplemented.
func NewTerminalHub(opts ...func(*TerminalHub)) *TerminalHub {
	h := &TerminalHub{logger: slog.Default()}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (*TerminalHub) Name() string { return "terminal" }

func (h *TerminalHub) OnConnected(_ context.Context, _ *Connection) {
	if h.StatsLifecycle != nil {
		h.StatsLifecycle.ClientConnected()
	}
}

func (h *TerminalHub) OnDisconnected(_ context.Context, _ *Connection, _ error) {
	if h.StatsLifecycle != nil {
		h.StatsLifecycle.ClientDisconnected()
	}
}

func (*TerminalHub) OnPing(_ context.Context, _ *Connection) {}

// OnInvocation dispatches by the target method name. The C# client sends
// the method name verbatim, so the dispatch table keys match the C# method
// names 1:1.
func (h *TerminalHub) OnInvocation(ctx context.Context, conn *Connection, inv Invocation) (any, error) {
	if conn.UserID == "" {
		return nil, errors.New("unauthenticated")
	}
	switch inv.Target {
	case "echo":
		// Echo method retained from the thin-slice phase so smoke tests
		// can exercise the message loop without depending on the full
		// SessionLaunch / WorkerCommand subsystems. Returns the first
		// argument unchanged, mirroring the original EchoHub behaviour.
		if len(inv.Arguments) == 0 {
			return nil, nil
		}
		return inv.Arguments, nil
	case "CreateSession":
		var req CreateSessionRequest
		if err := bindFirstArg(inv.Arguments, &req); err != nil {
			return nil, err
		}
		return h.createSession(ctx, conn, req)
	case "DetachSession":
		sessionID, err := bindFirstStringArg(inv.Arguments)
		if err != nil {
			return nil, err
		}
		return nil, h.detachSession(ctx, conn, sessionID)
	case "ReattachSession":
		var req ReattachSessionRequest
		if err := bindFirstArg(inv.Arguments, &req); err != nil {
			return nil, err
		}
		return h.reattachSession(ctx, conn, req)
	case "WriteInput":
		var frame WriteInputFrame
		if err := bindFirstArg(inv.Arguments, &frame); err != nil {
			return nil, err
		}
		return nil, h.writeInput(ctx, conn, frame)
	case "ProbeLatency":
		var frame ProbeLatencyFrame
		if err := bindFirstArg(inv.Arguments, &frame); err != nil {
			return nil, err
		}
		return nil, h.probeLatency(ctx, conn, frame)
	case "ResizeSession":
		var req ResizePtyRequest
		if err := bindFirstArg(inv.Arguments, &req); err != nil {
			return nil, err
		}
		return nil, h.resizeSession(ctx, conn, req)
	case "CloseSession":
		var req CloseSessionRequest
		if err := bindFirstArg(inv.Arguments, &req); err != nil {
			return nil, err
		}
		return nil, h.closeSession(ctx, conn, req)
	default:
		return nil, fmt.Errorf("terminal hub: unknown method %q", inv.Target)
	}
}

// WriteInputFrame matches CortexTerminal.Contracts.Streaming.WriteInputFrame.
type WriteInputFrame struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	Payload   []byte `json:"payload" msgpack:"payload"`
}

// --- Method bodies. Each one returns ErrNotImplemented until the matching
// subsystem is wired up. The dispatch + binding + auth checks are real.

func (h *TerminalHub) createSession(ctx context.Context, conn *Connection, req CreateSessionRequest) (CreateSessionResult, error) {
	if h.LaunchCoordinator == nil {
		return CreateSessionResult{}, ErrNotImplemented
	}
	return h.LaunchCoordinator.CreateSession(ctx, conn.UserID, req, conn.ID)
}

func (h *TerminalHub) detachSession(ctx context.Context, conn *Connection, sessionID string) error {
	if h.Sessions == nil {
		return ErrNotImplemented
	}
	return h.Sessions.DetachSession(ctx, conn.UserID, sessionID, timeNow{UnixNano: nowUnixNano()})
}

func (h *TerminalHub) reattachSession(ctx context.Context, conn *Connection, req ReattachSessionRequest) (ReattachSessionResult, error) {
	if h.Sessions == nil || h.Replay == nil {
		return ReattachSessionResult{}, ErrNotImplemented
	}
	h.Replay.BeginReplay(req.SessionID, conn.ID)
	result, err := h.Sessions.ReattachSession(ctx, conn.UserID, req, conn.ID, timeNow{UnixNano: nowUnixNano()})
	if err != nil || !result.Success {
		h.Replay.AbortReplay(req.SessionID)
		return result, err
	}
	return result, h.Sessions.MarkReplayCompleted(ctx, req.SessionID, conn.ID)
}

func (h *TerminalHub) writeInput(ctx context.Context, conn *Connection, frame WriteInputFrame) error {
	session, err := h.requireOwnedSession(conn, frame.SessionID)
	if err != nil {
		return err
	}
	if h.WorkerCommands == nil {
		return ErrNotImplemented
	}
	return h.WorkerCommands.WriteInput(ctx, session.WorkerConnectionID, frame.SessionID, frame.Payload)
}

func (h *TerminalHub) probeLatency(ctx context.Context, conn *Connection, frame ProbeLatencyFrame) error {
	session, err := h.requireOwnedSession(conn, frame.SessionID)
	if err != nil {
		return err
	}
	if h.WorkerCommands == nil {
		return ErrNotImplemented
	}
	return h.WorkerCommands.ProbeLatency(ctx, session.WorkerConnectionID, frame.SessionID, frame)
}

func (h *TerminalHub) resizeSession(ctx context.Context, conn *Connection, req ResizePtyRequest) error {
	if req.Columns < 1 || req.Rows < 1 || req.Columns > 1000 || req.Rows > 1000 {
		return errors.New("invalid terminal size")
	}
	session, err := h.requireOwnedSession(conn, req.SessionID)
	if err != nil {
		return err
	}
	if h.WorkerCommands == nil {
		return ErrNotImplemented
	}
	return h.WorkerCommands.ResizeSession(ctx, session.WorkerConnectionID, req)
}

func (h *TerminalHub) closeSession(ctx context.Context, conn *Connection, req CloseSessionRequest) error {
	session, err := h.requireOwnedSession(conn, req.SessionID)
	if err != nil {
		return err
	}
	if h.WorkerCommands == nil {
		return ErrNotImplemented
	}
	return h.WorkerCommands.CloseSession(ctx, session.WorkerConnectionID, req)
}

func (h *TerminalHub) requireOwnedSession(conn *Connection, sessionID string) (SessionRecord, error) {
	if h.Sessions == nil {
		return SessionRecord{}, ErrNotImplemented
	}
	session, ok := h.Sessions.TryGetSession(sessionID)
	if !ok {
		return SessionRecord{}, errors.New("unknown session")
	}
	if session.UserID != conn.UserID {
		// Don't leak existence to a different user.
		return SessionRecord{}, errors.New("unknown session")
	}
	if session.AttachedClientConnectionID != conn.ID {
		return SessionRecord{}, errors.New("session is attached to a different client")
	}
	return session, nil
}