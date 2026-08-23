// Package sessions is the Go port of CortexTerminal.Gateway.Sessions — the
// session lifecycle coordinator plus its supporting types. Mirrors the C#
// state machine in DbSessionCoordinator.cs and SessionLaunchCoordinator.cs.
//
// State diagram (matches C#):
//
//	   Attached ──(client detaches)──► DetachedGracePeriod
//	   Attached ──(worker disconnects)──► Recovering
//	   Recovering ──(worker reconnects + Rebind)──► Attached
//	   DetachedGracePeriod ──(client reattaches)──► Attached
//	   Recovering ──(worker reports session not in live set)──► Expired
//	   Attached/DetachedGracePeriod ──(session exits)──► Exited
//
// Expired and Exited are terminal states — no transitions out.
package sessions

// SessionAttachmentState matches CortexTerminal.Gateway.Sessions.SessionAttachmentState.
// The C# enum integer values must not change — they appear on the wire in
// some MessagePack contracts.
type SessionAttachmentState int

const (
	StateAttached           SessionAttachmentState = 0
	StateDetachedGracePeriod SessionAttachmentState = 1
	StateExpired            SessionAttachmentState = 2
	StateExited             SessionAttachmentState = 3
	StateRecovering         SessionAttachmentState = 4
)

// String mirrors the C# Enum.ToString() — the wire format is the integer
// value, but logs and SQLite attachment_state column use the name.
func (s SessionAttachmentState) String() string {
	switch s {
	case StateAttached:
		return "Attached"
	case StateDetachedGracePeriod:
		return "DetachedGracePeriod"
	case StateExpired:
		return "Expired"
	case StateExited:
		return "Exited"
	case StateRecovering:
		return "Recovering"
	default:
		return "Unknown"
	}
}

// ParseAttachmentState is the inverse of String. Unknown values return
// StateAttached to match the C# fallback in MapEntityToRecord.
func ParseAttachmentState(s string) SessionAttachmentState {
	switch s {
	case "Attached":
		return StateAttached
	case "DetachedGracePeriod":
		return StateDetachedGracePeriod
	case "Expired":
		return StateExpired
	case "Exited":
		return StateExited
	case "Recovering":
		return StateRecovering
	default:
		return StateAttached
	}
}

// SessionRecord mirrors CortexTerminal.Gateway.Sessions.SessionRecord.
// The Go version drops the `with` operator — `Update` builds a new value.
type SessionRecord struct {
	SessionID                  string
	UserID                     string
	WorkerID                   string
	WorkerConnectionID         string
	Columns                    int
	Rows                       int
	CreatedAtUTC               int64 // UnixNano
	LastActivityAtUTC          int64 // UnixNano
	AttachmentState            SessionAttachmentState
	AttachedClientConnectionID string
	ExitCode                   *int
	ExitReason                 string
	ReplayPending              bool
	Name                       string
}

// Update returns a copy with the supplied mutator applied. Mirrors the C#
// `record with { ... }` pattern.
func (s SessionRecord) Update(mutate func(*SessionRecord)) SessionRecord {
	mutate(&s)
	return s
}

// CreateSessionResult mirrors the C# CreateSessionResult. The C# version
// uses an `IsSuccess` boolean plus a nullable `Response`. We keep the
// same shape with a single struct — the C# client reads `success`,
// `sessionId`, and `error` fields off the wire.
type CreateSessionResult struct {
	Success   bool   `json:"success" msgpack:"success"`
	SessionID string `json:"sessionId,omitempty" msgpack:"sessionId,omitempty"`
	WorkerID  string `json:"workerId,omitempty" msgpack:"workerId,omitempty"`
	Error     string `json:"error,omitempty" msgpack:"error,omitempty"`
}

// ReattachSessionResult mirrors CortexTerminal.Contracts.Sessions.ReattachSessionResult.
type ReattachSessionResult struct {
	Success bool   `json:"success" msgpack:"success"`
	Error   string `json:"error,omitempty" msgpack:"error,omitempty"`
}

// DeleteSessionResult mirrors the C# result struct.
type DeleteSessionResult struct {
	Success bool   `json:"success" msgpack:"success"`
	Error   string `json:"error,omitempty" msgpack:"error,omitempty"`
}

// RenameSessionResult mirrors the C# result struct.
type RenameSessionResult struct {
	Success bool   `json:"success" msgpack:"success"`
	Error   string `json:"error,omitempty" msgpack:"error,omitempty"`
}

// ScrollbackSettings mirrors CortexTerminal.Gateway.Sessions.ScrollbackSettings.
// Defaults match the C# defaults (5 MB max, 16 KB min, 5 MB hard max).
type ScrollbackSettings struct {
	MaxMegabytes    int
	MaxBytesOverride *int
	MinAllowedBytes int
	MaxAllowedBytes int
}

func (s ScrollbackSettings) MaxBytes() int {
	if s.MaxBytesOverride != nil {
		return *s.MaxBytesOverride
	}
	return s.MaxMegabytes * 1024 * 1024
}

func DefaultScrollbackSettings() ScrollbackSettings {
	return ScrollbackSettings{
		MaxMegabytes:    5,
		MinAllowedBytes: 16 * 1024,
		MaxAllowedBytes: 5 * 1024 * 1024,
	}
}

// Clamp returns the requested scrollback bytes clamped to the [min,max]
// range, falling back to the settings' default when nil. Mirrors the C#
// `Math.Clamp(prefBytes ?? scrollbackSettings.MaxBytes, …)` logic.
func (s ScrollbackSettings) Clamp(preferred *int) int {
	v := s.MaxBytes()
	if preferred != nil {
		v = *preferred
	}
	if v < s.MinAllowedBytes {
		v = s.MinAllowedBytes
	}
	if v > s.MaxAllowedBytes {
		v = s.MaxAllowedBytes
	}
	return v
}

// ErrSessionNotFound is the canonical "session does not exist for this
// user" error. TerminalHub translates it to a HubException in C#; here we
// surface it directly and let the hub layer decide how to render.
type ErrSessionNotFound struct{ SessionID string }

func (e *ErrSessionNotFound) Error() string {
	return "session not found: " + e.SessionID
}