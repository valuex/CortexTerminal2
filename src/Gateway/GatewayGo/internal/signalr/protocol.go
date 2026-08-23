// Package signalr implements the SignalR Hub Protocol server side so that the
// existing C# Worker and Console clients can connect without modification.
//
// The package covers only what the C# Gateway exposes:
//
//   - WebSockets transport only (no SSE / Long Polling).
//   - JSON and MessagePack codecs.
//   - Message types: HandshakeRequest/Response, Ping, Invocation (server→client
//     SendAsync), Completion (client→server Invoke return), Close. Streaming
//     items are intentionally not implemented.
//
// Wire specification:
// https://github.com/dotnet/aspnetcore/blob/main/src/SignalR/docs/specs/HubProtocol.md
package signalr

// MessageType values are exactly the integers in the SignalR wire spec.
type MessageType byte

const (
	MessageTypeInvocation     MessageType = 1
	MessageTypeStreamItem     MessageType = 2
	MessageTypeCompletion     MessageType = 3
	MessageTypeStreamInvocation MessageType = 4
	MessageTypeCancelInvocation MessageType = 5
	MessageTypePing           MessageType = 6
	MessageTypeClose         MessageType = 7
)

// HandshakeRequest is the very first frame a client sends. It selects the
// protocol + version. The C# Worker sends `{"protocol":"json","version":1}`.
// The current .NET SignalR MessagePack library writes both fields.
type HandshakeRequest struct {
	Protocol string `json:"protocol" msgpack:"protocol"`
	Version  int    `json:"version" msgpack:"version"`
}

// HandshakeResponse is empty on success. The C# client treats a non-empty
// `error` field as a fatal handshake failure.
type HandshakeResponse struct {
	Error string `json:"error,omitempty" msgpack:"error,omitempty"`
}

// Invocation is the wire shape for both client→server and server→client
// method calls. Headers are MessagePack-only; JSON carries them as an
// empty object `{}` so the codec stays symmetric.
type Invocation struct {
	Type         MessageType `msgpack:"type"  json:"type"`
	Target       string      `msgpack:"target" json:"target"`
	InvocationID string      `msgpack:"invocationId,omitempty" json:"invocationId,omitempty"`
	Arguments    []any       `msgpack:"arguments,omitempty" json:"arguments,omitempty"`
	Headers      map[string]string `msgpack:"headers,omitempty" json:"headers,omitempty"`
}

// Completion is sent by the client in response to a server-initiated Invoke.
// We do not stream results, so Result + Error are mutually exclusive.
type Completion struct {
	Type         MessageType `msgpack:"type" json:"type"`
	InvocationID string      `msgpack:"invocationId" json:"invocationId"`
	Result       any         `msgpack:"result,omitempty" json:"result,omitempty"`
	Error        string      `msgpack:"error,omitempty" json:"error,omitempty"`
}

// Ping is the keepalive. The server replies with the same shape (the spec
// allows an empty object too — we send `{"type":6}` to match what the C#
// client expects).
type Ping struct {
	Type MessageType `msgpack:"type" json:"type"`
}

// Close terminates the connection. AllowReconnect is a client-side hint and
// the server may ignore it.
type Close struct {
	Type           MessageType `msgpack:"type" json:"type"`
	Error          string      `msgpack:"error,omitempty" json:"error,omitempty"`
	AllowReconnect bool        `msgpack:"allowReconnect,omitempty" json:"allowReconnect,omitempty"`
}

// RecordSeparator is the frame delimiter used by the JSON codec. The MessagePack
// codec uses raw length-prefixed frames instead.
const RecordSeparator = '\x1e'