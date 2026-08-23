package tunnels

// TunnelHttpRequest mirrors CortexTerminal.Contracts.Streaming.TunnelHttpRequest.
// The wire uses MessagePack Key(0..6); the Go wire frame must round-trip
// through the existing C# Worker daemon, so the field tags must stay
// byte-stable.
type TunnelHttpRequest struct {
	TunnelID string              `json:"tunnelId" msgpack:"tunnelId"`
	Port     int                 `json:"port"     msgpack:"port"`
	Method   string              `json:"method"   msgpack:"method"`
	Path     string              `json:"path"     msgpack:"path"`
	Query    string              `json:"query"    msgpack:"query"`
	Headers  map[string][]string `json:"headers"  msgpack:"headers"`
	Body     []byte              `json:"body"     msgpack:"body"`
}

// TunnelHttpResponse mirrors CortexTerminal.Contracts.Streaming.TunnelHttpResponse.
// ErrorMessage non-nil means upstream error — middleware returns 502.
type TunnelHttpResponse struct {
	StatusCode   int                 `json:"statusCode"   msgpack:"statusCode"`
	Headers      map[string][]string `json:"headers"      msgpack:"headers"`
	Body         []byte              `json:"body"         msgpack:"body"`
	ErrorMessage string              `json:"errorMessage,omitempty" msgpack:"errorMessage,omitempty"`
}

// ProbePortRequest mirrors CortexTerminal.Contracts.Streaming.ProbePortRequest.
type ProbePortRequest struct {
	Port int `json:"port" msgpack:"port"`
}

// ProbePortResponse mirrors CortexTerminal.Contracts.Streaming.ProbePortResponse.
type ProbePortResponse struct {
	Open         bool   `json:"open"         msgpack:"open"`
	ErrorMessage string `json:"errorMessage,omitempty" msgpack:"errorMessage,omitempty"`
}