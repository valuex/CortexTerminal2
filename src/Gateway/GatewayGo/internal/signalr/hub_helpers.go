package signalr

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// bindFirstArg unmarshals the first element of args into out. Tries msgpack
// first (the C# client uses MessagePack), falls back to JSON for the JSON
// codec. The Connection's read loop has already deserialised the message
// envelope, so each element of args is one decoded positional parameter.
func bindFirstArg(args []any, out any) error {
	if len(args) == 0 {
		return fmt.Errorf("hub method requires at least one argument")
	}
	switch v := args[0].(type) {
	case []byte:
		// msgpack/v5 sometimes leaves the raw bytes behind when the
		// destination is an interface{}. Try msgpack first, then JSON.
		if err := msgpack.Unmarshal(v, out); err != nil {
			if jerr := json.Unmarshal(v, out); jerr != nil {
				return fmt.Errorf("bind argument: msgpack=%v json=%v", err, jerr)
			}
		}
		return nil
	default:
		// Re-marshal via msgpack so we round-trip through a known encoder.
		// This handles map[string]any, []any, and primitive decoded shapes.
		buf, err := msgpack.Marshal(v)
		if err != nil {
			return fmt.Errorf("re-marshal argument: %w", err)
		}
		if err := msgpack.Unmarshal(buf, out); err != nil {
			return fmt.Errorf("unmarshal argument: %w", err)
		}
		return nil
	}
}

// bindFirstStringArg extracts a string from args[0]. C# sometimes sends
// the bare string (SignalR positional primitive), other times sends it
// msgpack-encoded as a fixstr.
func bindFirstStringArg(args []any) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("hub method requires at least one argument")
	}
	switch v := args[0].(type) {
	case string:
		return v, nil
	case []byte:
		// msgpack fixstr → raw bytes that are also valid UTF-8 JSON string.
		var s string
		if err := msgpack.Unmarshal(v, &s); err == nil {
			return s, nil
		}
		return string(v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// nowUnixNano returns the current UTC time as a Unix nanosecond count. The
// C# Gateway passes DateTimeOffset.UtcNow through the subsystem; the Go
// equivalent is `time.Now().UTC().UnixNano()`.
func nowUnixNano() int64 {
	return time.Now().UTC().UnixNano()
}