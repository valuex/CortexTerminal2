package signalr

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// jsonCodec implements the JSON Hub Protocol. Frames are JSON objects
// separated by the Record-Separator byte (0x1e). This matches what the
// .NET SignalR client produces/consumes on the wire.
type jsonCodec struct{}

func newJSONCodec() *jsonCodec { return &jsonCodec{} }

// WireCodec compliance — JSON variant uses 0x1e record separators.

func (jsonCodec) EncodeHandshake(resp HandshakeResponse) ([]byte, error) {
	buf, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshal handshake: %w", err)
	}
	buf = append(buf, RecordSeparator)
	return buf, nil
}

func (jsonCodec) DecodeHandshake(frame []byte) (HandshakeRequest, error) {
	var req HandshakeRequest
	if err := json.Unmarshal(frame, &req); err != nil {
		return req, fmt.Errorf("parse handshake: %w", err)
	}
	if req.Protocol != "json" {
		return req, fmt.Errorf("unsupported protocol %q in handshake", req.Protocol)
	}
	if req.Version != 1 {
		return req, fmt.Errorf("unsupported version %d in handshake", req.Version)
	}
	return req, nil
}

func (jsonCodec) EncodeMessage(msg any) ([]byte, error) {
	buf, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal signalr message: %w", err)
	}
	buf = append(buf, RecordSeparator)
	return buf, nil
}

func (jsonCodec) DecodeMessage(raw []byte, out any) error {
	return json.Unmarshal(bytes.TrimRight(raw, "\x1e"), out)
}

// SplitFrames walks a buffer that may contain multiple 0x1e-terminated JSON
// frames and returns each one. Used when the read deadline expires and we
// want to extract whatever the client managed to send.
func (jsonCodec) SplitFrames(buf []byte) [][]byte {
	var out [][]byte
	for {
		idx := bytes.IndexByte(buf, RecordSeparator)
		if idx < 0 {
			return out
		}
		out = append(out, buf[:idx])
		buf = buf[idx+1:]
		if len(buf) == 0 {
			return out
		}
	}
}