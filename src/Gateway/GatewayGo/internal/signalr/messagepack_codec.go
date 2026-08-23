package signalr

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

// messagePackCodec implements the MessagePack Hub Protocol framing on top
// of the SignalR connection. Per the spec, frames are length-prefixed:
//
//	[varint length N][N bytes of MessagePack-encoded payload]
//
// The first frame on a connection is a HandshakeRequest containing
// `{"protocol":"messagepack","version":1}`. After the handshake response,
// every frame is a Hub Protocol message body.
//
// Frames are NOT separated by 0x1e (that's JSON-only).
type messagePackCodec struct{}

func newMessagePackCodec() *messagePackCodec { return &messagePackCodec{} }

// EncodeHandshake returns the bytes of a HandshakeRequest for the
// messagepack protocol. The C# client expects `{"protocol":"messagepack","version":1}`.
func (messagePackCodec) EncodeHandshake(resp HandshakeResponse) ([]byte, error) {
	body, err := msgpack.Marshal(resp)
	if err != nil {
		return nil, err
	}
	return wrapFrame(body), nil
}

// DecodeHandshake validates the messagepack-encoded handshake. The body is
// msgpack-encoded `{"protocol":"messagepack","version":1}`.
func (messagePackCodec) DecodeHandshake(body []byte) (HandshakeRequest, error) {
	var req HandshakeRequest
	if err := msgpack.Unmarshal(body, &req); err != nil {
		return req, fmt.Errorf("parse handshake: %w", err)
	}
	if req.Protocol != "messagepack" {
		return req, fmt.Errorf("unsupported protocol %q in handshake", req.Protocol)
	}
	if req.Version != 1 {
		return req, fmt.Errorf("unsupported version %d in handshake", req.Version)
	}
	return req, nil
}

// EncodeMessage encodes a Hub Protocol message as msgpack and wraps it in
// a length-prefixed frame.
func (messagePackCodec) EncodeMessage(msg any) ([]byte, error) {
	body, err := msgpack.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal msgpack: %w", err)
	}
	return wrapFrame(body), nil
}

// DecodeMessage decodes a single msgpack Hub Protocol message frame.
// lengthHeader is the varint frame length already consumed from the wire.
func (messagePackCodec) DecodeMessage(body []byte, msg any) error {
	if err := msgpack.Unmarshal(body, msg); err != nil {
		return fmt.Errorf("unmarshal msgpack: %w", err)
	}
	return nil
}

// wrapFrame prefixes a payload with a varint length. msgpack uses the same
// varint encoding as the MessagePack spec for arrays/maps, so we reuse
// msgpack's encoder to emit a single int.
func wrapFrame(payload []byte) []byte {
	// We can't reuse msgpack.Encoder directly because we need a raw int +
	// the raw payload without extra map/array framing. Easiest: prepend
	// the varint manually.
	n := len(payload)
	prefix := varintBytes(uint64(n))
	out := make([]byte, 0, len(prefix)+n)
	out = append(out, prefix...)
	out = append(out, payload...)
	return out
}

// varintBytes encodes n as a MessagePack varint (same as protobuf).
func varintBytes(n uint64) []byte {
	var buf [10]byte
	i := 0
	for n >= 0x80 {
		buf[i] = byte(n) | 0x80
		i++
		n >>= 7
	}
	buf[i] = byte(n)
	i++
	return buf[:i]
}

// readFrame reads exactly one length-prefixed messagepack frame from r.
func readFrame(r io.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(byteReader{r})
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, fmt.Errorf("msgpack: zero-length frame")
	}
	if n > 16*1024*1024 {
		return nil, fmt.Errorf("msgpack: frame too large (%d)", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// SplitMsgpackFrame reads the varint length prefix off buf and returns the
// prefix bytes and the body. The connection's read loop calls this to peel
// the framing off before handing the body to DecodeMessage.
func SplitMsgpackFrame(buf []byte) (prefix []byte, body []byte, err error) {
	br := &byteSliceReader{buf: buf}
	n, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, fmt.Errorf("msgpack: read varint length: %w", err)
	}
	consumed := len(buf) - len(br.buf)
	if n > uint64(len(buf)-consumed) {
		return nil, nil, fmt.Errorf("msgpack: declared frame length %d exceeds remaining %d", n, len(buf)-consumed)
	}
	return buf[:consumed], buf[consumed : consumed+int(n)], nil
}

type byteSliceReader struct {
	buf []byte
}

func (b *byteSliceReader) ReadByte() (byte, error) {
	if len(b.buf) == 0 {
		return 0, io.EOF
	}
	c := b.buf[0]
	b.buf = b.buf[1:]
	return c, nil
}

// byteReader adapts an io.Reader to a byte-by-byte reader for varints.
type byteReader struct{ r io.Reader }

func (b byteReader) ReadByte() (byte, error) {
	var p [1]byte
	n, err := b.r.Read(p[:])
	if n > 0 {
		return p[0], nil
	}
	return 0, err
}

// WireCodec is the public facade the read loop uses to (en)code frames.
// JSON variant uses record-separator framing; MessagePack variant uses
// length-prefixed framing. Both implement the same EncodeMessage/DecodeMessage
// contract so the read loop is codec-agnostic.
type WireCodec interface {
	EncodeHandshake(HandshakeResponse) ([]byte, error)
	DecodeHandshake([]byte) (HandshakeRequest, error)
	EncodeMessage(any) ([]byte, error)
	DecodeMessage([]byte, any) error
}

// NewJSONWireCodec returns the JSON WireCodec for the server side.
func NewJSONWireCodec() WireCodec { return newJSONCodec() }

// NewMessagePackWireCodec returns the MessagePack WireCodec for the server side.
func NewMessagePackWireCodec() WireCodec { return newMessagePackCodec() }

// NewWireCodecForTest is a tiny helper used by cmd/msgpack-smoke to verify
// the MessagePack codec without spinning up the full server.
func NewWireCodecForTest() WireCodec { return newMessagePackCodec() }