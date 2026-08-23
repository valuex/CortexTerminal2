package signalr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"
)

// ConnectionOptions parameterises how a Connection behaves.
type ConnectionOptions struct {
	// HandshakeTimeout caps the time between WebSocket upgrade and the
	// first Hub-Protocol handshake frame. The C# client is sub-second;
	// 5s is generous.
	HandshakeTimeout time.Duration
	// WriteTimeout caps any single WebSocket write.
	WriteTimeout time.Duration
	// IdleTimeout caps the gap between inbound frames. Used to detect
	// half-dead clients that drop frames without sending Close.
	IdleTimeout time.Duration
	// Logger receives per-connection lifecycle logs at debug level.
	Logger *slog.Logger
}

func (o ConnectionOptions) withDefaults() ConnectionOptions {
	if o.HandshakeTimeout == 0 {
		o.HandshakeTimeout = 5 * time.Second
	}
	if o.WriteTimeout == 0 {
		o.WriteTimeout = 10 * time.Second
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = 60 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// runConnection drives a single hub connection through its lifetime:
//
//  1. Read handshake (single frame in the negotiated codec).
//  2. Reply with empty HandshakeResponse.
//  3. Loop: read frame → if Ping respond with Ping → if Invocation dispatch
//     via hub.OnInvocation → if Completion discard (server does not initiate
//     invokes in this port) → if Close break.
//  4. On disconnect: hub.OnDisconnected, then close the WebSocket.
//
// The function blocks until the connection ends. All errors are returned to
// the caller; the hub registry logs them.
func runConnection(ctx context.Context, ws *websocket.Conn, conn *Connection, opts ConnectionOptions) error {
	opts = opts.withDefaults()
	codec := codecForRequest(conn.Request)

	// Handshake deadline.
	if err := ws.SetReadDeadline(time.Now().Add(opts.HandshakeTimeout)); err != nil {
		return fmt.Errorf("set handshake deadline: %w", err)
	}
	msgType, raw, err := ws.ReadMessage()
	if err != nil {
		return fmt.Errorf("read handshake frame: %w", err)
	}
	hs, err := decodeHandshakeWithCodec(codec, msgType, raw)
	if err != nil {
		// Per spec: respond with handshake error then close. The C# client
		// surfaces this on the negotiation promise.
		hsBytes, _ := codec.EncodeHandshake(HandshakeResponse{Error: err.Error()})
		_ = ws.WriteMessage(websocket.TextMessage, hsBytes)
		return fmt.Errorf("handshake: %w", err)
	}
	hsResp, _ := codec.EncodeHandshake(HandshakeResponse{})
	if err := ws.WriteMessage(websocket.TextMessage, hsResp); err != nil {
		return fmt.Errorf("write handshake response: %w", err)
	}
	conn.ID = hs.Protocol + ":" + time.Now().UTC().Format("20060102T150405.000")
	conn.Hub.OnConnected(ctx, conn)
	defer func() {
		conn.Hub.OnDisconnected(ctx, conn, nil)
		conn.closeOnce.Do(func() { close(conn.writePump) })
		_ = ws.Close()
	}()

	// Read loop. No more handshake deadline — apply IdleTimeout per read.
	_ = ws.SetReadDeadline(time.Now().Add(opts.IdleTimeout))
	for {
		msgType, raw, err := ws.ReadMessage()
		if err != nil {
			if errors.Is(err, io.EOF) || websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return nil
			}
			return fmt.Errorf("read frame: %w", err)
		}
		_ = ws.SetReadDeadline(time.Now().Add(opts.IdleTimeout))

		frames, err := splitFramesForCodec(codec, msgType, raw)
		if err != nil {
			opts.Logger.Debug("split frames", "err", err)
			continue
		}
		for _, body := range frames {
			if len(body) == 0 {
				continue
			}
			msg, err := decodeFrame(codec, body)
			if err != nil {
				opts.Logger.Debug("decode frame", "err", err)
				continue
			}
			if err := dispatchFrame(ctx, conn, codec, msg, opts.Logger); err != nil {
				return err
			}
		}
	}
}

// codecForRequest picks the WireCodec based on the X-SignalR-Codec header
// that HandleWebSocket sets from the Sec-WebSocket-Protocol subprotocol.
func codecForRequest(r *http.Request) WireCodec {
	if r != nil && r.Header.Get("X-SignalR-Codec") == "messagepack" {
		return NewMessagePackWireCodec()
	}
	return NewJSONWireCodec()
}

// decodeHandshakeWithCodec normalises a handshake frame for either codec.
// JSON frames may be TextMessages with a trailing 0x1e; MessagePack frames
// are BinaryMessages prefixed with a varint length.
func decodeHandshakeWithCodec(codec WireCodec, msgType int, raw []byte) (HandshakeRequest, error) {
	switch c := codec.(type) {
	case *messagePackCodec:
		if msgType != websocket.BinaryMessage {
			return HandshakeRequest{}, fmt.Errorf("msgpack handshake must be a BinaryMessage, got %d", msgType)
		}
		_, body, err := SplitMsgpackFrame(raw)
		if err != nil {
			return HandshakeRequest{}, err
		}
		return c.DecodeHandshake(body)
	default:
		return codec.DecodeHandshake(bytes.TrimRight(raw, "\x1e"))
	}
}

// splitFramesForCodec peels off individual frames from a WebSocket message.
// JSON: TextMessage may contain multiple 0x1e-separated records.
// MessagePack: BinaryMessage carries exactly one length-prefixed frame.
func splitFramesForCodec(codec WireCodec, msgType int, raw []byte) ([][]byte, error) {
	if _, ok := codec.(*messagePackCodec); ok {
		if msgType != websocket.BinaryMessage {
			return nil, fmt.Errorf("msgpack frame must be BinaryMessage, got %d", msgType)
		}
		_, body, err := SplitMsgpackFrame(raw)
		if err != nil {
			return nil, err
		}
		return [][]byte{body}, nil
	}
	return codec.(*jsonCodec).SplitFrames(raw), nil
}

// decodeFrame inspects the leading integer `type` field and returns the
// matching struct shape. We unmarshal twice for JSON (once to probe the type,
// once into the concrete struct) so each concrete type can use its own
// msgpack tag set without colliding on default values.
func decodeFrame(codec WireCodec, body []byte) (any, error) {
	if _, ok := codec.(*messagePackCodec); ok {
		return decodeMsgpackFrame(body)
	}
	return decodeJSONFrame(body)
}

func decodeJSONFrame(raw []byte) (any, error) {
	var probe struct {
		Type MessageType `json:"type"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, err
	}
	switch probe.Type {
	case MessageTypePing:
		var p Ping
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, err
		}
		return p, nil
	case MessageTypeInvocation:
		var inv Invocation
		if err := json.Unmarshal(raw, &inv); err != nil {
			return nil, err
		}
		return inv, nil
	case MessageTypeCompletion:
		var c Completion
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		return c, nil
	case MessageTypeClose:
		var c Close
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, err
		}
		return c, nil
	default:
		return nil, fmt.Errorf("unknown message type %d", probe.Type)
	}
}

// decodeMsgpackFrame inspects the msgpack map header to find the `type`
// field, then unmarshals into the matching concrete struct. We probe the
// type via a tiny dedicated decoder so the wire contract stays explicit.
func decodeMsgpackFrame(body []byte) (any, error) {
	var probe struct {
		Type MessageType `msgpack:"type"`
	}
	if err := msgpack.Unmarshal(body, &probe); err != nil {
		return nil, err
	}
	switch probe.Type {
	case MessageTypePing:
		var p Ping
		if err := msgpack.Unmarshal(body, &p); err != nil {
			return nil, err
		}
		return p, nil
	case MessageTypeInvocation:
		var inv Invocation
		if err := msgpack.Unmarshal(body, &inv); err != nil {
			return nil, err
		}
		return inv, nil
	case MessageTypeCompletion:
		var c Completion
		if err := msgpack.Unmarshal(body, &c); err != nil {
			return nil, err
		}
		return c, nil
	case MessageTypeClose:
		var c Close
		if err := msgpack.Unmarshal(body, &c); err != nil {
			return nil, err
		}
		return c, nil
	default:
		return nil, fmt.Errorf("unknown message type %d", probe.Type)
	}
}

func dispatchFrame(ctx context.Context, conn *Connection, codec WireCodec, msg any, logger *slog.Logger) error {
	switch m := msg.(type) {
	case Ping:
		conn.Hub.OnPing(ctx, conn)
		return writeFrame(conn, codec, Ping{Type: MessageTypePing}, logger)
	case Invocation:
		result, err := conn.Hub.OnInvocation(ctx, conn, m)
		if m.InvocationID == "" {
			// SendAsync — no Completion expected.
			return nil
		}
		comp := Completion{Type: MessageTypeCompletion, InvocationID: m.InvocationID}
		if err != nil {
			comp.Error = err.Error()
		} else {
			comp.Result = result
		}
		return writeFrame(conn, codec, comp, logger)
	case Completion:
		// Server does not initiate invokes in this port; ignore stray
		// completions from clients that re-used our connectionId.
		return nil
	case Close:
		return io.EOF
	}
	return nil
}

func writeFrame(conn *Connection, codec WireCodec, msg any, logger *slog.Logger) error {
	buf, err := codec.EncodeMessage(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	select {
	case conn.writePump <- buf:
		return nil
	default:
		logger.Warn("writePump full, dropping frame", "connId", conn.ID)
		return errors.New("writePump full")
	}
}

// writePumpLoop drains conn.writePump and writes each frame to the WebSocket.
// The message type (Text vs Binary) is chosen from the codec at connection
// setup so the C# client gets the right frame on its end.
func writePumpLoop(ws *websocket.Conn, conn *Connection, opts ConnectionOptions) {
	opts = opts.withDefaults()
	msgType := websocket.TextMessage
	if conn.Request != nil && conn.Request.Header.Get("X-SignalR-Codec") == "messagepack" {
		msgType = websocket.BinaryMessage
	}
	for buf := range conn.writePump {
		if err := ws.SetWriteDeadline(time.Now().Add(opts.WriteTimeout)); err != nil {
			opts.Logger.Debug("set write deadline", "err", err)
			return
		}
		if err := ws.WriteMessage(msgType, buf); err != nil {
			opts.Logger.Debug("write frame", "err", err)
			return
		}
	}
}