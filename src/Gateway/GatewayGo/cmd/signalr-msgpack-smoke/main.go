// Command signalr-msgpack-smoke verifies the /hubs/terminal endpoint when
// negotiated with the MessagePack sub-protocol:
//
//  1. POST to /api/auth/password/login to mint a JWT.
//  2. Fetch /hubs/terminal/negotiate?access_token=<jwt>.
//  3. Open WebSocket with sub-protocol "messagepack".
//  4. Send the msgpack-encoded HandshakeRequest as a BinaryMessage.
//  5. Receive the empty HandshakeResponse on a BinaryMessage.
//  6. Send a msgpack-encoded Ping as a BinaryMessage.
//  7. Receive a msgpack-encoded Pong as a BinaryMessage.
//  8. Send a msgpack-encoded Invocation {type:1, target:"echo", ...}.
//  9. Receive a msgpack-encoded Completion with the echoed arguments.
//
// Exits 0 on full round-trip; non-zero with reason on any mismatch.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	msgTypePing       byte = 6
	msgTypeInvocation byte = 1
	msgTypeCompletion byte = 3
)

type handshakeReq struct {
	Protocol string `msgpack:"protocol"`
	Version  int    `msgpack:"version"`
}

type ping struct {
	Type byte `msgpack:"type"`
}

type invocation struct {
	Type         byte     `msgpack:"type"`
	Target       string   `msgpack:"target"`
	InvocationID string   `msgpack:"invocationId"`
	Arguments    []string `msgpack:"arguments"`
}

type completion struct {
	Type         byte     `msgpack:"type"`
	InvocationID string   `msgpack:"invocationId"`
	Result       []string `msgpack:"result"`
}

func main() {
	addr := flag.String("addr", "localhost:5045", "gateway listen address")
	flag.Parse()

	// 1. Mint a JWT via the dev seed user.
	token := login(*addr)
	fmt.Println("login OK, token len:", len(token))

	// 2. Negotiate with the JWT in the query string.
	resp, err := http.Get("http://" + *addr + "/hubs/terminal/negotiate?access_token=" + token)
	if err != nil {
		fail("negotiate GET", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		fail("negotiate status", fmt.Errorf("got %d", resp.StatusCode))
	}
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("negotiate OK:", string(bytes.TrimSpace(body)))

	// 3. Open WebSocket with MessagePack sub-protocol and the JWT.
	u := url.URL{Scheme: "ws", Host: *addr, Path: "/hubs/terminal", RawQuery: "access_token=" + token}
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{"messagepack"}
	ws, hr, err := dialer.Dial(u.String(), nil)
	if err != nil {
		if hr != nil {
			fail("ws dial", fmt.Errorf("%w (status=%d, sub=%q)", err, hr.StatusCode, hr.Header.Get("Sec-WebSocket-Protocol")))
		}
		fail("ws dial", err)
	}
	defer ws.Close()
	if sub := ws.Subprotocol(); sub != "messagepack" {
		fail("ws subprotocol", fmt.Errorf("got %q want messagepack", sub))
	}

	// 3-4. Handshake: msgpack frame as a BinaryMessage.
	hs, err := msgpack.Marshal(handshakeReq{Protocol: "messagepack", Version: 1})
	if err != nil {
		fail("marshal handshake", err)
	}
	hsFrame := wrapMsgpackFrame(hs)
	ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := ws.WriteMessage(websocket.BinaryMessage, hsFrame); err != nil {
		fail("handshake write", err)
	}
	_, hsRespRaw, err := ws.ReadMessage()
	if err != nil {
		fail("handshake read", err)
	}
	hsPrefix, hsBody, err := splitMsgpackFrame(hsRespRaw)
	if err != nil {
		fail("handshake split", err)
	}
	if len(hsBody) == 0 || hsBody[0] != 0x80 {
		// empty fixmap == {}
		fail("handshake body", fmt.Errorf("expected empty msgpack map, got bytes %x", hsBody))
	}
	fmt.Printf("handshake OK (prefix=%d bytes, body=%d bytes)\n", len(hsPrefix), len(hsBody))

	// 5-6. Ping → Pong.
	p, err := msgpack.Marshal(ping{Type: msgTypePing})
	if err != nil {
		fail("marshal ping", err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, wrapMsgpackFrame(p)); err != nil {
		fail("ping write", err)
	}
	_, pongRaw, err := ws.ReadMessage()
	if err != nil {
		fail("pong read", err)
	}
	_, pongBody, err := splitMsgpackFrame(pongRaw)
	if err != nil {
		fail("pong split", err)
	}
	var pong ping
	if err := msgpack.Unmarshal(pongBody, &pong); err != nil {
		fail("pong decode", err)
	}
	if pong.Type != msgTypePing {
		fail("pong type", fmt.Errorf("got %d want %d", pong.Type, msgTypePing))
	}
	fmt.Println("ping/pong OK")

	// 7-8. Invoke Echo.
	inv, err := msgpack.Marshal(invocation{
		Type:         msgTypeInvocation,
		Target:       "echo",
		InvocationID: "1",
		Arguments:    []string{"hello"},
	})
	if err != nil {
		fail("marshal invoke", err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, wrapMsgpackFrame(inv)); err != nil {
		fail("invoke write", err)
	}
	_, resRaw, err := ws.ReadMessage()
	if err != nil {
		fail("invoke read", err)
	}
	_, resBody, err := splitMsgpackFrame(resRaw)
	if err != nil {
		fail("invoke split", err)
	}
	var comp completion
	if err := msgpack.Unmarshal(resBody, &comp); err != nil {
		fail("invoke decode", err)
	}
	if comp.Type != msgTypeCompletion {
		fail("invoke type", fmt.Errorf("got %d want %d", comp.Type, msgTypeCompletion))
	}
	if comp.InvocationID != "1" {
		fail("invoke id", fmt.Errorf("got %q want 1", comp.InvocationID))
	}
	if len(comp.Result) != 1 || comp.Result[0] != "hello" {
		fail("invoke result", fmt.Errorf("got %v want [hello]", comp.Result))
	}
	fmt.Printf("echo invocation OK: result=%v\n", comp.Result)

	fmt.Println("ALL OK")
}

// wrapMsgpackFrame prepends a varint length to body so the result matches
// the SignalR MessagePack wire framing. Mirrors wrapFrame() in
// internal/signalr/messagepack_codec.go.
func wrapMsgpackFrame(body []byte) []byte {
	var prefix [10]byte
	n := uint64(len(body))
	i := 0
	for n >= 0x80 {
		prefix[i] = byte(n) | 0x80
		i++
		n >>= 7
	}
	prefix[i] = byte(n)
	i++
	out := make([]byte, 0, i+len(body))
	out = append(out, prefix[:i]...)
	out = append(out, body...)
	return out
}

// splitMsgpackFrame peels the varint length off buf and returns the body.
// Mirrors SplitMsgpackFrame() in internal/signalr/messagepack_codec.go.
func splitMsgpackFrame(buf []byte) (prefix []byte, body []byte, err error) {
	var consumed int
	var n uint64
	for {
		if consumed >= len(buf) {
			return nil, nil, fmt.Errorf("msgpack: truncated varint")
		}
		b := buf[consumed]
		consumed++
		n |= uint64(b&0x7f) << (7 * (consumed - 1))
		if b&0x80 == 0 {
			break
		}
	}
	if consumed+int(n) > len(buf) {
		return nil, nil, fmt.Errorf("msgpack: declared length %d exceeds remaining %d", n, len(buf)-consumed)
	}
	return buf[:consumed], buf[consumed : consumed+int(n)], nil
}

func fail(label string, err error) {
	fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", label, err)
	os.Exit(1)
}

// login mints a JWT via /api/auth/password/login with the dev seed user.
func login(addr string) string {
	body := bytes.NewReader([]byte(`{"username":"test","password":"test123"}`))
	resp, err := http.Post("http://"+addr+"/api/auth/password/login", "application/json", body)
	if err != nil {
		fail("login POST", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		buf, _ := io.ReadAll(resp.Body)
		fail("login status", fmt.Errorf("got %d: %s", resp.StatusCode, string(buf)))
	}
	var out struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		fail("login decode", err)
	}
	if out.AccessToken == "" {
		fail("login token", fmt.Errorf("empty accessToken"))
	}
	return out.AccessToken
}