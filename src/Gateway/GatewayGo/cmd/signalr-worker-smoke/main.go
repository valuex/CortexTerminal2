// Command signalr-worker-smoke verifies /hubs/worker RegisterWorker via the
// MessagePack sub-protocol:
//
//  1. POST to /api/auth/password/login → JWT.
//  2. Fetch /hubs/worker/negotiate?access_token=<jwt>.
//  3. Open WebSocket with sub-protocol "messagepack".
//  4. Send msgpack HandshakeRequest as a BinaryMessage.
//  5. Receive empty HandshakeResponse on a BinaryMessage.
//  6. Send a msgpack Invocation {type:1, target:"RegisterWorker", arguments:["worker-123"]}.
//  7. Receive Completion with invocationId="1" and no error.
//
// This is the first end-to-end proof that the Go WorkerHub dispatch works
// against the same wire contract the C# Worker daemon uses.
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

type handshakeReq struct {
	Protocol string `msgpack:"protocol"`
	Version  int    `msgpack:"version"`
}

type invocation struct {
	Type         byte     `msgpack:"type"`
	Target       string   `msgpack:"target"`
	InvocationID string   `msgpack:"invocationId"`
	Arguments    []string `msgpack:"arguments"`
}

type completion struct {
	Type         byte   `msgpack:"type"`
	InvocationID string `msgpack:"invocationId"`
	Error        string `msgpack:"error,omitempty"`
}

func main() {
	addr := flag.String("addr", "localhost:5045", "gateway listen address")
	workerID := flag.String("worker", "worker-smoke-001", "workerId to register")
	flag.Parse()

	token := login(*addr)
	fmt.Printf("login OK (token len %d)\n", len(token))

	// Negotiate.
	resp, err := http.Get("http://" + *addr + "/hubs/worker/negotiate?access_token=" + token)
	if err != nil {
		fail("negotiate GET", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		fail("negotiate status", fmt.Errorf("got %d", resp.StatusCode))
	}
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("negotiate OK:", string(bytes.TrimSpace(body)))

	u := url.URL{Scheme: "ws", Host: *addr, Path: "/hubs/worker", RawQuery: "access_token=" + token}
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

	// Handshake.
	hs, err := msgpack.Marshal(handshakeReq{Protocol: "messagepack", Version: 1})
	if err != nil {
		fail("marshal handshake", err)
	}
	ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := ws.WriteMessage(websocket.BinaryMessage, wrapFrame(hs)); err != nil {
		fail("handshake write", err)
	}
	_, hsRespRaw, err := ws.ReadMessage()
	if err != nil {
		fail("handshake read", err)
	}
	_, hsBody, err := splitFrame(hsRespRaw)
	if err != nil {
		fail("handshake split", err)
	}
	if len(hsBody) == 0 || hsBody[0] != 0x80 {
		fail("handshake body", fmt.Errorf("expected empty map (0x80), got %x", hsBody))
	}
	fmt.Println("handshake OK")

	// RegisterWorker invocation.
	inv, err := msgpack.Marshal(invocation{
		Type:         1, // MessageTypeInvocation
		Target:       "RegisterWorker",
		InvocationID: "1",
		Arguments:    []string{*workerID},
	})
	if err != nil {
		fail("marshal invoke", err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, wrapFrame(inv)); err != nil {
		fail("invoke write", err)
	}
	_, compRaw, err := ws.ReadMessage()
	if err != nil {
		fail("invoke read", err)
	}
	_, compBody, err := splitFrame(compRaw)
	if err != nil {
		fail("invoke split", err)
	}
	var comp completion
	if err := msgpack.Unmarshal(compBody, &comp); err != nil {
		fail("invoke decode", err)
	}
	if comp.Type != 3 {
		fail("invoke type", fmt.Errorf("got %d want 3 (Completion)", comp.Type))
	}
	if comp.InvocationID != "1" {
		fail("invoke id", fmt.Errorf("got %q want 1", comp.InvocationID))
	}
	if comp.Error != "" {
		fail("invoke error", fmt.Errorf("got error: %s", comp.Error))
	}
	fmt.Printf("RegisterWorker OK (workerId=%s)\n", *workerID)

	fmt.Println("ALL OK")
}

func wrapFrame(body []byte) []byte {
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

func splitFrame(buf []byte) (prefix []byte, body []byte, err error) {
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

func fail(label string, err error) {
	fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", label, err)
	os.Exit(1)
}