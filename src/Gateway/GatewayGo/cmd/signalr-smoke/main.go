// Command signalr-smoke verifies the /hubs/terminal SignalR endpoint by:
//   1. POSTing to /api/auth/password/login with the dev seed user to mint a JWT.
//   2. Fetching /hubs/terminal/negotiate?access_token=<jwt>.
//   3. Opening a WebSocket to /hubs/terminal with sub-protocol "json".
//   4. Sending {"protocol":"json","version":1}\x1e — the JSON handshake.
//   5. Receiving the handshake response {}\x1e.
//   6. Sending {"type":6}\x1e — Ping.
//   7. Receiving {"type":6}\x1e — Pong (TerminalHub mirrors ping back).
//   8. Sending {"type":1,"target":"echo","arguments":["hello"],"invocationId":"1"}\x1e.
//   9. Receiving {"type":3,"invocationId":"1","result":["hello"]}\x1e.
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
)

const rs = "\x1e" // SignalR record-separator byte

func main() {
	addr := flag.String("addr", "localhost:5045", "gateway listen address")
	flag.Parse()

	// 1. Mint a JWT via the dev seed user. TerminalHub requires an
	// authenticated user on every state-changing method.
	token := login(*addr)
	fmt.Println("login OK, token len:", len(token))

	// 2. Negotiate.
	negotiateURL := "http://" + *addr + "/hubs/terminal/negotiate?access_token=" + token
	resp, err := http.Get(negotiateURL)
	if err != nil {
		fail("negotiate GET", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		fail("negotiate status", fmt.Errorf("got %d", resp.StatusCode))
	}
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("negotiate OK:", string(bytes.TrimSpace(body)))

	// 3. Open WebSocket with the JWT in the query string. The auth middleware
	// accepts ?access_token= for /hubs/* paths (matches C# JwtBearerEvents).
	u := url.URL{Scheme: "ws", Host: *addr, Path: "/hubs/terminal", RawQuery: "access_token=" + token}
	dialer := *websocket.DefaultDialer
	ws, hr, err := dialer.Dial(u.String(), nil)
	if err != nil {
		if hr != nil {
			fail("ws dial", fmt.Errorf("%w (status=%d, sub=%q, hdrs=%v)", err, hr.StatusCode, hr.Header.Get("Sec-WebSocket-Protocol"), hr.Header))
		}
		fail("ws dial", err)
	}
	defer ws.Close()
	if sub := ws.Subprotocol(); sub != "" && sub != "json" {
		fail("ws subprotocol", fmt.Errorf("got %q want json", sub))
	}

	// 3-4. Handshake. The .NET SignalR client sends each 0x1e-terminated
	// record as its own WebSocket TextMessage (no embedded 0x1e).
	ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := ws.WriteMessage(websocket.TextMessage, []byte(`{"protocol":"json","version":1}`+rs)); err != nil {
		fail("handshake write", err)
	}
	_, hsRaw, err := ws.ReadMessage()
	if err != nil {
		fail("handshake read", err)
	}
	hsExpect := []byte(`{}` + rs)
	if !bytes.Equal(hsRaw, hsExpect) {
		fail("handshake mismatch", fmt.Errorf("got %q want %q", hsRaw, hsExpect))
	}
	fmt.Println("handshake OK")

	// 5-6. Ping → Pong.
	if err := ws.WriteMessage(websocket.TextMessage, []byte(`{"type":6}`+rs)); err != nil {
		fail("ping write", err)
	}
	_, pongRaw, err := ws.ReadMessage()
	if err != nil {
		fail("pong read", err)
	}
	if !bytes.Contains(pongRaw, []byte(`"type":6`)) {
		fail("pong mismatch", fmt.Errorf("got %q", pongRaw))
	}
	fmt.Println("ping/pong OK")

	// 7-8. Invoke Echo.
	inv := `{"type":1,"target":"echo","arguments":["hello"],"invocationId":"1"}` + rs
	_ = inv
	inv = `{"type":1,"target":"echo","arguments":["hello"],"invocationId":"1"}` + rs
	if err := ws.WriteMessage(websocket.TextMessage, []byte(inv)); err != nil {
		fail("invoke write", err)
	}
	_, resRaw, err := ws.ReadMessage()
	if err != nil {
		fail("invoke read", err)
	}
	// EchoHub echoes Arguments back unchanged → ["hello"]. The completion
	// wire shape is {"type":3,"invocationId":"1","result":["hello"]}.
	if !bytes.Contains(resRaw, []byte(`"invocationId":"1"`)) || !bytes.Contains(resRaw, []byte(`["hello"]`)) {
		fail("echo result mismatch", fmt.Errorf("got %q", resRaw))
	}
	fmt.Println("echo invocation OK:", string(bytes.TrimSpace(resRaw)))

	fmt.Println("ALL OK")
}

func fail(label string, err error) {
	fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", label, err)
	os.Exit(1)
}

// login mints a JWT by POSTing to /api/auth/password/login with the seeded
// dev user (test / test123).
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