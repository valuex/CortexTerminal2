// Command sessions-smoke proves the Sessions subsystem end-to-end against
// an in-memory SQLite database. Walks the state machine the same way the
// C# integration tests do, then asserts the final DB row matches.
//
// This slice is the coordinator-only port — no SignalR round-trip here.
// The earlier worker-smoke binary already proved the SignalR wire path;
// this one proves the in-memory + SQLite side of the new sessions slice.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/data"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/sessions"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/signalr"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	must(err, "open in-memory db")
	defer db.Close()
	must(data.RunMigrations(context.Background(), db), "migrate")

	const userID = "user-aaa"
	const workerID = "worker-bbb"
	const clientConnID = "client-ccc"

	repo := data.NewSessionsRepo(db)
	coord := sessions.NewCoordinator(repo, &stubResolver{
		worker: sessions.WorkerHandle{WorkerID: workerID, ConnectionID: "worker-conn-1"},
	}, logger)
	launch := sessions.NewSessionLaunchCoordinator(logger)
	replay := sessions.NewReplayCoordinator(logger)

	ctx := context.Background()

	// 1. CreateSession — same path the hub calls.
	result, err := launch.RunLaunch(userID, "req-1", clientConnID, func() (sessions.CreateSessionResult, error) {
		return coord.CreateSession(ctx, userID, sessions.CreateSessionRequest{
			WorkerID: workerID, Columns: 100, Rows: 30,
		}, clientConnID)
	})
	must(err, "create session")
	if !result.Success {
		fatal("CreateSession not success: %+v", result)
	}
	sessionID := result.SessionID
	logger.Info("created", "session", sessionID)

	// 2. The DB row must be Attached.
	row := mustFind(repo, sessionID)
	if row.Status != "Attached" {
		fatal("expected Attached got %q", row.Status)
	}
	if row.User != userID || row.Worker != workerID {
		fatal("identity mismatch")
	}
	if row.Columns != 100 || row.Rows != 30 {
		fatal("size mismatch: %+v", row)
	}

	// 3. Buffered stdout while the Console is detached lands in the replay
	// queue. DetachSession flips the state; the hub layer opens the
	// replay buffer alongside (mirrors the C# TerminalHub.OnDisconnected).
	coord.DetachSession(ctx, userID, sessionID, time.Now())
	replay.OpenBuffer(sessionID)
	replay.BufferStdout(sessionID, terminalChunk(sessionID, "hello"))
	replay.BufferStdout(sessionID, terminalChunk(sessionID, "world"))
	replay.BufferStderr(sessionID, terminalChunk(sessionID, "boom"))
	if got := replay.Size(sessionID); got != 3 {
		fatal("expected 3 buffered, got %d", got)
	}

	// 4. Reattach flips state to Attached and drains the buffer.
	const clientConn2 = "client-ddd"
	r, err := coord.ReattachSession(ctx, userID, sessions.ReattachSessionRequest{SessionID: sessionID}, clientConn2, time.Now())
	must(err, "reattach")
	if !r.Success {
		fatal("ReattachSession not success: %+v", r)
	}
	bundle := replay.CloseBuffer(sessionID)
	if len(bundle.Stdout) != 2 {
		fatal("replay stdout count = %d, want 2", len(bundle.Stdout))
	}
	if len(bundle.Stderr) != 1 {
		fatal("replay stderr count = %d, want 1", len(bundle.Stderr))
	}
	row = mustFind(repo, sessionID)
	if row.Status != "Attached" {
		fatal("after reattach expected Attached, got %q", row.Status)
	}

	// 5. Worker disconnects → TransitionToRecovering flips every Attached
	// session whose worker connection matches.
	orig, err := coord.TransitionToRecovering(ctx, workerID, "worker-conn-1")
	must(err, "transition")
	if len(orig) != 1 || orig[0].SessionID != sessionID {
		fatal("TransitionToRecovering: %+v", orig)
	}
	row = mustFind(repo, sessionID)
	if row.Status != "Recovering" {
		fatal("after worker disconnect expected Recovering, got %q", row.Status)
	}

	// 6. Rebind after worker comes back flips Recovering → Attached and
	// rebinds to the new connection.
	n, err := coord.RebindActiveSessions(ctx, userID, workerID, "worker-conn-2")
	must(err, "rebind")
	if n != 1 {
		fatal("rebind count = %d", n)
	}
	row = mustFind(repo, sessionID)
	if row.Status != "Attached" {
		fatal("after rebind expected Attached, got %q", row.Status)
	}

	// 7. Worker reports the session is gone — ReconcileWorkerSessions
	// expires it.
	expired, err := coord.ReconcileWorkerSessions(ctx, userID, workerID, nil)
	must(err, "reconcile")
	if len(expired) != 1 || expired[0] != sessionID {
		fatal("reconcile expired = %v", expired)
	}
	row = mustFind(repo, sessionID)
	if row.Status != "Expired" {
		fatal("after reconcile expected Expired, got %q", row.Status)
	}

	logger.Info("PASS", "session", sessionID)
}

type stubResolver struct{ worker sessions.WorkerHandle }

func (s *stubResolver) TryGetWorker(string) (sessions.WorkerHandle, bool)         { return s.worker, true }
func (s *stubResolver) TryGetLeastBusyForUser(string) (sessions.WorkerHandle, bool) { return s.worker, true }
func (*stubResolver) SetWorkerOwner(string, string) bool                             { return true }

func must(err error, label string) {
	if err != nil && !strings.Contains(err.Error(), "nil") {
		fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", label, err)
		os.Exit(1)
	}
}

type rowView struct {
	Status  string
	User    string
	Worker  string
	Columns int
	Rows    int
}

func mustFind(repo *data.SessionsRepo, sessionID string) rowView {
	ctx := context.Background()
	ent, err := repo.Find(ctx, sessionID)
	must(err, "find "+sessionID)
	if ent == nil {
		fatal("session %s not in DB", sessionID)
	}
	return rowView{
		Status:  ent.AttachmentState,
		User:    ent.UserID,
		Worker:  ent.WorkerID,
		Columns: ent.Columns,
		Rows:    ent.Rows,
	}
}

func terminalChunk(sessionID, payload string) signalr.TerminalChunkWire {
	return signalr.TerminalChunkWire{
		SessionID: sessionID,
		Stream:    "stdout",
		Payload:   []byte(payload),
		Timestamp: time.Now().UnixNano(),
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}