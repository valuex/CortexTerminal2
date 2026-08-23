// Command artifacts-smoke proves the ArtifactService end-to-end against
// the in-memory SQLite + memstorage backend. Walks the full upload lifecycle:
// create → put bytes → complete → list → download → delete → cleanup.
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
	"github.com/monster-echo/CortexTerminal2/gateway/internal/storage"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/storage/memstorage"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	must(err, "open in-memory db")
	defer db.Close()
	must(data.RunMigrations(context.Background(), db), "migrate")

	memStore := memstorage.NewInMemoryStorage()
	storageOpts := storage.ArtifactStorageOptions{}.WithDefaults()

	// Create a session first so the artifact service has something to attach to.
	sessionsRepo := data.NewSessionsRepo(db)
	coord := sessions.NewCoordinator(sessionsRepo, &stubResolver{}, logger)
	launch := sessions.NewSessionLaunchCoordinator(logger)
	repo := sessionsRepo

	const userID = "user-fff"
	const workerID = "worker-ggg"
	createResult, err := launch.RunLaunch(userID, "req-aaa", "client-hhh", func() (sessions.CreateSessionResult, error) {
		return coord.CreateSession(context.Background(), userID, sessions.CreateSessionRequest{
			WorkerID: workerID, Columns: 80, Rows: 24,
		}, "client-hhh")
	})
	must(err, "create session")
	sessionID := createResult.SessionID
	logger.Info("seed session", "session", sessionID)

	artifactsRepo := data.NewArtifactsRepo(db)
	svc := sessions.NewArtifactService(
		artifactsRepo, memStore,
		coord, &stubAudit{}, &stubBroadcast{}, &stubWorker{},
		storageOpts, logger,
	)
	ctx := context.Background()

	// 1. CreateForConsoleUpload — should insert a Pending row.
	const filename = "hello.txt"
	upload, err := svc.CreateForConsoleUpload(ctx, userID, sessions.CreateArtifactRequest{
		SessionID: sessionID,
		Filename:  filename,
		SizeBytes: int64(len("hello, world!")),
		Origin:    sessions.ArtifactOriginConsole,
	})
	must(err, "create upload")
	if upload.ArtifactID == "" || upload.UploadURL == "" {
		fatal("upload response missing fields: %+v", upload)
	}
	logger.Info("artifact created (pending)",
		"id", upload.ArtifactID, "url", upload.UploadURL)

	// 2. Simulate the client PUT-ing to S3: write directly to the in-mem store.
	must(memStore.Put(sessionID, filename, []byte("hello, world!")), "put bytes")

	// 3. CompleteConsoleUpload — should verify size, flip status to Ready.
	must(svc.CompleteConsoleUpload(ctx, userID, upload.ArtifactID, ""), "complete")

	row, err := artifactsRepo.Find(ctx, upload.ArtifactID)
	must(err, "find after complete")
	if row == nil || row.Status != sessions.ArtifactStatusReady {
		fatal("expected Ready, got %+v", row)
	}
	logger.Info("artifact ready",
		"id", upload.ArtifactID,
		"category", row.FileCategory,
		"size", row.SizeBytes)

	// 4. List — should return one artifact.
	list, err := svc.List(ctx, userID, sessionID)
	must(err, "list")
	if len(list) != 1 {
		fatal("expected 1 artifact, got %d", len(list))
	}
	if list[0].FileCategory != sessions.CategoryText {
		fatal("expected text category, got %q", list[0].FileCategory)
	}

	// 5. Download URL — should succeed because artifact is Ready.
	dl, err := svc.GetDownloadURL(ctx, userID, upload.ArtifactID)
	must(err, "download url")
	if dl.DownloadURL == "" {
		fatal("empty download url")
	}

	// 6. Validation — empty filename should reject.
	_, err = svc.CreateForConsoleUpload(ctx, userID, sessions.CreateArtifactRequest{
		SessionID: sessionID, Filename: "", SizeBytes: 10, Origin: sessions.ArtifactOriginConsole,
	})
	if err == nil {
		fatal("expected empty-filename validation error")
	}

	// 7. Validation — path traversal should reject.
	_, err = svc.CreateForConsoleUpload(ctx, userID, sessions.CreateArtifactRequest{
		SessionID: sessionID, Filename: "../etc/passwd", SizeBytes: 10, Origin: sessions.ArtifactOriginConsole,
	})
	if err == nil {
		fatal("expected path-traversal validation error")
	}

	// 8. Validation — Windows reserved name.
	_, err = svc.CreateForConsoleUpload(ctx, userID, sessions.CreateArtifactRequest{
		SessionID: sessionID, Filename: "CON", SizeBytes: 10, Origin: sessions.ArtifactOriginConsole,
	})
	if err == nil {
		fatal("expected Windows-reserved validation error")
	}

	// 9. Duplicate filename should reject (Console uploads are strict).
	_, err = svc.CreateForConsoleUpload(ctx, userID, sessions.CreateArtifactRequest{
		SessionID: sessionID, Filename: filename, SizeBytes: 10, Origin: sessions.ArtifactOriginConsole,
	})
	if err == nil {
		fatal("expected duplicate-filename error")
	}

	// 10. Size mismatch: create a second artifact, write wrong-size bytes, try to complete.
	upload2, err := svc.CreateForConsoleUpload(ctx, userID, sessions.CreateArtifactRequest{
		SessionID: sessionID, Filename: "size-mismatch.bin", SizeBytes: 100, Origin: sessions.ArtifactOriginConsole,
	})
	must(err, "create upload 2")
	must(memStore.Put(sessionID, "size-mismatch.bin", []byte("only 14 bytes")), "put mismatch")
	err = svc.CompleteConsoleUpload(ctx, userID, upload2.ArtifactID, "")
	if err == nil || !strings.Contains(err.Error(), "size mismatch") {
		fatal("expected size mismatch error, got %v", err)
	}

	// 11. Delete — should flip to Deleted, broadcast, drop the object.
	must(svc.Delete(ctx, userID, upload.ArtifactID), "delete")
	row, err = artifactsRepo.Find(ctx, upload.ArtifactID)
	must(err, "find after delete")
	if row.Status != sessions.ArtifactStatusDeleted {
		fatal("after delete expected Deleted, got %q", row.Status)
	}
	if memStore.Has(sessionID, filename) {
		fatal("object still present after delete")
	}

	// 12. List after delete — should not include the deleted artifact.
	list, err = svc.List(ctx, userID, sessionID)
	must(err, "list after delete")
	for _, a := range list {
		if a.ID == upload.ArtifactID {
			fatal("deleted artifact still in list")
		}
	}

	// 13. CleanExpired — no-op since we haven't expired anything yet.
	n, err := svc.CleanExpired(ctx)
	must(err, "clean expired")
	logger.Info("cleaned expired", "n", n)

	// 14. Worker upload path — separate worker connection, separate filename.
	const workerConn = "worker-conn-7"
	wUpload, err := svc.CreateForWorkerUpload(ctx, workerConn, userID, sessions.CreateArtifactRequest{
		SessionID: sessionID, Filename: "worker-upload.bin", SizeBytes: 8,
		Origin: sessions.ArtifactOriginWorker,
	})
	must(err, "worker create")
	must(memStore.Put(sessionID, "worker-upload.bin", []byte("12345678")), "worker put")
	must(svc.CompleteWorkerUpload(ctx, workerConn, userID, wUpload.ArtifactID, ""), "worker complete")
	wRow, err := artifactsRepo.Find(ctx, wUpload.ArtifactID)
	must(err, "find worker upload")
	if wRow.Status != sessions.ArtifactStatusReady {
		fatal("worker upload not Ready: %q", wRow.Status)
	}
	if wRow.Origin != sessions.ArtifactOriginWorker {
		fatal("origin mismatch: %q", wRow.Origin)
	}
	logger.Info("worker upload complete",
		"id", wUpload.ArtifactID,
		"origin", wRow.Origin)

	// 15. File category detection across the major buckets.
	cases2 := map[string]string{
		"photo.png":  sessions.CategoryImage,
		"doc.pdf":    sessions.CategoryPdf,
		"clip.mp4":   sessions.CategoryVideo,
		"track.mp3":  sessions.CategoryAudio,
		"x.zip":      sessions.CategoryArchive,
		"main.go":    sessions.CategoryCode,
		"readme.md":  sessions.CategoryText,
		"weird.bin":  sessions.CategoryUnknown,
	}
	for f, want := range cases2 {
		got := sessions.DetectFileCategory(f)
		if got != want {
			fatal("category %s: got %q want %q", f, got, want)
		}
	}

	_ = repo
	_ = time.Now

	logger.Info("PASS")
}

type stubResolver struct{}

func (*stubResolver) TryGetWorker(string) (sessions.WorkerHandle, bool) {
	return sessions.WorkerHandle{WorkerID: "worker-ggg", ConnectionID: "worker-conn-7"}, true
}
func (*stubResolver) TryGetLeastBusyForUser(string) (sessions.WorkerHandle, bool) {
	return sessions.WorkerHandle{WorkerID: "worker-ggg", ConnectionID: "worker-conn-7"}, true
}
func (*stubResolver) SetWorkerOwner(string, string) bool { return true }

type stubAudit struct{}

func (*stubAudit) Record(userID, action, targetEntity, targetID string) {}

type stubBroadcast struct{}

func (*stubBroadcast) BroadcastArtifactChanged(userID string, evt sessions.ArtifactChangedEvent) error { return nil }

type stubWorker struct{}

func (*stubWorker) NotifyArtifactUploaded(connectionID string, frame sessions.NotifyArtifactUploadedFrame) error { return nil }

func must(err error, label string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", label, err)
		os.Exit(1)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}

// storage_DefaultOptions is no longer needed — ArtifactStorageOptions.WithDefaults() does it.