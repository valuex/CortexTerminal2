package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ArtifactEntity mirrors the Artifacts table. Status, Origin, and
// FileCategory are stored as strings (EF convention). Date columns are
// ISO-8601 UTC strings.
type ArtifactEntity struct {
	ID              string
	SessionID       string
	Filename        string
	SizeBytes       int64
	Status          string
	Origin          string
	OwnerUserID     string
	ContentSHA256   sql.NullString
	FileCategory    string
	CreatedAtUTC    string // ISO-8601 UTC
	CompletedAtUTC  sql.NullString
	ExpiresAtUTC    string // ISO-8601 UTC
}

// ArtifactStatus constants mirror CortexTerminal.Contracts.Sessions.ArtifactStatus.
// They live in `sessions` (re-exported via re-declaration there) so wire
// code references the same symbol as the storage layer.
const (
	ArtifactStatusPending = "pending"
	ArtifactStatusReady   = "ready"
	ArtifactStatusDeleted = "deleted"
)

// ArtifactOrigin constants mirror CortexTerminal.Contracts.Sessions.ArtifactOrigin.
const (
	ArtifactOriginConsole = "console"
	ArtifactOriginWorker  = "worker"
)

// ArtifactsRepo is the persistence layer for the Artifacts table. Mirrors
// the read/write set ArtifactService needs.
type ArtifactsRepo struct {
	db *sql.DB
}

func NewArtifactsRepo(db *sql.DB) *ArtifactsRepo { return &ArtifactsRepo{db: db} }

// Insert creates a new artifact row in Pending status. Mirrors the
// INSERT in ArtifactService.CreateForConsoleUploadAsync.
func (r *ArtifactsRepo) Insert(ctx context.Context, e ArtifactEntity) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO Artifacts (
			id, session_id, filename, size_bytes, status, origin,
			owner_user_id, content_sha256, file_category,
			created_at_utc, completed_at_utc, expires_at_utc
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.SessionID, e.Filename, e.SizeBytes, e.Status, e.Origin,
		e.OwnerUserID, e.ContentSHA256, e.FileCategory,
		e.CreatedAtUTC, e.CompletedAtUTC, e.ExpiresAtUTC)
	return err
}

// Find loads an artifact by id.
func (r *ArtifactsRepo) Find(ctx context.Context, id string) (*ArtifactEntity, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, session_id, filename, size_bytes, status, origin,
		        owner_user_id, content_sha256, file_category,
		        created_at_utc, completed_at_utc, expires_at_utc
		 FROM Artifacts WHERE id = ?`, id)
	return scanArtifact(row)
}

// FindBySessionFilename loads a specific artifact by its (sessionId,
// filename) tuple. Used by the worker upload path for last-write-wins.
func (r *ArtifactsRepo) FindBySessionFilename(ctx context.Context, sessionID, filename string) (*ArtifactEntity, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, session_id, filename, size_bytes, status, origin,
		        owner_user_id, content_sha256, file_category,
		        created_at_utc, completed_at_utc, expires_at_utc
		 FROM Artifacts WHERE session_id = ? AND filename = ?`,
		sessionID, filename)
	return scanArtifact(row)
}

// ListBySession returns every non-deleted artifact for sessionID, oldest first
// (CompletedAtUtc || CreatedAtUtc ASC). Mirrors ArtifactService.ListAsync.
func (r *ArtifactsRepo) ListBySession(ctx context.Context, sessionID string) ([]ArtifactEntity, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, session_id, filename, size_bytes, status, origin,
		        owner_user_id, content_sha256, file_category,
		        created_at_utc, completed_at_utc, expires_at_utc
		 FROM Artifacts
		 WHERE session_id = ? AND status != ?
		 ORDER BY COALESCE(completed_at_utc, created_at_utc) ASC`,
		sessionID, ArtifactStatusDeleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ArtifactEntity
	for rows.Next() {
		e, err := scanArtifactRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// CountBySession returns the number of non-deleted artifacts for sessionID.
// Used by the quota check.
func (r *ArtifactsRepo) CountBySession(ctx context.Context, sessionID string) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM Artifacts
		 WHERE session_id = ? AND status != ?`,
		sessionID, ArtifactStatusDeleted).Scan(&n)
	return n, err
}

// UpdateStatus flips status and, optionally, the content_sha256 +
// completed_at_utc columns. Used by CompleteConsoleUpload / Delete /
// CleanExpired paths. Mirrors the inline UPDATE statements in
// ArtifactService.cs.
func (r *ArtifactsRepo) UpdateStatus(
	ctx context.Context, id string, status string,
	contentSHA256 *string, completedAtUTC *time.Time,
) error {
	sets := []string{"status = ?"}
	args := []any{status}
	if contentSHA256 != nil {
		sets = append(sets, "content_sha256 = ?")
		args = append(args, *contentSHA256)
	}
	if completedAtUTC != nil {
		sets = append(sets, "completed_at_utc = ?")
		args = append(args, completedAtUTC.UTC().Format(time.RFC3339Nano))
	}
	args = append(args, id)
	q := "UPDATE Artifacts SET "
	for i, s := range sets {
		if i > 0 {
			q += ", "
		}
		q += s
	}
	q += " WHERE id = ?"
	_, err := r.db.ExecContext(ctx, q, args...)
	return err
}

// UpdateExpiry rewrites expires_at_utc for a single artifact.
func (r *ArtifactsRepo) UpdateExpiry(ctx context.Context, id string, expiresAt time.Time) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE Artifacts SET expires_at_utc = ? WHERE id = ?`,
		expiresAt.UTC().Format(time.RFC3339Nano), id)
	return err
}

// ListExpired returns every non-deleted artifact whose expires_at_utc has
// passed at the supplied time. Used by CleanExpiredAsync.
func (r *ArtifactsRepo) ListExpired(ctx context.Context, before time.Time) ([]ArtifactEntity, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, session_id, filename, size_bytes, status, origin,
		        owner_user_id, content_sha256, file_category,
		        created_at_utc, completed_at_utc, expires_at_utc
		 FROM Artifacts
		 WHERE status != ? AND expires_at_utc <= ?`,
		ArtifactStatusDeleted, before.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ArtifactEntity
	for rows.Next() {
		e, err := scanArtifactRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// ListBySessionWithFutureExpiry returns every non-deleted artifact for
// sessionID whose current expiry is strictly after `after`. Used by
// OnSessionTerminatedAsync to clamp the TTL to the grace window.
func (r *ArtifactsRepo) ListBySessionWithFutureExpiry(
	ctx context.Context, sessionID string, after time.Time,
) ([]ArtifactEntity, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, session_id, filename, size_bytes, status, origin,
		        owner_user_id, content_sha256, file_category,
		        created_at_utc, completed_at_utc, expires_at_utc
		 FROM Artifacts
		 WHERE session_id = ? AND status != ? AND expires_at_utc > ?`,
		sessionID, ArtifactStatusDeleted, after.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ArtifactEntity
	for rows.Next() {
		e, err := scanArtifactRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// --- helpers ---

func scanArtifact(row *sql.Row) (*ArtifactEntity, error) {
	var e ArtifactEntity
	err := row.Scan(
		&e.ID, &e.SessionID, &e.Filename, &e.SizeBytes,
		&e.Status, &e.Origin, &e.OwnerUserID, &e.ContentSHA256,
		&e.FileCategory, &e.CreatedAtUTC, &e.CompletedAtUTC, &e.ExpiresAtUTC,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan artifact: %w", err)
	}
	return &e, nil
}

func scanArtifactRows(rows *sql.Rows) (*ArtifactEntity, error) {
	var e ArtifactEntity
	err := rows.Scan(
		&e.ID, &e.SessionID, &e.Filename, &e.SizeBytes,
		&e.Status, &e.Origin, &e.OwnerUserID, &e.ContentSHA256,
		&e.FileCategory, &e.CreatedAtUTC, &e.CompletedAtUTC, &e.ExpiresAtUTC,
	)
	if err != nil {
		return nil, fmt.Errorf("scan artifact row: %w", err)
	}
	return &e, nil
}