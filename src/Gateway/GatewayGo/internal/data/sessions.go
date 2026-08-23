package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// SessionsRepo is the persistence layer for the Sessions table. Mirrors
// the read/write set DbSessionCoordinator needs. All date columns stay
// as ISO-8601 strings (EF Core convention).
type SessionsRepo struct {
	db *sql.DB
}

func NewSessionsRepo(db *sql.DB) *SessionsRepo { return &SessionsRepo{db: db} }

// SessionEntity mirrors the Sessions table layout. Date columns are kept
// as strings (driver.Value conversion path) — the EF migration pinned
// created_at_utc, last_activity_at_utc as TEXT. lease_expires_at_utc was
// dropped in a later EF migration and is intentionally absent here.
type SessionEntity struct {
	SessionID                  string
	UserID                     string
	WorkerID                   string
	WorkerConnectionID         sql.NullString
	Columns                    int
	Rows                       int
	CreatedAtUTC               string // ISO-8601 UTC
	LastActivityAtUTC          string // ISO-8601 UTC
	AttachmentState            string
	AttachedClientConnectionID sql.NullString
	ExitCode                   sql.NullInt64
	ExitReason                 sql.NullString
	ReplayPending              bool
}

// Insert creates a new Sessions row. Mirrors the CreateSessionAsync
// INSERT in DbSessionCoordinator. The state is always "Attached" on
// insert (the C# code creates the row in Attached and never inserts any
// other state).
func (r *SessionsRepo) Insert(ctx context.Context, e SessionEntity) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO Sessions (
			session_id, user_id, worker_id, worker_connection_id,
			columns, rows, created_at_utc, last_activity_at_utc,
			attachment_state, attached_client_connection_id,
			exit_code, exit_reason, replay_pending
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.SessionID, e.UserID, e.WorkerID, e.WorkerConnectionID,
		e.Columns, e.Rows, e.CreatedAtUTC, e.LastActivityAtUTC,
		e.AttachmentState, e.AttachedClientConnectionID,
		e.ExitCode, e.ExitReason, e.ReplayPending)
	return err
}

// Find loads one session row by id. Returns (nil, nil) when not found.
func (r *SessionsRepo) Find(ctx context.Context, sessionID string) (*SessionEntity, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT session_id, user_id, worker_id, worker_connection_id,
		        columns, rows, created_at_utc, last_activity_at_utc,
		        attachment_state, attached_client_connection_id,
		        exit_code, exit_reason, replay_pending
		 FROM Sessions WHERE session_id = ?`, sessionID)
	return scanSession(row)
}

// FindForUser loads a session iff it belongs to userID. The C# code uses
// this pattern to avoid leaking existence across users.
func (r *SessionsRepo) FindForUser(ctx context.Context, sessionID, userID string) (*SessionEntity, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT session_id, user_id, worker_id, worker_connection_id,
		        columns, rows, created_at_utc, last_activity_at_utc,
		        attachment_state, attached_client_connection_id,
		        exit_code, exit_reason, replay_pending
		 FROM Sessions WHERE session_id = ? AND user_id = ?`, sessionID, userID)
	return scanSession(row)
}

// ListForUser returns every session belonging to userID, newest-first.
func (r *SessionsRepo) ListForUser(ctx context.Context, userID string) ([]SessionEntity, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT session_id, user_id, worker_id, worker_connection_id,
		        columns, rows, created_at_utc, last_activity_at_utc,
		        attachment_state, attached_client_connection_id,
		        exit_code, exit_reason, replay_pending
		 FROM Sessions WHERE user_id = ? ORDER BY created_at_utc DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionEntity
	for rows.Next() {
		e, err := scanSessionRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// ListByAttachmentStates returns every session whose attachment_state is
// in the supplied list — used by RecoverActiveSessions to lift Attached /
// DetachedGracePeriod / Recovering rows after gateway restart.
func (r *SessionsRepo) ListByAttachmentStates(ctx context.Context, states []string) ([]SessionEntity, error) {
	if len(states) == 0 {
		return nil, nil
	}
	// Build `?, ?, ?` placeholders.
	q := `SELECT session_id, user_id, worker_id, worker_connection_id,
	             columns, rows, created_at_utc, last_activity_at_utc,
	             attachment_state, attached_client_connection_id,
	             exit_code, exit_reason, replay_pending
	      FROM Sessions WHERE attachment_state IN (?` + repeatComma(len(states)-1) + `)`
	args := make([]any, len(states))
	for i, s := range states {
		args[i] = s
	}
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionEntity
	for rows.Next() {
		e, err := scanSessionRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

func repeatComma(n int) string {
	if n <= 0 {
		return ""
	}
	out := make([]byte, 0, 2*n)
	for i := 0; i < n; i++ {
		out = append(out, ',', '?')
	}
	return string(out)
}

// Delete removes a session row by id.
func (r *SessionsRepo) Delete(ctx context.Context, sessionID string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM Sessions WHERE session_id = ?`, sessionID)
	return err
}

// UpdateState mutates attachment_state + connected client + last-activity
// + optional replay flag in a single UPDATE. Mirrors PersistSessionStateAsync
// in DbSessionCoordinator.cs. Any nil pointer arg means "do not change".
func (r *SessionsRepo) UpdateState(ctx context.Context, sessionID string, state string,
	attachedClient *string, workerConnection *string,
	exitCode *int, exitReason *string, replayPending *bool,
	lastActivityUTC *time.Time) error {

	// Build a dynamic UPDATE so we only touch the columns the caller asked
	// about. The C# code uses entity-level tracking which is awkward in Go.
	sets := []string{"attachment_state = ?"}
	args := []any{state}

	if attachedClient != nil {
		sets = append(sets, "attached_client_connection_id = ?")
		if *attachedClient == "" {
			args = append(args, nil)
		} else {
			args = append(args, *attachedClient)
		}
	}
	if workerConnection != nil {
		sets = append(sets, "worker_connection_id = ?")
		if *workerConnection == "" {
			args = append(args, nil)
		} else {
			args = append(args, *workerConnection)
		}
	}
	if exitCode != nil {
		sets = append(sets, "exit_code = ?")
		args = append(args, *exitCode)
	}
	if exitReason != nil {
		sets = append(sets, "exit_reason = ?")
		if *exitReason == "" {
			args = append(args, nil)
		} else {
			args = append(args, *exitReason)
		}
	}
	if replayPending != nil {
		sets = append(sets, "replay_pending = ?")
		args = append(args, *replayPending)
	}
	if lastActivityUTC != nil {
		sets = append(sets, "last_activity_at_utc = ?")
		args = append(args, lastActivityUTC.UTC().Format(time.RFC3339Nano))
	}
	args = append(args, sessionID)

	q := "UPDATE Sessions SET "
	for i, s := range sets {
		if i > 0 {
			q += ", "
		}
		q += s
	}
	q += " WHERE session_id = ?"
	_, err := r.db.ExecContext(ctx, q, args...)
	return err
}

// TouchActivity bumps last_activity_at_utc if the new value is at least
// 5 seconds ahead of the last touch. Mirrors TouchSessionActivity.
func (r *SessionsRepo) TouchActivity(ctx context.Context, sessionID string, now time.Time) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE Sessions SET last_activity_at_utc = ? WHERE session_id = ?`,
		now.UTC().Format(time.RFC3339Nano), sessionID)
	return err
}

// scanSession wraps a *sql.Row produced by Find/FindForUser.
func scanSession(row *sql.Row) (*SessionEntity, error) {
	var e SessionEntity
	err := row.Scan(
		&e.SessionID, &e.UserID, &e.WorkerID, &e.WorkerConnectionID,
		&e.Columns, &e.Rows, &e.CreatedAtUTC, &e.LastActivityAtUTC,
		&e.AttachmentState, &e.AttachedClientConnectionID,
		&e.ExitCode, &e.ExitReason, &e.ReplayPending)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan session: %w", err)
	}
	return &e, nil
}

// scanSessionRows wraps *sql.Rows from List*/ListByAttachmentStates.
func scanSessionRows(rows *sql.Rows) (*SessionEntity, error) {
	var e SessionEntity
	err := rows.Scan(
		&e.SessionID, &e.UserID, &e.WorkerID, &e.WorkerConnectionID,
		&e.Columns, &e.Rows, &e.CreatedAtUTC, &e.LastActivityAtUTC,
		&e.AttachmentState, &e.AttachedClientConnectionID,
		&e.ExitCode, &e.ExitReason, &e.ReplayPending)
	if err != nil {
		return nil, fmt.Errorf("scan session row: %w", err)
	}
	return &e, nil
}