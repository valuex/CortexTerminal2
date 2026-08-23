package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// TunnelEntity mirrors the Tunnels table. All date columns are ISO-8601
// UTC strings (EF convention).
type TunnelEntity struct {
	ID                 string
	TunnelKey          string
	OwnerUserID        string
	WorkerID           string
	WorkerConnectionID string
	SessionID          string
	Port               int
	SecretHash         string
	TransportType      string
	ExpiresAtUTC       string // ISO-8601 UTC
	CreatedAtUTC       string // ISO-8601 UTC
	RevokedAtUTC       sql.NullString
}

// TransportType constants — C# always writes "http" today. The column is
// reserved for future protocols (e.g. tcp/tls).
const (
	TunnelTransportHTTP = "http"
)

// TunnelsRepo is the persistence layer for the Tunnels table. Mirrors
// the read/write set TunnelRegistry needs.
type TunnelsRepo struct {
	db *sql.DB
}

func NewTunnelsRepo(db *sql.DB) *TunnelsRepo { return &TunnelsRepo{db: db} }

// Insert creates a new tunnel row.
func (r *TunnelsRepo) Insert(ctx context.Context, e TunnelEntity) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO Tunnels (
			id, tunnel_key, owner_user_id, worker_id, worker_connection_id,
			session_id, port, secret_hash, transport_type,
			expires_at_utc, created_at_utc, revoked_at_utc
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.TunnelKey, e.OwnerUserID, e.WorkerID, e.WorkerConnectionID,
		e.SessionID, e.Port, e.SecretHash, e.TransportType,
		e.ExpiresAtUTC, e.CreatedAtUTC, e.RevokedAtUTC)
	return err
}

// FindByKey loads a non-revoked tunnel by its public key. Returns
// (nil, nil) when missing or revoked.
func (r *TunnelsRepo) FindByKey(ctx context.Context, tunnelKey string) (*TunnelEntity, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, tunnel_key, owner_user_id, worker_id, worker_connection_id,
		        session_id, port, secret_hash, transport_type,
		        expires_at_utc, created_at_utc, revoked_at_utc
		 FROM Tunnels
		 WHERE tunnel_key = ? AND revoked_at_utc IS NULL`,
		tunnelKey)
	return scanTunnel(row)
}

// ListForSession returns every non-revoked tunnel owned by ownerUserID
// for sessionID, oldest first.
func (r *TunnelsRepo) ListForSession(ctx context.Context, sessionID, ownerUserID string) ([]TunnelEntity, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, tunnel_key, owner_user_id, worker_id, worker_connection_id,
		        session_id, port, secret_hash, transport_type,
		        expires_at_utc, created_at_utc, revoked_at_utc
		 FROM Tunnels
		 WHERE session_id = ? AND owner_user_id = ? AND revoked_at_utc IS NULL
		 ORDER BY created_at_utc ASC`,
		sessionID, ownerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TunnelEntity
	for rows.Next() {
		e, err := scanTunnelRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// CountActiveForSession returns the count of non-revoked tunnels for
// sessionID. Used by the quota check before creating a new tunnel.
func (r *TunnelsRepo) CountActiveForSession(ctx context.Context, sessionID string) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM Tunnels
		 WHERE session_id = ? AND revoked_at_utc IS NULL`,
		sessionID).Scan(&n)
	return n, err
}

// Revoke flips revoked_at_utc to now for the tunnel iff it is owned by
// ownerUserID and not already revoked. Returns true when a row was
// updated.
func (r *TunnelsRepo) Revoke(ctx context.Context, tunnelID, ownerUserID string, now time.Time) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE Tunnels SET revoked_at_utc = ?
		 WHERE id = ? AND owner_user_id = ? AND revoked_at_utc IS NULL`,
		now.UTC().Format(time.RFC3339Nano), tunnelID, ownerUserID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func scanTunnel(row *sql.Row) (*TunnelEntity, error) {
	var e TunnelEntity
	err := row.Scan(
		&e.ID, &e.TunnelKey, &e.OwnerUserID, &e.WorkerID, &e.WorkerConnectionID,
		&e.SessionID, &e.Port, &e.SecretHash, &e.TransportType,
		&e.ExpiresAtUTC, &e.CreatedAtUTC, &e.RevokedAtUTC,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan tunnel: %w", err)
	}
	return &e, nil
}

func scanTunnelRows(rows *sql.Rows) (*TunnelEntity, error) {
	var e TunnelEntity
	err := rows.Scan(
		&e.ID, &e.TunnelKey, &e.OwnerUserID, &e.WorkerID, &e.WorkerConnectionID,
		&e.SessionID, &e.Port, &e.SecretHash, &e.TransportType,
		&e.ExpiresAtUTC, &e.CreatedAtUTC, &e.RevokedAtUTC,
	)
	if err != nil {
		return nil, fmt.Errorf("scan tunnel row: %w", err)
	}
	return &e, nil
}