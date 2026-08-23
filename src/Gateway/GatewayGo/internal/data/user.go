package data

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// User mirrors the Users table row. The Go port only reads/writes the
// columns it actively uses; legacy columns stay in the schema for
// compatibility with C#-provisioned databases. Date columns are stored
// as TEXT (ISO 8601 UTC) per the EF Core → SQLite mapping.
type User struct {
	ID                string
	Username          string
	Email             sql.NullString
	DisplayName       sql.NullString
	AvatarURL         sql.NullString
	Role              string
	Status            string
	AuthProvider      sql.NullString
	AuthProviderID    sql.NullString
	PasswordHash      sql.NullString
	AppleRefreshToken sql.NullString
	DeletedAtUTC      sql.NullString
	CreatedAtUTC      string
	UpdatedAtUTC      string
	LastLoginAtUTC    sql.NullString
}

// ErrNotFound is returned when a lookup yields no rows.
var ErrNotFound = errors.New("data: not found")

// SeedDevUser inserts the `test / test123` admin account if the Users
// table is empty. Matches C# Program.cs lines 360-378.
func SeedDevUser(ctx context.Context, db *sql.DB) error {
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM Users`).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	hash, err := bcrypt.GenerateFromPassword([]byte("test123"), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	id := "dev-test-" + now[:10] // matches the C# GUID.NewGuid().ToString("N") length behaviour
	// Generate 32 hex chars like C# does.
	const hex = "0123456789abcdef"
	b := make([]byte, 32)
	for i := range b {
		b[i] = hex[int(time.Now().UnixNano()+int64(i))%16]
	}
	id = string(b)

	_, err = db.ExecContext(ctx, `
		INSERT INTO Users (
			id, username, email, display_name, role, status,
			auth_provider, auth_provider_id, password_hash,
			created_at_utc, updated_at_utc
		) VALUES (?, ?, NULL, NULL, 'admin', 'active', 'password', 'test', ?, ?, ?)`,
		id, "test", string(hash), now, now,
	)
	if err != nil {
		return err
	}
	// Also seed a password UserIdentity so Phase-1 multi-auth lookup works
	// (mirrors the AddUserIdentities seed).
	_, err = db.ExecContext(ctx, `
		INSERT OR IGNORE INTO UserIdentities
			(id, user_id, auth_provider, auth_provider_id, password_hash, created_at_utc)
		VALUES (?, ?, 'password', 'test', ?, ?)`,
		string(b[:16]), id, string(hash), now,
	)
	return err
}

// FindByUsername returns the user row with the given username, or
// ErrNotFound. Case-insensitive match (SQLite default for TEXT ASCII).
func FindByUsername(ctx context.Context, db *sql.DB, username string) (*User, error) {
	row := db.QueryRowContext(ctx, `
		SELECT id, username, email, display_name, avatar_url, role, status,
		       auth_provider, auth_provider_id, password_hash,
		       apple_refresh_token, deleted_at_utc,
		       created_at_utc, updated_at_utc, last_login_at_utc
		FROM Users WHERE username = ?`, username)
	var u User
	var createdAt, updatedAt string
	if err := row.Scan(
		&u.ID, &u.Username, &u.Email, &u.DisplayName, &u.AvatarURL,
		&u.Role, &u.Status, &u.AuthProvider, &u.AuthProviderID,
		&u.PasswordHash, &u.AppleRefreshToken, &u.DeletedAtUTC,
		&createdAt, &updatedAt, &u.LastLoginAtUTC,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	u.CreatedAtUTC = strings.TrimSpace(createdAt)
	u.UpdatedAtUTC = strings.TrimSpace(updatedAt)
	return &u, nil
}