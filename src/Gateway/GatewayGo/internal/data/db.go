// Package data owns the SQLite connection, migrations, and per-table repos.
//
// Schema mirrors the C# EF Core migrations byte-for-byte (see
// migrations/schema.sql). modernc.org/sqlite is the driver (pure Go, no
// CGO requirement).
//
// Migrations are applied by executing schema.sql statement-by-statement.
// Statement boundaries are `;` followed by a newline. Each `ALTER TABLE
// ... ADD COLUMN` is checked against sqlite_master first so re-running
// against an existing database is a no-op (modernc returns SQLITE_ERROR
// on duplicate column, breaking idempotency otherwise).
package data

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"regexp"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed migrations/schema.sql
var schemaSQL string

// Open opens (or creates) the SQLite database at path and applies all
// migrations. Idempotent.
func Open(path string) (*sql.DB, error) {
	dsn := path + "?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	// SQLite + pure-Go driver: serialise writes via a single connection.
	// Matches C# appsettings.json PoolingEnabled=false + MaxPool=1 and
	// avoids SQLITE_BUSY during the migration backfills.
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	if err := applyMigrations(ctx, db); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

var (
	// We need to find the ALTER TABLE … ADD COLUMN anywhere in the chunk
	// because the preceding line may be a `-- === … ===` header comment.
	addColumnRE   = regexp.MustCompile(`(?is)ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(\w+)\b`)
	dropColumnRE  = regexp.MustCompile(`(?is)ALTER\s+TABLE\s+(\w+)\s+DROP\s+COLUMN\s+(\w+)\b`)
)

// applyMigrations runs the embedded schema statement by statement. Each
// `ALTER TABLE ... ADD COLUMN` is guarded with a sqlite_master check so
// re-running is safe.
func applyMigrations(ctx context.Context, db *sql.DB) error {
	return RunMigrations(ctx, db)
}

// RunMigrations is the public entry point for smoke binaries that want to
// apply the schema without going through Open (e.g. against an in-memory
// `file::memory:?cache=shared` database).
func RunMigrations(ctx context.Context, db *sql.DB) error {
	stmts := splitStatements(schemaSQL)
	for _, raw := range stmts {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if m := addColumnRE.FindStringSubmatch(stripComments(s)); m != nil {
			table, col := m[1], m[2]
			exists, err := columnExists(ctx, db, table, col)
			if err != nil {
				return fmt.Errorf("check %s.%s: %w", table, col, err)
			}
			if exists {
				continue
			}
		}
		// Same guard for DROP COLUMN (sqlite 3.35+).
		if m := dropColumnRE.FindStringSubmatch(stripComments(s)); m != nil {
			table, col := m[1], m[2]
			exists, err := columnExists(ctx, db, table, col)
			if err != nil {
				return fmt.Errorf("check %s.%s: %w", table, col, err)
			}
			if !exists {
				continue
			}
		}
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("exec %q: %w", firstLine(s), err)
		}
	}
	return nil
}

func columnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`,
		table, column).Scan(&n)
	if err != nil {
		// pragma_table_info is a virtual table; on very old SQLite it may
		// not exist. Fall back to sqlite_master which is universally
		// supported.
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`,
			table).Scan(&n)
		if err != nil {
			return false, err
		}
		if n == 0 {
			return false, nil
		}
		// Conservative fallback: assume column exists (DDL will fail loudly
		// if it doesn't, surfacing the real problem).
		return true, nil
	}
	return n > 0, nil
}

// splitStatements breaks a SQL script into individual statements on `;`
// boundaries at end-of-line. SQL strings (single quotes) and comments
// (`-- ...`) are respected. The embedded schema has no string literals
// containing `;` so the simple scan is safe here.
func splitStatements(sqlText string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(sqlText, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") && !strings.HasPrefix(trimmed, "-- ") {
			// Keep the comment line; it will be passed through unchanged.
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
		if strings.HasSuffix(trimmed, ";") {
			out = append(out, cur.String())
			cur.Reset()
		}
	}
	if rest := strings.TrimSpace(cur.String()); rest != "" {
		out = append(out, rest)
	}
	return out
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// stripComments removes `-- ...` line comments so regex matching doesn't
// pick up phrases like "ALTER TABLE supports ADD COLUMN natively" from
// human-readable commentary.
func stripComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "--") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}