// Command db-smoke verifies the SQLite schema matches the C# Gateway after
// migrations. Opens an empty DB at -db, applies schema.sql, then dumps the
// list of tables + indexes. Exits non-zero on any expected table missing.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/data"
)

var expectedTables = []string{
	"Users",
	"UserIdentities",
	"UserPreferences",
	"AuditLogs",
	"Workers",
	"Sessions",
	"Artifacts",
	"Tunnels",
	"SessionAgentEvents",
}

var expectedIndexes = []string{
	"IX_Users_username",
	"IX_UserIdentities_auth_provider_auth_provider_id",
	"IX_UserPreferences_user_id_key",
	"IX_AuditLogs_timestamp",
	"IX_Sessions_user_id",
	"IX_Sessions_worker_id",
	"IX_Sessions_attachment_state",
	"IX_Sessions_created_at_utc",
	"IX_Workers_owner_user_id",
	"IX_Workers_is_online",
	"IX_Artifacts_session_id_filename",
	"IX_Tunnels_tunnel_key",
	"IX_SessionAgentEvents_session_id_created_at_utc",
}

func main() {
	dbPath := flag.String("db", "corterm_gateway.db", "SQLite file path")
	flag.Parse()

	if err := run(*dbPath); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("ALL OK")
}

func run(dbPath string) error {
	db, err := data.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer db.Close()

	tables, err := listTables(db)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	indexes, err := listIndexes(db)
	if err != nil {
		return fmt.Errorf("list indexes: %w", err)
	}

	missing := diff(expectedTables, tables)
	if len(missing) > 0 {
		return fmt.Errorf("missing tables: %v", missing)
	}
	missingIdx := diff(expectedIndexes, indexes)
	if len(missingIdx) > 0 {
		return fmt.Errorf("missing indexes: %v", missingIdx)
	}

	fmt.Println("tables:", strings.Join(tables, ", "))
	fmt.Println("indexes:", strings.Join(indexes, ", "))

	// Bonus: print row counts so the seed step is observable.
	for _, t := range tables {
		var n int
		if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+t).Scan(&n); err != nil {
			fmt.Fprintf(os.Stderr, "count %s: %v\n", t, err)
			continue
		}
		fmt.Printf("  %-22s %d rows\n", t, n)
	}
	return nil
}

func listTables(db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func listIndexes(db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func diff(expected, got []string) []string {
	set := make(map[string]struct{}, len(got))
	for _, g := range got {
		set[g] = struct{}{}
	}
	sort.Strings(expected)
	var missing []string
	for _, e := range expected {
		if _, ok := set[e]; !ok {
			missing = append(missing, e)
		}
	}
	return missing
}