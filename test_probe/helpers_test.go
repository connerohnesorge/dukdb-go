package test_probe

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

// openDB opens a fresh in-memory dukdb database and registers cleanup.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open dukdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// logResult logs success or failure for a sub-operation.
func logResult(t *testing.T, label string, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("[FAIL] %s: %v", label, err)
	} else {
		t.Logf("[OK] %s", label)
	}
}
