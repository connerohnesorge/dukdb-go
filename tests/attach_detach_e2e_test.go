package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/require"
)

// TestAttachDetach tests basic ATTACH and DETACH operations.
func TestAttachDetach(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Attach a new database
	_, err = db.Exec("ATTACH ':memory:' AS mydb")
	require.NoError(t, err)

	// Detach it
	_, err = db.Exec("DETACH mydb")
	require.NoError(t, err)
}

// TestAttachDatabaseKeyword tests ATTACH with the optional DATABASE keyword.
func TestAttachDatabaseKeyword(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("ATTACH DATABASE ':memory:' AS testdb")
	require.NoError(t, err)

	_, err = db.Exec("DETACH DATABASE testdb")
	require.NoError(t, err)
}

// TestCreateDropDatabase tests CREATE DATABASE and DROP DATABASE.
func TestCreateDropDatabase(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE DATABASE testdb")
	require.NoError(t, err)

	_, err = db.Exec("DROP DATABASE testdb")
	require.NoError(t, err)
}

// TestUseDatabase tests the USE statement.
func TestUseDatabase(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE DATABASE newdb")
	require.NoError(t, err)

	_, err = db.Exec("USE newdb")
	require.NoError(t, err)
}

// TestDetachIfExists tests that DETACH IF EXISTS does not error on non-existent databases.
func TestDetachIfExists(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Should not error
	_, err = db.Exec("DETACH IF EXISTS nonexistent")
	require.NoError(t, err)
}

// TestCreateDatabaseIfNotExists tests that CREATE DATABASE IF NOT EXISTS
// does not error when the database already exists.
func TestCreateDatabaseIfNotExists(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("CREATE DATABASE dup")
	require.NoError(t, err)

	// Should not error
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS dup")
	require.NoError(t, err)

	// Without IF NOT EXISTS, should error
	_, err = db.Exec("CREATE DATABASE dup")
	require.Error(t, err)
}

// TestDropDatabaseIfExists tests that DROP DATABASE IF EXISTS
// does not error when the database does not exist.
func TestDropDatabaseIfExists(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Should not error
	_, err = db.Exec("DROP DATABASE IF EXISTS nonexistent")
	require.NoError(t, err)
}

// TestAttachDuplicateError tests that attaching a database with a duplicate alias fails.
func TestAttachDuplicateError(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("ATTACH ':memory:' AS dupdb")
	require.NoError(t, err)

	_, err = db.Exec("ATTACH ':memory:' AS dupdb")
	require.Error(t, err)
}

// TestDetachNonExistentError tests that detaching a non-existent database fails.
func TestDetachNonExistentError(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("DETACH nonexistent")
	require.Error(t, err)
}

// TestUseNonExistentError tests that USE with a non-existent database fails.
func TestUseNonExistentError(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("USE nonexistent")
	require.Error(t, err)
}

// TestDropNonExistentError tests that DROP DATABASE on a non-existent database fails.
func TestDropNonExistentError(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("DROP DATABASE nonexistent")
	require.Error(t, err)
}

// TestDetachDefaultDatabaseError tests that detaching the default database fails.
func TestDetachDefaultDatabaseError(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("DETACH memory")
	require.Error(t, err)
}

// TestDropDefaultDatabaseError tests that dropping the default database fails.
func TestDropDefaultDatabaseError(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec("DROP DATABASE memory")
	require.Error(t, err)
}
