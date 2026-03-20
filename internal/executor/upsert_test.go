package executor

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupUpsertTable creates a "kv" table with (key INTEGER PRIMARY KEY, value VARCHAR).
func setupUpsertTable(t *testing.T, exec *Executor, cat *catalog.Catalog) {
	t.Helper()

	cols := []*catalog.ColumnDef{
		{Name: "key", Type: dukdb.TYPE_INTEGER, Nullable: false},
		{Name: "value", Type: dukdb.TYPE_VARCHAR, Nullable: true},
	}
	tableDef := catalog.NewTableDef("kv", cols)
	tableDef.PrimaryKey = []int{0}
	err := cat.CreateTableInSchema("main", tableDef)
	require.NoError(t, err)

	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	_, err = exec.storage.CreateTable("kv", types)
	require.NoError(t, err)
}

func TestUpsert_DoNothing_SkipsConflict(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	setupUpsertTable(t, exec, cat)

	// Insert initial row
	_, err := executeSQL(t, exec, cat, "INSERT INTO kv (key, value) VALUES (1, 'hello')")
	require.NoError(t, err)

	// Try to insert same key with ON CONFLICT DO NOTHING
	result, err := executeSQL(t, exec, cat,
		"INSERT INTO kv (key, value) VALUES (1, 'world') ON CONFLICT (key) DO NOTHING")
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsAffected, "DO NOTHING should skip conflicting row")

	// Verify original value is unchanged
	selectResult, err := executeSQL(t, exec, cat, "SELECT value FROM kv WHERE key = 1")
	require.NoError(t, err)
	require.Len(t, selectResult.Rows, 1)
	assert.Equal(t, "hello", selectResult.Rows[0]["value"])
}

func TestUpsert_DoNothing_InsertsNonConflict(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	setupUpsertTable(t, exec, cat)

	_, err := executeSQL(t, exec, cat, "INSERT INTO kv (key, value) VALUES (1, 'hello')")
	require.NoError(t, err)

	result, err := executeSQL(t, exec, cat,
		"INSERT INTO kv (key, value) VALUES (2, 'world') ON CONFLICT (key) DO NOTHING")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected, "Non-conflicting row should be inserted")

	selectResult, err := executeSQL(t, exec, cat, "SELECT key, value FROM kv ORDER BY key")
	require.NoError(t, err)
	require.Len(t, selectResult.Rows, 2)
	assert.Equal(t, "hello", selectResult.Rows[0]["value"])
	assert.Equal(t, "world", selectResult.Rows[1]["value"])
}

func TestUpsert_DoNothing_MixedConflictAndNon(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	setupUpsertTable(t, exec, cat)

	_, err := executeSQL(t, exec, cat, "INSERT INTO kv (key, value) VALUES (1, 'hello')")
	require.NoError(t, err)

	result, err := executeSQL(t, exec, cat,
		"INSERT INTO kv (key, value) VALUES (1, 'conflict'), (2, 'new') ON CONFLICT (key) DO NOTHING")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected, "Should insert only the non-conflicting row")

	selectResult, err := executeSQL(t, exec, cat, "SELECT key, value FROM kv ORDER BY key")
	require.NoError(t, err)
	require.Len(t, selectResult.Rows, 2)
	assert.Equal(t, "hello", selectResult.Rows[0]["value"], "key=1 should keep original value")
	assert.Equal(t, "new", selectResult.Rows[1]["value"], "key=2 should be inserted")
}

func TestUpsert_DoUpdate_UpdatesConflict(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	setupUpsertTable(t, exec, cat)

	_, err := executeSQL(t, exec, cat, "INSERT INTO kv (key, value) VALUES (1, 'hello')")
	require.NoError(t, err)

	result, err := executeSQL(t, exec, cat,
		"INSERT INTO kv (key, value) VALUES (1, 'world') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected, "DO UPDATE should count as 1 affected row")

	selectResult, err := executeSQL(t, exec, cat, "SELECT value FROM kv WHERE key = 1")
	require.NoError(t, err)
	require.Len(t, selectResult.Rows, 1)
	assert.Equal(t, "world", selectResult.Rows[0]["value"], "Value should be updated to EXCLUDED.value")
}

func TestUpsert_DoUpdate_InsertsNonConflict(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	setupUpsertTable(t, exec, cat)

	_, err := executeSQL(t, exec, cat, "INSERT INTO kv (key, value) VALUES (1, 'hello')")
	require.NoError(t, err)

	result, err := executeSQL(t, exec, cat,
		"INSERT INTO kv (key, value) VALUES (2, 'world') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RowsAffected, "Non-conflicting row should be inserted")

	selectResult, err := executeSQL(t, exec, cat, "SELECT key, value FROM kv ORDER BY key")
	require.NoError(t, err)
	require.Len(t, selectResult.Rows, 2)
	assert.Equal(t, "hello", selectResult.Rows[0]["value"])
	assert.Equal(t, "world", selectResult.Rows[1]["value"])
}

func TestUpsert_DoUpdate_MultipleRows(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	setupUpsertTable(t, exec, cat)

	_, err := executeSQL(t, exec, cat, "INSERT INTO kv (key, value) VALUES (1, 'a'), (2, 'b')")
	require.NoError(t, err)

	result, err := executeSQL(t, exec, cat,
		"INSERT INTO kv (key, value) VALUES (1, 'x'), (2, 'y'), (3, 'z') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
	require.NoError(t, err)
	assert.Equal(t, int64(3), result.RowsAffected, "Should affect 3 rows (2 updates + 1 insert)")

	selectResult, err := executeSQL(t, exec, cat, "SELECT key, value FROM kv ORDER BY key")
	require.NoError(t, err)
	require.Len(t, selectResult.Rows, 3)
	assert.Equal(t, "x", selectResult.Rows[0]["value"])
	assert.Equal(t, "y", selectResult.Rows[1]["value"])
	assert.Equal(t, "z", selectResult.Rows[2]["value"])
}

func TestUpsert_DoNothing_InfersPKWhenNoConflictTarget(t *testing.T) {
	exec, cat, _ := setupTestExecutor()
	setupUpsertTable(t, exec, cat)

	_, err := executeSQL(t, exec, cat, "INSERT INTO kv (key, value) VALUES (1, 'hello')")
	require.NoError(t, err)

	result, err := executeSQL(t, exec, cat,
		"INSERT INTO kv (key, value) VALUES (1, 'world') ON CONFLICT DO NOTHING")
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.RowsAffected, "Should skip conflicting row")

	selectResult, err := executeSQL(t, exec, cat, "SELECT value FROM kv WHERE key = 1")
	require.NoError(t, err)
	require.Len(t, selectResult.Rows, 1)
	assert.Equal(t, "hello", selectResult.Rows[0]["value"])
}

// Parser-only tests

func TestUpsert_Parser_DoNothing(t *testing.T) {
	stmt, err := parser.Parse("INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO NOTHING")
	require.NoError(t, err)

	insertStmt, ok := stmt.(*parser.InsertStmt)
	require.True(t, ok)
	require.NotNil(t, insertStmt.OnConflict)
	assert.Equal(t, parser.OnConflictDoNothing, insertStmt.OnConflict.Action)
	assert.Equal(t, []string{"a"}, insertStmt.OnConflict.ConflictColumns)
	assert.Len(t, insertStmt.OnConflict.UpdateSet, 0)
}

func TestUpsert_Parser_DoUpdate(t *testing.T) {
	stmt, err := parser.Parse(
		"INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b")
	require.NoError(t, err)

	insertStmt, ok := stmt.(*parser.InsertStmt)
	require.True(t, ok)
	require.NotNil(t, insertStmt.OnConflict)
	assert.Equal(t, parser.OnConflictDoUpdate, insertStmt.OnConflict.Action)
	assert.Equal(t, []string{"a"}, insertStmt.OnConflict.ConflictColumns)
	require.Len(t, insertStmt.OnConflict.UpdateSet, 1)
	assert.Equal(t, "b", insertStmt.OnConflict.UpdateSet[0].Column)
}

func TestUpsert_Parser_DoUpdateNoConflictTarget(t *testing.T) {
	stmt, err := parser.Parse(
		"INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT DO NOTHING")
	require.NoError(t, err)

	insertStmt, ok := stmt.(*parser.InsertStmt)
	require.True(t, ok)
	require.NotNil(t, insertStmt.OnConflict)
	assert.Equal(t, parser.OnConflictDoNothing, insertStmt.OnConflict.Action)
	assert.Len(t, insertStmt.OnConflict.ConflictColumns, 0)
}

func TestUpsert_Parser_DoUpdateMultipleSetClauses(t *testing.T) {
	stmt, err := parser.Parse(
		"INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b, c = EXCLUDED.c")
	require.NoError(t, err)

	insertStmt, ok := stmt.(*parser.InsertStmt)
	require.True(t, ok)
	require.NotNil(t, insertStmt.OnConflict)
	assert.Equal(t, parser.OnConflictDoUpdate, insertStmt.OnConflict.Action)
	require.Len(t, insertStmt.OnConflict.UpdateSet, 2)
	assert.Equal(t, "b", insertStmt.OnConflict.UpdateSet[0].Column)
	assert.Equal(t, "c", insertStmt.OnConflict.UpdateSet[1].Column)
}

func TestUpsert_Parser_WithReturning(t *testing.T) {
	stmt, err := parser.Parse(
		"INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO NOTHING RETURNING *")
	require.NoError(t, err)

	insertStmt, ok := stmt.(*parser.InsertStmt)
	require.True(t, ok)
	require.NotNil(t, insertStmt.OnConflict)
	assert.Equal(t, parser.OnConflictDoNothing, insertStmt.OnConflict.Action)
	assert.Len(t, insertStmt.Returning, 1)
}
