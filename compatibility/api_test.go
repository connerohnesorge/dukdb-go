package compatibility

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// APICompatibilityTests verifies driver interface compatibility.
var APICompatibilityTests = []CompatibilityTest{
	// Connection lifecycle
	{Name: "OpenClose", Category: "api", Test: testOpenClose},
	{Name: "Ping", Category: "api", Test: testPing},
	{Name: "PingContext", Category: "api", Test: testPingContext},
	{Name: "ConnRaw", Category: "api", Test: testConnRaw},

	// Transactions
	{Name: "BeginCommit", Category: "api", Test: testBeginCommit},
	{Name: "BeginRollback", Category: "api", Test: testBeginRollback},
	{Name: "TxSavepoint", Category: "api", Test: testTxSavepoint},
	{Name: "TxNestedSavepoint", Category: "api", Test: testTxNestedSavepoint},

	// Prepared statements
	{Name: "PrepareExec", Category: "api", Test: testPrepareExec},
	{Name: "PrepareQuery", Category: "api", Test: testPrepareQuery},
	{Name: "PrepareReuse", Category: "api", Test: testPrepareReuse},

	// Parameter binding
	{Name: "PositionalParams", Category: "api", Test: testPositionalParams},
	{Name: "NamedParams", Category: "api", Test: testNamedParams},
	{Name: "NullParams", Category: "api", Test: testNullParams},
	{Name: "ParamReuse", Category: "api", Test: testParamReuse},

	// Result scanning
	{Name: "ScanPrimitives", Category: "api", Test: testScanPrimitives},
	{Name: "ScanNulls", Category: "api", Test: testScanNulls},

	// Row iteration
	{Name: "RowsColumns", Category: "api", Test: testRowsColumns},
	{Name: "RowsNext", Category: "api", Test: testRowsNext},
	{Name: "RowsClose", Category: "api", Test: testRowsClose},
	{Name: "RowsErr", Category: "api", Test: testRowsErr},

	// Result info
	{Name: "ResultRowsAffected", Category: "api", Test: testResultRowsAffected},
	{Name: "ResultLastInsertId", Category: "api", Test: testResultLastInsertId},
}

// ErrorCompatibilityTests verifies error behavior matches.
var ErrorCompatibilityTests = []CompatibilityTest{
	// Syntax errors
	{Name: "ErrSyntaxInvalid", Category: "error", Test: testErrSyntaxInvalid},

	// Constraint errors
	{Name: "ErrConstraintPrimaryKey", Category: "error", Test: testErrConstraintPrimaryKey},
	{Name: "ErrConstraintUnique", Category: "error", Test: testErrConstraintUnique},
	{Name: "ErrConstraintNotNull", Category: "error", Test: testErrConstraintNotNull},

	// Not found errors
	{Name: "ErrTableNotFound", Category: "error", Test: testErrTableNotFound},
	{Name: "ErrColumnNotFound", Category: "error", Test: testErrColumnNotFound},
}

// Connection lifecycle tests

func testOpenClose(t *testing.T, db *sql.DB) {
	// db is already open from framework
	// Verify we can use it
	var result int
	err := db.QueryRow("SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}

func testPing(t *testing.T, db *sql.DB) {
	err := db.Ping()
	require.NoError(t, err)
}

func testPingContext(t *testing.T, db *sql.DB) {
	ctx := context.Background()
	err := db.PingContext(ctx)
	require.NoError(t, err)
}

func testConnRaw(t *testing.T, db *sql.DB) {
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	// Test Raw() access to underlying driver connection
	err = conn.Raw(func(driverConn any) error {
		// Both implementations should expose a non-nil connection
		assert.NotNil(t, driverConn)
		return nil
	})
	require.NoError(t, err)
}

// Transaction tests

func testBeginCommit(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE tx_commit (val INTEGER)`)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO tx_commit VALUES (42)`)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify data persisted
	var val int
	err = db.QueryRow(`SELECT val FROM tx_commit`).Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, 42, val)
}

func testBeginRollback(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE tx_rollback (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO tx_rollback VALUES (1)`)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO tx_rollback VALUES (2)`)
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)

	// Verify data was rolled back
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM tx_rollback`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func testTxSavepoint(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE sp_test (val INTEGER)`)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO sp_test VALUES (1)`)
	require.NoError(t, err)

	_, err = tx.Exec(`SAVEPOINT sp1`)
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO sp_test VALUES (2)`)
	require.NoError(t, err)

	_, err = tx.Exec(`ROLLBACK TO sp1`)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	// Only value 1 should exist
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM sp_test`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func testTxNestedSavepoint(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE nested_sp (val INTEGER)`)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO nested_sp VALUES (1)`)
	require.NoError(t, err)

	_, err = tx.Exec(`SAVEPOINT sp1`)
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO nested_sp VALUES (2)`)
	require.NoError(t, err)

	_, err = tx.Exec(`SAVEPOINT sp2`)
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO nested_sp VALUES (3)`)
	require.NoError(t, err)

	// Rollback to sp2 (value 3 should be gone)
	_, err = tx.Exec(`ROLLBACK TO sp2`)
	require.NoError(t, err)

	// Rollback to sp1 (value 2 should be gone)
	_, err = tx.Exec(`ROLLBACK TO sp1`)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	// Only value 1 should exist
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM nested_sp`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// Prepared statement tests

func testPrepareExec(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE prep_exec (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	stmt, err := db.Prepare(`INSERT INTO prep_exec VALUES ($1, $2)`)
	require.NoError(t, err)
	defer stmt.Close()

	result, err := stmt.Exec(1, "Alice")
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)
}

func testPrepareQuery(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE prep_query (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO prep_query VALUES (1, 'Alice'), (2, 'Bob')`)
	require.NoError(t, err)

	stmt, err := db.Prepare(`SELECT name FROM prep_query WHERE id = $1`)
	require.NoError(t, err)
	defer stmt.Close()

	var name string
	err = stmt.QueryRow(2).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "Bob", name)
}

func testPrepareReuse(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE prep_reuse (id INTEGER)`)
	require.NoError(t, err)

	stmt, err := db.Prepare(`INSERT INTO prep_reuse VALUES ($1)`)
	require.NoError(t, err)
	defer stmt.Close()

	// Execute multiple times
	for i := 1; i <= 5; i++ {
		_, err = stmt.Exec(i)
		require.NoError(t, err)
	}

	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM prep_reuse`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 5, count)
}

// Parameter binding tests

func testPositionalParams(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE pos_params (a INTEGER, b INTEGER, c INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO pos_params VALUES ($1, $2, $3)`, 1, 2, 3)
	require.NoError(t, err)

	var a, b, c int
	err = db.QueryRow(`SELECT * FROM pos_params`).Scan(&a, &b, &c)
	require.NoError(t, err)
	assert.Equal(t, 1, a)
	assert.Equal(t, 2, b)
	assert.Equal(t, 3, c)
}

func testNamedParams(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE named_params (name VARCHAR, age INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO named_params VALUES ($name, $age)`,
		sql.Named("name", "Alice"),
		sql.Named("age", 30))
	require.NoError(t, err)

	var name string
	var age int
	err = db.QueryRow(`SELECT * FROM named_params`).Scan(&name, &age)
	require.NoError(t, err)
	assert.Equal(t, "Alice", name)
	assert.Equal(t, 30, age)
}

func testNullParams(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE null_params (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO null_params VALUES ($1)`, nil)
	require.NoError(t, err)

	var val sql.NullInt64
	err = db.QueryRow(`SELECT val FROM null_params`).Scan(&val)
	require.NoError(t, err)
	assert.False(t, val.Valid)
}

func testParamReuse(t *testing.T, db *sql.DB) {
	// Test using the same parameter multiple times
	var a, b int
	err := db.QueryRow(`SELECT $1, $1`, 42).Scan(&a, &b)
	require.NoError(t, err)
	assert.Equal(t, 42, a)
	assert.Equal(t, 42, b)
}

// Result scanning tests

func testScanPrimitives(t *testing.T, db *sql.DB) {
	var (
		intVal    int
		int64Val  int64
		floatVal  float64
		stringVal string
		boolVal   bool
	)

	err := db.QueryRow(`SELECT 42, 9223372036854775807, 3.14, 'hello', true`).
		Scan(&intVal, &int64Val, &floatVal, &stringVal, &boolVal)
	require.NoError(t, err)

	assert.Equal(t, 42, intVal)
	assert.Equal(t, int64(9223372036854775807), int64Val)
	assert.InDelta(t, 3.14, floatVal, 0.001)
	assert.Equal(t, "hello", stringVal)
	assert.True(t, boolVal)
}

func testScanNulls(t *testing.T, db *sql.DB) {
	var (
		intVal    sql.NullInt64
		floatVal  sql.NullFloat64
		stringVal sql.NullString
		boolVal   sql.NullBool
	)

	err := db.QueryRow(`SELECT NULL, NULL, NULL, NULL`).
		Scan(&intVal, &floatVal, &stringVal, &boolVal)
	require.NoError(t, err)

	assert.False(t, intVal.Valid)
	assert.False(t, floatVal.Valid)
	assert.False(t, stringVal.Valid)
	assert.False(t, boolVal.Valid)
}

// Row iteration tests

func testRowsColumns(t *testing.T, db *sql.DB) {
	rows, err := db.Query(`SELECT 1 as col_a, 'hello' as col_b, 3.14 as col_c`)
	require.NoError(t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(t, err)

	require.Len(t, cols, 3)
	assert.Equal(t, "col_a", cols[0])
	assert.Equal(t, "col_b", cols[1])
	assert.Equal(t, "col_c", cols[2])
}

func testRowsNext(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE rows_next (id INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO rows_next VALUES (1), (2), (3)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT id FROM rows_next ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int{1, 2, 3}, ids)
}

func testRowsClose(t *testing.T, db *sql.DB) {
	rows, err := db.Query(`SELECT 1, 2, 3`)
	require.NoError(t, err)

	// Close without iterating
	err = rows.Close()
	require.NoError(t, err)

	// Closing again should be safe
	err = rows.Close()
	require.NoError(t, err)
}

func testRowsErr(t *testing.T, db *sql.DB) {
	rows, err := db.Query(`SELECT 1`)
	require.NoError(t, err)

	// Iterate to completion
	for rows.Next() {
		var val int
		require.NoError(t, rows.Scan(&val))
	}

	// Err should be nil after successful iteration
	require.NoError(t, rows.Err())
	rows.Close()
}

// Result info tests

func testResultRowsAffected(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE rows_affected (id INTEGER)`)
	require.NoError(t, err)

	result, err := db.Exec(`INSERT INTO rows_affected VALUES (1), (2), (3)`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(3), rowsAffected)
}

func testResultLastInsertId(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE last_insert (id INTEGER)`)
	require.NoError(t, err)

	result, err := db.Exec(`INSERT INTO last_insert VALUES (42)`)
	require.NoError(t, err)

	// DuckDB doesn't support LastInsertId in the same way as MySQL
	// but we verify the method exists and returns without panic
	_, _ = result.LastInsertId()
}

// Error tests

func testErrSyntaxInvalid(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`SELEKT 1`)
	require.Error(t, err)
}

func testErrConstraintPrimaryKey(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE pk_test (id INTEGER PRIMARY KEY)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO pk_test VALUES (1)`)
	require.NoError(t, err)

	// Duplicate key should fail
	_, err = db.Exec(`INSERT INTO pk_test VALUES (1)`)
	require.Error(t, err)
}

func testErrConstraintUnique(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE unique_test (val INTEGER UNIQUE)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO unique_test VALUES (1)`)
	require.NoError(t, err)

	// Duplicate unique value should fail
	_, err = db.Exec(`INSERT INTO unique_test VALUES (1)`)
	require.Error(t, err)
}

func testErrConstraintNotNull(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE notnull_test (val INTEGER NOT NULL)`)
	require.NoError(t, err)

	// NULL in NOT NULL column should fail
	_, err = db.Exec(`INSERT INTO notnull_test VALUES (NULL)`)
	require.Error(t, err)
}

func testErrTableNotFound(t *testing.T, db *sql.DB) {
	_, err := db.Query(`SELECT * FROM nonexistent_table_xyz`)
	require.Error(t, err)
}

func testErrColumnNotFound(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE col_test (id INTEGER)`)
	require.NoError(t, err)

	_, err = db.Query(`SELECT nonexistent_column FROM col_test`)
	require.Error(t, err)
}
