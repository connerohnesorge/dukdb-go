package engine

import (
	"context"
	"database/sql/driver"
	"sync"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
)

func TestEngineOpen(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	if conn == nil {
		t.Error("Connection should not be nil")
	}
}

func TestEnginePing(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	err = conn.Ping(context.Background())
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestEngineCreateAndQuery(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data
	affected, err := conn.Execute(
		context.Background(),
		"INSERT INTO users (id, name) VALUES (1, 'Alice')",
		nil,
	)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if affected != 1 {
		t.Errorf(
			"Expected 1 row affected, got %d",
			affected,
		)
	}

	// Query data
	rows, columns, err := conn.Query(
		context.Background(),
		"SELECT * FROM users",
		nil,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf(
			"Expected 1 row, got %d",
			len(rows),
		)
	}

	if len(columns) != 2 {
		t.Errorf(
			"Expected 2 columns, got %d",
			len(columns),
		)
	}
}

func TestEngineMultipleOperations(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert multiple rows
	inserts := []string{
		"INSERT INTO products (id, name, price) VALUES (1, 'Apple', 1.50)",
		"INSERT INTO products (id, name, price) VALUES (2, 'Banana', 0.75)",
		"INSERT INTO products (id, name, price) VALUES (3, 'Orange', 2.00)",
	}

	for _, sql := range inserts {
		_, err = conn.Execute(
			context.Background(),
			sql,
			nil,
		)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Query with filter
	rows, _, err := conn.Query(
		context.Background(),
		"SELECT * FROM products WHERE price > 1.0",
		nil,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf(
			"Expected 2 rows where price > 1.0, got %d",
			len(rows),
		)
	}

	// Query with order
	rows, _, err = conn.Query(
		context.Background(),
		"SELECT * FROM products ORDER BY price",
		nil,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf(
			"Expected 3 rows, got %d",
			len(rows),
		)
	}

	// Query with limit
	rows, _, err = conn.Query(
		context.Background(),
		"SELECT * FROM products LIMIT 2",
		nil,
	)
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf(
			"Expected 2 rows with LIMIT, got %d",
			len(rows),
		)
	}

	// Drop table
	_, err = conn.Execute(
		context.Background(),
		"DROP TABLE products",
		nil,
	)
	if err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}
}

func TestEnginePreparedStatement(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE users (id INTEGER, name VARCHAR)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Prepare statement
	stmt, err := conn.Prepare(
		context.Background(),
		"SELECT * FROM users",
	)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	if stmt.NumInput() != 0 {
		t.Errorf(
			"Expected 0 parameters, got %d",
			stmt.NumInput(),
		)
	}
}

func TestEngineCloseConnection(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = conn.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Operations on closed connection should fail
	err = conn.Ping(context.Background())
	if err != dukdb.ErrConnectionClosed {
		t.Errorf(
			"Expected ErrConnectionClosed, got %v",
			err,
		)
	}
}

func TestEngineConcurrentQueries(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE counters (id INTEGER, val INTEGER)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert initial data
	_, err = conn.Execute(
		context.Background(),
		"INSERT INTO counters (id, val) VALUES (1, 0)",
		nil,
	)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Run concurrent queries
	var wg sync.WaitGroup
	numGoroutines := 10
	numQueries := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numQueries; j++ {
				_, _, err := conn.Query(
					context.Background(),
					"SELECT * FROM counters",
					nil,
				)
				if err != nil {
					t.Errorf(
						"Concurrent query failed: %v",
						err,
					)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestEngineMultipleConnections(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	// Open multiple connections
	var conns []dukdb.BackendConn
	for i := 0; i < 5; i++ {
		conn, err := engine.Open(":memory:", nil)
		if err != nil {
			t.Fatalf("Open failed: %v", err)
		}
		conns = append(conns, conn)
	}

	// Close all connections
	for _, conn := range conns {
		err := conn.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}
	}
}

func TestEngineConfig(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	config := &dukdb.Config{
		AccessMode: "read_write",
		Threads:    4,
		MaxMemory:  "1GB",
	}

	conn, err := engine.Open(":memory:", config)
	if err != nil {
		t.Fatalf(
			"Open with config failed: %v",
			err,
		)
	}
	defer conn.Close()

	// Verify connection works
	err = conn.Ping(context.Background())
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestEngineExpressionQuery(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Query without table
	rows, _, err := conn.Query(
		context.Background(),
		"SELECT 1 + 2",
		nil,
	)
	if err != nil {
		t.Fatalf(
			"SELECT expression failed: %v",
			err,
		)
	}

	if len(rows) != 1 {
		t.Errorf(
			"Expected 1 row, got %d",
			len(rows),
		)
	}
}

func TestTransactionManager(t *testing.T) {
	tm := NewTransactionManager()

	// Begin transaction
	txn := tm.Begin()
	if !txn.IsActive() {
		t.Error(
			"Transaction should be active after Begin",
		)
	}

	// Commit transaction
	err := tm.Commit(txn)
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	if txn.IsActive() {
		t.Error(
			"Transaction should not be active after Commit",
		)
	}

	// Commit again should fail
	err = tm.Commit(txn)
	if err != dukdb.ErrTransactionAlreadyEnded {
		t.Errorf(
			"Expected ErrTransactionAlreadyEnded, got %v",
			err,
		)
	}
}

func TestTransactionRollback(t *testing.T) {
	tm := NewTransactionManager()

	// Begin transaction
	txn := tm.Begin()

	// Rollback transaction
	err := tm.Rollback(txn)
	if err != nil {
		t.Errorf("Rollback failed: %v", err)
	}

	if txn.IsActive() {
		t.Error(
			"Transaction should not be active after Rollback",
		)
	}
}

func TestBackendInterface(t *testing.T) {
	// Verify that Engine implements Backend interface
	var _ dukdb.Backend = (*Engine)(nil)

	// Verify that EngineConn implements BackendConn interface
	var _ dukdb.BackendConn = (*EngineConn)(nil)

	// Verify that EngineStmt implements BackendStmt interface
	var _ dukdb.BackendStmt = (*EngineStmt)(nil)

	// Verify that EngineStmt implements BackendStmtIntrospector interface
	var _ dukdb.BackendStmtIntrospector = (*EngineStmt)(nil)
}

// Statement Introspection Tests

func TestStatementType(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create a test table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE test_types (id INTEGER, name VARCHAR)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	testCases := []struct {
		name     string
		sql      string
		expected dukdb.StmtType
	}{
		{"SELECT", "SELECT * FROM test_types", dukdb.STATEMENT_TYPE_SELECT},
		{"INSERT", "INSERT INTO test_types VALUES (1, 'test')", dukdb.STATEMENT_TYPE_INSERT},
		{"UPDATE", "UPDATE test_types SET name = 'updated' WHERE id = 1", dukdb.STATEMENT_TYPE_UPDATE},
		{"DELETE", "DELETE FROM test_types WHERE id = 1", dukdb.STATEMENT_TYPE_DELETE},
		{"CREATE", "CREATE TABLE new_table (id INTEGER)", dukdb.STATEMENT_TYPE_CREATE},
		{"DROP", "DROP TABLE IF EXISTS new_table", dukdb.STATEMENT_TYPE_DROP},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := conn.Prepare(context.Background(), tc.sql)
			if err != nil {
				t.Fatalf("Prepare failed: %v", err)
			}
			defer stmt.Close()

			intro, ok := stmt.(dukdb.BackendStmtIntrospector)
			if !ok {
				t.Fatal("Statement does not implement BackendStmtIntrospector")
			}

			stmtType := intro.StatementType()
			if stmtType != tc.expected {
				t.Errorf("StatementType() = %v, want %v", stmtType, tc.expected)
			}
		})
	}
}

func TestStatementTypeClosedStatement(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	// Close the statement first
	stmt.Close()

	intro := stmt.(*EngineStmt)
	stmtType := intro.StatementType()

	// Should return INVALID for closed statement (stmt is nil)
	if stmtType != dukdb.STATEMENT_TYPE_INVALID {
		t.Errorf("StatementType() on closed = %v, want INVALID", stmtType)
	}
}

func TestParameterMetadata(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	t.Run("positional parameters", func(t *testing.T) {
		stmt, err := conn.Prepare(context.Background(), "SELECT $1, $2, $3")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		if stmt.NumInput() != 3 {
			t.Errorf("NumInput() = %d, want 3", stmt.NumInput())
		}
	})

	t.Run("question mark parameters", func(t *testing.T) {
		stmt, err := conn.Prepare(context.Background(), "SELECT ?, ?, ?")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		if stmt.NumInput() != 3 {
			t.Errorf("NumInput() = %d, want 3", stmt.NumInput())
		}
	})

	t.Run("no parameters", func(t *testing.T) {
		stmt, err := conn.Prepare(context.Background(), "SELECT 1")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		if stmt.NumInput() != 0 {
			t.Errorf("NumInput() = %d, want 0", stmt.NumInput())
		}
	})
}

func TestColumnMetadata(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create a test table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE test_cols (id INTEGER, name VARCHAR, price DOUBLE)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	t.Run("column count and types", func(t *testing.T) {
		stmt, err := conn.Prepare(
			context.Background(),
			"SELECT id, name, price FROM test_cols",
		)
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		intro := stmt.(dukdb.BackendStmtIntrospector)

		// Test column count
		count := intro.ColumnCount()
		if count != 3 {
			t.Errorf("ColumnCount() = %d, want 3", count)
		}

		// Test column names
		name0 := intro.ColumnName(0)
		if name0 != "id" {
			t.Errorf("ColumnName(0) = %q, want \"id\"", name0)
		}

		name1 := intro.ColumnName(1)
		if name1 != "name" {
			t.Errorf("ColumnName(1) = %q, want \"name\"", name1)
		}

		name2 := intro.ColumnName(2)
		if name2 != "price" {
			t.Errorf("ColumnName(2) = %q, want \"price\"", name2)
		}

		// Test column types
		type0 := intro.ColumnType(0)
		if type0 != dukdb.TYPE_INTEGER {
			t.Errorf("ColumnType(0) = %v, want INTEGER", type0)
		}

		type1 := intro.ColumnType(1)
		if type1 != dukdb.TYPE_VARCHAR {
			t.Errorf("ColumnType(1) = %v, want VARCHAR", type1)
		}

		type2 := intro.ColumnType(2)
		if type2 != dukdb.TYPE_DOUBLE {
			t.Errorf("ColumnType(2) = %v, want DOUBLE", type2)
		}
	})

	t.Run("out of bounds", func(t *testing.T) {
		stmt, err := conn.Prepare(context.Background(), "SELECT id FROM test_cols")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		intro := stmt.(dukdb.BackendStmtIntrospector)

		// Out of bounds column name
		name := intro.ColumnName(5)
		if name != "" {
			t.Errorf("ColumnName(5) = %q, want empty", name)
		}

		// Out of bounds column type
		colType := intro.ColumnType(5)
		if colType != dukdb.TYPE_INVALID {
			t.Errorf("ColumnType(5) = %v, want INVALID", colType)
		}
	})

	t.Run("non-SELECT statement", func(t *testing.T) {
		stmt, err := conn.Prepare(
			context.Background(),
			"INSERT INTO test_cols VALUES (1, 'test', 9.99)",
		)
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		intro := stmt.(dukdb.BackendStmtIntrospector)

		// Non-SELECT should have 0 columns
		count := intro.ColumnCount()
		if count != 0 {
			t.Errorf("ColumnCount() for INSERT = %d, want 0", count)
		}
	})
}

func TestParamNameAndType(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.Prepare(context.Background(), "SELECT $1, $2")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	intro := stmt.(dukdb.BackendStmtIntrospector)

	// ParamName returns empty for positional parameters
	name1 := intro.ParamName(1)
	if name1 != "" {
		t.Errorf("ParamName(1) = %q, want empty", name1)
	}

	name2 := intro.ParamName(2)
	if name2 != "" {
		t.Errorf("ParamName(2) = %q, want empty", name2)
	}

	// ParamType returns TYPE_ANY since params are untyped
	type1 := intro.ParamType(1)
	if type1 != dukdb.TYPE_ANY {
		t.Errorf("ParamType(1) = %v, want TYPE_ANY", type1)
	}

	type2 := intro.ParamType(2)
	if type2 != dukdb.TYPE_ANY {
		t.Errorf("ParamType(2) = %v, want TYPE_ANY", type2)
	}

	// Out of bounds
	outOfBounds := intro.ParamType(10)
	if outOfBounds != dukdb.TYPE_INVALID {
		t.Errorf("ParamType(10) = %v, want TYPE_INVALID", outOfBounds)
	}
}

func TestStatementIntrospectionWithAliases(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create a test table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE test_alias (id INTEGER, name VARCHAR)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	stmt, err := conn.Prepare(
		context.Background(),
		"SELECT id AS user_id, name AS user_name FROM test_alias",
	)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	intro := stmt.(dukdb.BackendStmtIntrospector)

	count := intro.ColumnCount()
	if count != 2 {
		t.Errorf("ColumnCount() = %d, want 2", count)
	}

	// Column names should be the aliases
	name0 := intro.ColumnName(0)
	if name0 != "user_id" {
		t.Errorf("ColumnName(0) = %q, want \"user_id\"", name0)
	}

	name1 := intro.ColumnName(1)
	if name1 != "user_name" {
		t.Errorf("ColumnName(1) = %q, want \"user_name\"", name1)
	}
}

func TestStmtTypeConstants(t *testing.T) {
	// Verify all statement type constants are defined correctly
	tests := []struct {
		name     string
		stmtType dukdb.StmtType
		expected int
	}{
		{"INVALID", dukdb.STATEMENT_TYPE_INVALID, 0},
		{"SELECT", dukdb.STATEMENT_TYPE_SELECT, 1},
		{"INSERT", dukdb.STATEMENT_TYPE_INSERT, 2},
		{"UPDATE", dukdb.STATEMENT_TYPE_UPDATE, 3},
		{"DELETE", dukdb.STATEMENT_TYPE_DELETE, 5},
		{"CREATE", dukdb.STATEMENT_TYPE_CREATE, 7},
		{"DROP", dukdb.STATEMENT_TYPE_DROP, 15},
		{"MULTI", dukdb.STATEMENT_TYPE_MULTI, 27},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if int(tc.stmtType) != tc.expected {
				t.Errorf("%s = %d, want %d", tc.name, tc.stmtType, tc.expected)
			}
		})
	}
}

func TestBoundExecution(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create a test table
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE test_bound (id INTEGER, name VARCHAR)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = conn.Execute(
		context.Background(),
		"INSERT INTO test_bound VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
		nil,
	)
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	t.Run("query with bound parameters", func(t *testing.T) {
		stmt, err := conn.Prepare(context.Background(), "SELECT * FROM test_bound WHERE id = $1")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		// Execute with bound parameter
		rows, cols, err := stmt.Query(context.Background(), []driver.NamedValue{
			{Ordinal: 1, Value: int64(2)},
		})
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
		if len(cols) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(cols))
		}
		if rows[0]["name"] != "Bob" {
			t.Errorf("Expected name 'Bob', got %v", rows[0]["name"])
		}
	})

	t.Run("exec with bound parameters", func(t *testing.T) {
		stmt, err := conn.Prepare(context.Background(), "INSERT INTO test_bound VALUES ($1, $2)")
		if err != nil {
			t.Fatalf("Prepare failed: %v", err)
		}
		defer stmt.Close()

		// Execute with bound parameters
		affected, err := stmt.Execute(context.Background(), []driver.NamedValue{
			{Ordinal: 1, Value: int64(4)},
			{Ordinal: 2, Value: "Diana"},
		})
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		if affected != 1 {
			t.Errorf("Expected 1 row affected, got %d", affected)
		}

		// Verify the insert
		rows, _, err := conn.Query(
			context.Background(),
			"SELECT * FROM test_bound WHERE id = 4",
			nil,
		)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})
}

func TestColumnTypeInfo(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create a test table with various types
	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE test_type_info (id INTEGER, name VARCHAR, price DOUBLE, active BOOLEAN)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	stmt, err := conn.Prepare(
		context.Background(),
		"SELECT id, name, price, active FROM test_type_info",
	)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	intro := stmt.(dukdb.BackendStmtIntrospector)

	t.Run("valid index returns TypeInfo", func(t *testing.T) {
		// INTEGER column
		typeInfo0 := intro.ColumnTypeInfo(0)
		if typeInfo0 == nil {
			t.Fatal("ColumnTypeInfo(0) returned nil")
		}
		if typeInfo0.InternalType() != dukdb.TYPE_INTEGER {
			t.Errorf("ColumnTypeInfo(0).InternalType() = %v, want INTEGER", typeInfo0.InternalType())
		}

		// VARCHAR column
		typeInfo1 := intro.ColumnTypeInfo(1)
		if typeInfo1 == nil {
			t.Fatal("ColumnTypeInfo(1) returned nil")
		}
		if typeInfo1.InternalType() != dukdb.TYPE_VARCHAR {
			t.Errorf("ColumnTypeInfo(1).InternalType() = %v, want VARCHAR", typeInfo1.InternalType())
		}

		// DOUBLE column
		typeInfo2 := intro.ColumnTypeInfo(2)
		if typeInfo2 == nil {
			t.Fatal("ColumnTypeInfo(2) returned nil")
		}
		if typeInfo2.InternalType() != dukdb.TYPE_DOUBLE {
			t.Errorf("ColumnTypeInfo(2).InternalType() = %v, want DOUBLE", typeInfo2.InternalType())
		}

		// BOOLEAN column
		typeInfo3 := intro.ColumnTypeInfo(3)
		if typeInfo3 == nil {
			t.Fatal("ColumnTypeInfo(3) returned nil")
		}
		if typeInfo3.InternalType() != dukdb.TYPE_BOOLEAN {
			t.Errorf("ColumnTypeInfo(3).InternalType() = %v, want BOOLEAN", typeInfo3.InternalType())
		}
	})

	t.Run("out of bounds returns nil", func(t *testing.T) {
		typeInfo := intro.ColumnTypeInfo(10)
		if typeInfo != nil {
			t.Errorf("ColumnTypeInfo(10) = %v, want nil", typeInfo)
		}
	})

	t.Run("primitive types have nil details", func(t *testing.T) {
		typeInfo := intro.ColumnTypeInfo(0)
		if typeInfo.Details() != nil {
			t.Errorf("ColumnTypeInfo(0).Details() = %v, want nil for primitive type", typeInfo.Details())
		}
	})
}

func TestParameterTypeInference(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	// Create a test table with various column types
	_, err = conn.Execute(
		context.Background(),
		`CREATE TABLE test_types (
			id INTEGER,
			name VARCHAR,
			value DOUBLE,
			active BOOLEAN
		)`,
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	testCases := []struct {
		name         string
		sql          string
		paramIndex   int
		expectedType dukdb.Type
	}{
		// Column comparison inference
		{"WHERE id = ?", "SELECT * FROM test_types WHERE id = ?", 1, dukdb.TYPE_INTEGER},
		{"WHERE name = ?", "SELECT * FROM test_types WHERE name = $1", 1, dukdb.TYPE_VARCHAR},
		{"WHERE value > ?", "SELECT * FROM test_types WHERE value > ?", 1, dukdb.TYPE_DOUBLE},

		// INSERT value inference
		{"INSERT id", "INSERT INTO test_types (id) VALUES (?)", 1, dukdb.TYPE_INTEGER},
		{"INSERT name", "INSERT INTO test_types (name) VALUES (?)", 1, dukdb.TYPE_VARCHAR},
		{"INSERT value", "INSERT INTO test_types (value) VALUES (?)", 1, dukdb.TYPE_DOUBLE},

		// UPDATE value inference
		{"UPDATE SET value", "UPDATE test_types SET value = ? WHERE id = 1", 1, dukdb.TYPE_DOUBLE},
		{"UPDATE SET name", "UPDATE test_types SET name = ? WHERE id = 1", 1, dukdb.TYPE_VARCHAR},
		{"UPDATE WHERE id", "UPDATE test_types SET name = 'x' WHERE id = ?", 1, dukdb.TYPE_INTEGER},

		// Arithmetic context inference
		{"arithmetic", "SELECT ? + ?", 1, dukdb.TYPE_DOUBLE},
		{"arithmetic second param", "SELECT ? + ?", 2, dukdb.TYPE_DOUBLE},

		// No context (standalone expressions) - should return ANY
		{"standalone param", "SELECT ?", 1, dukdb.TYPE_ANY},

		// LIMIT/OFFSET inference
		{"LIMIT", "SELECT * FROM test_types LIMIT ?", 1, dukdb.TYPE_BIGINT},
		{"OFFSET", "SELECT * FROM test_types LIMIT 10 OFFSET ?", 1, dukdb.TYPE_BIGINT},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := conn.Prepare(context.Background(), tc.sql)
			if err != nil {
				t.Fatalf("Prepare failed: %v", err)
			}
			defer stmt.Close()

			intro := stmt.(dukdb.BackendStmtIntrospector)
			actualType := intro.ParamType(tc.paramIndex)

			if actualType != tc.expectedType {
				t.Errorf("ParamType(%d) = %v, want %v for SQL: %s",
					tc.paramIndex, actualType, tc.expectedType, tc.sql)
			}
		})
	}
}

func TestParameterTypeInferenceBETWEEN(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE nums (val INTEGER)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// BETWEEN should infer from the column type
	stmt, err := conn.Prepare(
		context.Background(),
		"SELECT * FROM nums WHERE val BETWEEN ? AND ?",
	)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	intro := stmt.(dukdb.BackendStmtIntrospector)

	type1 := intro.ParamType(1)
	if type1 != dukdb.TYPE_INTEGER {
		t.Errorf("ParamType(1) = %v, want INTEGER for BETWEEN low bound", type1)
	}

	type2 := intro.ParamType(2)
	if type2 != dukdb.TYPE_INTEGER {
		t.Errorf("ParamType(2) = %v, want INTEGER for BETWEEN high bound", type2)
	}
}

func TestParameterTypeInferenceINList(t *testing.T) {
	engine := NewEngine()
	defer engine.Close()

	conn, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	_, err = conn.Execute(
		context.Background(),
		"CREATE TABLE items (name VARCHAR)",
		nil,
	)
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// IN list values should infer from the column type
	stmt, err := conn.Prepare(
		context.Background(),
		"SELECT * FROM items WHERE name IN (?, ?, ?)",
	)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	intro := stmt.(dukdb.BackendStmtIntrospector)

	for i := 1; i <= 3; i++ {
		typ := intro.ParamType(i)
		if typ != dukdb.TYPE_VARCHAR {
			t.Errorf("ParamType(%d) = %v, want VARCHAR for IN list value", i, typ)
		}
	}
}
