package binder

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// Helper function to create a test catalog with a table
func setupTestCatalog() *catalog.Catalog {
	cat := catalog.NewCatalog()

	// Create a test table with columns: id (INTEGER), name (VARCHAR), age (INTEGER), active (BOOLEAN)
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("age", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("active", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("x", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("y", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("z", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("b", dukdb.TYPE_INTEGER),
	}

	tableDef := catalog.NewTableDef("test_table", columns)
	err := cat.CreateTableInSchema("main", tableDef)
	if err != nil {
		panic(err)
	}

	// Create another table "t" with the same columns for shorter test queries
	tColumns := []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("age", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("active", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("x", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("y", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("z", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("b", dukdb.TYPE_INTEGER),
	}
	tTableDef := catalog.NewTableDef("t", tColumns)
	err = cat.CreateTableInSchema("main", tTableDef)
	if err != nil {
		panic(err)
	}

	return cat
}

// Task 1.5: Verify binder resolves column references in UPDATE WHERE clauses
func TestBindUpdateWhereClause(t *testing.T) {
	cat := setupTestCatalog()
	binder := NewBinder(cat)

	sql := "UPDATE test_table SET name = 'John' WHERE age > 18"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	boundStmt, err := binder.Bind(stmt)
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}

	boundUpdate, ok := boundStmt.(*BoundUpdateStmt)
	if !ok {
		t.Fatalf("Expected BoundUpdateStmt, got %T", boundStmt)
	}

	if boundUpdate.Where == nil {
		t.Fatal("Expected WHERE clause to be bound, got nil")
	}

	// Verify WHERE clause is a binary expression
	binExpr, ok := boundUpdate.Where.(*BoundBinaryExpr)
	if !ok {
		t.Fatalf("Expected BoundBinaryExpr, got %T", boundUpdate.Where)
	}

	// Verify left side is a column reference to 'age'
	leftCol, ok := binExpr.Left.(*BoundColumnRef)
	if !ok {
		t.Fatalf("Expected BoundColumnRef on left, got %T", binExpr.Left)
	}

	if leftCol.Column != "age" {
		t.Errorf("Expected column 'age', got '%s'", leftCol.Column)
	}

	if leftCol.ColType != dukdb.TYPE_INTEGER {
		t.Errorf("Expected column type INTEGER, got %v", leftCol.ColType)
	}

	// Verify the column index is correct (age is at index 2)
	if leftCol.ColumnIdx != 2 {
		t.Errorf("Expected column index 2, got %d", leftCol.ColumnIdx)
	}
}

// Task 1.6: Verify binder resolves column references in DELETE WHERE clauses
func TestBindDeleteWhereClause(t *testing.T) {
	cat := setupTestCatalog()
	binder := NewBinder(cat)

	sql := "DELETE FROM test_table WHERE id = 1"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	boundStmt, err := binder.Bind(stmt)
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}

	boundDelete, ok := boundStmt.(*BoundDeleteStmt)
	if !ok {
		t.Fatalf("Expected BoundDeleteStmt, got %T", boundStmt)
	}

	if boundDelete.Where == nil {
		t.Fatal("Expected WHERE clause to be bound, got nil")
	}

	// Verify WHERE clause is a binary expression
	binExpr, ok := boundDelete.Where.(*BoundBinaryExpr)
	if !ok {
		t.Fatalf("Expected BoundBinaryExpr, got %T", boundDelete.Where)
	}

	// Verify left side is a column reference to 'id'
	leftCol, ok := binExpr.Left.(*BoundColumnRef)
	if !ok {
		t.Fatalf("Expected BoundColumnRef on left, got %T", binExpr.Left)
	}

	if leftCol.Column != "id" {
		t.Errorf("Expected column 'id', got '%s'", leftCol.Column)
	}

	if leftCol.ColType != dukdb.TYPE_INTEGER {
		t.Errorf("Expected column type INTEGER, got %v", leftCol.ColType)
	}

	// Verify the column index is correct (id is at index 0)
	if leftCol.ColumnIdx != 0 {
		t.Errorf("Expected column index 0, got %d", leftCol.ColumnIdx)
	}
}

// Task 1.7: Test: Bind "DELETE FROM t WHERE nonexistent > 5" returns ErrorTypeBinder
func TestBindDeleteNonexistentColumn(t *testing.T) {
	cat := setupTestCatalog()
	binder := NewBinder(cat)

	sql := "DELETE FROM t WHERE nonexistent > 5"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	_, err = binder.Bind(stmt)
	if err == nil {
		t.Fatal("Expected error for nonexistent column, got nil")
	}

	// Verify the error is of type ErrorTypeBinder
	dukdbErr, ok := err.(*dukdb.Error)
	if !ok {
		t.Fatalf("Expected *dukdb.Error, got %T", err)
	}

	if dukdbErr.Type != dukdb.ErrorTypeBinder {
		t.Errorf("Expected ErrorTypeBinder, got %v", dukdbErr.Type)
	}

	// Verify error message mentions the column
	if dukdbErr.Msg == "" {
		t.Error("Expected error message but got empty string")
	}
}

// Task 1.8: Test: Bind "UPDATE t SET x = y WHERE z > 10" type-checks expressions
func TestBindUpdateTypeChecking(t *testing.T) {
	cat := setupTestCatalog()
	binder := NewBinder(cat)

	sql := "UPDATE t SET x = y WHERE z > 10"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	boundStmt, err := binder.Bind(stmt)
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}

	boundUpdate, ok := boundStmt.(*BoundUpdateStmt)
	if !ok {
		t.Fatalf("Expected BoundUpdateStmt, got %T", boundStmt)
	}

	// Verify SET clause
	if len(boundUpdate.Set) != 1 {
		t.Fatalf("Expected 1 SET clause, got %d", len(boundUpdate.Set))
	}

	setClause := boundUpdate.Set[0]

	// Verify SET column index (x is at index 4)
	if setClause.ColumnIdx != 4 {
		t.Errorf("Expected column index 4, got %d", setClause.ColumnIdx)
	}

	// Verify SET value is a column reference to 'y'
	colRef, ok := setClause.Value.(*BoundColumnRef)
	if !ok {
		t.Fatalf("Expected BoundColumnRef for SET value, got %T", setClause.Value)
	}

	if colRef.Column != "y" {
		t.Errorf("Expected column 'y', got '%s'", colRef.Column)
	}

	if colRef.ColType != dukdb.TYPE_INTEGER {
		t.Errorf("Expected column type INTEGER for 'y', got %v", colRef.ColType)
	}

	// Verify WHERE clause
	if boundUpdate.Where == nil {
		t.Fatal("Expected WHERE clause to be bound, got nil")
	}

	binExpr, ok := boundUpdate.Where.(*BoundBinaryExpr)
	if !ok {
		t.Fatalf("Expected BoundBinaryExpr in WHERE, got %T", boundUpdate.Where)
	}

	// Verify WHERE left side is column reference to 'z'
	whereCol, ok := binExpr.Left.(*BoundColumnRef)
	if !ok {
		t.Fatalf("Expected BoundColumnRef on left of WHERE, got %T", binExpr.Left)
	}

	if whereCol.Column != "z" {
		t.Errorf("Expected column 'z', got '%s'", whereCol.Column)
	}

	if whereCol.ColType != dukdb.TYPE_INTEGER {
		t.Errorf("Expected column type INTEGER for 'z', got %v", whereCol.ColType)
	}

	// Verify WHERE result type is BOOLEAN
	if binExpr.ResType != dukdb.TYPE_BOOLEAN {
		t.Errorf("Expected WHERE expression result type BOOLEAN, got %v", binExpr.ResType)
	}
}

// Additional test: Complex WHERE with AND/OR
func TestBindDeleteComplexWhere(t *testing.T) {
	cat := setupTestCatalog()
	binder := NewBinder(cat)

	sql := "DELETE FROM t WHERE age > 18 AND (active = true OR name = 'John')"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	boundStmt, err := binder.Bind(stmt)
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}

	boundDelete, ok := boundStmt.(*BoundDeleteStmt)
	if !ok {
		t.Fatalf("Expected BoundDeleteStmt, got %T", boundStmt)
	}

	if boundDelete.Where == nil {
		t.Fatal("Expected WHERE clause to be bound, got nil")
	}

	// Should be AND expression at top level
	andExpr, ok := boundDelete.Where.(*BoundBinaryExpr)
	if !ok {
		t.Fatalf("Expected BoundBinaryExpr for AND, got %T", boundDelete.Where)
	}

	if andExpr.ResType != dukdb.TYPE_BOOLEAN {
		t.Errorf("Expected AND result type BOOLEAN, got %v", andExpr.ResType)
	}
}

// Additional test: UPDATE with IN clause
func TestBindUpdateWhereInClause(t *testing.T) {
	cat := setupTestCatalog()
	binder := NewBinder(cat)

	sql := "UPDATE t SET name = 'Updated' WHERE id IN (1, 2, 3)"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	boundStmt, err := binder.Bind(stmt)
	if err != nil {
		t.Fatalf("Bind() error = %v", err)
	}

	boundUpdate, ok := boundStmt.(*BoundUpdateStmt)
	if !ok {
		t.Fatalf("Expected BoundUpdateStmt, got %T", boundStmt)
	}

	if boundUpdate.Where == nil {
		t.Fatal("Expected WHERE clause to be bound, got nil")
	}

	// Should be IN expression
	inExpr, ok := boundUpdate.Where.(*BoundInListExpr)
	if !ok {
		t.Fatalf("Expected BoundInListExpr, got %T", boundUpdate.Where)
	}

	// Verify column reference
	colRef, ok := inExpr.Expr.(*BoundColumnRef)
	if !ok {
		t.Fatalf("Expected BoundColumnRef, got %T", inExpr.Expr)
	}

	if colRef.Column != "id" {
		t.Errorf("Expected column 'id', got '%s'", colRef.Column)
	}

	// Verify values
	if len(inExpr.Values) != 3 {
		t.Errorf("Expected 3 values in IN list, got %d", len(inExpr.Values))
	}
}

// Additional test: Nonexistent table
func TestBindUpdateNonexistentTable(t *testing.T) {
	cat := setupTestCatalog()
	binder := NewBinder(cat)

	sql := "UPDATE nonexistent_table SET name = 'John' WHERE id = 1"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	_, err = binder.Bind(stmt)
	if err == nil {
		t.Fatal("Expected error for nonexistent table, got nil")
	}

	// Verify the error is of type ErrorTypeBinder (table not found)
	dukdbErr, ok := err.(*dukdb.Error)
	if !ok {
		t.Fatalf("Expected *dukdb.Error, got %T", err)
	}

	if dukdbErr.Type != dukdb.ErrorTypeBinder {
		t.Errorf("Expected ErrorTypeBinder, got %v", dukdbErr.Type)
	}
}
