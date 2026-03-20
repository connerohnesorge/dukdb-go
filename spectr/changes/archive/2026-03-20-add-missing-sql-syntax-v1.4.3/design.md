# Design: Complete Missing SQL Syntax for DuckDB v1.4.3

## Overview

This design covers targeted fixes to existing partial implementations. Each change is small and independent.

## 1. TRUNCATE IF EXISTS

### AST Change

File: `internal/parser/ast.go`, line 426

```go
// Before:
type TruncateStmt struct {
    Schema string
    Table  string
}

// After:
type TruncateStmt struct {
    Schema   string
    Table    string
    IfExists bool
}
```

### Parser Change

File: `internal/parser/parser.go`, in `parseTruncate()` (line ~2416)

Insert IF EXISTS check after the optional TABLE keyword and before schema.table parsing:

```go
func (p *parser) parseTruncate() (*TruncateStmt, error) {
    p.advance() // consume TRUNCATE

    // Optional TABLE keyword
    if p.isKeyword("TABLE") {
        p.advance()
    }

    // NEW: Optional IF EXISTS
    ifExists := false
    if p.isKeyword("IF") {
        p.advance()
        if err := p.expectKeyword("EXISTS"); err != nil {
            return nil, err
        }
        ifExists = true
    }

    // ... existing schema.table parsing ...

    return &TruncateStmt{
        Schema:   schema,
        Table:    table,
        IfExists: ifExists,
    }, nil
}
```

### Binder Change

File: `internal/binder/bind_stmt.go`, near line 3093

Currently:
```go
_, exists := b.catalog.GetTableInSchema(stmt.Schema, stmt.Table)
if !exists {
    return nil, fmt.Errorf("table %q not found", stmt.Table)
}
```

Change to:
```go
_, exists := b.catalog.GetTableInSchema(stmt.Schema, stmt.Table)
if !exists {
    if stmt.IfExists {
        return &BoundTruncateStmt{Schema: stmt.Schema, Table: stmt.Table, IfExists: true, NoOp: true}, nil
    }
    return nil, fmt.Errorf("table %q not found", stmt.Table)
}
```

### Executor Change — Schema-Qualified Storage Lookup

File: `internal/executor/ddl.go`, line 846

Currently:
```go
table, ok := e.storage.GetTable(plan.Table)
```

Fix to:
```go
// Use schema-qualified lookup for multi-schema support
table, ok := e.storage.GetTableInSchema(plan.Schema, plan.Table)
if !ok {
    // Fallback to unqualified lookup for backward compatibility
    table, ok = e.storage.GetTable(plan.Table)
}
```

Also add IF EXISTS no-op check:
```go
if plan.NoOp {
    return &ExecutionResult{RowsAffected: 0}, nil
}
```

### WAL Integration

File: `internal/wal/wal.go` (or similar)

Add new WAL entry type constant:
```go
const WALEntryTruncate byte = 95
```

In executeTruncate, after `table.Truncate()`:
```go
if e.wal != nil && e.txnID != 0 {
    e.wal.WriteEntry(WALEntryTruncate, e.txnID, []byte(plan.Schema+"."+plan.Table))
}
```

### Undo Recording

For transaction rollback support, before calling `table.Truncate()`:
```go
if e.undoRecorder != nil {
    // Snapshot current state for rollback
    e.undoRecorder.RecordTableSnapshot(plan.Schema, plan.Table)
}
```

The undo recorder saves a reference to the current rowGroups slice (shallow copy). On ROLLBACK, it restores the slice pointer, totalRows, and nextRowID.

## 2. Storage: Clear Index Entries on Truncate

File: `internal/storage/table.go`, in `Truncate()` method (line 166)

Check if the table has associated indexes via the catalog and clear them:

```go
func (t *Table) Truncate() int64 {
    t.mu.Lock()
    defer t.mu.Unlock()

    prevRows := t.totalRows
    t.rowGroups = make([]*RowGroup, 0)
    t.totalRows = 0
    t.nextRowID = 0
    t.tombstones = NewBitmap(1024)
    t.rowIDMap = make(map[RowID]*rowLocation)
    t.rowVersions = make(map[RowID]*VersionInfo)

    t.versionsMu.Lock()
    t.versions = make(map[RowID]*VersionChain)
    t.versionsMu.Unlock()

    return prevRows
}
```

Note: Index clearing should happen at the executor level via the catalog's index registry, not directly in the storage table, since the Table struct doesn't hold index references. The executor should:

```go
// After table.Truncate(), clear indexes
indexes := e.catalog.GetIndexesForTable(plan.Schema, plan.Table)
for _, idx := range indexes {
    storageIdx, ok := e.storage.GetIndex(idx.Name)
    if ok {
        storageIdx.Clear()
    }
}
```

## 3. VALUES Type Inference

File: `internal/binder/bind_stmt.go`, near line 501

### Current Behavior
For each column, scans rows and takes the first non-NULL type. This fails for:
```sql
VALUES (1, 'text'), (2.5, NULL)
-- column1 should be DOUBLE (promote INT + DOUBLE)
-- Current: uses INTEGER (first non-NULL)
```

### Proposed Fix

Replace with proper supertype promotion:

```go
func inferValuesColumnTypes(rows [][]BoundExpr) []types.Type {
    if len(rows) == 0 {
        return nil
    }
    numCols := len(rows[0])
    colTypes := make([]types.Type, numCols)

    for col := 0; col < numCols; col++ {
        // Start with NULL type
        resultType := types.TYPE_NULL

        for _, row := range rows {
            exprType := row[col].ResultType()
            if exprType == types.TYPE_NULL {
                continue
            }
            if resultType == types.TYPE_NULL {
                resultType = exprType
            } else {
                // Use existing supertype promotion (same as UNION)
                resultType = types.GetCommonSupertype(resultType, exprType)
            }
        }

        // Default NULL-only columns to VARCHAR (match DuckDB behavior)
        if resultType == types.TYPE_NULL {
            resultType = types.TYPE_VARCHAR
        }
        colTypes[col] = resultType
    }

    return colTypes
}
```

After inferring types, insert implicit CAST nodes where a row's expression type differs from the inferred column type:

```go
for rowIdx, row := range boundRows {
    for colIdx, expr := range row {
        if expr.ResultType() != colTypes[colIdx] && expr.ResultType() != types.TYPE_NULL {
            boundRows[rowIdx][colIdx] = &BoundCastExpr{
                Expr:       expr,
                TargetType: colTypes[colIdx],
            }
        }
    }
}
```

## Testing

### TRUNCATE IF EXISTS
```sql
-- Should succeed without error
TRUNCATE TABLE IF EXISTS nonexistent_table;

-- Should truncate and return rows affected
CREATE TABLE t(id INT);
INSERT INTO t VALUES (1), (2), (3);
TRUNCATE TABLE IF EXISTS t;
-- rows affected: 3

-- Transaction rollback
BEGIN;
TRUNCATE TABLE t;
ROLLBACK;
SELECT count(*) FROM t; -- should return original count
```

### TRUNCATE Schema-Qualified
```sql
CREATE SCHEMA myschema;
CREATE TABLE myschema.t(id INT);
INSERT INTO myschema.t VALUES (1);
TRUNCATE TABLE myschema.t;
-- Should succeed (currently fails due to unqualified storage lookup)
```

### VALUES Type Promotion
```sql
VALUES (1, 'text'), (2.5, NULL);
-- column1 type should be DOUBLE, not INTEGER
-- column2 type should be VARCHAR

VALUES (1), (2.5), (3);
-- column1 should be DOUBLE (promoted from INT + DOUBLE)
```
