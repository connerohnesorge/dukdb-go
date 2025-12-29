# Execution Engine Specification Delta

## Summary

This change **IMPLEMENTS** the existing `execution-engine` specification (292 lines, 11 requirements) by integrating existing parser/binder/planner/executor components into an end-to-end query execution pipeline.

**Implementation Approach**: REFACTORING & INTEGRATION (not greenfield)

**Existing Code**: 7418+ lines in internal/parser, internal/binder, internal/planner, internal/executor, internal/engine

**What's New**: Pipeline wiring, DataChunk integration, result set implementation

## Integration with Existing Implementation

### Existing Components (Already Implemented)
- **Parser** (`internal/parser/parser.go`, 2219 lines): Full SQL parsing for SELECT/INSERT/UPDATE/DELETE/CREATE TABLE
- **Binder** (`internal/binder/binder.go`, 1672 lines): Name resolution, type checking
- **Logical Planner** (`internal/planner/logical.go`, 379 lines): LogicalScan, LogicalFilter, LogicalProject
- **Physical Planner** (`internal/planner/physical.go`, 1011 lines): PhysicalScan, PhysicalFilter, PhysicalProject
- **Operators** (`internal/executor/operator.go`, 1062 lines): Scan, Filter, Project (partial)
- **Expression Evaluator** (`internal/executor/expr.go`, 1075 lines): BinaryOp, function calls
- **Engine Framework** (`internal/engine/engine.go`, 336 lines): Catalog, Storage, Transactions

### New Integration (This Change)
- **Pipeline Orchestration**: PipelineContext, ExecPipeline, QueryPipeline
- **DataChunk Integration**: Update operators to use P0-2 DataChunk format
- **Result Set**: Implement driver.Rows wrapping DataChunks
- **EngineConn Routing**: Wire ExecContext/QueryContext to pipeline
- **Column Metadata**: Expose TypeInfo for P0-3 completion

## ADDED Requirements

**Note**: The pipeline Parse→Bind→Plan→Execute is ALREADY IMPLEMENTED in `internal/engine/conn.go`. This spec focuses on the MISSING pieces.

### Requirement: DataChunk Operator Interface

All physical operators MUST produce and consume DataChunks from P0-2.

**Context**: Integrates columnar storage into execution pipeline.

#### Scenario: Scan produces DataChunk

```go
scan := PhysicalScan{table: t}
chunk, err := scan.Next()
assert.NoError(t, err)
assert.IsType(t, &DataChunk{}, chunk)
assert.Equal(t, 2048, chunk.Capacity())
```

#### Scenario: Filter consumes/produces DataChunk

```go
filter := PhysicalFilter{child: scan, predicate: expr}
chunk, err := filter.Next()
assert.NoError(t, err)
assert.IsType(t, &DataChunk{}, chunk)
```

#### Scenario: Operators propagate TypeInfo

```go
chunk, _ := scan.Next()
types := chunk.GetTypes()
assert.Len(t, types, 2)
assert.Equal(t, TYPE_INTEGER, types[0].InternalType())
```

---

### Requirement: Result Set Implementation

QueryContext MUST return ResultSet implementing driver.Rows with DataChunk backing.

**Context**: Provides row-by-row access to query results.

#### Scenario: ResultSet wraps DataChunks

```go
chunks := []*DataChunk{chunk1, chunk2}
rs := NewResultSet(chunks)
assert.Len(t, rs.chunks, 2)
```

#### Scenario: ResultSet iterates rows

```go
rs := NewResultSet(chunks)
count := 0
dest := make([]driver.Value, 2)
for rs.Next(dest) == nil {
    count++
}
assert.Equal(t, 2048, count) // Full chunk
```

#### Scenario: ColumnTypeDatabaseTypeName for P0-3

```go
rs := NewResultSet(chunks)
typeName := rs.ColumnTypeDatabaseTypeName(0)
assert.Equal(t, "INTEGER", typeName)
```

---

### Requirement: EngineConn Pipeline Integration

EngineConn MUST route ExecContext and QueryContext through the execution pipeline.

**Context**: Wires database/sql driver to execution engine.

#### Scenario: ExecContext routes through pipeline

```go
conn, _ := engine.Open(":memory:", nil)
result, err := conn.ExecContext(ctx, "INSERT INTO t VALUES (1)", nil)
assert.NoError(t, err)
affected, _ := result.RowsAffected()
assert.Equal(t, int64(1), affected)
```

#### Scenario: QueryContext routes through pipeline

```go
conn, _ := engine.Open(":memory:", nil)
rows, err := conn.QueryContext(ctx, "SELECT * FROM t", nil)
assert.NoError(t, err)
assert.NotNil(t, rows)
```

#### Scenario: Error types propagated correctly

```go
conn, _ := engine.Open(":memory:", nil)
_, err := conn.QueryContext(ctx, "SELECT * FROM nonexistent", nil)
assert.Error(t, err)
assert.Equal(t, ErrorTypeCatalog, err.(*Error).Type)
```

---

### Requirement: Column Metadata for P0-3

PreparedStmt MUST expose column metadata via TypeInfo after binding.

**Context**: Enables P0-3 statement introspection completion.

#### Scenario: Prepared statement has column metadata

```go
stmt, _ := conn.Prepare("SELECT id, name FROM users")
// After binding in EngineConn.Prepare:
plan := stmt.(*PreparedPlan)
assert.Len(t, plan.ColumnTypes, 2)
assert.Equal(t, TYPE_INTEGER, plan.ColumnTypes[0].InternalType())
assert.Equal(t, TYPE_VARCHAR, plan.ColumnTypes[1].InternalType())
```

#### Scenario: ColumnTypeInfo returns full TypeInfo

```go
typeInfo := plan.ColumnTypeInfo(0)
assert.Equal(t, TYPE_INTEGER, typeInfo.InternalType())
assert.Equal(t, "INTEGER", typeInfo.SQLType())
```

---

## MODIFIED Requirements

### Requirement: Columnar Storage

**Base Spec** (lines 174-192): Engine SHALL store data in columnar format.

**Enhancement**: Integrate P0-2 DataChunk/Vector for columnar storage.

#### Scenario: Column data stored in DataChunk vectors

```go
// Original spec says: "data is stored as contiguous []int64"
// Enhanced: Data stored in Vector from P0-2
table, _ := storage.GetTable("t")
chunk := table.GetChunk(0)
vec := chunk.GetVector(0) // INT column
assert.IsType(t, &IntVector{}, vec)
```

#### Scenario: NULL handling via ValidityMask

```go
// Original spec says: "null bitmap correctly tracks NULL positions"
// Enhanced: Use ValidityMask from P0-2
vec := chunk.GetVector(0)
assert.False(t, vec.IsValid(5)) // Row 5 is NULL
assert.True(t, vec.IsValid(0))  // Row 0 is valid
```

---

## Implementation Notes

### Phased Rollout

This change implements the spec across 5 phases:
- **Phase A**: Pipeline integration (wire parser → binder → planner → executor)
- **Phase B**: DataChunk integration (update operators to use P0-2)
- **Phase C**: Result set (implement driver.Rows)
- **Phase D**: Operator completion (aggregates, joins, ORDER BY, DML)
- **Phase E**: Testing & validation (all 292 spec lines)

### Dependencies

- **Requires**: P0-1a Core TypeInfo (for column type metadata)
- **Requires**: P0-2 Vector/DataChunk (for columnar storage)
- **Enables**: P0-3 Statement Introspection (column metadata methods)

### Testing Strategy

Each phase has milestone tests:
- Phase A: SELECT 1 works
- Phase B: SELECT * FROM t works
- Phase C: Result set iteration works
- Phase D: Complex queries work (GROUP BY, JOIN, ORDER BY)
- Phase E: All 292 spec lines pass

**Final Validation**: All execution-engine spec scenarios pass (11 requirements, 292 lines)
