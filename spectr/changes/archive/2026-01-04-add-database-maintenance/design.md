# Design: Database Maintenance Commands

## Context

This change adds comprehensive database maintenance command support to dukdb-go, including PRAGMA statements, EXPLAIN/EXPLAIN ANALYZE, VACUUM, ANALYZE, enhanced CHECKPOINT, and system tables. The implementation follows existing patterns in the codebase and maintains API compatibility with DuckDB.

## Goals / Non-Goals

### Goals
- Implement PRAGMA statement parsing and execution for introspection and configuration
- Implement EXPLAIN and EXPLAIN ANALYZE for query plan visualization
- Implement VACUUM for storage optimization
- Implement ANALYZE for statistics collection
- Enhance CHECKPOINT for full database checkpoint
- Implement system table functions (duckdb_tables, duckdb_columns, etc.)

### Non-Goals
- Full PRAGMA compatibility with every DuckDB PRAGMA (focus on common/useful ones)
- Query optimization based on ANALYZE statistics (future work)
- Parallel VACUUM operations (single-threaded initially)
- Advanced EXPLAIN formats (JSON, YAML - text format only)

## Decisions

### 1. PRAGMA Implementation Strategy

**Decision**: Implement PRAGMA as a special statement type that dispatches to handler functions based on pragma name.

**Rationale**: PRAGMAs are heterogeneous - some return result sets, some modify configuration. A dispatcher pattern allows each pragma to have custom handling.

**Implementation**:
```go
// internal/parser/ast.go
type PragmaStmt struct {
    Name       string      // Pragma name (e.g., "database_size")
    Args       []Expr      // Pragma arguments
    Assignment *SetClause  // For SET PRAGMA name = value
}

func (*PragmaStmt) stmtNode() {}
func (*PragmaStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_PRAGMA }

// internal/binder/bind_stmt.go
type BoundPragmaStmt struct {
    Name       string       // Pragma name
    PragmaType PragmaType   // INFO, CONFIG, PROFILING
    Args       []BoundExpr  // Bound arguments
    Value      BoundExpr    // For SET PRAGMA
}

// internal/executor/physical_pragma.go
type PhysicalPragma struct {
    Stmt    *BoundPragmaStmt
    Child   PhysicalPlan  // For pragmas that execute a query
}

func (e *Executor) executePragma(ctx *ExecutionContext, plan *PhysicalPragma) (*ExecutionResult, error) {
    handler := pragmaHandlers[plan.Stmt.Name]
    return handler(ctx, plan.Stmt)
}
```

### 2. EXPLAIN Output Format

**Decision**: Use text-based tree format matching DuckDB's default EXPLAIN output.

**Rationale**: Text format is most useful for debugging and matches what users see in DuckDB. JSON/YAML can be added later.

**Implementation**:
```
───────────────────────────────
┌─────────────────────────────┐
│      GROUP BY AGGREGATE     │
│   GROUP BY: [sum(val)]      │
└─────────────────────────────┘
───────────────────────────────
│         HASH_JOIN           │
│   LEFT => RIGHT (id = id)   │
└─────────────────────────────┘
```

**Code**:
```go
// internal/executor/physical_explain.go
func formatPlan(plan planner.PhysicalPlan, indent int) string {
    // Generate tree-style output with operator info
    // Include estimated rows, costs for EXPLAIN ANALYZE
}
```

### 3. EXPLAIN ANALYZE Implementation

**Decision**: Execute the query while collecting timing and cardinality data, then format the plan with actual metrics.

**Rationale**: EXPLAIN ANALYZE requires actual execution. We'll instrument operators to collect metrics.

**Implementation**:
```go
type PhysicalExplain struct {
    Child    PhysicalPlan
    Analyze  bool  // true for EXPLAIN ANALYZE
}

type OperatorMetrics struct {
    RowsProduced    int64
    CPUCycles       int64
    WallTime        time.Duration
    MemoryBytes     int64
}

func (e *Executor) executeExplainAnalyze(ctx *ExecutionContext, plan *PhysicalExplain) (*ExecutionResult, error) {
    metrics := make(map[string]*OperatorMetrics)
    // Execute with instrumentation
    // Collect metrics from each operator
    // Return formatted plan with actual vs estimated
}
```

### 4. VACUUM Implementation

**Decision**: Implement VACUUM as a two-phase operation: mark deleted rows, then compact storage.

**Rationale**: Simple and effective. Full garbage collection can be added later.

**Implementation**:
```go
// internal/storage/table.go
func (t *Table) Vacuum() (int64, error) {
    // Phase 1: Scan and identify valid rows
    validRows := make([]map[int]any, 0)
    scanner := t.Scan()
    for {
        chunk := scanner.Next()
        if chunk == nil { break }
        for i := 0; i < chunk.Count(); i++ {
            if !chunk.IsDeleted(i) {
                validRows = append(validRows, chunk.GetRow(i))
            }
        }
    }
    
    // Phase 2: Rewrite storage with only valid rows
    oldSize := t.Size()
    if err := t.Truncate(); err != nil {
        return 0, err
    }
    
    // Re-insert valid rows
    for _, row := range validRows {
        t.InsertRow(row)
    }
    
    return oldSize - t.Size(), nil
}
```

### 5. ANALYZE Statistics Storage

**Decision**: Store statistics in the catalog at column level.

**Implementation**:
```go
// internal/catalog/column.go
type ColumnDef struct {
    Name       string
    Type       dukdb.Type
    Nullable   bool
    HasDefault bool
    Statistics *ColumnStatistics  // ADDED
}

// internal/catalog/statistics.go
type ColumnStatistics struct {
    MinValue       any
    MaxValue       any
    NullCount      int64
    DistinctCount  int64
    Histogram      []int64  // For string columns
    LastUpdated    time.Time
}
```

### 6. CHECKPOINT Enhancement

**Decision**: Implement full checkpoint that writes all pending changes and creates a new checkpoint header.

**Implementation**:
```go
// internal/storage/storage.go
func (s *Storage) Checkpoint() error {
    // 1. Flush all pending writes
    // 2. Merge row groups in each table
    // 3. Write checkpoint metadata
    // 4. Create new checkpoint header
    // 5. Truncate WAL
}
```

### 7. System Tables as Virtual Tables

**Decision**: Implement system tables (`duckdb_tables`, etc.) as virtual tables using the existing virtual table infrastructure.

**Implementation**:
```go
// internal/catalog/system_tables.go
func RegisterSystemTables(catalog *catalog.Catalog) error {
    // duckdb_tables - returns all tables with schema info
    // duckdb_columns - returns all columns with type info
    // duckdb_functions - returns all available functions
    // duckdb_settings - returns all configuration settings
    
    tables := []dukdb.VirtualTable{
        &SystemTable{def: NewSystemTableDef("duckdb_tables", systemTableSchema...)},
        &SystemTable{def: NewSystemTableDef("duckdb_columns", columnsSchema...)},
        // ...
    }
    
    for _, vt := range tables {
        if err := catalog.RegisterVirtualTable(vt); err != nil {
            return err
        }
    }
    return nil
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| PRAGMA implementation complexity | Medium | Start with core PRAGMAs, add more incrementally |
| EXPLAIN ANALYZE performance overhead | Medium | Only collect metrics when EXPLAIN ANALYZE is used |
| VACUUM blocking operations | High | Run VACUUM asynchronously or warn users |
| Statistics accuracy | Low | Start with basic min/max/null counts |

## Migration Plan

1. No migration needed - new commands don't affect existing data
2. PRAGMA statements that were parsing but not executing will now execute
3. ANALYZE statistics are advisory and don't affect existing queries

## Open Questions

- Should PRAGMA setting changes persist across restarts? (DuckDB doesn't persist by default)
- How detailed should EXPLAIN ANALYZE metrics be? (Start simple, add more)
- VACUUM frequency recommendations? (Document in user guide)
