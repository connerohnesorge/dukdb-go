# Statement Introspection Specification Delta

## Summary

This change **PARTIALLY IMPLEMENTS** the existing `statement-introspection` specification. Column metadata APIs are **DEFERRED** to P0-4 (requires SQL Execution Engine).

**Implementation Scope (P0-3)**:
- Statement Type Detection (StatementType method)
- Parameter Metadata (ParamName method)
- Explicit Binding API (Bind, ExecBound, QueryBound methods)

**Deferred to P0-4**:
- Column Metadata (ColumnCount, ColumnName, ColumnType, ColumnTypeInfo)
- Parameter Type Inference (ParamType)

**Reason for Deferral**: Column metadata requires SQL query preparation in execution engine to introspect result schema. Parameter type inference requires full SQL parser. Both are P0-4 scope.

## Integration with Existing Implementation

The implementation extends existing PreparedStmt from `prepared.go`:
- Reuses existing placeholder extraction (extractPositionalPlaceholders, extractNamedPlaceholders)
- Integrates with existing StmtType enum from `stmt_type.go`
- Maintains backward compatibility with ExecContext/QueryContext

## ADDED Requirements

### Requirement: Statement Type Method

PreparedStmt MUST expose statement type via StatementType() method.

**Context**: Allows applications to inspect statement type before execution.

#### Scenario: StatementType for SELECT

```go
stmt, _ := conn.Prepare("SELECT * FROM t")
stmtType := stmt.StatementType()
assert.Equal(t, STATEMENT_TYPE_SELECT, stmtType)
```

#### Scenario: StatementType for INSERT

```go
stmt, _ := conn.Prepare("INSERT INTO t VALUES (1)")
stmtType := stmt.StatementType()
assert.Equal(t, STATEMENT_TYPE_INSERT, stmtType)
```

### Requirement: Parameter Name Extraction

PreparedStmt MUST expose parameter names via ParamName(idx) method.

**Context**: Enables parameter metadata inspection for tooling.

#### Scenario: ParamName for named parameter

```go
stmt, _ := conn.Prepare("SELECT @name, @age")
assert.Equal(t, "name", stmt.ParamName(0))
assert.Equal(t, "age", stmt.ParamName(1))
```

#### Scenario: ParamName for positional parameter

```go
stmt, _ := conn.Prepare("SELECT $1, $2")
assert.Equal(t, "1", stmt.ParamName(0))
assert.Equal(t, "2", stmt.ParamName(1))
```

### Requirement: Explicit Binding API

PreparedStmt MUST support explicit parameter binding via Bind() method.

**Context**: Provides alternative to driver.NamedValue for parameter binding.

#### Scenario: Bind and execute

```go
stmt, _ := conn.Prepare("INSERT INTO t VALUES ($1, $2)")
stmt.Bind(0, 42)
stmt.Bind(1, "hello")
result, err := stmt.ExecBound()
assert.NoError(t, err)
```

#### Scenario: QueryBound returns results

```go
stmt, _ := conn.Prepare("SELECT * FROM t WHERE id = $1")
stmt.Bind(0, 42)
rows, err := stmt.QueryBound()
assert.NoError(t, err)
defer rows.Close()
```

**Note**: P0-3 is a partial implementation. Column metadata and type inference are deferred to P0-4 (requires execution engine).
