# Change: Add Statement Type Detection and Properties

## Why

The dukdb-go driver has basic `StatementType()` implemented via `BackendStmtIntrospector`, but lacks:

1. **StatementProperties API** - DuckDB C++ exposes rich statement metadata (IsReadOnly, ReturnType, transaction requirements) that neither Go driver surfaces
2. **Missing Statement Types** - MERGE_INTO, UPDATE_EXTENSIONS, COPY_DATABASE constants missing
3. **Statement Return Type** - No way to know if statement returns rows, changed count, or nothing
4. **Read-Only Detection** - No API to determine if statement modifies data

**Current State** (from exploration):
- `backend.go`: 27 StmtType constants (missing 3)
- `internal/engine/conn.go`: StatementType() returns type from parser AST
- No StatementProperties equivalent

**duckdb-go Reference**:
- Has `Stmt.StatementType()` returning `StmtType`
- 25 statement type constants
- No StatementProperties API either (gap in both drivers)

**DuckDB C++ Source** (`statement_type.hpp`):
```cpp
enum class StatementReturnType : uint8_t {
    QUERY_RESULT,    // Returns rows (SELECT, PRAGMA, SHOW)
    CHANGED_ROWS,    // Returns count (INSERT, UPDATE, DELETE)
    NOTHING          // Returns nothing (DDL, SET, etc.)
};

struct StatementProperties {
    bool requires_valid_transaction;
    QueryResultOutputType output_type;
    bool bound_all_parameters;
    StatementReturnType return_type;
    bool allow_stream_result;
    bool read_databases, modified_databases;
    bool IsReadOnly() const;
};
```

## What Changes

### 1. Complete Statement Type Constants (backend.go - MODIFIED)

```go
// Add missing statement types to match DuckDB C++ enum
const (
    // Existing 27 types...

    // ADD:
    STATEMENT_TYPE_MERGE_INTO       StmtType = 28
    STATEMENT_TYPE_UPDATE_EXTENSIONS StmtType = 29
    STATEMENT_TYPE_COPY_DATABASE    StmtType = 30
)

// StmtTypeName returns the string name of a statement type
func StmtTypeName(t StmtType) string {
    switch t {
    case STATEMENT_TYPE_SELECT:
        return "SELECT"
    case STATEMENT_TYPE_INSERT:
        return "INSERT"
    // ... all 30 types
    default:
        return "UNKNOWN"
    }
}
```

### 2. Statement Return Type (backend.go - NEW)

```go
// StmtReturnType indicates what a statement returns when executed
type StmtReturnType uint8

const (
    // RETURN_QUERY_RESULT - Statement returns rows (SELECT, PRAGMA, SHOW, EXPLAIN)
    RETURN_QUERY_RESULT StmtReturnType = iota

    // RETURN_CHANGED_ROWS - Statement returns affected row count (INSERT, UPDATE, DELETE)
    RETURN_CHANGED_ROWS

    // RETURN_NOTHING - Statement returns nothing (DDL, SET, ATTACH, etc.)
    RETURN_NOTHING
)

// ReturnType returns what kind of result the statement produces
func (t StmtType) ReturnType() StmtReturnType {
    switch t {
    case STATEMENT_TYPE_SELECT, STATEMENT_TYPE_EXPLAIN,
         STATEMENT_TYPE_PRAGMA, STATEMENT_TYPE_CALL,
         STATEMENT_TYPE_RELATION, STATEMENT_TYPE_LOGICAL_PLAN:
        return RETURN_QUERY_RESULT

    case STATEMENT_TYPE_INSERT, STATEMENT_TYPE_UPDATE,
         STATEMENT_TYPE_DELETE, STATEMENT_TYPE_MERGE_INTO:
        return RETURN_CHANGED_ROWS

    default:
        return RETURN_NOTHING
    }
}
```

### 3. Statement Properties Interface (backend.go - NEW)

```go
// StmtProperties provides metadata about statement behavior
type StmtProperties struct {
    Type         StmtType       // Statement type (SELECT, INSERT, etc.)
    ReturnType   StmtReturnType // What the statement returns
    IsReadOnly   bool           // True if statement doesn't modify data
    IsStreaming  bool           // True if result can be streamed
    ColumnCount  int            // Number of result columns (0 for non-query)
    ParamCount   int            // Number of parameters
}

// BackendStmtProperties extends BackendStmt with properties access
type BackendStmtProperties interface {
    BackendStmt
    Properties() StmtProperties
}
```

### 4. Statement Properties Implementation (internal/engine/conn.go - MODIFIED)

```go
// Properties returns metadata about the prepared statement
func (s *EngineStmt) Properties() StmtProperties {
    stmtType := s.StatementType()

    return StmtProperties{
        Type:        stmtType,
        ReturnType:  stmtType.ReturnType(),
        IsReadOnly:  s.isReadOnly(),
        IsStreaming: stmtType.ReturnType() == RETURN_QUERY_RESULT,
        ColumnCount: s.ColumnCount(),
        ParamCount:  s.NumInput(),
    }
}

// isReadOnly returns true if the statement doesn't modify any data
func (s *EngineStmt) isReadOnly() bool {
    switch s.StatementType() {
    case STATEMENT_TYPE_SELECT, STATEMENT_TYPE_EXPLAIN,
         STATEMENT_TYPE_PRAGMA, STATEMENT_TYPE_PREPARE,
         STATEMENT_TYPE_RELATION, STATEMENT_TYPE_LOGICAL_PLAN:
        return true
    default:
        return false
    }
}
```

### 5. Statement Type Classification Helpers (stmt_helpers.go - NEW)

```go
// IsDML returns true for INSERT, UPDATE, DELETE, MERGE statements
func (t StmtType) IsDML() bool {
    return t == STATEMENT_TYPE_INSERT ||
           t == STATEMENT_TYPE_UPDATE ||
           t == STATEMENT_TYPE_DELETE ||
           t == STATEMENT_TYPE_MERGE_INTO
}

// IsDDL returns true for CREATE, DROP, ALTER statements
func (t StmtType) IsDDL() bool {
    return t == STATEMENT_TYPE_CREATE ||
           t == STATEMENT_TYPE_DROP ||
           t == STATEMENT_TYPE_ALTER
}

// IsQuery returns true for statements that return result sets
func (t StmtType) IsQuery() bool {
    return t.ReturnType() == RETURN_QUERY_RESULT
}

// ModifiesData returns true if statement writes to database
func (t StmtType) ModifiesData() bool {
    return t.IsDML() || t.IsDDL()
}

// IsTransaction returns true for BEGIN, COMMIT, ROLLBACK
func (t StmtType) IsTransaction() bool {
    return t == STATEMENT_TYPE_TRANSACTION
}
```

### 6. Parser Statement Type Support (internal/parser/ - MODIFIED)

Add `Type()` method to all statement AST nodes:

```go
// In each statement struct, implement Type() method:

func (s *MergeStmt) Type() StmtType {
    return STATEMENT_TYPE_MERGE_INTO
}

func (s *UpdateExtensionsStmt) Type() StmtType {
    return STATEMENT_TYPE_UPDATE_EXTENSIONS
}

func (s *CopyDatabaseStmt) Type() StmtType {
    return STATEMENT_TYPE_COPY_DATABASE
}
```

### 7. Public Stmt API (stmt.go - MODIFIED)

Expose statement properties on the public `Stmt` type:

```go
// Stmt wraps a prepared statement
type Stmt struct {
    backend BackendStmt
    // ...
}

// StatementType returns the type of SQL statement
func (s *Stmt) StatementType() (StmtType, error) {
    if s.backend == nil {
        return STATEMENT_TYPE_INVALID, errStmtClosed
    }
    if intro, ok := s.backend.(BackendStmtIntrospector); ok {
        return intro.StatementType(), nil
    }
    return STATEMENT_TYPE_INVALID, errors.New("statement introspection not supported")
}

// Properties returns metadata about the statement's behavior
func (s *Stmt) Properties() (StmtProperties, error) {
    if s.backend == nil {
        return StmtProperties{}, errStmtClosed
    }
    if props, ok := s.backend.(BackendStmtProperties); ok {
        return props.Properties(), nil
    }
    // Fallback: compute from StatementType if introspector available
    if intro, ok := s.backend.(BackendStmtIntrospector); ok {
        stmtType := intro.StatementType()
        return StmtProperties{
            Type:        stmtType,
            ReturnType:  stmtType.ReturnType(),
            IsReadOnly:  !stmtType.ModifiesData(),
            IsStreaming: stmtType.IsQuery(),
            ColumnCount: intro.ColumnCount(),
            ParamCount:  intro.NumInput(),
        }, nil
    }
    return StmtProperties{}, errors.New("statement properties not supported")
}

// IsReadOnly returns true if statement doesn't modify data
func (s *Stmt) IsReadOnly() (bool, error) {
    props, err := s.Properties()
    if err != nil {
        return false, err
    }
    return props.IsReadOnly, nil
}

// IsQuery returns true if statement returns a result set
func (s *Stmt) IsQuery() (bool, error) {
    props, err := s.Properties()
    if err != nil {
        return false, err
    }
    return props.ReturnType == RETURN_QUERY_RESULT, nil
}
```

### 8. Deterministic Testing Support

Statement type detection is purely based on parse-time analysis - no clock required.
Tests should verify consistent type detection across all statement categories.

```go
func TestStatementTypeDetection(t *testing.T) {
    testCases := []struct {
        sql      string
        expected StmtType
        isQuery  bool
        isDML    bool
        isDDL    bool
        readOnly bool
    }{
        {"SELECT 1", STATEMENT_TYPE_SELECT, true, false, false, true},
        {"INSERT INTO t VALUES (1)", STATEMENT_TYPE_INSERT, false, true, false, false},
        {"UPDATE t SET x = 1", STATEMENT_TYPE_UPDATE, false, true, false, false},
        {"DELETE FROM t", STATEMENT_TYPE_DELETE, false, true, false, false},
        {"CREATE TABLE t (x INT)", STATEMENT_TYPE_CREATE, false, false, true, false},
        {"DROP TABLE t", STATEMENT_TYPE_DROP, false, false, true, false},
        {"EXPLAIN SELECT 1", STATEMENT_TYPE_EXPLAIN, true, false, false, true},
        {"PRAGMA table_info('t')", STATEMENT_TYPE_PRAGMA, true, false, false, true},
    }

    for _, tc := range testCases {
        stmt, err := db.Prepare(tc.sql)
        require.NoError(t, err)

        stmtType, err := stmt.StatementType()
        require.NoError(t, err)
        assert.Equal(t, tc.expected, stmtType)

        props, err := stmt.Properties()
        require.NoError(t, err)
        assert.Equal(t, tc.isQuery, stmtType.IsQuery())
        assert.Equal(t, tc.isDML, stmtType.IsDML())
        assert.Equal(t, tc.isDDL, stmtType.IsDDL())
        assert.Equal(t, tc.readOnly, props.IsReadOnly)

        stmt.Close()
    }
}
```

## Impact

- **Affected specs**: statement-detection (NEW)
- **Affected code**:
  - MODIFIED: `backend.go` (~100 lines - types, constants, interfaces)
  - NEW: `stmt_helpers.go` (~50 lines - classification methods)
  - MODIFIED: `stmt.go` (~80 lines - public API)
  - MODIFIED: `internal/engine/conn.go` (~40 lines - Properties implementation)
  - MODIFIED: `internal/parser/*.go` (~30 lines - Type() methods for missing stmts)

- **Dependencies**:
  - Existing parser AST statement types
  - Existing BackendStmtIntrospector interface

## Breaking Changes

None for Go API. All changes are additive. Existing StatementType() API preserved.

**Note on C++ compatibility**: Numeric constant values differ from C++ for historical reasons (positions 4, 7, 14 differ). Applications should use named constants (STATEMENT_TYPE_SELECT), not numeric values.

## Statement Type Complete List

After this change, all 30 DuckDB statement types will be supported:

| Type | Constant | ReturnType | ReadOnly |
|------|----------|------------|----------|
| SELECT | STATEMENT_TYPE_SELECT | QUERY_RESULT | Yes |
| INSERT | STATEMENT_TYPE_INSERT | CHANGED_ROWS | No |
| UPDATE | STATEMENT_TYPE_UPDATE | CHANGED_ROWS | No |
| DELETE | STATEMENT_TYPE_DELETE | CHANGED_ROWS | No |
| CREATE | STATEMENT_TYPE_CREATE | NOTHING | No |
| DROP | STATEMENT_TYPE_DROP | NOTHING | No |
| ALTER | STATEMENT_TYPE_ALTER | NOTHING | No |
| EXPLAIN | STATEMENT_TYPE_EXPLAIN | QUERY_RESULT | Yes |
| PREPARE | STATEMENT_TYPE_PREPARE | NOTHING | Yes |
| EXECUTE | STATEMENT_TYPE_EXECUTE | varies | varies |
| TRANSACTION | STATEMENT_TYPE_TRANSACTION | NOTHING | No |
| SET | STATEMENT_TYPE_SET | NOTHING | No |
| PRAGMA | STATEMENT_TYPE_PRAGMA | QUERY_RESULT | Yes |
| CALL | STATEMENT_TYPE_CALL | QUERY_RESULT | No* |
| COPY | STATEMENT_TYPE_COPY | CHANGED_ROWS | No |
| ANALYZE | STATEMENT_TYPE_ANALYZE | NOTHING | No |
| VACUUM | STATEMENT_TYPE_VACUUM | NOTHING | No |
| LOAD | STATEMENT_TYPE_LOAD | NOTHING | No |
| EXPORT | STATEMENT_TYPE_EXPORT | NOTHING | No |
| ATTACH | STATEMENT_TYPE_ATTACH | NOTHING | No |
| DETACH | STATEMENT_TYPE_DETACH | NOTHING | No |
| VARIABLE_SET | STATEMENT_TYPE_VARIABLE_SET | NOTHING | No |
| CREATE_FUNC | STATEMENT_TYPE_CREATE_FUNC | NOTHING | No |
| EXTENSION | STATEMENT_TYPE_EXTENSION | NOTHING | No |
| RELATION | STATEMENT_TYPE_RELATION | QUERY_RESULT | Yes |
| LOGICAL_PLAN | STATEMENT_TYPE_LOGICAL_PLAN | QUERY_RESULT | Yes |
| MULTI_STATEMENT | STATEMENT_TYPE_MULTI_STATEMENT | varies | varies |
| MERGE_INTO | STATEMENT_TYPE_MERGE_INTO | CHANGED_ROWS | No |
| UPDATE_EXTENSIONS | STATEMENT_TYPE_UPDATE_EXTENSIONS | NOTHING | No |
| COPY_DATABASE | STATEMENT_TYPE_COPY_DATABASE | NOTHING | No |
