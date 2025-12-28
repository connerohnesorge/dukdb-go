# Change: Add Statement Analysis & Introspection

## Why

Users need the ability to analyze prepared statements before execution for:
- Determining statement type (SELECT, INSERT, UPDATE, DELETE, etc.)
- Parameter metadata (count, names, types)
- Result column metadata (names, types)
- Query validation without execution

## What Changes

### Core Types

```go
// StmtType represents the type of SQL statement
type StmtType int

const (
    STATEMENT_TYPE_INVALID StmtType = iota
    STATEMENT_TYPE_SELECT
    STATEMENT_TYPE_INSERT
    STATEMENT_TYPE_UPDATE
    STATEMENT_TYPE_EXPLAIN
    STATEMENT_TYPE_DELETE
    // ... 27 total statement types
)
```

### Statement Methods

```go
// StatementType returns the type of the prepared statement
func (s *Stmt) StatementType() StmtType

// NumInput returns the number of input parameters
func (s *Stmt) NumInput() int

// ParamName returns the name of parameter at index (1-based)
func (s *Stmt) ParamName(index int) (string, error)

// ParamType returns the type of parameter at index (1-based)
func (s *Stmt) ParamType(index int) (Type, error)

// ColumnCount returns number of result columns
func (s *Stmt) ColumnCount() int

// ColumnName returns the name of result column at index
func (s *Stmt) ColumnName(index int) (string, error)

// ColumnType returns the type of result column at index
func (s *Stmt) ColumnType(index int) (Type, error)

// ColumnTypeInfo returns TypeInfo for result column
func (s *Stmt) ColumnTypeInfo(index int) (TypeInfo, error)
```

### Bind Method

```go
// Bind binds a value to parameter at index
func (s *Stmt) Bind(index int, value any) error

// ExecBound executes with bound parameters
func (s *Stmt) ExecBound() (driver.Result, error)

// QueryBound queries with bound parameters
func (s *Stmt) QueryBound() (driver.Rows, error)
```

## Impact

- **Affected specs**: Extends prepared-statements capability
- **Affected code**: Modifications to `statement.go`
- **Dependencies**: None
- **Consumers**: Query analysis tools, ORMs, debugging tools

## Breaking Changes

None. This adds new methods to existing types.
