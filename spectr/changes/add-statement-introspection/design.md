## Context

Statement introspection allows analysis of prepared statements before execution. This is useful for query builders, ORMs, and debugging tools.

**Stakeholders**: ORM developers, query builder authors, debugging tools

**Constraints**:
- Must work with prepared statements
- Must not require execution
- Must handle all statement types

## Goals / Non-Goals

### Goals
- Statement type identification
- Parameter metadata (count, names, types)
- Result column metadata
- Bound parameter execution
- API compatibility with duckdb-go

### Non-Goals
- Query plan analysis (separate profiling API)
- Cost estimation
- Index hints

## Decisions

### Decision 1: Statement Type Detection

**What**: Detect statement type from prepared statement

**Why**: Different handling for SELECT vs DML

**Implementation**:
```go
func (s *Stmt) StatementType() StmtType {
    // Parse the query and return the root statement type
    return s.parsedStmt.Type()
}
```

### Decision 2: Parameter Metadata

**What**: Expose parameter names and types

**Why**: Enables type-safe parameter binding

**Implementation**:
```go
func (s *Stmt) ParamName(index int) (string, error) {
    if index < 1 || index > s.NumInput() {
        return "", errors.New("index out of range")
    }
    return s.params[index-1].Name, nil
}
```

### Decision 3: Bound Execution

**What**: Separate bind and execute steps

**Why**: Allows binding without immediate execution

**Implementation**:
```go
func (s *Stmt) Bind(index int, value any) error {
    if s.boundParams == nil {
        s.boundParams = make([]any, s.NumInput())
    }
    s.boundParams[index-1] = value
    return nil
}

func (s *Stmt) ExecBound() (driver.Result, error) {
    args := make([]driver.NamedValue, len(s.boundParams))
    for i, v := range s.boundParams {
        args[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
    }
    return s.ExecContext(context.Background(), args)
}
```

## Risks / Trade-offs

### Risk 1: Memory for Bound Parameters
**Risk**: Long-lived statements accumulate bound values
**Mitigation**: Clear bounds after execution or explicit clear method

## Migration Plan

New capability with no migration required.
