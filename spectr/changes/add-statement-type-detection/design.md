# Design: Statement Type Detection and Properties

## Context

Statement type detection enables applications to determine what kind of SQL statement was parsed (SELECT, INSERT, DDL, etc.) and its expected behavior (returns rows, returns count, modifies data).

**Stakeholders**:
- Application developers needing conditional logic based on statement type
- Query builders and ORMs that need to differentiate queries from mutations
- Transaction managers that need read-only detection
- Streaming implementations that need to know result type

**Constraints**:
- Must be determinable at prepare time (before execution)
- Must not require actual query execution
- Must work with all valid DuckDB SQL statements
- No clock/timing dependencies (pure parse-time analysis)

## Goals / Non-Goals

**Goals**:
1. Expose statement type classification on prepared statements
2. Provide read-only/modifying classification
3. Add return type classification (rows, count, nothing)
4. Support all 30 DuckDB statement types
5. Provide convenience classification methods (IsDML, IsDDL, IsQuery)

**Non-Goals**:
1. Statement execution metrics
2. Query plan analysis
3. Parameter type inference (separate proposal)
4. Column type introspection (already exists)
5. Statement rewriting or transformation

## Decisions

### Decision 1: Statement Type Source

**Options**:
A. Parse SQL string with regex patterns
B. Use parser AST statement type
C. Execute statement and inspect result
D. Call DuckDB C API (CGO only)

**Choice**: B - Use parser AST statement type

**Rationale**:
- Already implemented in parser via `Type()` method on AST nodes
- No execution required
- Deterministic and fast
- Works for all SQL syntax

```go
// Parser AST already has this pattern
func (s *SelectStmt) Type() StmtType {
    return STATEMENT_TYPE_SELECT
}
```

### Decision 2: Properties vs Methods

**Options**:
A. Individual methods (IsReadOnly(), IsDML(), etc.)
B. Single Properties() returning struct
C. Both - struct for batch access, methods for convenience
D. Separate interface per property category

**Choice**: C - Both struct and convenience methods

**Rationale**:
- Struct efficient for accessing multiple properties
- Individual methods more readable for single checks
- Common pattern in database drivers

```go
// Struct for batch access
props, _ := stmt.Properties()
if props.IsReadOnly && props.ReturnType == RETURN_QUERY_RESULT {
    // streaming-safe read query
}

// Methods for single checks
if readOnly, _ := stmt.IsReadOnly(); readOnly {
    // can run on replica
}
```

### Decision 3: Return Type Classification

**Options**:
A. Boolean flags (returnsRows, returnsCount)
B. Enum with three states (QUERY_RESULT, CHANGED_ROWS, NOTHING)
C. Interface with type-specific behavior
D. String constants

**Choice**: B - Enum with three states

**Rationale**:
- Matches DuckDB C++ StatementReturnType
- Clear, exhaustive categorization
- Switch statements work well
- Type-safe

```go
type StmtReturnType uint8

const (
    RETURN_QUERY_RESULT StmtReturnType = iota
    RETURN_CHANGED_ROWS
    RETURN_NOTHING
)
```

### Decision 4: Read-Only Determination

**Options**:
A. Whitelist of read-only statement types
B. Blacklist of modifying statement types
C. Parse AST for modifying operations
D. Track modified database names

**Choice**: A - Whitelist of read-only statement types

**Rationale**:
- Simpler and safer (unknown types treated as modifying)
- Known set of read-only types is small
- No AST traversal needed
- Easy to verify correctness

```go
func (t StmtType) IsReadOnly() bool {
    switch t {
    case STATEMENT_TYPE_SELECT, STATEMENT_TYPE_EXPLAIN,
         STATEMENT_TYPE_PRAGMA, STATEMENT_TYPE_PREPARE,
         STATEMENT_TYPE_RELATION, STATEMENT_TYPE_LOGICAL_PLAN:
        return true
    default:
        return false // safer default
    }
}
```

### Decision 5: Classification Helper Methods Location

**Options**:
A. Methods on StmtType (receiver methods)
B. Standalone functions (IsQueryType(t StmtType))
C. Methods on StmtProperties struct
D. Separate classifier type

**Choice**: A - Methods on StmtType

**Rationale**:
- Most Go-idiomatic
- Discoverability via IDE autocomplete
- Chainable from StatementType()

```go
stmtType, _ := stmt.StatementType()
if stmtType.IsDML() && !stmtType.IsQuery() {
    // mutation, no result set
}
```

### Decision 6: Error Handling

**Options**:
A. Return error for invalid/closed statements
B. Return zero value with ok boolean
C. Panic on invalid state
D. Default values for missing info

**Choice**: A - Return error for invalid/closed statements

**Rationale**:
- Consistent with existing StatementType() API
- Clear error messaging
- Caller must handle statement lifecycle
- Matches database/sql patterns

```go
func (s *Stmt) Properties() (StmtProperties, error) {
    if s.backend == nil {
        return StmtProperties{}, errStmtClosed
    }
    // ...
}
```

### Decision 7: Interface Extension Strategy

**Options**:
A. Add methods to existing BackendStmtIntrospector
B. Create new BackendStmtProperties interface
C. Single combined interface
D. No interface, direct implementation

**Choice**: B - Create new BackendStmtProperties interface

**Rationale**:
- Backward compatible (existing interface unchanged)
- Optional capability (not all backends may support)
- Interface segregation principle
- Can check capability with type assertion

```go
type BackendStmtProperties interface {
    BackendStmt
    Properties() StmtProperties
}

// Usage
if props, ok := s.backend.(BackendStmtProperties); ok {
    return props.Properties(), nil
}
```

### Decision 8: Statement Type String Names

**Options**:
A. fmt.Stringer on StmtType
B. Separate StmtTypeName function
C. Map lookup
D. String() method with switch

**Choice**: A + B - Both Stringer and explicit function

**Rationale**:
- Stringer for logging/debugging
- Explicit function for programmatic access
- Map would require initialization

```go
func (t StmtType) String() string {
    return StmtTypeName(t)
}

func StmtTypeName(t StmtType) string {
    switch t {
    case STATEMENT_TYPE_SELECT:
        return "SELECT"
    // ...
    }
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Missing statement type in parser | Medium | Add Type() to all AST nodes |
| Read-only whitelist incomplete | Low | Conservative default (treat unknown as modifying) |
| Interface proliferation | Low | Clear separation of concerns |
| API surface increase | Low | Consistent patterns, good documentation |

## Performance Considerations

1. **No execution required**: All analysis at prepare time
2. **Constant-time lookups**: Switch statements, no iteration
3. **No allocations**: Properties struct returned by value
4. **Cached in statement**: Computed once, reused

## Migration Plan

### Phase 1: Constants
1. Add missing statement type constants (MERGE_INTO, UPDATE_EXTENSIONS, COPY_DATABASE)
2. Add StmtReturnType enum
3. Add StmtTypeName() function

### Phase 2: Classification Methods
1. Add methods on StmtType (ReturnType, IsDML, IsDDL, IsQuery, IsReadOnly)
2. Add tests for all statement types

### Phase 3: Properties Interface
1. Define StmtProperties struct
2. Define BackendStmtProperties interface
3. Implement in EngineStmt

### Phase 4: Public API
1. Add Properties() to Stmt
2. Add convenience methods (IsReadOnly, IsQuery)
3. Add comprehensive tests

### Phase 5: Documentation
1. Document all statement types
2. Document return type behavior
3. Add examples

### Decision 9: Constant Ordering Compatibility

**Status**: ACKNOWLEDGED INCOMPATIBILITY

The Go constants have historical ordering differences from C++:
- Go backend.go has EXPLAIN at position 4, C++ has CREATE at 4
- This prevents binary-level compatibility with C++ enum values
- The proposal preserves existing Go values for backward compatibility
- New constants (28-30) match C++ exactly
- Applications should use named constants, not numeric values

**Rationale**:
- Breaking existing constant values would break existing code
- Named constants are the correct pattern
- Only internal tools would care about numeric values

### Decision 10: CALL Statement Classification

**Issue**: CALL can execute functions with side effects OR be read-only.

**Choice**: Classify CALL as non-read-only and QUERY_RESULT return type.

**Rationale**:
- Cannot determine read-only status at prepare time
- Conservative approach (assume modifying)
- Return type is QUERY_RESULT since functions can return tables
- Document that actual behavior varies at runtime

## Open Questions (Resolved)

1. **Should EXECUTE return type depend on underlying statement?**
   - Answer: Yes, EXECUTE return type varies - document as "varies"

2. **Should CALL be read-only?**
   - Answer: No, conservative approach assumes side effects possible

3. **What about COPY?**
   - Answer: COPY modifies data (import) or reads (export), classify as modifying to be safe

4. **Should we expose modified_databases/read_databases like C++?**
   - Answer: No, that requires tracking at execution time, beyond scope
