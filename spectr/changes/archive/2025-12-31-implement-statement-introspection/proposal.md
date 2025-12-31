# Change: Implement Statement Introspection API

## Why

**Current State**: dukdb-go HAS working prepared statement implementation in:
- `prepared.go` - PreparedStmt with NumInput(), ExecContext, QueryContext
- `stmt_type.go` - StmtType enum with 30+ statement types and helper methods

**Problem**: The implementation lacks introspection APIs required by the spec:
1. **No Statement Type Detection**: PreparedStmt doesn't expose StatementType() method
2. **No Parameter Metadata**: Cannot get parameter names or infer types
3. **No Result Column Metadata**: Cannot introspect result schema without executing
4. **No Explicit Binding API**: Uses driver.NamedValue directly, no Bind() method
5. **Limited Parameter Support**: Only counts parameters, doesn't extract names/types

**Without These APIs**:
- Applications can't inspect statement type before execution
- Parameter metadata unavailable for validation/tooling
- Result schema unknown until query executes
- Cannot implement prepared statement debugging/profiling tools
- Violates database/sql driver best practices (missing Stmt metadata)

**This Proposal**: Add introspection APIs to PreparedStmt to expose statement metadata.

## What

Extend PreparedStmt with introspection capabilities:

1. **Statement Type Detection** - Parse SQL to detect statement type
   - StatementType() returns StmtType (SELECT, INSERT, UPDATE, DELETE, etc.)
   - Integrates existing StmtType enum from stmt_type.go
   - Simple keyword-based detection (no full parser needed for P0)

2. **Parameter Metadata** - Extract and expose parameter information
   - ParamName(idx) returns parameter name (@name → "name")
   - ParamCount() alias for NumInput() (consistency)
   - ParamType(idx) deferred to P1 (requires type inference)

3. **Result Column Metadata** - **DEFERRED TO P0-4**
   - Requires SQL execution engine to prepare query and get schema
   - ColumnCount(), ColumnName(), ColumnType(), ColumnTypeInfo()
   - Cannot implement without P0-4 (Execution Engine)

4. **Explicit Binding API** - Add Bind() method
   - Bind(idx, value) stores parameter for later execution
   - ExecBound() / QueryBound() execute with bound parameters
   - Maintains backward compatibility with ExecContext/QueryContext

**Scope Limitation**: Result column metadata requires P0-4 (SQL Execution Engine) and is deferred.

## Implementation Strategy

This is a **REFACTORING** proposal with **PARTIAL** implementation.

**Existing Implementation** (`prepared.go`, `stmt_type.go`):
- ✅ PreparedStmt struct with query storage
- ✅ NumInput() parameter counting (positional + named)
- ✅ ExecContext/QueryContext execution
- ✅ StmtType enum with 30+ types
- ✅ extractPositionalPlaceholders() and extractNamedPlaceholders()
- ❌ No statement type detection on PreparedStmt
- ❌ No parameter name extraction API
- ❌ No result schema introspection
- ❌ No explicit Bind() API

**Refactoring Goals (P0-3)**:
1. **Add StatementType()**: Parse SQL query to detect type (SELECT/INSERT/etc.)
   - Simple keyword extraction (first non-comment keyword)
   - Maps to existing StmtType enum
   - No full SQL parser needed (defer complex parsing to P1)

2. **Add ParamName()**: Expose parameter names
   - Reuse existing extractPositionalPlaceholders/extractNamedPlaceholders
   - Return parameter name by index
   - Handle both $1, $2 (positional) and @name (named) styles

3. **Add Bind() API**: Explicit parameter binding
   - Store bound values in PreparedStmt
   - Add ExecBound()/QueryBound() methods
   - Maintain compatibility with ExecContext/QueryContext

**Deferred to P0-4** (requires execution engine):
- ParamType(idx) - Needs type inference from SQL parser
- ColumnCount(), ColumnName(), ColumnType(), ColumnTypeInfo()
- Full SQL parsing and result schema introspection

**Migration Path** (backward compatible):
- Phase 1: Add StatementType() using simple keyword detection
- Phase 2: Add ParamName() using existing placeholder extraction
- Phase 3: Add Bind()/ExecBound()/QueryBound() API
- Phase 4: Update tests, verify no regressions
- Phase 5 (P0-4): Add column metadata after execution engine complete

**Breaking Changes**: **NONE** (all additive APIs, existing methods unchanged)

**Dependency**: **Requires P0-4** for complete implementation (column metadata)

## Impact

### Users
- ✅ **Enables**: Statement type inspection before execution
- ✅ **Enables**: Parameter name extraction for tooling/debugging
- ✅ **Enables**: Explicit parameter binding workflow
- ⚠️ **Partial**: Column metadata deferred to P0-4
- ⚠️ **Breaking**: None (pure additions)

### Codebase
- **Refactored Files** (existing → improved):
  - `prepared.go` → Add StatementType(), ParamName(), Bind() methods
  - `stmt_type.go` → No changes (already complete)
- **New Files**:
  - `internal/parser/simple_detector.go` - Keyword-based statement type detection
- **Dependencies**:
  - **Requires**: StmtType enum (exists in stmt_type.go)
  - **Requires**: P0-4 (Execution Engine) for column metadata
  - **Uses**: Existing placeholder extraction from prepared.go
- **Blocks**: Statement profiling, debugging tools, advanced tooling

### Risks
- **Parsing Complexity**: Simple keyword detection may fail for complex SQL (deferred to P1 full parser)
- **Column Metadata Unavailable**: Cannot implement without execution engine (P0-4 dependency)
- **Mitigation**: Clear documentation that column metadata requires P0-4, simple parsing is best-effort

### Alternatives Considered
1. **Full SQL Parser in P0-3** - Rejected: Too complex, deferred to P1
2. **No introspection APIs** - Rejected: Required by spec and database/sql best practices
3. **Column metadata without engine** - Rejected: Impossible without preparing query

## Success Criteria

- [ ] StatementType() correctly detects SELECT, INSERT, UPDATE, DELETE, CREATE, DROP
- [ ] ParamName() returns correct names for named parameters (@name)
- [ ] ParamName() returns positional names for positional parameters ($1, $2)
- [ ] Bind() stores parameters for later execution
- [ ] ExecBound() executes with bound parameters
- [ ] QueryBound() queries with bound parameters
- [ ] All existing tests pass (backward compatibility)
- [ ] New introspection tests cover all scenarios
- [ ] Documentation updated with P0-4 dependency for column metadata

**Deferred to P0-4** (after execution engine):
- [ ] ParamType() infers parameter types
- [ ] ColumnCount() returns result column count
- [ ] ColumnName() returns column names
- [ ] ColumnType() returns column types
- [ ] ColumnTypeInfo() returns TypeInfo for columns

## Dependencies

### Required Before
- ✅ StmtType enum (exists in stmt_type.go)
- ✅ PreparedStmt struct (exists in prepared.go)
- ⏳ P0-4 SQL Execution Engine (for column metadata)

### Enables After
- Statement profiling APIs
- Query plan visualization
- Advanced debugging tools
- Parameter validation middleware

## Related Specs

- `statement-introspection` - PARTIALLY IMPLEMENTS (statement type + params, column metadata deferred)
- `prepared-statements` - EXTENDS existing implementation

## Rollout Plan

### Phase 1: Statement Type Detection
- Add simple keyword-based SQL parsing
- Implement StatementType() method
- Map to existing StmtType enum

### Phase 2: Parameter Metadata
- Refactor placeholder extraction into reusable functions
- Implement ParamName() method
- Add ParamCount() alias

### Phase 3: Explicit Binding API
- Add bound parameters storage to PreparedStmt
- Implement Bind() method
- Implement ExecBound()/QueryBound()

### Phase 4: Testing
- Unit tests for each statement type
- Integration tests for binding workflow
- Backward compatibility tests

### Phase 5: Column Metadata (P0-4 Dependency)
- Requires execution engine to prepare query
- Implement ColumnCount(), ColumnName(), ColumnType(), ColumnTypeInfo()
- Add result schema caching

## Approval Checklist

- [ ] Design reviewed (see design.md)
- [ ] Spec validated (spectr validate implement-statement-introspection)
- [ ] Tasks sequenced (see tasks.md)
- [ ] Dependencies confirmed (P0-4 required for column metadata)
- [ ] Testing strategy approved (introspection + backward compat tests)
