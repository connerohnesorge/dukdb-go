# P0 Change Proposals - Implementation Timeline

**Generated**: 2025-12-29
**Total Proposals**: 5 (+ 1 incomplete)
**Total Estimated Effort**: 8-11 weeks single engineer

---

## Executive Summary

This timeline orders all active change proposals in chronologically dependent order, enabling efficient parallel execution where dependencies allow.

```
                          ┌─────────────────────────┐
                          │   P0-1a: TypeInfo       │
                          │   (Foundation)          │
                          │   3 weeks               │
                          └───────────┬─────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
              ▼                       ▼                       │
   ┌─────────────────────┐  ┌─────────────────────┐           │
   │  P0-1b: Binary Fmt  │  │  P0-2: DataChunk    │           │
   │  (Persistence)      │  │  (Columnar Storage) │           │
   │  3 weeks            │  │  3 weeks            │           │
   └─────────────────────┘  └──────────┬──────────┘           │
              │                        │                      │
              │                        ▼                      │
              │             ┌─────────────────────┐           │
              │             │  P0-4: Execution    │           │
              │             │  (SQL Engine)       │◄──────────┘
              │             │  5-6 weeks          │
              │             └──────────┬──────────┘
              │                        │
              │                        ▼
              │             ┌─────────────────────┐
              │             │  P0-3: Introspection│
              │             │  (Column Metadata)  │
              │             │  1 week (completion)│
              │             └─────────────────────┘
              │
              └──────► Full Persistence (uses P0-1b)
```

---

## Phase 1: Foundation

### P0-1a: Core TypeInfo Metadata System

**Directory**: `spectr/changes/implement-typeinfo-metadata/`
**Duration**: 3 weeks
**Dependencies**: None (uses existing Type enum, errors.go, catalog)

**What**: Implement TypeInfo interface with 8 constructors, 7 TypeDetails structs, validation.

**Why First**: Foundation for ALL other proposals:
- P0-1b needs TypeInfo for binary serialization
- P0-2 needs TypeInfo for vector type metadata
- P0-4 needs TypeInfo for column metadata
- P0-3 needs TypeInfo for statement introspection

**Key Deliverables**:
- TypeInfo interface: `InternalType()`, `Details()`, `logicalType()`
- 8 constructors: `NewTypeInfo()`, `NewDecimalInfo()`, `NewEnumInfo()`, `NewListInfo()`, `NewStructInfo()`, `NewMapInfo()`, `NewArrayInfo()`, `NewUnionInfo()`
- 7 TypeDetails: `DecimalDetails`, `EnumDetails`, `ListDetails`, etc.
- Validation: Width/scale limits, uniqueness, name constraints

**Rollout**:
- Week 1: Core infrastructure, primitives
- Week 2: Complex types (DECIMAL, ENUM, nested)
- Week 3: Integration, testing, documentation

**Status**: ✅ APPROVED (graded 2 waves)

---

## Phase 2: Parallel Track (after P0-1a)

Two proposals can proceed in PARALLEL after P0-1a completes:

### P0-1b: DuckDB Binary Format (Persistence Track)

**Directory**: `spectr/changes/implement-duckdb-binary-format/`
**Duration**: 3 weeks
**Dependencies**: P0-1a TypeInfo ✅

**What**: Implement DuckDB v64 binary format for catalog persistence.

**Why Parallel**: Only depends on P0-1a, not on P0-2.

**Key Deliverables**:
- Binary format specification (v64)
- TypeInfo serialization (DECIMAL, ENUM, LIST, ARRAY, STRUCT, MAP)
- Catalog serialization
- .duckdb file reader/writer
- Compatibility with DuckDB C++ v1.1.3

**Rollout**:
- Week 1: Binary format spec, ExtraTypeInfo property IDs
- Week 2: TypeInfo + Catalog serialization
- Week 3: Reader, compatibility testing

**Status**: ✅ APPROVED (graded 2 waves)

---

### P0-2: Vector/DataChunk API (Storage Track)

**Directory**: `spectr/changes/implement-vector-datachunk-api/`
**Duration**: 3 weeks
**Dependencies**: P0-1a TypeInfo ✅

**What**: Refactor existing Vector/DataChunk to use TypeInfo, add lifecycle methods.

**Why Parallel**: Only depends on P0-1a, not on P0-1b.

**Key Deliverables**:
- TypeInfo integration (replace `vectorTypeInfo`)
- ValidityMask abstraction
- Vector.Reset(), Vector.Close() lifecycle methods
- Fix DataChunk.reset() bug (size=0, not 2048)
- VectorPool for allocation reduction

**Rollout**:
- Week 1: Core vector, ValidityMask
- Week 2: Complex types, DataChunk
- Week 3: Appender, pooling, benchmarks

**Status**: ✅ APPROVED (graded 2 waves)

---

## Phase 3: Execution Engine

### P0-4: SQL Execution Engine

**Directory**: `spectr/changes/implement-execution-engine/`
**Duration**: 5-6 weeks
**Dependencies**: P0-1a TypeInfo ✅, P0-2 DataChunk ✅

**What**: Complete operator implementations, implement ResultSet, comprehensive testing.

**Why After Phase 2**: Requires both TypeInfo (type metadata) and DataChunk (columnar storage).

**Key Finding**: Pipeline already wired in `conn.go`. Real work is:
- ResultSet wrapping DataChunks
- Operator completion (Aggregate, JOIN, DML)
- Comprehensive testing

**Key Deliverables**:
- DataChunk integration in operators
- ResultSet implementing driver.Rows
- Aggregate operators (SUM, COUNT, AVG, MIN, MAX)
- GROUP BY, JOIN (hash join), ORDER BY, LIMIT
- Complete DML (UPDATE, DELETE, INSERT)
- Performance benchmarks, concurrent access tests

**Phases**:
- Phase A: DataChunk Integration (60-80h, 1.5-2 weeks)
- Phase B: Result Set (20-30h, 0.5-0.75 weeks)
- Phase C: Operator Completion (80-100h, 2-2.5 weeks)
- Phase D: Testing & Integration (30-40h, 0.75-1 week)

**Status**: ✅ APPROVED (graded 3 waves)

---

## Phase 4: Statement Introspection Completion

### P0-3: Statement Introspection API

**Directory**: `spectr/changes/implement-statement-introspection/`
**Duration**: 1 week (partial now) + 1 week (completion after P0-4)
**Dependencies**:
- Partial (now): None (uses existing StmtType, PreparedStmt)
- Completion: P0-4 (for column metadata)

**What**: Add introspection APIs to PreparedStmt.

**Why Split**:
- StatementType(), ParamName(), Bind() - can implement NOW
- ColumnCount(), ColumnName(), ColumnTypeInfo() - require execution engine (P0-4)

**Key Deliverables (Partial - Now)**:
- StatementType() - keyword-based detection
- ParamName() - extract parameter names
- Bind()/ExecBound()/QueryBound() - explicit binding API

**Key Deliverables (After P0-4)**:
- ColumnCount(), ColumnName(), ColumnType()
- ColumnTypeInfo() - TypeInfo for result columns

**Status**: ✅ APPROVED (graded 2 waves)

---

## Incomplete Proposals

### improve-arrow-integration

**Directory**: `spectr/changes/improve-arrow-integration/`
**Status**: ⚠️ INCOMPLETE (specs only, no proposal.md)

**Note**: Contains only spec deltas, no proposal document. Likely deferred to P1.

---

## Dependency Graph

```
Layer 0 (Foundation):
  P0-1a TypeInfo ────────┬────────────────────────────────┐
                         │                                │
Layer 1 (Parallel):      │                                │
  P0-1b Binary Format ◄──┤                                │
  P0-2 DataChunk ◄───────┤                                │
                         │                                │
Layer 2 (Execution):     │                                │
  P0-4 Execution ◄───────┴── requires P0-2 ───────────────┤
                                                          │
Layer 3 (Completion):                                     │
  P0-3 Introspection ◄─── requires P0-4 ──────────────────┘
```

**Critical Path**: P0-1a → P0-2 → P0-4 → P0-3 (completion)

---

## Timeline Summary

| Week | Proposal | Task | Parallel Work |
|------|----------|------|---------------|
| 1 | P0-1a | Core infrastructure, primitives | - |
| 2 | P0-1a | Complex types (DECIMAL, ENUM, nested) | - |
| 3 | P0-1a | Integration, testing | - |
| 4 | P0-1b | Binary format spec | P0-2: Core vector |
| 5 | P0-1b | Serialization | P0-2: Complex types |
| 6 | P0-1b | Compatibility testing | P0-2: Appender, pooling |
| 7 | P0-4 | Phase A: DataChunk integration | P0-3 partial |
| 8 | P0-4 | Phase B: Result Set | - |
| 9 | P0-4 | Phase C: Operators (aggregates) | - |
| 10 | P0-4 | Phase C: Operators (join, DML) | - |
| 11 | P0-4 | Phase D: Testing | P0-3 completion |

**Total**: 11 weeks single engineer
**With Parallelization**: 8-9 weeks (P0-1b || P0-2, P0-3 partial || P0-4)

---

## Time Estimates by Proposal

| Proposal | Hours | Weeks | Dependencies |
|----------|-------|-------|--------------|
| P0-1a TypeInfo | 120-180h | 3 | None |
| P0-1b Binary Format | 100-140h | 3 | P0-1a |
| P0-2 DataChunk | 120-160h | 3 | P0-1a |
| P0-4 Execution Engine | 190-250h | 5-6 | P0-1a, P0-2 |
| P0-3 Introspection | 40-60h | 1-2 | P0-4 (completion) |
| **Total** | **570-790h** | **8-11** | - |

---

## Implementation Order Rationale

1. **P0-1a First**: Foundation for type system. All other proposals need TypeInfo.

2. **P0-1b/P0-2 Parallel**: Both only depend on P0-1a, no interdependencies.
   - P0-1b enables persistence (separate concern)
   - P0-2 enables columnar storage (required for P0-4)

3. **P0-4 After P0-2**: Execution engine needs DataChunk for columnar operators.
   - Cannot build ResultSet without DataChunk
   - Cannot optimize operators without columnar format

4. **P0-3 Split**:
   - Partial (parallel with P0-4): StatementType, ParamName, Bind
   - Completion (after P0-4): Column metadata requires execution engine

---

## Risk Mitigation

### Critical Path Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| P0-1a delays | Blocks ALL work | Prioritize, add resources |
| P0-2 delays | Blocks P0-4 | Keep P0-1b moving, buffer time |
| P0-4 scope creep | Delays P0-3 completion | Fixed scope (basic ops only) |

### Parallelization Opportunities

- **Two Engineers**: One on persistence track (P0-1b), one on storage track (P0-2)
- **After P0-2**: Second engineer can start P0-4 while first finishes P0-1b
- **P0-3 Partial**: Can be done by either engineer during P0-4

---

## Success Criteria

### Phase 1 Complete (P0-1a)
- [ ] TypeInfo interface with 8 constructors
- [ ] All 7 TypeDetails structs
- [ ] 100+ unit tests passing
- [ ] API compatible with duckdb-go v1.4.3

### Phase 2 Complete (P0-1b + P0-2)
- [ ] .duckdb files readable/writable
- [ ] Vector/DataChunk refactored with TypeInfo
- [ ] Appender 1M rows <1 second
- [ ] 90% allocation reduction with pooling

### Phase 3 Complete (P0-4)
- [ ] SELECT/INSERT/UPDATE/DELETE work end-to-end
- [ ] GROUP BY, JOIN, ORDER BY, LIMIT work
- [ ] All 11 execution-engine spec requirements pass
- [ ] 1M row scan <1 second

### Phase 4 Complete (P0-3)
- [ ] StatementType(), ParamName(), Bind() work
- [ ] ColumnCount(), ColumnName(), ColumnTypeInfo() work
- [ ] Full statement introspection API complete

---

## Approval Status

| Proposal | Waves | Issues Fixed | Status |
|----------|-------|--------------|--------|
| P0-1a TypeInfo | 2 | - | ✅ APPROVED |
| P0-1b Binary Format | 2 | 16 | ✅ APPROVED |
| P0-2 DataChunk | 2 | 48 | ✅ APPROVED |
| P0-3 Introspection | 2 | 0 | ✅ APPROVED |
| P0-4 Execution | 3 | 79 | ✅ APPROVED |

**All P0 proposals APPROVED and ready for implementation.**
