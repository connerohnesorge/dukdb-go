# Implementation Timeline

This document outlines the recommended order for implementing change proposals based on dependencies and foundational requirements.

## Dependency Graph

```
                    ┌─────────────────────┐
                    │  add-data-chunk-api │ (Foundation)
                    └──────────┬──────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
           ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌──────────────────────┐
│  add-scalar-udf │  │  add-table-udf  │  │ add-arrow-integration│
└────────┬────────┘  └────────┬────────┘  └──────────────────────┘
         │                    │                    (optional)
         │                    ▼
         │           ┌──────────────────────┐
         │           │ add-replacement-scan │
         │           └──────────────────────┘
         │
         └─────────► (enables advanced UDF patterns)

Independent tracks:
┌──────────────────────────┐  ┌─────────────────┐  ┌───────────────────┐
│ add-extended-types       │  │ add-profiling   │  │ add-query-appender│
│ (UHUGEINT, BIT, TIME_NS) │  │     -api        │  │                   │
└──────────────────────────┘  └─────────────────┘  └───────────────────┘

┌────────────────────────────┐
│ add-statement-introspection│
└────────────────────────────┘
```

## Phase 1: Foundation (Week 1-2)

### 1.1 add-data-chunk-api
**Priority: CRITICAL**

The DataChunk vectorized API is the foundation for all UDF functionality and performance-critical operations.

- No dependencies
- Required by: Scalar UDFs, Table UDFs, Arrow Integration
- Complexity: High (32 type handlers, vector operations)

### 1.2 add-extended-types
**Priority: HIGH**

Extended type support can be implemented in parallel with DataChunk since it only depends on the existing type system.

- Dependencies: Existing TypeInfo system
- Required by: None (but enables broader type coverage)
- Complexity: Medium (3 new types with full integration)

## Phase 2: User-Defined Functions (Week 3-4)

### 2.1 add-scalar-udf
**Priority: HIGH**

Scalar UDFs enable custom SQL functions and are simpler than Table UDFs.

- Dependencies: **add-data-chunk-api**
- Required by: Advanced UDF patterns
- Complexity: Medium-High (executor callbacks, NULL handling)

### 2.2 add-table-udf
**Priority: HIGH**

Table UDFs build on patterns established by Scalar UDFs.

- Dependencies: **add-data-chunk-api**
- Required by: **add-replacement-scan**
- Complexity: High (4 variants, parallel execution)

## Phase 3: Advanced Features (Week 5-6)

### 3.1 add-replacement-scan
**Priority: MEDIUM**

Replacement scans require Table UDFs to be functional for the function replacement mechanism.

- Dependencies: **add-table-udf**
- Required by: None
- Complexity: Medium (callback registration, binder integration)

### 3.2 add-statement-introspection
**Priority: MEDIUM**

Statement introspection is independent and can be implemented any time after Phase 1.

- Dependencies: None (uses existing prepared statement infrastructure)
- Required by: None
- Complexity: Low-Medium (metadata extraction, bound execution)

### 3.3 add-query-appender
**Priority: MEDIUM**

Query appender extends the existing Appender and can be implemented independently.

- Dependencies: Existing Appender, TypeInfo system
- Required by: None
- Complexity: Low-Medium (temp table management, query execution)

## Phase 4: Optional/Performance (Week 7+)

### 4.1 add-profiling-api
**Priority: LOW**

Profiling is a standalone debugging feature with no dependencies.

- Dependencies: None
- Required by: None
- Complexity: Low (PRAGMA wrapper, tree traversal)

### 4.2 add-arrow-integration
**Priority: LOW (Optional)**

Arrow integration is opt-in via build tag and adds a heavy dependency.

- Dependencies: **add-data-chunk-api**
- Required by: None
- Complexity: Medium-High (type mapping, apache/arrow-go dependency)
- Note: Requires build tag `duckdb_arrow`

## Implementation Order Summary

| Order | Proposal | Dependencies | Parallel Track |
|-------|----------|--------------|----------------|
| 1 | add-data-chunk-api | None | A |
| 1 | add-extended-types | None | B |
| 2 | add-scalar-udf | DataChunk | A |
| 2 | add-statement-introspection | None | B |
| 2 | add-query-appender | None | C |
| 3 | add-table-udf | DataChunk | A |
| 3 | add-profiling-api | None | B |
| 4 | add-replacement-scan | Table UDF | A |
| 4 | add-arrow-integration | DataChunk | A (optional) |

## Parallel Execution Strategy

Three independent tracks can proceed simultaneously:

**Track A (Core UDF Pipeline):**
```
data-chunk-api → scalar-udf → table-udf → replacement-scan → arrow-integration
```

**Track B (Type & Debugging):**
```
extended-types → profiling-api
```

**Track C (Appender Enhancements):**
```
query-appender → statement-introspection
```

## Validation Checkpoints

After each phase, run:
```bash
go test -race ./...
golangci-lint run
spectr validate <change-id>
```

## Risk Mitigation

1. **DataChunk API is the critical path** - Block other UDF work if this slips
2. **Arrow integration is optional** - Can be deferred or dropped without affecting core functionality
3. **Extended types are independent** - Can be accelerated or deferred based on user demand
4. **Profiling is low-risk** - Simple PRAGMA wrapper, implement when convenient
