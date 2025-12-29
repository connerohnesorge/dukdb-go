# Design: Arrow RegisterView Method

## Context

The Arrow integration is 95% complete (`arrow.go` has QueryContext, type conversions, RecordReader). Only `RegisterView()` is missing. This method allows querying external Arrow data without importing it.

**Current stakeholders**:
- Users integrating with Arrow-based systems (Pandas, Polars, PyArrow)
- Internal: Catalog, query execution engine

**Constraints**:
- Must be pure Go (no CGO)
- Must maintain API compatibility with duckdb-go
- Must integrate with existing deterministic testing (quartz)
- Must support all Arrow types that dukdb-go supports

## Goals / Non-Goals

**Goals**:
1. Add `RegisterView(reader, viewName)` method matching duckdb-go signature
2. Allow SQL queries against external Arrow data
3. Support lazy evaluation (don't materialize Arrow data upfront)
4. Clean resource management via release function

**Non-Goals**:
1. Support Arrow C Data Interface (requires CGO)
2. Optimize for million-row Arrow datasets (focus on correctness first)
3. Support Arrow dictionary encoding (defer to later)
4. Support Arrow extension types (defer to later)

## Decisions

### Decision 1: Virtual Table Abstraction

**Options**:
A. Materialize Arrow data to regular table on registration
B. Create virtual table that scans Arrow RecordReader on demand
C. Hybrid: Cache Arrow batches in memory, serve from cache

**Choice**: B - Virtual table with lazy Arrow scanning

**Rationale**:
- Matches DuckDB's approach (Arrow scan is a table function)
- Avoids memory duplication (don't copy Arrow data)
- Allows streaming large Arrow datasets
- A wastes memory and breaks streaming semantics
- C adds complexity without clear benefit for initial implementation

**Implementation**:
```go
type VirtualTable interface {
    Schema() []TypeInfo
    Scan(ctx context.Context, projection []int) (RowIterator, error)
}

type ArrowVirtualTable struct {
    reader arrow.RecordReader
    schema []TypeInfo
}

func (t *ArrowVirtualTable) Scan(ctx context.Context, projection []int) (RowIterator, error) {
    return newArrowRowIterator(t.reader, projection), nil
}
```

### Decision 2: Catalog Integration

**Options**:
A. Add `RegisterVirtualTable()` to catalog
B. Store as special entry in existing table map
C. Create separate view registry

**Choice**: A - New virtual table API in catalog

**Rationale**:
- Clean separation: persistent tables vs ephemeral views
- Future-proof for other virtual table types (CSV, Parquet, etc.)
- Matches DuckDB architecture (table functions → virtual tables)

**Implementation**:
```go
// internal/catalog/catalog.go

func (c *Catalog) RegisterVirtualTable(name string, vtable VirtualTable) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if c.virtualTables == nil {
        c.virtualTables = make(map[string]VirtualTable)
    }

    if _, exists := c.virtualTables[name]; exists {
        return fmt.Errorf("view %s already exists", name)
    }

    c.virtualTables[name] = vtable
    return nil
}

func (c *Catalog) UnregisterVirtualTable(name string) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    delete(c.virtualTables, name)
    return nil
}
```

### Decision 3: Resource Management

**Options**:
A. Automatic cleanup when connection closes
B. Manual cleanup via release function (duckdb-go approach)
C. Reference counting with finalizer

**Choice**: B - Explicit release function

**Rationale**:
- Matches duckdb-go API exactly
- Explicit resource management (no hidden magic)
- Clear ownership semantics (caller controls lifecycle)

**Implementation**:
```go
func (a *Arrow) RegisterView(reader array.RecordReader, viewName string) (release func(), err error) {
    // Create virtual table
    vtable := &ArrowVirtualTable{
        reader: reader,
        schema: arrowSchemaToTypeInfo(reader.Schema()),
    }

    // Register in catalog
    if err := a.conn.catalog.RegisterVirtualTable(viewName, vtable); err != nil {
        return nil, err
    }

    // Return cleanup function
    release = func() {
        a.conn.catalog.UnregisterVirtualTable(viewName)
        reader.Release()
    }

    return release, nil
}
```

### Decision 4: Arrow Type Conversion

**Options**:
A. Reuse existing `duckdbTypeToArrow()` but reverse it
B. Write new `arrowTypeToDuckDB()` function
C. Use bidirectional type mapping table

**Choice**: B - New conversion function (reuse existing patterns)

**Rationale**:
- Existing conversion is DuckDB → Arrow (for QueryContext)
- Need reverse: Arrow → DuckDB (for RegisterView)
- Symmetric functions clearer than trying to reverse existing one

**Implementation**:
```go
func arrowTypeToDuckDB(arrowType arrow.DataType) (TypeInfo, error) {
    switch t := arrowType.(type) {
    case *arrow.BooleanType:
        return TypeInfo{Type: TYPE_BOOLEAN}, nil
    case *arrow.Int32Type:
        return TypeInfo{Type: TYPE_INTEGER}, nil
    case *arrow.StringType:
        return TypeInfo{Type: TYPE_VARCHAR}, nil
    case *arrow.ListType:
        childType, err := arrowTypeToDuckDB(t.Elem())
        if err != nil {
            return TypeInfo{}, err
        }
        return TypeInfo{
            Type: TYPE_LIST,
            typeDetails: &ListDetails{Child: childType},
        }, nil
    // ... all other types
    }
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Virtual table scan performance | Medium | Profile and optimize hot paths, benchmark vs materialization |
| Arrow type conversion bugs | High | Comprehensive type coverage tests, cross-validate with duckdb-go |
| Resource leaks if release not called | Medium | Document release requirement, add test for leak detection |
| Concurrent view registration | Low | Catalog already has mutex protection |

## Open Questions

1. **Arrow batch buffering**: Read entire RecordReader upfront or stream batches?
   - **Answer**: Stream batches on demand (lazy evaluation)

2. **View name conflicts**: What if view name conflicts with existing table?
   - **Answer**: Return error (catalog check before registration)

3. **Arrow reader exhaustion**: Can view be queried multiple times or is reader one-shot?
   - **Answer**: One-shot (matches Arrow RecordReader semantics). Document that view becomes unusable after scan completes.

4. **NULL handling**: How to handle Arrow validity bitmaps?
   - **Answer**: Convert Arrow null bitmap to DuckDB null representation during row iteration
