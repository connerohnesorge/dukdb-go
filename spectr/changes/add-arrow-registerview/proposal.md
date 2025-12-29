# Change: Add Arrow RegisterView Method

## Why

The current dukdb-go Arrow implementation (`arrow.go` lines 1-640) provides comprehensive Arrow integration including `QueryContext()` for querying as Arrow RecordReaders. However, it lacks the `RegisterView()` method that allows registering external Arrow data as queryable views within the database.

The reference duckdb-go implementation (`duckdb-go/arrow.go` lines 130-168) provides `RegisterView()` which:
- Registers an `arrow.RecordReader` as a named view in DuckDB
- Allows SQL queries against external Arrow data without importing
- Uses Arrow C Data Interface via CGO (`cdata.ExportRecordReader`)
- Returns a release function to clean up Arrow stream resources

This change adds `RegisterView()` to complete dukdb-go Arrow API parity with duckdb-go.

## What Changes

### Core Modification

Add `RegisterView()` method to `Arrow` struct (`arrow.go`):

```go
// RegisterView registers an Arrow RecordReader as a queryable view.
// The view can be queried with SQL until the returned release function is called.
// The caller must call the release function when done to free Arrow resources.
func (a *Arrow) RegisterView(reader array.RecordReader, viewName string) (release func(), err error) {
    // Implementation details in design.md
}
```

### Implementation Approach

**Challenge**: duckdb-go uses CGO to export Arrow data via `cdata.ExportRecordReader()` and DuckDB's C++ `ArrowScan()` function. We need a pure Go approach.

**Solution**: Create a virtual table backed by the Arrow RecordReader:

1. Convert Arrow schema to DuckDB column types
2. Register virtual table in catalog
3. Implement table scan that reads from Arrow RecordReader
4. Return release function that unregisters view and releases Arrow data

## Impact

- **Affected specs**: arrow-api (ADDED), catalog-api (MODIFIED for virtual table support)
- **Affected code**:
  - `arrow.go` - Add `RegisterView()` method (~80 lines)
  - `internal/catalog/catalog.go` - Add virtual table registration (~40 lines)
  - `internal/engine/virtual_table.go` - New file for virtual table interface (~100 lines)
- **Dependencies**:
  - ✅ `arrow.go` - Existing Arrow integration (complete)
  - ✅ `type_info.go` - Type conversion already exists
  - ⚠️ Virtual table system - New abstraction needed

## Breaking Changes

None. This is an additive API:
- `Arrow.RegisterView(reader, name) (release func(), error)` - NEW method

## Notes on Reference Implementation

**duckdb-go Implementation** (`duckdb-go/arrow.go:130-168`):
- Uses `cdata.ExportRecordReader()` to export Arrow stream to C Data Interface
- Calls `arrowmapping.ArrowScan()` CGO function to register with DuckDB C++
- Returns release function that calls `cdata.ReleaseCArrowArrayStream()` and `C.free()`

**Pure Go Approach**:
- No C Data Interface (no CGO)
- Virtual table pattern: SQL queries → VirtualTable.Scan() → Arrow RecordReader.Next()
- Release function unregisters view from catalog and releases Arrow reader

**API Compatibility**:
Must match duckdb-go signature exactly:
```go
func (a *Arrow) RegisterView(reader array.RecordReader, name string) (release func(), error)
```

## Deterministic Testing Requirements

Per `spectr/specs/deterministic-testing/spec.md`:

**Additions Needed**:
- Tag Arrow view registration: `mClock.Now("Arrow", "register", "start")`
- Tag view queries: `mClock.Now("Arrow", "view", "scan")`
- Zero `time.Sleep` in tests
- Trap-based concurrent view registration tests

See delta spec for detailed test scenarios.
