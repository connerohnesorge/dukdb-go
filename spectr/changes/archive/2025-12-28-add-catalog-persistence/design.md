# Design: Catalog Persistence

## Context

Currently, dukdb-go's catalog and storage are 100% in-memory. The `path` parameter to `Open()` is stored but never used - both `:memory:` and `/path/to/db.duckdb` behave identically. All table schemas and row data are lost when the engine closes.

**Stakeholders**:
- Application developers needing persistent databases
- Users migrating from duckdb-go expecting file persistence
- Systems requiring durability across restarts

**Constraints**:
- Pure Go implementation (no CGO)
- Must not break existing in-memory behavior
- File format should be extensible for future features (WAL, compression)
- Must handle concurrent read during save operation

## Goals / Non-Goals

**Goals**:
1. Persist catalog metadata (schemas, tables, columns) to disk
2. Persist table row data to disk
3. Load existing databases on Open()
4. Save databases on Close()
5. Maintain data integrity with checksums
6. Support incremental future enhancements

**Non-Goals**:
1. Write-ahead logging (separate proposal)
2. Crash recovery beyond clean shutdown
3. Concurrent access from multiple processes
4. Incremental writes (full save on close)
5. Compression of data blocks (future)
6. Encryption (future)

## Decisions

### Decision 1: File Format Structure

**Options**:
A. Single JSON file (simple, human-readable)
B. SQLite-style pages (proven, complex)
C. Custom binary with JSON catalog (balanced)
D. Multiple files (catalog.json + data files)

**Choice**: C - Custom binary with JSON catalog

**Rationale**:
- JSON catalog is human-debuggable
- Binary data blocks are efficient
- Single file is easier to manage
- Extensible header for future features

```
┌────────────────────────────────┐
│ Header (64 bytes)              │
├────────────────────────────────┤
│ Data Blocks (variable)         │
├────────────────────────────────┤
│ Catalog (JSON, gzip)           │
├────────────────────────────────┤
│ Block Index (binary)           │
├────────────────────────────────┤
│ Footer (32 bytes)              │
└────────────────────────────────┘
```

### Decision 2: Catalog Serialization Format

**Options**:
A. JSON (human-readable, flexible)
B. Protocol Buffers (compact, typed)
C. MessagePack (compact, JSON-like)
D. Custom binary (most efficient)

**Choice**: A - JSON with gzip compression

**Rationale**:
- Human-readable for debugging
- Flexible schema evolution
- Standard library support
- Compression negates size overhead
- Easy to inspect with standard tools

```go
type CatalogData struct {
    Version int                     `json:"version"`
    Schemas map[string]*SchemaData  `json:"schemas"`
}
```

### Decision 3: Data Block Format

**Options**:
A. Row-oriented storage (simple)
B. Column-oriented storage (matches internal format)
C. Hybrid (row groups with columnar data)
D. Direct memory dump

**Choice**: C - Hybrid row groups with columnar data

**Rationale**:
- Matches internal RowGroup structure
- Column-oriented efficient for analytics
- Row groups allow partial loading (future)
- Direct mapping reduces conversion overhead

```
Block Format:
┌─────────────────────────┐
│ Magic (4 bytes)         │
│ RowCount (4 bytes)      │
│ ColumnCount (2 bytes)   │
│ Reserved (6 bytes)      │
├─────────────────────────┤
│ Column 0 Data           │
│ - Type (1 byte)         │
│ - Validity mask         │
│ - Values                │
├─────────────────────────┤
│ Column 1 Data           │
│ ...                     │
└─────────────────────────┘
```

### Decision 4: Checksum Strategy

**Options**:
A. No checksums (simple, risky)
B. Per-block checksums only
C. Full file checksum only
D. Both per-block and footer checksum

**Choice**: D - Both per-block and footer checksum

**Rationale**:
- Per-block allows detecting which block is corrupt
- Footer checksum validates block index
- SHA-256 for security and collision resistance
- Minimal overhead for data integrity

```go
type BlockInfo struct {
    TableName  string
    RowGroupID int
    Offset     int64
    Size       int64
    Checksum   [32]byte  // SHA-256
}
```

### Decision 5: Open Behavior

**Options**:
A. Always create new file
B. Load if exists, create if not
C. Error if file exists (explicit create)
D. Error if file doesn't exist (explicit open)

**Choice**: B - Load if exists, create if not

**Rationale**:
- Matches duckdb-go behavior
- Most intuitive for users
- Supports both new and existing databases
- `:memory:` path skips file operations

```go
func (e *Engine) Open(path string, config *Config) {
    if path != ":memory:" && fileExists(path) {
        e.loadFromFile(path)
    }
    e.persistent = (path != ":memory:")
}
```

### Decision 6: Close Behavior

**Options**:
A. Always save (even if no changes)
B. Save only if modified (dirty tracking)
C. Never auto-save (explicit Save() required)
D. Save on close, explicit checkpoint for periodic saves

**Choice**: A - Always save on close (for simplicity)

**Rationale**:
- Simple implementation
- No dirty tracking needed initially
- Guarantees persistence on clean shutdown
- Future: add dirty tracking for optimization

```go
func (e *Engine) Close() error {
    if e.persistent && e.path != ":memory:" {
        return e.saveToFile(e.path)
    }
    return nil
}
```

### Decision 7: String Encoding in Data Blocks

**Options**:
A. Fixed-size with padding
B. Length-prefixed (varint)
C. Offset table + string data
D. Null-terminated

**Choice**: B - Length-prefixed with varint

**Rationale**:
- No wasted space from padding
- Efficient for variable-length strings
- Varint encoding compact for short strings
- Sequential read-friendly

```go
func writeString(buf *bytes.Buffer, s string) {
    writeVarint(buf, len(s))
    buf.WriteString(s)
}
```

### Decision 8: Null Handling in Data Blocks

**Options**:
A. Sentinel values per type
B. Separate validity bitmap
C. Optional/tagged values
D. Null bitmap in column header

**Choice**: D - Null bitmap in column header

**Rationale**:
- Matches internal Vector validity mask
- Compact (1 bit per row)
- Fast null checking
- Standard columnar format approach

```go
func writeValidityMask(buf *bytes.Buffer, validity []uint64, rowCount int) {
    // Convert uint64[] to packed bytes
    byteCount := (rowCount + 7) / 8
    for i := 0; i < byteCount; i++ {
        buf.WriteByte(getValidityByte(validity, i))
    }
}
```

### Decision 9: File Extension

**Options**:
A. `.duckdb` (matches official)
B. `.dukdb` (unique to this driver)
C. `.db` (generic)
D. No extension requirement

**Choice**: D - No extension requirement

**Rationale**:
- User chooses file name/extension
- `.duckdb` acceptable for compatibility claims
- Don't enforce naming conventions
- Magic number identifies file, not extension

### Decision 10: Atomic Save

**Options**:
A. Write directly to target file
B. Write to temp, rename on success
C. Write to .new, swap with original
D. Copy-on-write with rollback

**Choice**: B - Write to temp file, rename on success

**Rationale**:
- Atomic on most filesystems
- No partial writes on crash
- Simple implementation
- Standard pattern for safe file updates

```go
func (e *Engine) saveToFile(path string) error {
    tmpPath := path + ".tmp"
    if err := e.writeToFile(tmpPath); err != nil {
        os.Remove(tmpPath)
        return err
    }
    return os.Rename(tmpPath, path)
}
```

### Decision 11: Type Serialization

**Options**:
A. Type name strings
B. Type enum integers
C. Type ID with version
D. Full type info JSON

**Choice**: B - Type enum integers

**Rationale**:
- Compact
- Fast to parse
- Already have Type constants
- Version in catalog handles evolution

```go
type ColumnData struct {
    Name     string `json:"name"`
    Type     int    `json:"type"`     // dukdb.Type value
    Nullable bool   `json:"nullable"`
}
```

### Decision 12: Nested Type Handling

**Options**:
A. Flatten nested structures
B. Recursive serialization
C. Store as JSON blobs
D. Separate nested type tables

**Choice**: B - Recursive serialization

**Rationale**:
- Preserves structure
- Handles LIST, STRUCT, MAP
- Consistent with type system
- Matches internal representation

```go
func writeVector(buf *bytes.Buffer, vec *Vector) error {
    switch vec.Type {
    case TYPE_LIST:
        return writeListVector(buf, vec)
    case TYPE_STRUCT:
        return writeStructVector(buf, vec)
    case TYPE_MAP:
        return writeMapVector(buf, vec)
    // ... primitives
    }
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| File corruption | High | Checksums, atomic save |
| Large file sizes | Medium | Future compression support |
| Slow save on large DBs | Medium | Future incremental writes |
| Schema evolution | Medium | Version field, JSON flexibility |
| Concurrent access | Low | Single-process only (documented) |

## Performance Considerations

1. **Lazy loading**: Future enhancement to load row groups on demand
2. **Buffered I/O**: Use buffered writers for sequential writes
3. **Memory mapping**: Consider mmap for large files (future)
4. **Compression**: gzip catalog, future block compression

## Migration Plan

### Phase 1: Core Infrastructure
1. Create persistence package
2. Implement FileManager with header/footer
3. Implement basic read/write

### Phase 2: Catalog Serialization
1. Add Export/Import to Catalog
2. Test round-trip for all schema types
3. Handle nested types

### Phase 3: Data Serialization
1. Implement RowGroup export
2. Implement Vector serialization per type
3. Test round-trip for all data types

### Phase 4: Engine Integration
1. Wire loadFromFile in Open()
2. Wire saveToFile in Close()
3. Add atomic save with temp file

### Phase 5: Testing
1. Unit tests for serialization
2. Integration tests for persistence
3. Corruption/recovery tests

## Open Questions (Resolved)

1. **Should we support multiple schemas?**
   - Answer: Yes, CatalogData.Schemas is a map

2. **What about views?**
   - Answer: Not in scope (views are query-based, not persisted data)

3. **How to handle open connections on save?**
   - Answer: Save happens on engine close, connections already closed

4. **What about indexes?**
   - Answer: Not in scope (indexes are future feature)
