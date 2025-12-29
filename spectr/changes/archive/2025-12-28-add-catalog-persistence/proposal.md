# Change: Add Catalog Persistence

## Why

Currently, dukdb-go's catalog is **100% in-memory**. All table schemas and data are lost when the engine closes. The `path` parameter to `Open()` is stored but never used - both `:memory:` and `/path/to/db.duckdb` behave identically.

**Current State** (from exploration):
- `internal/catalog/`: In-memory maps for schemas and tables
- `internal/storage/`: In-memory vectors for row data
- `internal/engine/`: Path stored but ignored
- No serialization, no WAL, no crash recovery

**Impact**:
- Users cannot persist databases across sessions
- Database state lost on any restart
- Cannot use dukdb-go for persistent applications

## What Changes

### 1. Persistence Package (internal/persistence/ - NEW)

```go
// FileManager handles database file I/O
type FileManager struct {
    path     string
    file     *os.File
    metadata *DatabaseMetadata
}

// DatabaseMetadata stores file layout information
type DatabaseMetadata struct {
    MagicNumber    [8]byte   // "DUKDBGO\x00"
    Version        uint32    // File format version
    CatalogOffset  int64     // Position of catalog JSON
    DataBlockMap   []BlockInfo // Data block locations
    Checksum       [32]byte  // SHA-256 of metadata
}

type BlockInfo struct {
    TableName  string
    RowGroupID int
    Offset     int64
    Size       int64
    Checksum   [32]byte
}
```

### 2. Catalog Serialization (internal/catalog/serialize.go - NEW)

```go
// CatalogData represents serializable catalog state
type CatalogData struct {
    Version int                     `json:"version"`
    Schemas map[string]*SchemaData  `json:"schemas"`
}

type SchemaData struct {
    Name   string                  `json:"name"`
    Tables map[string]*TableData   `json:"tables"`
}

type TableData struct {
    Name       string        `json:"name"`
    Schema     string        `json:"schema"`
    Columns    []ColumnData  `json:"columns"`
    PrimaryKey []int         `json:"primary_key"`
}

type ColumnData struct {
    Name         string    `json:"name"`
    Type         int       `json:"type"`     // dukdb.Type value
    Nullable     bool      `json:"nullable"`
    HasDefault   bool      `json:"has_default"`
    DefaultValue any       `json:"default_value,omitempty"` // Literal only (string/int/float/bool/nil)
    TypeInfo     *TypeInfo `json:"type_info,omitempty"`     // For DECIMAL, nested types
}

// TypeInfo stores precision/scale and nested type definitions
type TypeInfo struct {
    Precision   int         `json:"precision,omitempty"`   // DECIMAL
    Scale       int         `json:"scale,omitempty"`       // DECIMAL
    ElementType *ColumnData `json:"element_type,omitempty"` // LIST, ARRAY
    ArraySize   int         `json:"array_size,omitempty"`  // ARRAY fixed size
    Fields      []ColumnData `json:"fields,omitempty"`     // STRUCT
    KeyType     *ColumnData `json:"key_type,omitempty"`    // MAP
    ValueType   *ColumnData `json:"value_type,omitempty"`  // MAP
    EnumValues  []string    `json:"enum_values,omitempty"` // ENUM dictionary
}

// Export catalog to JSON-serializable structure
func (c *Catalog) Export() *CatalogData {
    c.mu.RLock()
    defer c.mu.RUnlock()

    data := &CatalogData{
        Version: 1,
        Schemas: make(map[string]*SchemaData),
    }

    for name, schema := range c.schemas {
        schemaData := &SchemaData{
            Name:   name,
            Tables: make(map[string]*TableData),
        }
        for tableName, tableDef := range schema.tables {
            schemaData.Tables[tableName] = exportTableDef(tableDef)
        }
        data.Schemas[name] = schemaData
    }

    return data
}

// Import catalog from serialized structure
func (c *Catalog) Import(data *CatalogData) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    for schemaName, schemaData := range data.Schemas {
        schema := c.getOrCreateSchema(schemaName)
        for _, tableData := range schemaData.Tables {
            tableDef := importTableDef(tableData)
            schema.tables[tableDef.Name] = tableDef
        }
    }
    return nil
}
```

### 3. Storage Serialization (internal/storage/serialize.go - NEW)

```go
// RowGroup binary format:
// - Header (24 bytes): magic, rowCount, columnCount, validityOffset
// - Column data (variable): sequential column vectors
// - Validity masks (variable): bit-packed nullability

func (t *Table) ExportRowGroup(rg *RowGroup) ([]byte, error) {
    buf := new(bytes.Buffer)

    // Write header
    binary.Write(buf, binary.LittleEndian, uint32(RowGroupMagic))
    binary.Write(buf, binary.LittleEndian, uint32(rg.RowCount))
    binary.Write(buf, binary.LittleEndian, uint16(len(rg.Columns)))

    // Write column data
    for _, vec := range rg.Columns {
        if err := writeVector(buf, vec); err != nil {
            return nil, err
        }
    }

    return buf.Bytes(), nil
}

func writeVector(buf *bytes.Buffer, vec *Vector) error {
    // Write type
    binary.Write(buf, binary.LittleEndian, uint8(vec.Type))

    // Write validity mask (LSB-first within each byte)
    writeValidityMask(buf, vec.Validity)

    // Write data based on type (all little-endian)
    switch vec.Type {
    // Integer types
    case TYPE_BOOLEAN:
        return writeBoolArray(buf, vec.Data.([]bool))
    case TYPE_TINYINT:
        return writeInt8Array(buf, vec.Data.([]int8))
    case TYPE_SMALLINT:
        return writeInt16Array(buf, vec.Data.([]int16))
    case TYPE_INTEGER:
        return writeInt32Array(buf, vec.Data.([]int32))
    case TYPE_BIGINT:
        return writeInt64Array(buf, vec.Data.([]int64))
    case TYPE_HUGEINT:
        return writeInt128Array(buf, vec.Data.([][2]int64)) // [0]=low, [1]=high
    case TYPE_UTINYINT:
        return writeUint8Array(buf, vec.Data.([]uint8))
    case TYPE_USMALLINT:
        return writeUint16Array(buf, vec.Data.([]uint16))
    case TYPE_UINTEGER:
        return writeUint32Array(buf, vec.Data.([]uint32))
    case TYPE_UBIGINT:
        return writeUint64Array(buf, vec.Data.([]uint64))
    case TYPE_UHUGEINT:
        return writeUint128Array(buf, vec.Data.([][2]uint64)) // [0]=low, [1]=high

    // Floating point (IEEE 754, little-endian)
    case TYPE_FLOAT:
        return writeFloat32Array(buf, vec.Data.([]float32))
    case TYPE_DOUBLE:
        return writeFloat64Array(buf, vec.Data.([]float64))

    // String types (UTF-8, varint length prefix)
    case TYPE_VARCHAR:
        return writeStringArray(buf, vec.Data.([]string))
    case TYPE_BLOB:
        return writeBlobArray(buf, vec.Data.([][]byte))

    // Temporal types
    case TYPE_DATE:
        return writeInt32Array(buf, vec.Data.([]int32)) // days since 1970-01-01
    case TYPE_TIME:
        return writeInt64Array(buf, vec.Data.([]int64)) // microseconds since midnight
    case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ:
        return writeInt64Array(buf, vec.Data.([]int64)) // microseconds since epoch
    case TYPE_INTERVAL:
        return writeIntervalArray(buf, vec.Data) // months(4)+days(4)+micros(8)

    // Fixed-size types
    case TYPE_UUID:
        return writeUUIDArray(buf, vec.Data.([][16]byte)) // 16 bytes each
    case TYPE_DECIMAL:
        return writeDecimalArray(buf, vec.Data, vec.TypeInfo) // size based on precision

    // Nested types
    case TYPE_LIST:
        return writeListVector(buf, vec)
    case TYPE_STRUCT:
        return writeStructVector(buf, vec)
    case TYPE_MAP:
        return writeMapVector(buf, vec)
    case TYPE_ARRAY:
        return writeArrayVector(buf, vec) // fixed-size list
    case TYPE_ENUM:
        return writeEnumVector(buf, vec) // stored as uint32 indices

    default:
        return fmt.Errorf("unsupported type for serialization: %v", vec.Type)
    }
}
```

### 4. Engine Integration (internal/engine/engine.go - MODIFIED)

```go
func (e *Engine) Open(path string, config *Config) (BackendConn, error) {
    e.path = path

    if path != ":memory:" {
        if fileExists(path) {
            // Load existing database
            if err := e.loadFromFile(path); err != nil {
                return nil, fmt.Errorf("failed to load database: %w", err)
            }
        }
        // Mark as persistent (will save on close)
        e.persistent = true
    }

    return e.newConn(config)
}

func (e *Engine) loadFromFile(path string) error {
    fm, err := persistence.OpenFile(path)
    if err != nil {
        return err
    }
    defer fm.Close()

    // Load catalog metadata
    catalogData, err := fm.ReadCatalog()
    if err != nil {
        return err
    }
    if err := e.catalog.Import(catalogData); err != nil {
        return err
    }

    // Load table data
    for _, blockInfo := range fm.DataBlocks() {
        tableData, err := fm.ReadBlock(blockInfo)
        if err != nil {
            return err
        }
        if err := e.storage.ImportTable(blockInfo.TableName, tableData); err != nil {
            return err
        }
    }

    return nil
}

func (e *Engine) Close() error {
    if e.persistent && e.path != ":memory:" {
        if err := e.saveToFile(e.path); err != nil {
            return fmt.Errorf("failed to save database: %w", err)
        }
    }
    return nil
}

func (e *Engine) saveToFile(path string) error {
    // Atomic save: write to temp file, verify, then rename
    tmpPath := path + ".tmp"

    fm, err := persistence.CreateFile(tmpPath)
    if err != nil {
        return err
    }

    // Write catalog
    catalogData := e.catalog.Export()
    if err := fm.WriteCatalog(catalogData); err != nil {
        fm.Close()
        os.Remove(tmpPath)
        return err
    }

    // Write table data
    for tableName, table := range e.storage.Tables() {
        for i, rg := range table.RowGroups {
            data, err := table.ExportRowGroup(rg)
            if err != nil {
                fm.Close()
                os.Remove(tmpPath)
                return err
            }
            if err := fm.WriteBlock(tableName, i, data); err != nil {
                fm.Close()
                os.Remove(tmpPath)
                return err
            }
        }
    }

    // Finalize with metadata and checksum
    if err := fm.Finalize(); err != nil {
        fm.Close()
        os.Remove(tmpPath)
        return err
    }
    fm.Close()

    // Verify checksum before rename
    if err := persistence.VerifyFile(tmpPath); err != nil {
        os.Remove(tmpPath)
        return fmt.Errorf("save verification failed: %w", err)
    }

    // Atomic rename (preserves original on failure)
    if err := os.Rename(tmpPath, path); err != nil {
        os.Remove(tmpPath)
        return err
    }

    return nil
}
```

### 5. File Format (database file structure)

**Constraints**:
- **Single-process only**: Concurrent access from multiple processes is undefined behavior
- **All integers**: Little-endian byte order
- **All floats**: IEEE 754, little-endian
- **All strings**: UTF-8 encoded, varint length prefix (max 2GB per string)
- **Validity masks**: LSB-first within each byte, unused bits in final byte are 0

```
Database File (.dukdb / .duckdb)
┌────────────────────────────────┐
│ Header (64 bytes)              │
│ - Magic: 0x44554B4442474F00 (8)│  "DUKDBGO\x00"
│ - Version: 1 (4, uint32 LE)    │
│ - Flags: 0 (4, reserved)       │
│ - Catalog offset (8, int64 LE) │
│ - Block index offset (8)       │
│ - Block count (4, uint32 LE)   │
│ - Reserved (32)                │
├────────────────────────────────┤
│ Data Block 0                   │
│ - Magic: 0x524F5747 (4)        │  "ROWG"
│ - RowCount (4, uint32 LE)      │
│ - ColumnCount (2, uint16 LE)   │
│ - Reserved (6)                 │
│ - Column 0: type(1) + validity │
│   + data (variable)            │
│ - Column 1: ...                │
│ - Block checksum (32, SHA-256) │
├────────────────────────────────┤
│ Data Block 1 ...               │
├────────────────────────────────┤
│ Data Block N                   │
├────────────────────────────────┤
│ Catalog (JSON, gzip)           │
│ - Schemas, tables, columns     │
│ - Compressed with gzip level 6 │
├────────────────────────────────┤
│ Block Index (binary)           │
│ - Entry count (4, uint32 LE)   │
│ - Per entry:                   │
│   - TableName (varint + UTF-8) │
│   - RowGroupID (4, uint32 LE)  │
│   - Offset (8, int64 LE)       │
│   - Size (8, int64 LE)         │
│   - Checksum (32, SHA-256)     │
├────────────────────────────────┤
│ Footer (32 bytes)              │
│ - SHA-256 of (header + blocks  │
│   + catalog + block index)     │
└────────────────────────────────┘
```

**Validation Order**:
1. Read header, verify magic and version
2. Read block index, verify each block's checksum
3. Read catalog, decompress and parse
4. Compute footer checksum and compare

### 6. Deterministic Testing Support

**Requirements**:
- All tests MUST use `t.TempDir()` for file isolation
- No hardcoded paths or shared test files
- Tests must be safe for parallel execution (`go test -parallel`)
- No time-dependent behavior (no clocks needed for persistence)

```go
func TestCatalogPersistence(t *testing.T) {
    t.Parallel() // Safe for parallel execution
    tmpDir := t.TempDir() // Automatic cleanup

    dbPath := filepath.Join(tmpDir, "test.duckdb")

    // Create and populate database
    db1, err := sql.Open("dukdb", dbPath)
    require.NoError(t, err)
    _, err = db1.Exec(`CREATE TABLE users (id INTEGER, name VARCHAR)`)
    require.NoError(t, err)
    _, err = db1.Exec(`INSERT INTO users VALUES (1, 'Alice')`)
    require.NoError(t, err)
    db1.Close()

    // Reopen and verify
    db2, err := sql.Open("dukdb", dbPath)
    require.NoError(t, err)
    defer db2.Close()

    var id int
    var name string
    err = db2.QueryRow(`SELECT * FROM users`).Scan(&id, &name)
    require.NoError(t, err)
    assert.Equal(t, 1, id)
    assert.Equal(t, "Alice", name)
}

func TestAtomicSavePreservesOriginal(t *testing.T) {
    t.Parallel()
    tmpDir := t.TempDir()
    dbPath := filepath.Join(tmpDir, "test.duckdb")

    // Create initial database
    db1, _ := sql.Open("dukdb", dbPath)
    db1.Exec(`CREATE TABLE t (x INTEGER)`)
    db1.Exec(`INSERT INTO t VALUES (1)`)
    db1.Close()

    // Get original file content
    original, _ := os.ReadFile(dbPath)

    // Simulate failed save (e.g., via mock filesystem error)
    // After failure, original file should be unchanged
    current, _ := os.ReadFile(dbPath)
    assert.Equal(t, original, current)
}

func TestAllTypesRoundTrip(t *testing.T) {
    t.Parallel()
    tmpDir := t.TempDir()
    dbPath := filepath.Join(tmpDir, "types.duckdb")

    testCases := []struct {
        typeName string
        create   string
        insert   string
        expected any
    }{
        {"BOOLEAN", "v BOOLEAN", "true", true},
        {"INTEGER", "v INTEGER", "42", int32(42)},
        {"BIGINT", "v BIGINT", "9223372036854775807", int64(9223372036854775807)},
        {"HUGEINT", "v HUGEINT", "170141183460469231731687303715884105727", /* max HUGEINT */},
        {"DOUBLE", "v DOUBLE", "3.14159", float64(3.14159)},
        {"VARCHAR", "v VARCHAR", "'Hello 世界 🌍'", "Hello 世界 🌍"},
        {"DECIMAL(18,4)", "v DECIMAL(18,4)", "12345.6789", /* Decimal */},
        {"UUID", "v UUID", "'550e8400-e29b-41d4-a716-446655440000'", /* UUID */},
        // ... test all types
    }

    for _, tc := range testCases {
        t.Run(tc.typeName, func(t *testing.T) {
            // Create, insert, close, reopen, verify
        })
    }
}
```

## Impact

- **Affected specs**: catalog-persistence (NEW)
- **Affected code**:
  - NEW: `internal/persistence/` (~600 lines - file management)
  - NEW: `internal/catalog/serialize.go` (~200 lines)
  - NEW: `internal/storage/serialize.go` (~400 lines)
  - MODIFIED: `internal/engine/engine.go` (~100 lines)

- **Dependencies**:
  - `encoding/json` - Catalog serialization
  - `encoding/binary` - Data block encoding
  - `compress/gzip` - Catalog compression
  - `crypto/sha256` - Checksums

## Breaking Changes

None. Existing `:memory:` databases work unchanged. File paths now create/load persistent databases.

## Future Extensions

1. **WAL Support** - Separate proposal for write-ahead logging
2. **Incremental Writes** - Don't rewrite entire file on every close
3. **Concurrent Access** - Multiple readers, single writer
4. **Encryption** - Encrypted database files
