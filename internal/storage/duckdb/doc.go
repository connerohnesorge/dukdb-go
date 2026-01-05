// Package duckdb implements native DuckDB file format (.duckdb) support for
// reading and writing databases compatible with DuckDB v1.4.3 CLI and tools.
//
// # Overview
//
// This package provides bidirectional compatibility with DuckDB's native storage
// format, enabling:
//   - Opening .duckdb files created by DuckDB CLI
//   - Creating .duckdb files readable by DuckDB CLI
//   - Full data type mapping for 46+ DuckDB types
//   - Compression algorithm support (CONSTANT, RLE, DICTIONARY, BITPACKING, PFOR_DELTA)
//
// # File Format
//
// DuckDB uses a block-based columnar storage format:
//   - File Header: Magic bytes "DUCK" at offset 8, version, flags
//   - Database Headers: Dual headers at 4096/8192 for crash recovery
//   - Data Blocks: 256KB blocks with checksum at beginning
//   - Row Groups: ~122,880 rows per group with columnar storage
//
// The dual-header design enables crash recovery. On checkpoint, DuckDB writes
// to the inactive header slot and increments an iteration counter. The header
// with the higher iteration is considered active. If a crash occurs during
// checkpoint, the previous header remains valid.
//
// # Usage
//
// To open an existing DuckDB file:
//
//	storage, err := duckdb.NewDuckDBStorage("database.duckdb", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer storage.Close()
//
//	// Load catalog
//	cat, err := storage.LoadCatalog()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// To create a new DuckDB file:
//
//	config := duckdb.DefaultConfig()
//	storage, err := duckdb.CreateDuckDBStorage("new.duckdb", config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer storage.Close()
//
// To scan a table with projection pushdown:
//
//	// Read only columns 0 and 2
//	iter, err := storage.ScanTable("main", "users", []int{0, 2})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer iter.Close()
//
//	for iter.Next() {
//	    row := iter.Row()
//	    fmt.Printf("id=%v, email=%v\n", row[0], row[1])
//	}
//
// # Compatibility
//
// This implementation targets DuckDB v1.4.3 (storage version 67).
// Files created by newer DuckDB versions may not be readable.
//
// Supported compression algorithms:
//   - UNCOMPRESSED: Raw data without compression
//   - CONSTANT: Single value repeated for entire segment
//   - RLE: Run-length encoding with varint counts
//   - DICTIONARY: Dictionary compression with index lookup
//   - BITPACKING: Bit-packed integers using minimum bits
//   - PFOR_DELTA: Packed Frame of Reference with delta encoding
//
// Not yet supported compression algorithms:
//   - FSST: Fast static symbol table (string compression)
//   - ALP: Adaptive lossless floating point compression
//   - ALPRD: ALP with real doubles
//   - CHIMP: Time series float compression
//   - PATAS: Time series compression
//   - ZSTD: Zstandard block compression
//   - ROARING: Roaring bitmap compression
//
// # Type Mapping
//
// All 46+ DuckDB logical types are supported including:
//
// Primitive types:
//   - BOOLEAN: bool
//   - TINYINT: int8
//   - SMALLINT: int16
//   - INTEGER: int32
//   - BIGINT: int64
//   - UTINYINT: uint8
//   - USMALLINT: uint16
//   - UINTEGER: uint32
//   - UBIGINT: uint64
//   - FLOAT: float32
//   - DOUBLE: float64
//
// Large integer types:
//   - HUGEINT: HugeInt (128-bit signed)
//   - UHUGEINT: UHugeInt (128-bit unsigned)
//
// String types:
//   - VARCHAR: string
//   - CHAR(n): string (fixed-length, space-padded)
//   - BLOB: []byte
//   - BIT: BitString (variable-length bit string)
//
// Temporal types:
//   - DATE: time.Time (days since epoch)
//   - TIME: time.Duration (microseconds since midnight)
//   - TIME_NS: TimeNS (nanoseconds since midnight)
//   - TIME_TZ: TimeTZ (time with timezone offset)
//   - TIMESTAMP: time.Time (microseconds since epoch)
//   - TIMESTAMP_S: time.Time (seconds since epoch)
//   - TIMESTAMP_MS: time.Time (milliseconds since epoch)
//   - TIMESTAMP_NS: time.Time (nanoseconds since epoch)
//   - TIMESTAMP_TZ: time.Time (with timezone)
//   - INTERVAL: Interval (months, days, microseconds)
//
// Special types:
//   - DECIMAL(p,s): Decimal (variable precision)
//   - UUID: UUID (128-bit)
//   - ENUM: EnumValue (index + optional string value)
//
// Complex/Nested types:
//   - LIST: ListValue (variable-length array)
//   - STRUCT: StructValue (named fields)
//   - MAP: MapValue (key-value pairs)
//   - UNION: UnionValue (tagged union)
//   - ARRAY(n): ArrayValue (fixed-size array)
//
// # Thread Safety
//
// DuckDBStorage is safe for concurrent read access. Multiple goroutines can
// call ScanTable and LoadCatalog concurrently. Write operations (InsertRows,
// SaveCatalog, Checkpoint) require exclusive access and should not be called
// concurrently with reads.
//
// The TableScanner and RowGroupReader are not thread-safe and should only be
// used by a single goroutine. Create separate scanners for concurrent access
// to the same table.
//
// The BlockManager uses an LRU cache that is thread-safe for concurrent reads.
//
// # Storage Backend Interface
//
// DuckDBStorage implements the StorageBackend interface, allowing it to be
// used as a pluggable storage layer for the dukdb-go engine:
//
//	type StorageBackend interface {
//	    LoadCatalog() (*catalog.Catalog, error)
//	    SaveCatalog(cat *catalog.Catalog) error
//	    ScanTable(schema, table string, projection []int) (StorageRowIterator, error)
//	    InsertRows(schema, table string, rows [][]any) error
//	    DeleteRows(schema, table string, rowIDs []uint64) error
//	    UpdateRows(schema, table string, rowIDs []uint64, updates map[int]any) error
//	    BeginTransaction() (uint64, error)
//	    CommitTransaction(txnID uint64) error
//	    RollbackTransaction(txnID uint64) error
//	    Checkpoint() error
//	    Close() error
//	}
//
// # Performance Considerations
//
// For optimal performance:
//   - Use projection pushdown to read only needed columns
//   - Use batch scanning (ScanRowGroup) for bulk operations
//   - Configure appropriate BlockCacheSize in Config (default: 128 blocks)
//   - Compression is automatically selected based on data characteristics
//
// # Limitations
//
// Current limitations:
//   - Advanced compression algorithms (FSST, ALP, CHIMP) not supported
//   - Encryption not supported
//   - Concurrent checkpoint writes not supported
//   - Storage extensions not supported
//   - Delta/Iceberg format variants not supported
//
// See COMPATIBILITY.md in this package for detailed format compatibility notes.
package duckdb
