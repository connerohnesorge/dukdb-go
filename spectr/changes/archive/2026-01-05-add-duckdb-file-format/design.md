# Design: Native DuckDB File Format Support

## Context

DuckDB uses a custom binary storage format optimized for analytical workloads. The format is designed for:
- Columnar storage with efficient compression
- Fast sequential scans
- ACID compliance with WAL
- Block-based I/O with checksums

dukdb-go currently has its own persistence via WAL but cannot interoperate with DuckDB's native format. This design specifies how to implement bidirectional compatibility.

**Stakeholders**:
- Users migrating from/to DuckDB CLI
- Applications sharing databases between DuckDB and dukdb-go
- Data engineers needing format interoperability

**Constraints**:
- Must remain pure Go (no CGO)
- Must maintain API compatibility
- Performance should be within 3x of native DuckDB
- Must handle DuckDB v1.4.3 format (can add newer versions later)

## Goals / Non-Goals

**Goals**:
1. Read any `.duckdb` file created by DuckDB v1.4.3
2. Write `.duckdb` files readable by DuckDB v1.4.3
3. Support all data types present in dukdb-go
4. Implement core compression algorithms
5. Maintain ACID guarantees during writes
6. Support lazy/streaming reads for large databases

**Non-Goals**:
1. Support for DuckDB extensions' custom storage
2. Real-time format upgrades (newer DuckDB versions)
3. Advanced compression (ALP, FSST, CHIMP) - phase 2
4. Encrypted database support
5. Concurrent checkpoint writes
6. Direct memory mapping (mmap)

## Decisions

### Decision 1: File Header Format

**DuckDB File Layout** (from `storage_info.hpp` and `single_file_block_manager.cpp`):

DuckDB uses a dual-header design for crash recovery. The file layout is:
- Offset 0-4095: File Header (first 8 bytes are block header storage)
- Offset 4096-8191: Database Header 1 (h1)
- Offset 8192-12287: Database Header 2 (h2)
- Offset 12288+: Data blocks

**File Header Structure** (within first 4096 bytes):
```
Offset 0-7:     Block header storage (8 bytes, used by block manager)
Offset 8-11:    Magic bytes "DUCK" (4 bytes)
Offset 12-19:   Version number (uint64)
Offset 20-27:   Flags (uint64)
... padding to 4096 bytes ...
```

**Database Header Structure** (at offsets 4096 and 8192):

Each database header block has a checksum stored separately at the block header position, NOT as part of the DatabaseHeader data structure itself.

```
Block Layout:
Offset 4096 (or 8192):   Checksum (8 bytes) - stored in block header, NOT part of DatabaseHeader
Offset 4104 (or 8200):   Database Header DATA starts here

Database Header DATA fields (starting at offset 4104 or 8200):
Offset 0-7:     Iteration counter (uint64) - for selecting active header
Offset 8-15:    Meta block pointer (block_id + offset)
Offset 16-23:   Free list pointer (block_id + offset)
Offset 24-31:   Block count (uint64)
Offset 32-39:   Block allocation size (uint64)
Offset 40-47:   Vector size (uint64)
Offset 48-55:   Serialization compatibility (uint64)
```

Note: The checksum covers the DatabaseHeader DATA bytes and is stored at the start of each 4096-byte block (offset 4096 for header 1, offset 8192 for header 2).

**Checksum Algorithm**:
DuckDB uses a custom hash algorithm (not xxhash64):
- For 8-byte aligned chunks: `value * 0xbf58476d1ce4e5b9`
- For remaining bytes: MurmurHash variant with seed `0xe17a1465`

**Choice**: Implement exact DuckDB header format with dual-header support

**Go Implementation**:
```go
type FileHeader struct {
    BlockHeaderStorage [8]byte
    Magic              [4]byte  // "DUCK"
    Version            uint64   // Storage format version
    Flags              uint64   // Feature flags
}

type DatabaseHeader struct {
    Iteration                 uint64       // Incremented on each checkpoint
    MetaBlock                 BlockPointer
    FreeList                  BlockPointer
    BlockCount                uint64
    BlockAllocSize            uint64       // Usually 262144 (256KB)
    VectorSize                uint64       // Usually 2048
    SerializationCompatibility uint64
    // NOTE: Checksum is stored SEPARATELY in the block header (first 8 bytes of each
    // database header block), NOT as a field in this struct. The checksum at offset
    // 4096 covers DatabaseHeader 1 data starting at 4104, and the checksum at offset
    // 8192 covers DatabaseHeader 2 data starting at 8200.
}

type BlockPointer struct {
    BlockID uint64
    Offset  uint32
}

const (
    FileHeaderSize     = 4096
    DatabaseHeaderSize = 4096
    MagicBytes         = "DUCK"
    MagicByteOffset    = 8
    CurrentVersion     = 67  // DuckDB v1.4.3 storage version
    DefaultBlockSize   = 262144 // 256KB
    DefaultVectorSize  = 2048

    // Database header block offsets (where checksums are stored)
    DatabaseHeader1Offset = 4096   // Checksum at 4096, data starts at 4104
    DatabaseHeader2Offset = 8192   // Checksum at 8192, data starts at 8200
    BlockChecksumOffset   = 8      // Data starts 8 bytes after block start
)

// DuckDB custom checksum algorithm
func checksumBlock(data []byte) uint64 {
    var hash uint64

    // Process 8-byte chunks
    for i := 0; i+8 <= len(data); i += 8 {
        value := binary.LittleEndian.Uint64(data[i:])
        hash ^= value * 0xbf58476d1ce4e5b9
    }

    // Process remaining bytes with MurmurHash variant
    remaining := len(data) % 8
    if remaining > 0 {
        offset := len(data) - remaining
        hash ^= murmurHashBytes(data[offset:], 0xe17a1465)
    }

    return hash
}
```

**Header Selection Logic**:
```go
func (s *DuckDBStorage) getActiveHeader() (*DatabaseHeader, error) {
    h1, err1 := s.readDatabaseHeader(DatabaseHeader1Offset)
    h2, err2 := s.readDatabaseHeader(DatabaseHeader2Offset)

    if err1 != nil && err2 != nil {
        return nil, errors.New("both database headers are corrupted")
    }

    // Select header with higher iteration (more recent checkpoint)
    if err1 != nil || (err2 == nil && h2.Iteration > h1.Iteration) {
        return h2, nil
    }
    return h1, nil
}
```

**Rationale**:
- Dual-header design enables crash recovery (always one valid header)
- Iteration counter determines which header is current
- Custom checksum matches DuckDB's implementation exactly
- Version 67 is the correct storage version for DuckDB v1.4.3

### Decision 2: Block Management

**DuckDB Block Structure**:

Checksum is at the BEGINNING of each block, not the end:
```
+------------------+------------------+
|    Checksum      |      Data        |
|    (8 bytes)     |    (variable)    |
+------------------+------------------+
```

For encrypted blocks, the header can be up to 40 bytes (8 byte checksum + encryption metadata).

**Block Types**:
```go
type BlockType uint8

const (
    BlockInvalid    BlockType = 0
    BlockMetaData   BlockType = 1  // Catalog metadata
    BlockRowGroup   BlockType = 2  // Table data
    BlockFreeList   BlockType = 3  // Free block tracking
    BlockIndex      BlockType = 4  // Index data
)
```

**Free List Management**:
DuckDB tracks free blocks using multiple sets:
```go
type FreeListManager struct {
    freeBlocks       map[uint64]struct{} // Currently free blocks
    freeBlocksInUse  map[uint64]struct{} // Free blocks being used in current transaction
    newlyUsedBlocks  map[uint64]struct{} // Blocks used since last checkpoint
    mu               sync.RWMutex
}
```

**Choice**: Implement block manager with read-ahead, write buffering, and proper free list tracking

```go
type BlockManager struct {
    file        *os.File
    blockSize   uint64
    blockCount  uint64
    freeList    *FreeListManager
    cache       *BlockCache
    mu          sync.RWMutex
    version     uint64  // Storage version (67 for v1.4.3)
}

type Block struct {
    ID       uint64
    Type     BlockType
    Checksum uint64    // First 8 bytes of block
    Data     []byte    // Remaining bytes after checksum
}

const (
    BlockChecksumSize    = 8
    EncryptedHeaderSize  = 40  // Max header size for encrypted blocks
    StorageVersion       = 67  // DuckDB v1.4.3
)

func (bm *BlockManager) ReadBlock(id uint64) (*Block, error) {
    bm.mu.RLock()
    defer bm.mu.RUnlock()

    if cached := bm.cache.Get(id); cached != nil {
        return cached, nil
    }

    // Calculate offset (skip file header and both database headers)
    offset := int64(FileHeaderSize + 2*DatabaseHeaderSize) + int64(id)*int64(bm.blockSize)
    rawData := make([]byte, bm.blockSize)

    if _, err := bm.file.ReadAt(rawData, offset); err != nil {
        return nil, err
    }

    // Checksum is at the BEGINNING of the block
    storedChecksum := binary.LittleEndian.Uint64(rawData[:BlockChecksumSize])
    data := rawData[BlockChecksumSize:]

    block := &Block{
        ID:       id,
        Checksum: storedChecksum,
        Data:     data,
    }

    // Verify checksum
    computedChecksum := checksumBlock(data)
    if computedChecksum != storedChecksum {
        return nil, fmt.Errorf("block %d checksum failed: expected %x, got %x",
            id, storedChecksum, computedChecksum)
    }

    bm.cache.Put(block)
    return block, nil
}

func (bm *BlockManager) WriteBlock(block *Block) error {
    bm.mu.Lock()
    defer bm.mu.Unlock()

    // Compute checksum for data
    block.Checksum = checksumBlock(block.Data)

    // Build raw block with checksum at beginning
    rawData := make([]byte, bm.blockSize)
    binary.LittleEndian.PutUint64(rawData[:BlockChecksumSize], block.Checksum)
    copy(rawData[BlockChecksumSize:], block.Data)

    offset := int64(FileHeaderSize + 2*DatabaseHeaderSize) + int64(block.ID)*int64(bm.blockSize)

    if _, err := bm.file.WriteAt(rawData, offset); err != nil {
        return err
    }

    bm.cache.Put(block)
    return nil
}
```

**Rationale**:
- Checksum at beginning matches DuckDB's actual format
- Cache reduces repeated disk reads
- Mutex ensures thread safety
- Free list tracking enables proper block reuse and crash recovery
- Storage version 67 matches DuckDB v1.4.3

### Decision 3: Catalog Serialization

**DuckDB Catalog Structure** (serialized to metadata blocks):
- Schemas (namespaces)
- Tables (columns, types, constraints)
- Views (query definitions)
- Indexes (columns, types)
- Sequences (current value, increment)
- Collations (custom collation orders)
- Types (custom types, enums)
- Prepared Statements
- Functions (scalar, table, aggregate, macro)

**CatalogType Enum Values** (from `catalog_entry_type.hpp`):
```go
type CatalogType uint8

const (
    CatalogInvalid           CatalogType = 0
    CatalogTableEntry        CatalogType = 1   // TABLE_ENTRY
    CatalogSchemaEntry       CatalogType = 2   // SCHEMA_ENTRY
    CatalogViewEntry         CatalogType = 3   // VIEW_ENTRY
    CatalogIndexEntry        CatalogType = 4   // INDEX_ENTRY
    CatalogPreparedStatement CatalogType = 5   // PREPARED_STATEMENT
    CatalogSequenceEntry     CatalogType = 6   // SEQUENCE_ENTRY
    CatalogCollationEntry    CatalogType = 7   // COLLATION_ENTRY
    CatalogTypeEntry         CatalogType = 8   // TYPE_ENTRY
    CatalogDatabaseEntry     CatalogType = 9   // DATABASE_ENTRY

    // Function entries (25-31)
    CatalogTableFunctionEntry     CatalogType = 25
    CatalogScalarFunctionEntry    CatalogType = 26
    CatalogAggregateFunctionEntry CatalogType = 27
    CatalogPragmaFunctionEntry    CatalogType = 28
    CatalogCopyFunctionEntry      CatalogType = 29
    CatalogMacroEntry             CatalogType = 30
    CatalogTableMacroEntry        CatalogType = 31

    // Special entries
    CatalogDeletedEntry  CatalogType = 51  // DELETED_ENTRY
    CatalogRenamedEntry  CatalogType = 52  // RENAMED_ENTRY

    // Secret entries (71-73)
    CatalogSecretEntry           CatalogType = 71  // SECRET_ENTRY
    CatalogSecretTypeEntry       CatalogType = 72  // SECRET_TYPE_ENTRY
    CatalogSecretFunctionEntry   CatalogType = 73  // SECRET_FUNCTION_ENTRY

    // Dependency tracking
    CatalogDependencyEntry       CatalogType = 100 // DEPENDENCY_ENTRY
)
```

**Base CreateInfo Fields** (common to all catalog entries):
All catalog entries share base fields serialized with property IDs:
```go
// Property IDs 100-109 for base CreateInfo
const (
    PropCatalog      = 100
    PropSchema       = 101
    PropTemporary    = 102
    PropInternal     = 103
    PropOnConflict   = 104
    PropSQL          = 105
    PropComment      = 106
    PropTags         = 107
    PropDependencies = 108
)

type CreateInfo struct {
    Catalog      string            // Catalog name
    Schema       string            // Schema name
    Temporary    bool              // Is temporary object
    Internal     bool              // Is internal/system object
    OnConflict   OnCreateConflict  // Conflict resolution
    SQL          string            // Original SQL statement
    Comment      string            // User comment
    Tags         map[string]string // User-defined tags
    Dependencies []DependencyEntry // Object dependencies
}
```

**Serialization Format**:
```go
type CatalogEntry interface {
    Type() CatalogType
    Serialize(w *BinaryWriter) error
    Deserialize(r *BinaryReader) error
}

// Table entry serialization - NOTE: RowGroups are NOT stored here!
// Row groups are stored separately via MetadataManager
type TableCatalogEntry struct {
    CreateInfo           // Embedded base fields
    Name        string
    Columns     []ColumnDefinition
    Constraints []Constraint
    // NO RowGroups field - stored via MetadataManager
}

// Property IDs for TableCatalogEntry (200+)
const (
    PropTableColumns     = 200
    PropTableConstraints = 201
    PropTableStorage     = 202  // Points to MetadataManager for row groups
)

func (t *TableCatalogEntry) Serialize(w *BinaryWriter) error {
    // Write base CreateInfo with property IDs
    t.CreateInfo.Serialize(w)

    // Write table-specific properties
    w.WritePropertyID(PropTableColumns)
    w.WriteUint32(uint32(len(t.Columns)))
    for _, col := range t.Columns {
        col.Serialize(w)
    }

    w.WritePropertyID(PropTableConstraints)
    w.WriteUint32(uint32(len(t.Constraints)))
    for _, c := range t.Constraints {
        c.Serialize(w)
    }

    return w.Err()
}

// View entry serialization
type ViewCatalogEntry struct {
    CreateInfo
    Name    string
    Query   string       // The SELECT query
    Aliases []string     // Column aliases
    Types   []TypeInfo   // Column types
}

// Sequence entry serialization
type SequenceCatalogEntry struct {
    CreateInfo
    Name      string
    Usage     SequenceUsage
    StartWith int64
    Increment int64
    MinValue  int64
    MaxValue  int64
    Cycle     bool
    Counter   int64  // Current value
}

// Index entry serialization
type IndexCatalogEntry struct {
    CreateInfo
    Name            string
    TableName       string
    IndexType       IndexType
    Constraint      IndexConstraintType
    ColumnIDs       []uint64
    UnboundExpressions []Expression
    ParsedExpressions  []Expression
}
```

**Choice**: Match DuckDB's binary serialization format exactly with property IDs

**Rationale**:
- Binary compatibility with DuckDB
- Property IDs enable forward/backward compatibility
- Row groups stored separately via MetadataManager (not in catalog entry)
- Handles all catalog object types including functions and collations

### Decision 4: Row Group Storage

**DuckDB Row Group Structure**:
- Default: 122,880 rows per row group
- Each row group contains DataPointers (one per column)
- Statistics are per-segment (per DataPointer), not per row group
- Validity mask is encoded within ColumnSegmentState or segment data, not as separate field

**RowGroupPointer and DataPointer Structure** (from `data_pointer.hpp`):

Row groups use an indirection layer via MetaBlockPointers. The RowGroupPointer contains a vector of MetaBlockPointers (one per column), each pointing to a metadata block where the actual DataPointer is serialized.

```go
const DefaultRowGroupSize = 122880

// RowGroupPointer - stored in table metadata, points to column metadata
type RowGroupPointer struct {
    TableOID      uint64
    RowStart      uint64
    TupleCount    uint64
    DataPointers  []MetaBlockPointer  // One per column - points to serialized DataPointer
}

// MetaBlockPointer - points to a metadata block containing serialized data
type MetaBlockPointer struct {
    BlockID uint64
    Offset  uint64
}

// DataPointer - the actual column data location (stored IN metadata blocks, not directly in RowGroupPointer)
// This is deserialized from the metadata block pointed to by MetaBlockPointer
type DataPointer struct {
    RowStart     uint64              // Starting row within this segment
    TupleCount   uint64              // Number of tuples in this segment
    Block        BlockPointer        // Where actual column data is stored
    Compression  CompressionType     // How data is compressed
    Statistics   BaseStatistics      // Per-segment statistics
    SegmentState ColumnSegmentState  // Segment-specific state (includes validity info)
}

// BlockPointer for data location (used by DataPointer.Block)
type BlockPointer struct {
    BlockID uint64
    Offset  uint32
}

// ColumnSegmentState contains segment metadata including validity
type ColumnSegmentState struct {
    HasValidityMask bool
    ValidityBlock   BlockPointer  // Where validity mask is stored (if separate)
    // Additional compression-specific state
    StateData       []byte
}

// Statistics are per-segment (per DataPointer)
type BaseStatistics struct {
    HasStats   bool
    HasNull    bool
    NullCount  uint64
    DistinctCount uint64  // Approximate distinct values
    // Type-specific stats stored as bytes
    StatData   []byte
}

// Type-specific statistics
type NumericStatistics struct {
    HasMin bool
    HasMax bool
    Min    any
    Max    any
    Sum    *big.Int  // For numerics
}

type StringStatistics struct {
    HasStats  bool
    MinLen    uint32
    MaxLen    uint32
    HasMaxLen bool
}
```

**ValidityMask Encoding**:
The validity mask (for NULL values) is NOT a separate top-level field. It's either:
1. Encoded in ColumnSegmentState if nulls exist
2. Stored at the beginning of segment data
3. Omitted entirely if column has no nulls

```go
type ValidityMask struct {
    data     []uint64  // Bit array: 1 = valid, 0 = null
    allValid bool      // Optimization: if true, no nulls exist
}

func (v *ValidityMask) IsValid(rowIdx uint64) bool {
    if v.allValid {
        return true
    }
    wordIdx := rowIdx / 64
    bitIdx := rowIdx % 64
    return (v.data[wordIdx] & (1 << bitIdx)) != 0
}
```

**Choice**: Implement lazy row group loading with MetaBlockPointer indirection

```go
type RowGroupReader struct {
    blockManager *BlockManager
    rowGroupPtr  *RowGroupPointer
    currentRow   uint64
    // Cache for resolved DataPointers (lazily loaded from metadata blocks)
    dataPointerCache map[int]*DataPointer
}

func (r *RowGroupReader) ReadColumn(colIdx int) (*Vector, error) {
    // First, resolve the DataPointer from the MetaBlockPointer
    dp, err := r.resolveDataPointer(colIdx)
    if err != nil {
        return nil, err
    }

    // Read compressed data from block
    block, err := r.blockManager.ReadBlock(dp.Block.BlockID)
    if err != nil {
        return nil, err
    }

    // Extract segment data starting at offset
    compressedData := block.Data[dp.Block.Offset:]

    // Decompress based on compression type
    data, err := Decompress(dp.Compression, compressedData, dp.TupleCount)
    if err != nil {
        return nil, err
    }

    // Build vector with per-segment statistics
    vector := NewVector(data, dp.TupleCount)

    // Apply validity mask from segment state
    if dp.SegmentState.HasValidityMask {
        validity, err := r.readValidityMask(dp.SegmentState)
        if err != nil {
            return nil, err
        }
        vector.SetValidityMask(validity)
    }

    return vector, nil
}

// resolveDataPointer reads the DataPointer from the metadata block
// pointed to by the MetaBlockPointer at colIdx
func (r *RowGroupReader) resolveDataPointer(colIdx int) (*DataPointer, error) {
    // Check cache first
    if dp, ok := r.dataPointerCache[colIdx]; ok {
        return dp, nil
    }

    // Get the MetaBlockPointer for this column
    mbp := r.rowGroupPtr.DataPointers[colIdx]

    // Read the metadata block containing the serialized DataPointer
    metaBlock, err := r.blockManager.ReadBlock(mbp.BlockID)
    if err != nil {
        return nil, err
    }

    // Deserialize the DataPointer from the metadata block at the given offset
    dp, err := DeserializeDataPointer(metaBlock.Data[mbp.Offset:])
    if err != nil {
        return nil, err
    }

    // Cache for future access
    r.dataPointerCache[colIdx] = dp
    return dp, nil
}

func (r *RowGroupReader) readValidityMask(state ColumnSegmentState) (*ValidityMask, error) {
    if state.ValidityBlock.BlockID == 0 {
        // Validity is inlined in state data
        return DecodeValidityMask(state.StateData)
    }

    // Read validity from separate block
    block, err := r.blockManager.ReadBlock(state.ValidityBlock.BlockID)
    if err != nil {
        return nil, err
    }

    return DecodeValidityMask(block.Data[state.ValidityBlock.Offset:])
}
```

**Rationale**:
- RowGroupPointer contains MetaBlockPointers (indirection to metadata blocks)
- DataPointer is stored IN metadata blocks, deserialized on demand
- This matches DuckDB's actual serialization structure
- Per-segment statistics enable fine-grained predicate pushdown
- ValidityMask (not NullMask) matches DuckDB naming convention
- Lazy loading reduces memory for large tables
- Column-at-a-time reduces I/O for projections

### Decision 5: Compression Algorithms

**Priority 1 (Must Implement)**:
1. **UNCOMPRESSED** - Raw data
2. **CONSTANT** - Single value repeated (internal/statistics-based selection)
3. **RLE** - Run-length encoding
4. **DICTIONARY** - Dictionary compression
5. **PFOR_DELTA** - Packed Frame of Reference with Delta encoding
6. **BITPACKING** - Bit-packed integers (with multiple internal modes)

**Priority 2 (Future)**:
7. FSST - Fast Static Symbol Table (strings)
8. CHIMP - Time series floating point compression
9. PATAS - Time series compression
10. ALP/ALPRD - Adaptive Lossless floating Point
11. ZSTD - General purpose compression
12. ROARING - Bitmap compression
13. DICT_FSST - Dictionary + FSST combination

**Compression Type Constants** (from `compression_type.hpp`):
```go
type CompressionType uint8

const (
    CompressionAuto         CompressionType = 0   // AUTO - automatic selection
    CompressionUncompressed CompressionType = 1   // UNCOMPRESSED
    CompressionConstant     CompressionType = 2   // CONSTANT
    CompressionRLE          CompressionType = 3   // RLE
    CompressionDictionary   CompressionType = 4   // DICTIONARY
    CompressionPFORDelta    CompressionType = 5   // PFOR_DELTA (not "FOR")
    CompressionBitPacking   CompressionType = 6   // BITPACKING
    CompressionFSST         CompressionType = 7   // FSST
    CompressionCHIMP        CompressionType = 8   // CHIMP
    CompressionPATAS        CompressionType = 9   // PATAS
    CompressionALP          CompressionType = 10  // ALP
    CompressionALPRD        CompressionType = 11  // ALPRD
    CompressionZSTD         CompressionType = 12  // ZSTD
    CompressionRoaring      CompressionType = 13  // ROARING
    CompressionEmpty        CompressionType = 14  // EMPTY (internal)
    CompressionDictFSST     CompressionType = 15  // DICT_FSST
)
```

**BITPACKING Internal Modes**:
BITPACKING includes multiple internal modes for different data patterns:
```go
type BitpackingMode uint8

const (
    BitpackingAuto          BitpackingMode = 0  // AUTO - choose best
    BitpackingConstant      BitpackingMode = 1  // All same value
    BitpackingConstantDelta BitpackingMode = 2  // Constant difference
    BitpackingDeltaFOR      BitpackingMode = 3  // Delta + Frame of Reference
    BitpackingFOR           BitpackingMode = 4  // Frame of Reference only
)
```

**Note**: CONSTANT compression is typically selected automatically based on statistics (all values equal). It is an internal optimization, not usually explicitly selected.

**Constant Compression**:
```go
func DecompressConstant(data []byte, typ Type, count uint64) ([]byte, error) {
    // Data contains single value, repeat it count times
    valueSize := typ.Size()
    value := data[:valueSize]

    result := make([]byte, count*uint64(valueSize))
    for i := uint64(0); i < count; i++ {
        copy(result[i*uint64(valueSize):], value)
    }
    return result, nil
}

func CompressConstant(data []byte, typ Type) ([]byte, bool) {
    valueSize := typ.Size()
    if len(data) < valueSize*2 {
        return nil, false // Not worth compressing
    }

    firstValue := data[:valueSize]
    for i := valueSize; i < len(data); i += valueSize {
        if !bytes.Equal(data[i:i+valueSize], firstValue) {
            return nil, false // Not all same value
        }
    }
    return firstValue, true
}
```

**RLE Compression**:
```go
type RLERun struct {
    Value []byte
    Count uint64
}

func DecompressRLE(data []byte, typ Type) ([]byte, error) {
    r := bytes.NewReader(data)
    var result []byte
    valueSize := typ.Size()

    for r.Len() > 0 {
        count := binary.ReadUvarint(r)
        value := make([]byte, valueSize)
        r.Read(value)

        for i := uint64(0); i < count; i++ {
            result = append(result, value...)
        }
    }
    return result, nil
}
```

**Dictionary Compression**:
```go
type DictionarySegment struct {
    Dictionary [][]byte      // Unique values
    Indices    []uint32      // Index per row
}

func DecompressDictionary(data []byte, typ Type) ([]byte, error) {
    r := bytes.NewReader(data)

    // Read dictionary
    dictSize := binary.ReadUint32(r)
    dictionary := make([][]byte, dictSize)
    for i := uint32(0); i < dictSize; i++ {
        valueLen := binary.ReadUint32(r)
        value := make([]byte, valueLen)
        r.Read(value)
        dictionary[i] = value
    }

    // Read indices and expand
    indexCount := binary.ReadUint64(r)
    var result []byte
    for i := uint64(0); i < indexCount; i++ {
        idx := binary.ReadUint32(r)
        result = append(result, dictionary[idx]...)
    }

    return result, nil
}
```

**BitPacking Compression** (for integers):
```go
func DecompressBitPacking(data []byte, bitWidth uint8, count uint64) ([]uint64, error) {
    result := make([]uint64, count)

    bitPos := 0
    for i := uint64(0); i < count; i++ {
        value := uint64(0)
        for b := uint8(0); b < bitWidth; b++ {
            byteIdx := bitPos / 8
            bitIdx := bitPos % 8
            if data[byteIdx]&(1<<bitIdx) != 0 {
                value |= 1 << b
            }
            bitPos++
        }
        result[i] = value
    }

    return result, nil
}
```

**PFOR_DELTA (Packed Frame of Reference with Delta)**:
```go
// PFOR_DELTA combines frame of reference with delta encoding
// for efficient integer compression. Values are stored as:
// 1. Reference value (frame)
// 2. Bit-packed deltas from previous value (not from reference)
func DecompressPFORDelta(data []byte, typ Type) ([]int64, error) {
    r := bytes.NewReader(data)

    // Read reference (starting value / frame)
    reference := binary.ReadInt64(r)

    // Read bit width for deltas
    bitWidth := binary.ReadUint8(r)

    // Read count
    count := binary.ReadUint64(r)

    // Read bit-packed deltas
    deltas, err := DecompressBitPacking(data[r.Len():], bitWidth, count)
    if err != nil {
        return nil, err
    }

    // Apply deltas cumulatively (delta encoding)
    result := make([]int64, count)
    current := reference
    for i, delta := range deltas {
        current += int64(delta)  // Delta from previous, not from reference
        result[i] = current
    }

    return result, nil
}

// Note: For pure FOR (without delta), use BitPacking with FOR mode
func DecompressFOR(data []byte, typ Type) ([]int64, error) {
    r := bytes.NewReader(data)

    // Read reference (minimum value)
    reference := binary.ReadInt64(r)

    // Read bit width for offsets
    bitWidth := binary.ReadUint8(r)

    // Read count
    count := binary.ReadUint64(r)

    // Read bit-packed offsets from reference
    offsets, err := DecompressBitPacking(data[r.Len():], bitWidth, count)
    if err != nil {
        return nil, err
    }

    // Apply reference to each offset
    result := make([]int64, count)
    for i, offset := range offsets {
        result[i] = reference + int64(offset)
    }

    return result, nil
}
```

**Choice**: Implement algorithms in order of priority, with fallback to uncompressed

**Rationale**:
- Priority 1 covers 90%+ of real-world data patterns
- PFOR_DELTA is the correct name (not just "FOR")
- Fallback ensures we can always read files
- Write path can select best compression per segment

### Decision 6: Type Mapping

**DuckDB Type IDs to dukdb-go Types** (from `types.hpp`):

DuckDB LogicalTypeId values follow a specific numbering scheme with gaps for future expansion:

```go
type LogicalTypeId uint8

var typeMapping = map[uint8]Type{
    // Special/Internal types (0-9)
    0:  TYPE_INVALID,      // INVALID
    1:  TYPE_SQLNULL,      // SQLNULL
    2:  TYPE_UNKNOWN,      // UNKNOWN
    3:  TYPE_ANY,          // ANY (for function overloading)
    4:  TYPE_USER,         // USER (user-defined type reference)
    5:  TYPE_TEMPLATE,     // TEMPLATE

    // Core types (10-39)
    10: TYPE_BOOLEAN,      // BOOLEAN
    11: TYPE_TINYINT,      // TINYINT
    12: TYPE_SMALLINT,     // SMALLINT
    13: TYPE_INTEGER,      // INTEGER
    14: TYPE_BIGINT,       // BIGINT
    15: TYPE_DATE,         // DATE
    16: TYPE_TIME,         // TIME
    17: TYPE_TIMESTAMP_S,  // TIMESTAMP_SEC
    18: TYPE_TIMESTAMP_MS, // TIMESTAMP_MS
    19: TYPE_TIMESTAMP,    // TIMESTAMP
    20: TYPE_TIMESTAMP_NS, // TIMESTAMP_NS
    21: TYPE_DECIMAL,      // DECIMAL
    22: TYPE_FLOAT,        // FLOAT
    23: TYPE_DOUBLE,       // DOUBLE
    24: TYPE_CHAR,         // CHAR (fixed-length)
    25: TYPE_VARCHAR,      // VARCHAR
    26: TYPE_BLOB,         // BLOB
    27: TYPE_INTERVAL,     // INTERVAL
    28: TYPE_UTINYINT,     // UTINYINT
    29: TYPE_USMALLINT,    // USMALLINT
    30: TYPE_UINTEGER,     // UINTEGER
    31: TYPE_UBIGINT,      // UBIGINT
    32: TYPE_TIMESTAMP_TZ, // TIMESTAMP_TZ
    33: TYPE_TIME_TZ,      // TIME_TZ
    35: TYPE_TIME_NS,      // TIME_NS (nanosecond precision time)
    36: TYPE_BIT,          // BIT
    37: TYPE_STRING_LITERAL, // STRING_LITERAL (parsing)
    38: TYPE_INTEGER_LITERAL, // INTEGER_LITERAL (parsing)
    39: TYPE_BIGNUM,       // BIGNUM (arbitrary precision)

    // Large integer types (50-53)
    50: TYPE_HUGEINT,      // HUGEINT
    51: TYPE_UHUGEINT,     // UHUGEINT
    52: TYPE_POINTER,      // POINTER (internal)
    53: TYPE_VALIDITY,     // VALIDITY (internal)
    54: TYPE_UUID,         // UUID

    // Complex/Nested types (100-110)
    100: TYPE_STRUCT,      // STRUCT
    101: TYPE_LIST,        // LIST
    102: TYPE_MAP,         // MAP
    103: TYPE_TABLE,       // TABLE
    104: TYPE_ENUM,        // ENUM
    105: TYPE_AGGREGATE_STATE, // AGGREGATE_STATE
    106: TYPE_LAMBDA,      // LAMBDA
    107: TYPE_UNION,       // UNION
    108: TYPE_ARRAY,       // ARRAY (fixed-size)
    109: TYPE_GEOMETRY,    // GEOMETRY
    110: TYPE_VARIANT,     // VARIANT
}
```

Note: DuckDB's type IDs have gaps (e.g., no 34, no 40-49, no 55-99) to allow for future additions without breaking compatibility.

**Decimal Storage Format**:
Decimals are stored with variable precision using the minimum bytes needed:
```go
func DecimalStorageSize(width uint8) int {
    switch {
    case width <= 4:
        return 1  // int8
    case width <= 9:
        return 2  // int16
    case width <= 18:
        return 4  // int32
    case width <= 38:
        return 8  // int64
    default:
        return 16 // int128 (hugeint)
    }
}
```

**Handling Complex Types**:
```go
func DeserializeType(r *BinaryReader) (TypeInfo, error) {
    typeID := r.ReadUint8()
    baseType := typeMapping[typeID]

    switch baseType {
    case TYPE_DECIMAL:
        width := r.ReadUint8()
        scale := r.ReadUint8()
        return NewDecimalInfo(width, scale)

    case TYPE_CHAR:
        // Fixed-length character type
        length := r.ReadUint32()
        return NewCharInfo(length)

    case TYPE_LIST:
        childType, _ := DeserializeType(r)
        return NewListInfo(childType)

    case TYPE_STRUCT:
        fieldCount := r.ReadUint32()
        entries := make([]StructEntry, fieldCount)
        for i := uint32(0); i < fieldCount; i++ {
            name := r.ReadString()
            fieldType, _ := DeserializeType(r)
            entries[i] = StructEntry{Name: name, Type: fieldType}
        }
        return NewStructInfo(entries...)

    case TYPE_MAP:
        keyType, _ := DeserializeType(r)
        valueType, _ := DeserializeType(r)
        return NewMapInfo(keyType, valueType)

    case TYPE_ARRAY:
        childType, _ := DeserializeType(r)
        size := r.ReadUint32()  // Fixed size for array type
        return NewArrayInfo(childType, size)

    case TYPE_ENUM:
        valueCount := r.ReadUint32()
        values := make([]string, valueCount)
        for i := uint32(0); i < valueCount; i++ {
            values[i] = r.ReadString()
        }
        return NewEnumInfo(values...)

    case TYPE_UNION:
        memberCount := r.ReadUint8()
        members := make([]UnionMember, memberCount)
        for i := uint8(0); i < memberCount; i++ {
            tag := r.ReadString()
            memberType, _ := DeserializeType(r)
            members[i] = UnionMember{Tag: tag, Type: memberType}
        }
        return NewUnionInfo(members...)

    default:
        return NewTypeInfo(baseType)
    }
}
```

**Rationale**:
- Direct mapping for 46+ types including CHAR and TIME_NS
- Variable-size decimal storage for space efficiency
- Internal types documented for completeness (not typically in storage)
- Recursive handling for nested types
- Preserves type metadata (precision, scale, child types)

### Decision 7: File Open/Close Protocol

**Opening a DuckDB File**:
```go
func OpenDuckDBFile(path string, mode OpenMode) (*DuckDBStorage, error) {
    // 1. Open file
    file, err := os.OpenFile(path, modeFlags(mode), 0644)
    if err != nil {
        return nil, err
    }

    // 2. Read and validate header
    header, err := ReadFileHeader(file)
    if err != nil {
        file.Close()
        return nil, err
    }

    if !bytes.Equal(header.Magic[:], []byte(MagicBytes)) {
        file.Close()
        return nil, ErrNotDuckDBFile
    }

    if header.Version > CurrentVersion {
        file.Close()
        return nil, fmt.Errorf("unsupported version %d (max: %d)", header.Version, CurrentVersion)
    }

    // 3. Initialize block manager
    blockMgr := NewBlockManager(file, header.BlockAllocSize)

    // 4. Load catalog from metadata blocks
    catalog, err := LoadCatalog(blockMgr, header.MetaBlock)
    if err != nil {
        file.Close()
        return nil, err
    }

    // 5. Initialize storage with lazy row group loading
    storage := &DuckDBStorage{
        file:         file,
        header:       header,
        blockManager: blockMgr,
        catalog:      catalog,
        rowGroups:    make(map[uint64]*RowGroupHandle), // Lazy load
    }

    return storage, nil
}
```

**Closing with Checkpoint**:
```go
func (s *DuckDBStorage) Close() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 1. Flush any pending writes
    if err := s.Flush(); err != nil {
        return err
    }

    // 2. If modified, checkpoint
    if s.modified {
        if err := s.Checkpoint(); err != nil {
            return err
        }
    }

    // 3. Sync and close file
    if err := s.file.Sync(); err != nil {
        return err
    }

    return s.file.Close()
}
```

### Decision 8: Integration with Existing Engine

**Storage Backend Abstraction**:
```go
type StorageBackend interface {
    // Catalog operations
    LoadCatalog() (*catalog.Catalog, error)
    SaveCatalog(cat *catalog.Catalog) error

    // Data operations
    ScanTable(schema, table string, projection []int) (RowIterator, error)
    InsertRows(schema, table string, rows [][]any) error
    DeleteRows(schema, table string, rowIDs []uint64) error
    UpdateRows(schema, table string, rowIDs []uint64, updates map[int]any) error

    // Transaction support
    BeginTransaction() (uint64, error)
    CommitTransaction(txnID uint64) error
    RollbackTransaction(txnID uint64) error

    // Lifecycle
    Checkpoint() error
    Close() error
}

// Implementations
type MemoryStorage struct { ... }     // Current in-memory
type WALStorage struct { ... }         // Current WAL-based
type DuckDBStorage struct { ... }      // NEW: Native format
```

**Engine Modification**:
```go
func NewEngine(path string, config *Config) (*Engine, error) {
    var storage StorageBackend

    if path == ":memory:" {
        storage = NewMemoryStorage()
    } else if config.StorageFormat == "duckdb" || detectDuckDBFile(path) {
        storage = NewDuckDBStorage(path, config)
    } else {
        storage = NewWALStorage(path, config)
    }

    catalog, err := storage.LoadCatalog()
    if err != nil {
        return nil, err
    }

    return &Engine{
        storage: storage,
        catalog: catalog,
        // ...
    }, nil
}

func detectDuckDBFile(path string) bool {
    f, err := os.Open(path)
    if err != nil {
        return false
    }
    defer f.Close()

    magic := make([]byte, 4)
    f.ReadAt(magic, MagicByteOffset)
    return bytes.Equal(magic, []byte(MagicBytes))
}
```

### Decision 9: Write Path Strategy

**Compression Selection Heuristics**:
```go
func SelectCompression(data []any, typ Type) CompressionType {
    if len(data) == 0 {
        return CompressionNone
    }

    // Check for constant
    if isConstant(data) {
        return CompressionConstant
    }

    // Check for good RLE ratio
    runs := countRuns(data)
    if float64(runs)/float64(len(data)) < 0.3 {
        return CompressionRLE
    }

    // Check for good dictionary ratio
    unique := countUnique(data)
    if float64(unique)/float64(len(data)) < 0.5 {
        return CompressionDictionary
    }

    // For integers, check if bit-packing helps
    if typ.IsInteger() {
        bitWidth := requiredBitWidth(data)
        fullWidth := typ.Size() * 8
        if float64(bitWidth)/float64(fullWidth) < 0.7 {
            return CompressionFOR // FOR includes bit-packing
        }
    }

    return CompressionNone
}
```

**Write Flow**:
```go
func (s *DuckDBStorage) WriteRowGroup(tableOID uint64, rows [][]any) error {
    // 1. Organize by column
    columns := transposeRows(rows)

    // 2. Compress each column
    segments := make([]ColumnSegment, len(columns))
    for i, col := range columns {
        compression := SelectCompression(col, s.getColumnType(tableOID, i))
        compressed, err := Compress(compression, col)
        if err != nil {
            return err
        }

        segments[i] = ColumnSegment{
            ColumnID:    uint16(i),
            Compression: compression,
            Stats:       computeStats(col),
            Data:        compressed,
        }
    }

    // 3. Allocate blocks for segment data
    for i := range segments {
        blocks, err := s.blockManager.AllocateBlocks(len(segments[i].Data))
        if err != nil {
            return err
        }
        segments[i].DataBlocks = blocks

        // Write data to blocks
        s.writeToBlocks(blocks, segments[i].Data)
    }

    // 4. Create row group entry
    rowGroup := &RowGroup{
        TableOID: tableOID,
        RowStart: s.getNextRowID(tableOID),
        RowCount: uint64(len(rows)),
        Columns:  segments,
    }

    // 5. Register row group in catalog
    s.catalog.AddRowGroup(tableOID, rowGroup)

    return nil
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Format undocumented | High | Extensive testing with DuckDB-created files |
| Compression complexity | Medium | Start with simple algorithms, add more later |
| Large file memory | Medium | Streaming/lazy loading, don't load all at once |
| Type incompatibilities | Medium | Comprehensive type mapping tests |
| Performance gap | Medium | Profile and optimize hot paths |
| Version drift | Low | Pin to v1.4.3, add version detection |

## Migration Plan

### Phase 1: Core Infrastructure (Week 1)
1. Create `internal/storage/duckdb/` package
2. Implement file header read/write
3. Implement block manager with cache
4. Add basic tests for header parsing

### Phase 2: Catalog Support (Week 2)
1. Implement catalog serialization/deserialization
2. Map to existing dukdb-go catalog structures
3. Test with simple single-table databases
4. Verify round-trip catalog preservation

### Phase 3: Row Group Reading (Week 3)
1. Implement row group reader
2. Add decompression: Constant, RLE, Dictionary
3. Test reading DuckDB-created tables
4. Add BitPacking and FOR decompression

### Phase 4: Write Support (Week 4)
1. Implement compression (write path)
2. Implement row group writer
3. Test round-trip: write → read
4. Test interop: dukdb-go write → DuckDB read

### Phase 5: Integration (Week 5)
1. Integrate with Engine
2. Add format detection
3. Add configuration options
4. End-to-end tests

### Phase 6: Polish (Week 6)
1. Performance optimization
2. Edge case handling
3. Documentation
4. Benchmarks vs DuckDB

## Open Questions

1. **WAL integration?**
   - Answer: DuckDB format is self-contained; WAL is for our native format
   - For DuckDB files, modifications go directly to file with checkpoint

2. **Partial block writes?**
   - Answer: Buffer partial blocks, write when full or on checkpoint
   - Use partial block manager similar to DuckDB's

3. **Concurrent reads?**
   - Answer: Block cache is thread-safe, row groups load lazily
   - Multiple readers can access same file safely

4. **Unsupported compression?**
   - Answer: Return error with clear message about which compression
   - Log which file/table uses unsupported compression

5. **Type extension handling?**
   - Answer: Extension types (custom) map to closest base type
   - Log warning when precision may be lost
