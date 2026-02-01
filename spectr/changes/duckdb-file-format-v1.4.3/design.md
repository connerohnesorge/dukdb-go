# DuckDB File Format Design (v1.4.3)

## Table of Contents

1. [Introduction](#introduction)
2. [File Format Architecture](#file-format-architecture)
3. [Block Management System](#block-management-system)
4. [Metadata Storage Design](#metadata-storage-design)
5. [Data Serialization](#data-serialization)
6. [Index Storage Format](#index-storage-format)
7. [WAL Integration](#wal-integration)
8. [Version Compatibility](#version-compatibility)
9. [Memory Management](#memory-management)
10. [Performance Optimization](#performance-optimization)
11. [Error Handling and Recovery](#error-handling-and-recovery)
12. [Security Considerations](#security-considerations)

## Introduction

This document provides a detailed technical design for implementing DuckDB native file format support in dukdb-go. The design ensures full compatibility with DuckDB v1.4.3 file format specifications while maintaining the project's pure Go philosophy.

The DuckDB file format is a block-oriented storage format optimized for analytical workloads. It combines columnar storage with row groups, compression, and indexing to provide efficient query performance while maintaining ACID properties.

## File Format Architecture

### Overall Structure

DuckDB files are organized as a collection of fixed-size blocks (default 256KB) with the following structure:

```
┌─────────────────────────────────────────────────────────────┐
│ File Header (64 bytes)                                      │
├─────────────────────────────────────────────────────────────┤
│ Block 0 (256KB) - Catalog Metadata                          │
├─────────────────────────────────────────────────────────────┤
│ Block 1 (256KB) - Data Block 1                              │
├─────────────────────────────────────────────────────────────┤
│ Block 2 (256KB) - Data Block 2                              │
├─────────────────────────────────────────────────────────────┤
│                      ...                                    │
├─────────────────────────────────────────────────────────────┤
│ Block N (256KB) - Free Space Map                            │
└─────────────────────────────────────────────────────────────┘
```

### File Header Format

```go
type FileHeader struct {
    MagicNumber     [16]byte  // "DUCKDB_1.4.3\x00\x00\x00\x00"
    VersionMajor    uint32    // Major version (1)
    VersionMinor    uint32    // Minor version (4)
    VersionPatch    uint32    // Patch version (3)
    BlockSize       uint32    // Block size in bytes (262144)
    BlockCount      uint64    // Total blocks in file
    CatalogBlockID  uint64    // Block containing catalog root
    FreeListBlockID uint64    // Block containing free space map
    CreationTime    int64     // Unix timestamp
    DatabaseFlags   uint64    // Feature flags
    HeaderChecksum  uint64    // CRC64 checksum of header
}
```

### Block Header Format

Each block begins with a 32-byte header:

```go
type BlockHeader struct {
    BlockType      uint8     // Block type identifier
    BlockID        uint64    // Unique block identifier
    DataSize       uint32    // Size of data within block
    NextBlockID    uint64    // Next block in chain (0 if last)
    CompressionType uint8    // Compression algorithm used
    Flags          uint16    // Block-specific flags
    Checksum       uint32    // CRC32 checksum of block data
}
```

### Block Types

```go
const (
    BlockTypeHeader     = 0x01
    BlockTypeCatalog    = 0x02
    BlockTypeData       = 0x03
    BlockTypeIndex      = 0x04
    BlockTypeStatistics = 0x05
    BlockTypeWAL        = 0x06
    BlockTypeFreeSpace  = 0x07
    BlockTypeSchema     = 0x08
    BlockTypeView       = 0x09
    BlockTypeSequence   = 0x0A
)
```

## Block Management System

### Block Allocation Strategy

The block manager uses a free-space tracking mechanism with multiple allocation strategies:

1. **First Fit**: Allocate in the first suitable free block
2. **Best Fit**: Allocate in the smallest suitable free block
3. **Segmented Fit**: Maintain separate lists for different block sizes

```go
type BlockManager struct {
    file           *os.File
    header         *FileHeader
    freeList       *FreeSpaceManager
    allocatedBlocks map[uint64]*BlockInfo
    mu             sync.RWMutex
}

type BlockInfo struct {
    BlockID      uint64
    BlockType    uint8
    UsedSize     uint32
    FreeSize     uint32
    LastAccess   time.Time
    Dirty        bool
}
```

### Free Space Management

```go
type FreeSpaceManager struct {
    totalBlocks    uint64
    freeBlocks     uint64
    freeList       *FreeList
    segmentTable   map[uint32][]uint64 // size -> block IDs
    mu             sync.Mutex
}

type FreeList struct {
    head           *FreeNode
    tail           *FreeNode
}

type FreeNode struct {
    BlockID        uint64
    Size           uint32
    Next           *FreeNode
}
```

### Block Allocation Algorithm

```go
func (bm *BlockManager) AllocateBlock(blockType uint8, minSize uint32) (*BlockInfo, error) {
    bm.mu.Lock()
    defer bm.mu.Unlock()

    // Find suitable free block
    blockID := bm.freeList.Find(minSize)
    if blockID == 0 {
        // Extend file
        blockID = bm.header.BlockCount
        bm.header.BlockCount++

        // Truncate file
        newSize := int64(bm.header.BlockCount) * int64(bm.header.BlockSize)
        if err := bm.file.Truncate(newSize); err != nil {
            return nil, err
        }
    }

    // Create block info
    blockInfo := &BlockInfo{
        BlockID:   blockID,
        BlockType: blockType,
        UsedSize:  minSize,
        FreeSize:  bm.header.BlockSize - minSize - BlockHeaderSize,
        Dirty:     true,
    }

    bm.allocatedBlocks[blockID] = blockInfo
    return blockInfo, nil
}
```

## Metadata Storage Design

### Catalog Serialization

The catalog is serialized as a tree structure with nodes representing different database objects:

```go
type CatalogNode struct {
    NodeType    uint8      // Node type (schema, table, view, etc.)
    NodeID      uint64     // Unique node identifier
    ParentID    uint64     // Parent node ID
    Name        string     // Object name
    Metadata    []byte     // Serialized metadata
    ChildIDs    []uint64   // Child node IDs
    CreatedAt   int64
    ModifiedAt  int64
}

type CatalogSerializer struct {
    rootNode    *CatalogNode
    nodeMap     map[uint64]*CatalogNode
    nameIndex   map[string]uint64
}
```

### Schema Metadata

```go
type SchemaMetadata struct {
    SchemaID    uint64
    Name        string
    Owner       string
    Tables      map[string]uint64  // table name -> table ID
    Views       map[string]uint64  // view name -> view ID
    Sequences   map[string]uint64  // sequence name -> sequence ID
    Indexes     map[string]uint64  // index name -> index ID
}
```

### Table Metadata

```go
type TableMetadata struct {
    TableID          uint64
    SchemaID         uint64
    Name             string
    Columns          []ColumnMetadata
    Constraints      []ConstraintMetadata
    DataBlocks       []uint64          // Data block IDs
    IndexBlocks      map[string]uint64 // Index name -> block ID
    StatisticsBlock  uint64
    RowCount         uint64
    CreatedAt        int64
    LastModified     int64
}

type ColumnMetadata struct {
    ColumnID     uint32
    Name         string
    Type         TypeMetadata
    Nullable     bool
    DefaultValue []byte
    Comment      string
}

type TypeMetadata struct {
    TypeID       uint8
    TypeName     string
    Width        uint32
    Scale        uint32
    Precision    uint32
}
```

### Statistics Storage

Column statistics are stored per table to enable query optimization:

```go
type TableStatistics struct {
    TableID        uint64
    RowCount       uint64
    ColumnStats    map[uint32]*ColumnStatistics
    LastUpdated    int64
}

type ColumnStatistics struct {
    ColumnID       uint32
    NullCount      uint64
    DistinctCount  uint64
    MinValue       []byte
    MaxValue       []byte
    AvgValue       float64
    Histogram      *Histogram
}

type Histogram struct {
    Buckets        []Bucket
    NullBucket     Bucket
}

type Bucket struct {
    LowerBound     []byte
    UpperBound     []byte
    Count          uint64
}
```

## Data Serialization

### DataChunk Serialization

DataChunks are serialized in column-major format with compression:

```go
type DataChunkSerializer struct {
    compression    CompressionType
    buffer         *bytes.Buffer
    vectorCache    map[uint64]*VectorCache
}

type SerializedDataChunk struct {
    RowCount       uint32
    ColumnCount    uint32
    Vectors        []SerializedVector
    ValidityMasks  []ValidityMask
    CompressionInfo CompressionInfo
}

type SerializedVector struct {
    ColumnID       uint32
    Type           TypeMetadata
    Data           []byte
    ValidityMask   []byte
    Statistics     *VectorStatistics
}
```

### Type-Specific Serialization

Each data type has a specific serialization format:

#### Integer Types
- Stored as little-endian binary
- Delta compression for sorted data
- Bit-packing for low-cardinality data

#### String Types
- Dictionary encoding for repeated values
- Prefix compression for sorted strings
- Variable-length encoding with length prefix

#### Decimal Types
- Stored as scaled integers
- Scale and precision stored in metadata
- Specialized compression for financial data

#### Timestamp Types
- Stored as microseconds since epoch
- Delta compression for time-series data
- Time zone information in metadata

### Compression Algorithms

```go
const (
    CompressionNone     = 0x00
    CompressionLZ4      = 0x01
    CompressionZSTD     = 0x02
    CompressionGZIP     = 0x03
    CompressionSnappy   = 0x04
    CompressionDictionary = 0x05
    CompressionRLE       = 0x06
    CompressionDelta     = 0x07
)
```

## Index Storage Format

### Hash Index Storage

```go
type HashIndexStorage struct {
    IndexID        uint64
    TableID        uint64
    ColumnIDs      []uint32
    IsUnique       bool
    BucketCount    uint32
    Buckets        []HashBucket
}

type HashBucket struct {
    Entries        []HashEntry
    OverflowBlock  uint64 // Block ID of overflow bucket
}

type HashEntry struct {
    HashValue      uint64
    RowID          uint64
    KeyData        []byte
}
```

### ART Index Storage (Future)

```go
type ARTIndexStorage struct {
    IndexID        uint64
    TableID        uint64
    ColumnIDs      []uint32
    RootNode       uint64 // Block ID of root node
    NodeBlocks     []uint64
}
```

## WAL Integration

### WAL Record Format

```go
type WALRecord struct {
    LSN            uint64    // Log sequence number
    Timestamp      int64     // Unix timestamp
    TransactionID  uint64
    RecordType     uint8     // Insert, Update, Delete, DDL, etc.
    TableID        uint64
    BlockID        uint64
    Offset         uint32
    OldData        []byte    // For updates
    NewData        []byte    // For inserts/updates
    Checksum       uint32
}
```

### Checkpoint Process

```go
type CheckpointManager struct {
    wal             *WALManager
    blockManager    *BlockManager
    catalog         *Catalog
    lastCheckpointLSN uint64
}

func (cm *CheckpointManager) PerformCheckpoint() error {
    // 1. Flush all dirty blocks
    if err := cm.blockManager.FlushAll(); err != nil {
        return err
    }

    // 2. Write checkpoint record to WAL
    checkpointRecord := &WALRecord{
        LSN:           cm.wal.GetNextLSN(),
        RecordType:    WALRecordCheckpoint,
        Timestamp:     time.Now().Unix(),
    }

    if err := cm.wal.WriteRecord(checkpointRecord); err != nil {
        return err
    }

    // 3. Update catalog checkpoint LSN
    cm.catalog.SetCheckpointLSN(checkpointRecord.LSN)
    cm.lastCheckpointLSN = checkpointRecord.LSN

    // 4. Truncate WAL
    return cm.wal.Truncate(cm.lastCheckpointLSN)
}
```

### Recovery Process

```go
func (cm *CheckpointManager) Recover() error {
    // 1. Read last checkpoint LSN
    lastCheckpoint := cm.catalog.GetCheckpointLSN()

    // 2. Replay WAL from checkpoint
    records, err := cm.wal.ReadFromLSN(lastCheckpoint)
    if err != nil {
        return err
    }

    // 3. Apply each record
    for _, record := range records {
        if err := cm.applyWALRecord(record); err != nil {
            return err
        }
    }

    return nil
}
```

## Version Compatibility

### Version Detection

```go
type VersionManager struct {
    currentVersion Version
    supportedVersions []Version
    migrationScripts map[Version]MigrationScript
}

type Version struct {
    Major uint32
    Minor uint32
    Patch uint32
}

func (vm *VersionManager) DetectFileVersion(header *FileHeader) (*Version, error) {
    version := &Version{
        Major: header.VersionMajor,
        Minor: header.VersionMinor,
        Patch: header.VersionPatch,
    }

    if !vm.IsSupported(version) {
        return nil, fmt.Errorf("unsupported file version: %v", version)
    }

    return version, nil
}
```

### Migration Strategy

```go
type MigrationScript struct {
    FromVersion Version
    ToVersion   Version
    Steps       []MigrationStep
}

type MigrationStep struct {
    Description string
    Execute     func(*BlockManager) error
}
```

### Feature Flags

```go
const (
    FeatureCompressionLZ4    = 1 << 0
    FeatureCompressionZSTD   = 1 << 1
    FeatureEncryption        = 1 << 2
    FeatureChecksumCRC32     = 1 << 3
    FeatureChecksumCRC64     = 1 << 4
    FeatureMMAP              = 1 << 5
)
```

## Memory Management

### Buffer Pool

```go
type BufferPool struct {
    maxSize      uint64
    currentSize  uint64
    blocks       map[uint64]*BufferBlock
    lru          *LRUCache
    evictionChan chan uint64
}

type BufferBlock struct {
    BlockID      uint64
    Data         []byte
    PinCount     int32
    IsDirty      bool
    LastAccess   time.Time
}
```

### MMAP Integration

```go
type MMAPManager struct {
    file         *os.File
    size         int64
    mappings     map[uint64]*MMAPRegion
    pageSize     int
}

type MMAPRegion struct {
    BlockID      uint64
    Offset       int64
    Size         int64
    Data         []byte
    IsReadOnly   bool
}
```

### Caching Strategies

1. **Block Cache**: Recently accessed blocks
2. **Metadata Cache**: Catalog and statistics
3. **Index Cache**: Frequently used indexes
4. **Data Cache**: Hot data chunks

## Performance Optimization

### Parallel I/O

```go
type ParallelIO struct {
    workers      int
    workChan     chan *IORequest
    resultChan   chan *IOResult
}

type IORequest struct {
    BlockID      uint64
    Operation    uint8 // Read or Write
    Data         []byte
    Priority     int
}
```

### Prefetching

```go
type PrefetchManager struct {
    predictor    *AccessPatternPredictor
    prefetchChan chan uint64
    cache        *PrefetchCache
}
```

### Compression Optimization

- **Adaptive Compression**: Choose algorithm based on data characteristics
- **Dictionary Size Optimization**: Balance memory vs compression ratio
- **Parallel Compression**: Compress blocks in parallel

## Error Handling and Recovery

### Corruption Detection

```go
type CorruptionChecker struct {
    blockManager   *BlockManager
    checksumAlg    ChecksumAlgorithm
}

func (cc *CorruptionChecker) VerifyBlock(blockID uint64) error {
    block := cc.blockManager.GetBlock(blockID)

    // Verify checksum
    expectedChecksum := cc.calculateChecksum(block.Data)
    if expectedChecksum != block.Header.Checksum {
        return &CorruptionError{
            BlockID: blockID,
            Type:    ChecksumMismatch,
        }
    }

    // Verify block type consistency
    if !cc.isValidBlockType(block.Header.BlockType) {
        return &CorruptionError{
            BlockID: blockID,
            Type:    InvalidBlockType,
        }
    }

    return nil
}
```

### Recovery Strategies

1. **Block-Level Recovery**: Skip corrupted blocks
2. **Index Rebuilding**: Rebuild indexes from data
3. **Data Salvage**: Extract readable data from corrupted files
4. **Backup Restoration**: Restore from backup if available

## Security Considerations

### Checksum Algorithms

- CRC32 for block-level integrity
- CRC64 for header integrity
- Optional SHA256 for critical metadata

### Access Control

```go
type AccessController struct {
    permissions map[string]Permission
    auditLog    *AuditLog
}

type Permission struct {
    Read       bool
    Write      bool
    Create     bool
    Delete     bool
}
```

## Conclusion

This design provides a comprehensive foundation for implementing DuckDB file format support in dukdb-go. The modular architecture ensures maintainability while the detailed specifications enable precise implementation. Key aspects include:

1. **Block-based storage** for efficient space utilization
2. **Comprehensive metadata handling** for full compatibility
3. **Advanced compression** for optimal storage efficiency
4. **Robust error handling** for data integrity
5. **Performance optimizations** for competitive speed

The design maintains the project's pure Go commitment while achieving full compatibility with DuckDB v1.4.3 file format specifications.