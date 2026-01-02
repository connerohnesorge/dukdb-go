# Design: DuckDB File Format Compatibility

## Context

dukdb-go currently uses a proprietary file format that is incompatible with official DuckDB. This design document outlines the technical approach for implementing full DuckDB file format compatibility while maintaining backward compatibility during a transition period.

**Stakeholders**:
- Application developers needing compatibility with DuckDB ecosystem
- Data engineers requiring data portability between systems
- Cloud storage users needing S3/Cloud integration
- Users migrating from official DuckDB

**Constraints**:
- Pure Go implementation (no CGO)
- Must support reading existing DuckDB files (v64+)
- Must write files compatible with DuckDB v1.3+
- Should provide migration path for existing dukdb-go files
- Performance must be competitive with official DuckDB

## Goals / Non-Goals

**Goals**:
1. Read and write standard DuckDB `.duckdb` files
2. Implement dual 4KB rotating header blocks for crash safety
3. Support all DuckDB type serialization (including new types)
4. Implement DuckDB WAL format
5. Provide advanced compression (FSST, RLE, BitPacking, Chimp, Zstd)
6. Support ART index persistence
7. Implement row group format with column segments

**Non-Goals**:
1. ALP/Patas compression (requires native C++ implementations)
2. Full replication protocol
3. Parallel checkpointing (initial implementation)
4. Encryption support

## Decisions

### Decision 1: File Format Structure

**Options**:
A. Keep current single-header format with updated magic
B. Implement DuckDB's dual 4KB rotating headers
C. Hybrid approach (single header, DuckDB-compatible interior)

**Choice**: B - Implement DuckDB's dual 4KB rotating headers

**Rationale**:
- Required for crash safety and compatibility
- DuckDB uses this pattern for atomic header updates
- Allows detection of incomplete writes during crashes

```
DuckDB File Structure:
┌─────────────────────────────────────────────────────────────┐
│ Block A Header (4096 bytes) - Primary                       │
├─────────────────────────────────────────────────────────────┤
│ Block B Header (4096 bytes) - Mirror (for crash recovery)   │
├─────────────────────────────────────────────────────────────┤
│ Metadata (variable)                                         │
├─────────────────────────────────────────────────────────────┤
│ Checkpoint (optional)                                       │
├─────────────────────────────────────────────────────────────┤
│ Free List (variable)                                        │
├─────────────────────────────────────────────────────────────┤
│ Data Storage (Row Groups, Column Segments)                  │
└─────────────────────────────────────────────────────────────┘

Header Block A/B Format:
┌─────────────────────────────────────────────────────────────┐
│ Magic Number: "DUCK" (4 bytes)                              │
│ Version: 64 (4 bytes, little-endian)                        │
│ Header Size: 4096 (4 bytes)                                 │
│ Flags (4 bytes)                                             │
│ Salt A/B (8 bytes each) - for detecting write order         │
│ Checksum (4 bytes) - CRC32 of header                        │
│ Reserved (4060 bytes)                                       │
└─────────────────────────────────────────────────────────────┘
```

### Decision 2: Magic Number and Version

**Options**:
A. Keep `DUKDBGO\x00` with new version number
B. Change to `DUCK` with version 64+
C. Dual magic number (support both formats)

**Choice**: B - Change to `DUCK` with version 64+

**Rationale**:
- Required for DuckDB compatibility
- Version 64+ matches DuckDB's current versioning
- Enables reading files created by official DuckDB

```go
const (
    DuckDBMagicNumber = "DUCK"  // 0x4455434B
    DuckDBVersion     = 64      // Current DuckDB format version
)
```

### Decision 3: Catalog Serialization Format

**Options**:
A. Continue using GZIP-compressed JSON
B. Implement DuckDB's binary property-based serialization
C. Use Protocol Buffers
D. Use MessagePack

**Choice**: B - Implement DuckDB's binary property-based serialization

**Rationale**:
- Required for DuckDB compatibility
- More compact than JSON
- Supports all DuckDB types including nested structures
- Better performance for serialization/deserialization

**Property-Based Serialization Format**:
```
Property ID (varint) → Property Value (type-specific encoding)

Property IDs (partial list):
0:   END
1:   TYPE
2:   NAME
3:   NULLABLE
4:   DEFAULT
5:   PRIMARY_KEY
6:   FOREIGN_KEY
...
100: CHILD_TYPE
101: CHILD_NAME
...
```

**TypeInfo Serialization**:
```go
func SerializeTypeInfo(buf *Writer, ti TypeInfo) error {
    // Write base type
    buf.WriteVarint(PropertyIDType, int64(ti.Type()))
    
    // Write type name for unknown/custom types
    buf.WriteString(PropertyIDName, ti.TypeName())
    
    // Write child types for complex types
    switch t := ti.(type) {
    case *StructType:
        for i, child := range t.Children() {
            buf.WriteVarint(PropertyIDChildType, int64(child.Type()))
            buf.WriteString(PropertyIDChildName, t.Names()[i])
        }
    case *ListType:
        buf.WriteVarint(PropertyIDChildType, int64(t.ChildType()))
    case *MapType:
        buf.WriteVarint(PropertyIDChildType, int64(t.KeyType()))
        buf.WriteVarint(PropertyIDChildType+1, int64(t.ValueType()))
    case *UnionType:
        buf.WriteVarint(PropertyIDMemberCount, int64(t.NumMembers()))
        for i, member := range t.Members() {
            buf.WriteVarint(PropertyIDMemberType+int64(i), int64(member.Type()))
            buf.WriteString(PropertyIDMemberName+int64(i), member.Name())
        }
    }
    return nil
}
```

### Decision 4: Header Writing Strategy

**Options**:
A. Write directly to file
B. Write to temp, swap on success
C. Use mmap for header updates

**Choice**: B - Write to temp, swap on success

**Rationale**:
- Atomic on most filesystems
- No partial writes on crash
- Standard pattern for safe file updates

```go
func (h *Header) WriteToFile(path string) error {
    // Write Block A to temporary location
    tmpPathA := path + ".header.tmp.0"
    if err := h.writeToFile(tmpPathA); err != nil {
        return fmt.Errorf("failed to write header A: %w", err)
    }
    
    // Write Block B (flipped salt)
    h.saltB = h.saltA + 1
    tmpPathB := path + ".header.tmp.1"
    if err := h.writeToFile(tmpPathB); err != nil {
        os.Remove(tmpPathA)
        return fmt.Errorf("failed to write header B: %w", err)
    }
    
    // Atomic rename Block A
    if err := os.Rename(tmpPathA, path); err != nil {
        os.Remove(tmpPathB)
        return fmt.Errorf("failed to rename header A: %w", err)
    }
    
    // Atomic rename Block B
    if err := os.Rename(tmpPathB, path+".bak"); err != nil {
        // Block A is already in place, Block B backup failed
        // This is recoverable on next write
    }
    
    return nil
}

func (h *Header) ReadFromFile(path string) (*Header, error) {
    // Read Block A
    headerA, err := h.readFromFile(path, 0)
    if err != nil {
        // Try Block B
        return h.readFromFile(path+".bak", 4096)
    }
    
    // Read Block B and validate consistency
    headerB, err := h.readFromFile(path+".bak", 4096)
    if err != nil {
        return headerA, nil  // Block B missing, Block A is valid
    }
    
    // Choose the newer header based on salt
    if headerB.saltA > headerA.saltA {
        return headerB, nil
    }
    return headerA, nil
}
```

### Decision 5: Compression Algorithm Selection

**Options**:
A. Only support GZIP (current)
B. Implement all DuckDB compression algorithms
C. Implement pure Go alternatives for DuckDB algorithms

**Choice**: C - Implement pure Go alternatives for DuckDB algorithms

**Rationale**:
- Must support DuckDB-readable files
- ALP/Patas require native C++ - implement as unsupported
- FSST, RLE, BitPacking, Chimp, Zstd can be implemented in pure Go

**Compression Selection Strategy**:
```go
type CompressionType uint8

const (
    CompressionNone   CompressionType = 0
    CompressionRLE    CompressionType = 1
    CompressionBitPack CompressionType = 2
    CompressionFSST   CompressionType = 3
    CompressionChimp  CompressionType = 4
    CompressionZstd   CompressionType = 5
    // ALP and Patas require native C++ - not supported
)

func SelectCompression(colType LogicalType, data []byte) CompressionType {
    switch colType {
    case TypeBoolean, TypeTinyInt, TypeSmallInt, TypeInteger, TypeBigInt:
        return CompressionBitPack
    case TypeFloat, TypeDouble:
        return CompressionChimp
    case TypeVarchar, TypeBlob:
        return CompressionFSST
    case TypeDate, TypeTime, TypeTimestamp:
        return CompressionRLE
    default:
        return CompressionNone
    }
}
```

**FSST Implementation**:
```go
type FSST struct {
    symbolTable [256][]byte
    codeTable   [256]uint64  // 8-bit symbols packed into 64-bit words
}

func NewFSST() *FSST {
    return &FSST{
        symbolTable: [256][]byte{},
        codeTable:   [256]uint64{},
    }
}

func (fsst *FSST) Train(samples [][]byte) error {
    // Count symbol frequencies
    freq := make([]int, 256)
    for _, sample := range samples {
        for _, b := range sample {
            freq[b]++
        }
    }
    
    // Build optimal symbol table (simplified)
    sorted := make([]int, 256)
    for i := 0; i < 256; i++ {
        sorted[i] = i
    }
    sort.Slice(sorted, func(i, j int) bool {
        return freq[sorted[i]] > freq[sorted[j]]
    })
    
    // Assign symbols (most frequent = 0x00)
    for rank, idx := range sorted {
        if freq[idx] > 0 {
            fsst.symbolTable[rank] = []byte{byte(idx)}
        }
    }
    
    return nil
}

func (fsst *FSST) Compress(input []byte) ([]byte, error) {
    var result []byte
    var current uint64
    var bits int
    
    // Decompose into symbols
    for i := 0; i < len(input); {
        maxLen := 8
        if i+maxLen > len(input) {
            maxLen = len(input) - i
        }
        bestLen := 1
        bestIdx := int(input[i])
        
        for l := 2; l <= maxLen; l++ {
            idx := fsst.matchSymbol(input[i : i+l])
            if idx >= 0 && idx < 256 {
                bestLen = l
                bestIdx = idx
            }
        }
        
        // Pack symbol into output
        code := fsst.codeTable[bestIdx]
        current |= (code << bits)
        bits += 8
        
        if bits >= 56 {
            result = append(result, byte(current), byte(current>>8), byte(current>>16), 
                byte(current>>24), byte(current>>32), byte(current>>40), byte(current>>48))
            current >>= 56
            bits -= 56
        }
        
        i += bestLen
    }
    
    // Flush remaining bits
    if bits > 0 {
        result = append(result, byte(current))
    }
    
    return result, nil
}
```

### Decision 6: WAL Format

**Options**:
A. Keep current `DWAL` format
B. Implement DuckDB WAL format
C. Support both formats during transition

**Choice**: B - Implement DuckDB WAL format

**Rationale**:
- Required for compatibility with DuckDB tools
- DuckDB WAL is well-documented
- Enables recovery of DuckDB files

**DuckDB WAL Format**:
```
WAL Header (24 bytes):
┌─────────────────────────────────────────────────────────────┐
│ Magic Number: "WAL " (4 bytes)                              │
│ Version: 3 (4 bytes)                                        │
│ Header Size: 24 (4 bytes)                                   │
│ Sequence Number (8 bytes)                                   │
│ Checksum (4 bytes)                                          │
└─────────────────────────────────────────────────────────────┘

WAL Entry:
┌─────────────────────────────────────────────────────────────┐
│ Entry Header (16 bytes)                                     │
│ - Type (4 bytes)                                           │
│ - Flags (4 bytes)                                          │
│ - Length (4 bytes)                                         │
│ - Sequence Number (4 bytes)                                │
├─────────────────────────────────────────────────────────────┤
│ Entry Payload (variable)                                    │
│ - Transaction ID                                            │
│ - Table OID                                                 │
│ - Operation-specific data                                   │
└─────────────────────────────────────────────────────────────┘
```

### Decision 7: Row Group Format

**Options**:
A. Keep current chunk-based format
B. Implement DuckDB row group format with column segments
C. Hybrid approach

**Choice**: B - Implement DuckDB row group format with column segments

**Rationale**:
- Required for compatibility
- Column segments enable per-column compression
- Standard format for analytical databases

**Row Group Structure**:
```go
type RowGroup struct {
    MetaData       RowGroupMetadata
    ColumnData     []*ColumnSegment
}

type RowGroupMetadata struct {
    RowCount       uint64
    ColumnCount    uint16
    Flags          uint32
    StartId        uint64
}

type ColumnSegment struct {
    MetaData       SegmentMetadata
    Compression    CompressionType
    Data           []byte
    Validity       []uint64
}

type SegmentMetadata struct {
    Type           LogicalType
    SegmentSize    uint64
    BlockId        uint64
    Offset         uint64
    Length         uint64
}
```

### Decision 8: Type Support for New Types

**Types to Support**:
- UNION (new in DuckDB)
- BIT
- TIME_TZ
- TIMESTAMP_TZ
- INTERVAL (already supported)

**TypeInfo Serialization for UNION**:
```go
func SerializeUnionType(buf *Writer, ut *UnionType) error {
    buf.WriteVarint(PropertyIDType, int64(TypeUnion))
    buf.WriteVarint(PropertyIDMemberCount, int64(ut.NumMembers()))
    
    for i := 0; i < ut.NumMembers(); i++ {
        member := ut.Member(i)
        buf.WriteVarint(PropertyIDMemberType+int64(i), int64(member.Type()))
        buf.WriteString(PropertyIDMemberName+int64(i), member.Name())
    }
    
    return nil
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| ALP/Patas compression | High | Document as unsupported; use Zstd as alternative |
| Performance regression | Medium | Profile and optimize hot paths; add benchmarks |
| Migration complexity | High | Provide migration utility; extended compatibility period |
| Format version handling | Medium | Support multiple versions in reader |
| Memory usage for compression | Medium | Stream compression; limit buffer sizes |

## Performance Considerations

1. **Compression Overhead**: FSST training is expensive - use cached symbol tables
2. **Memory Usage**: Large compression buffers - use streaming where possible
3. **Serialization Speed**: Property-based serialization is faster than JSON - measure improvements
4. **WAL Throughput**: Group commit already implemented - maintain efficiency

## Migration Plan

### Phase 1: Reader Implementation (Reader First)
1. Implement DuckDB header parsing
2. Implement catalog deserialization
3. Implement data chunk reading
4. Add WAL reading capability

### Phase 2: Writer Implementation
1. Implement DuckDB header writing
2. Implement catalog serialization
3. Implement data chunk writing
4. Add WAL writing capability

### Phase 3: Compression
1. Implement RLE compression
2. Implement BitPacking
3. Implement FSST
4. Implement Chimp/Zstd

### Phase 4: Index Persistence
1. Implement ART index serialization
2. Add index storage to row groups
3. Update checkpoint to include indexes

### Phase 5: Migration Tools
1. Create migration utility for old dukdb-go files
2. Add compatibility mode flag
3. Add tests for both formats

## Open Questions

1. **Version Negotiation**: Should we support writing older versions for compatibility?
   - Current decision: Always write latest version (v64+)
   - Reader should handle versions 64 through current

2. **Feature Flags**: How to handle features DuckDB doesn't support?
   - Current decision: Use header flags for dukdb-go-specific features
   - DuckDB will ignore unknown flags

3. **ALP/Patas Fallback**: What when ALP/Patas data is encountered?
   - Current decision: Return error indicating unsupported compression
   - Document limitation clearly
