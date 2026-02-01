# DuckDB File Format Reading Specification

## Overview

This specification details the implementation of DuckDB file format reading capabilities in dukdb-go. The reader must correctly parse and interpret DuckDB v1.4.3 database files, extracting metadata, data, indexes, and statistics while maintaining data integrity.

## File Reading Architecture

### Core Components

```go
package format

type FileReader struct {
    file           *os.File
    header         *FileHeader
    blockManager   *BlockManager
    catalogReader  *CatalogReader
    dataReader     *DataReader
    indexReader    *IndexReader
    statsReader    *StatisticsReader
    versionMgr     *VersionManager
    corruptionChecker *CorruptionChecker
}
```

### Reading Process Flow

```
1. Open File → Validate Header → Read Catalog
      ↓
2. Parse Metadata → Build Schema Cache
      ↓
3. Load Statistics → Optimize Queries
      ↓
4. Read Data Chunks → Decompress → Deserialize
      ↓
5. Rebuild Indexes → Validate Integrity
```

## File Header Reading

### Header Validation

```go
func (fr *FileReader) ReadHeader() (*FileHeader, error) {
    headerBytes := make([]byte, FileHeaderSize)
    if _, err := fr.file.ReadAt(headerBytes, 0); err != nil {
        return nil, fmt.Errorf("failed to read header: %w", err)
    }

    header := &FileHeader{}
    if err := header.Deserialize(headerBytes); err != nil {
        return nil, fmt.Errorf("failed to deserialize header: %w", err)
    }

    // Validate magic number
    if !bytes.Equal(header.MagicNumber[:], []byte("DUCKDB_1.4.3\x00\x00\x00\x00")) {
        return nil, fmt.Errorf("invalid magic number")
    }

    // Validate version
    if header.VersionMajor != 1 || header.VersionMinor != 4 || header.VersionPatch != 3 {
        return nil, fmt.Errorf("unsupported version: %d.%d.%d",
            header.VersionMajor, header.VersionMinor, header.VersionPatch)
    }

    // Verify header checksum
    if err := fr.verifyHeaderChecksum(header); err != nil {
        return nil, fmt.Errorf("header checksum verification failed: %w", err)
    }

    return header, nil
}
```

### Feature Detection

```go
func (fr *FileReader) DetectFeatures(header *FileHeader) FileFeatures {
    features := FileFeatures{
        Version:        fmt.Sprintf("%d.%d.%d", header.VersionMajor,
                         header.VersionMinor, header.VersionPatch),
        BlockSize:      header.BlockSize,
        HasCompression: header.DatabaseFlags&FeatureCompression != 0,
        HasEncryption:  header.DatabaseFlags&FeatureEncryption != 0,
        HasChecksum:    header.DatabaseFlags&FeatureChecksum != 0,
        HasMMAP:        header.DatabaseFlags&FeatureMMAP != 0,
    }

    return features
}
```

## Block Reading

### Block Reader Interface

```go
type BlockReader interface {
    ReadBlock(blockID uint64) (*Block, error)
    ReadBlockHeader(blockID uint64) (*BlockHeader, error)
    ReadBlockData(blockID uint64, offset, size int64) ([]byte, error)
    IsBlockValid(blockID uint64) error
}
```

### Block Reading Implementation

```go
func (br *BlockReaderImpl) ReadBlock(blockID uint64) (*Block, error) {
    // Calculate block offset
    offset := int64(FileHeaderSize + blockID*br.blockSize)

    // Read block header
    header, err := br.ReadBlockHeaderAt(offset)
    if err != nil {
        return nil, fmt.Errorf("failed to read block header: %w", err)
    }

    // Verify block is for this block ID
    if header.BlockID != blockID {
        return nil, fmt.Errorf("block ID mismatch: expected %d, got %d",
            blockID, header.BlockID)
    }

    // Read block data
    dataOffset := offset + BlockHeaderSize
    data := make([]byte, header.DataSize)
    if _, err := br.file.ReadAt(data, dataOffset); err != nil {
        return nil, fmt.Errorf("failed to read block data: %w", err)
    }

    // Verify checksum
    if err := br.verifyBlockChecksum(header, data); err != nil {
        return nil, fmt.Errorf("block checksum verification failed: %w", err)
    }

    // Decompress if necessary
    if header.CompressionType != CompressionNone {
        decompressed, err := br.decompress(data, header.CompressionType)
        if err != nil {
            return nil, fmt.Errorf("failed to decompress block data: %w", err)
        }
        data = decompressed
    }

    return &Block{
        Header: header,
        Data:   data,
    }, nil
}
```

### Block Type-Specific Reading

```go
func (br *BlockReaderImpl) ReadCatalogBlock(blockID uint64) (*CatalogBlock, error) {
    block, err := br.ReadBlock(blockID)
    if err != nil {
        return nil, err
    }

    if block.Header.BlockType != BlockTypeCatalog {
        return nil, fmt.Errorf("expected catalog block, got %d", block.Header.BlockType)
    }

    catalog := &CatalogBlock{
        BlockID: blockID,
    }

    // Parse catalog data
    reader := bytes.NewReader(block.Data)

    // Read root node ID
    if err := binary.Read(reader, binary.LittleEndian, &catalog.RootNodeID); err != nil {
        return nil, err
    }

    // Read catalog nodes
    nodeCount := uint32(0)
    if err := binary.Read(reader, binary.LittleEndian, &nodeCount); err != nil {
        return nil, err
    }

    catalog.Nodes = make([]*CatalogNode, nodeCount)
    for i := uint32(0); i < nodeCount; i++ {
        node, err := br.readCatalogNode(reader)
        if err != nil {
            return nil, fmt.Errorf("failed to read catalog node %d: %w", i, err)
        }
        catalog.Nodes[i] = node
    }

    return catalog, nil
}
```

## Catalog Reading

### Catalog Structure Parsing

```go
type CatalogReader struct {
    blockReader    BlockReader
    nodeCache      map[uint64]*CatalogNode
    schemaCache    map[string]*Schema
}

func (cr *CatalogReader) ReadCatalog(catalogBlockID uint64) (*Catalog, error) {
    // Read catalog block
    catalogBlock, err := cr.blockReader.ReadCatalogBlock(catalogBlockID)
    if err != nil {
        return nil, fmt.Errorf("failed to read catalog block: %w", err)
    }

    // Build node tree
    rootNode, err := cr.buildNodeTree(catalogBlock)
    if err != nil {
        return nil, fmt.Errorf("failed to build node tree: %w", err)
    }

    // Convert to catalog
    catalog := &Catalog{
        RootNode: rootNode,
        Schemas:  make(map[string]*Schema),
    }

    // Parse schemas
    for _, schemaNode := range rootNode.Children {
        if schemaNode.NodeType == NodeTypeSchema {
            schema, err := cr.parseSchema(schemaNode)
            if err != nil {
                return nil, fmt.Errorf("failed to parse schema: %w", err)
            }
            catalog.Schemas[schema.Name] = schema
        }
    }

    return catalog, nil
}
```

### Schema Parsing

```go
func (cr *CatalogReader) parseSchema(node *CatalogNode) (*Schema, error) {
    schema := &Schema{
        ID:        node.NodeID,
        Name:      node.Name,
        Tables:    make(map[string]*Table),
        Views:     make(map[string]*View),
        Sequences: make(map[string]*Sequence),
    }

    // Parse child objects
    for _, childNode := range node.Children {
        switch childNode.NodeType {
        case NodeTypeTable:
            table, err := cr.parseTable(childNode)
            if err != nil {
                return nil, err
            }
            schema.Tables[table.Name] = table

        case NodeTypeView:
            view, err := cr.parseView(childNode)
            if err != nil {
                return nil, err
            }
            schema.Views[view.Name] = view

        case NodeTypeSequence:
            sequence, err := cr.parseSequence(childNode)
            if err != nil {
                return nil, err
            }
            schema.Sequences[sequence.Name] = sequence
        }
    }

    return schema, nil
}
```

### Table Metadata Parsing

```go
func (cr *CatalogReader) parseTable(node *CatalogNode) (*Table, error) {
    var meta TableMetadata
    if err := json.Unmarshal(node.Metadata, &meta); err != nil {
        return nil, fmt.Errorf("failed to unmarshal table metadata: %w", err)
    }

    table := &Table{
        ID:          node.NodeID,
        SchemaID:    meta.SchemaID,
        Name:        node.Name,
        Columns:     make([]*Column, 0, len(meta.Columns)),
        Constraints: make([]*Constraint, 0, len(meta.Constraints)),
    }

    // Parse columns
    for _, colMeta := range meta.Columns {
        column := &Column{
            ID:       colMeta.ColumnID,
            Name:     colMeta.Name,
            Type:     cr.parseType(colMeta.Type),
            Nullable: colMeta.Nullable,
            Default:  colMeta.DefaultValue,
            Comment:  colMeta.Comment,
        }
        table.Columns = append(table.Columns, column)
    }

    // Parse constraints
    for _, constraintMeta := range meta.Constraints {
        constraint := &Constraint{
            Type:       constraintMeta.Type,
            Name:       constraintMeta.Name,
            Columns:    constraintMeta.Columns,
            Expression: constraintMeta.Expression,
        }
        table.Constraints = append(table.Constraints, constraint)
    }

    return table, nil
}
```

## Data Reading

### DataChunk Reader

```go
type DataReader struct {
    blockReader    BlockReader
    decompression  Decompressor
    typeRegistry   TypeRegistry
}

func (dr *DataReader) ReadDataChunk(tableID uint64, blockIDs []uint64) (*DataChunk, error) {
    chunk := &DataChunk{
        RowCount:    0,
        ColumnCount: 0,
        Vectors:     make([]*Vector, 0),
    }

    // Read all blocks for this chunk
    var allData []byte
    for _, blockID := range blockIDs {
        block, err := dr.blockReader.ReadBlock(blockID)
        if err != nil {
            return nil, fmt.Errorf("failed to read data block %d: %w", blockID, err)
        }

        if block.Header.BlockType != BlockTypeData {
            return nil, fmt.Errorf("expected data block, got %d", block.Header.BlockType)
        }

        allData = append(allData, block.Data...)
    }

    // Deserialize chunk header
    reader := bytes.NewReader(allData)

    var header SerializedDataChunkHeader
    if err := binary.Read(reader, binary.LittleEndian, &header); err != nil {
        return nil, fmt.Errorf("failed to read chunk header: %w", err)
    }

    chunk.RowCount = header.RowCount
    chunk.ColumnCount = header.ColumnCount

    // Read each vector
    for i := uint32(0); i < header.ColumnCount; i++ {
        vector, err := dr.readVector(reader, tableID)
        if err != nil {
            return nil, fmt.Errorf("failed to read vector %d: %w", i, err)
        }
        chunk.Vectors = append(chunk.Vectors, vector)
    }

    return chunk, nil
}
```

### Vector Deserialization

```go
func (dr *DataReader) readVector(reader io.Reader, tableID uint64) (*Vector, error) {
    // Read vector header
    var header SerializedVectorHeader
    if err := binary.Read(reader, binary.LittleEndian, &header); err != nil {
        return nil, err
    }

    // Create vector based on type
    vec := &Vector{
        Type:     dr.typeRegistry.GetType(header.TypeID),
        Size:     header.RowCount,
        Data:     make([]byte, header.DataSize),
        Validity: NewValidityMask(header.RowCount),
    }

    // Read validity mask
    if _, err := reader.Read(vec.Validity.Bytes()); err != nil {
        return nil, err
    }

    // Read data based on type
    switch vec.Type.ID() {
    case TypeInt32:
        if err := dr.readInt32Vector(reader, vec); err != nil {
            return nil, err
        }
    case TypeInt64:
        if err := dr.readInt64Vector(reader, vec); err != nil {
            return nil, err
        }
    case TypeVarchar:
        if err := dr.readStringVector(reader, vec); err != nil {
            return nil, err
        }
    // ... other types
    }

    return vec, nil
}
```

### Type-Specific Reading

```go
func (dr *DataReader) readStringVector(reader io.Reader, vec *Vector) error {
    // Read dictionary if present
    var dictSize uint32
    if err := binary.Read(reader, binary.LittleEndian, &dictSize); err != nil {
        return err
    }

    if dictSize > 0 {
        // Dictionary-encoded strings
        dictionary := make([]string, dictSize)
        for i := uint32(0); i < dictSize; i++ {
            var strLen uint32
            if err := binary.Read(reader, binary.LittleEndian, &strLen); err != nil {
                return err
            }

            strBytes := make([]byte, strLen)
            if _, err := reader.Read(strBytes); err != nil {
                return err
            }
            dictionary[i] = string(strBytes)
        }

        // Read indices
        vec.Dictionary = dictionary
        vec.Indices = make([]uint32, vec.Size)
        if err := binary.Read(reader, binary.LittleEndian, vec.Indices); err != nil {
            return err
        }
    } else {
        // Plain strings
        vec.Strings = make([]string, vec.Size)
        for i := uint32(0); i < vec.Size; i++ {
            var strLen uint32
            if err := binary.Read(reader, binary.LittleEndian, &strLen); err != nil {
                return err
            }

            strBytes := make([]byte, strLen)
            if _, err := reader.Read(strBytes); err != nil {
                return err
            }
            vec.Strings[i] = string(strBytes)
        }
    }

    return nil
}
```

## Index Reading

### Index Reader Interface

```go
type IndexReader interface {
    ReadIndex(indexID uint64) (Index, error)
    ReadHashIndex(blockIDs []uint64) (*HashIndex, error)
    ReadARTIndex(blockIDs []uint64) (*ARTIndex, error)
}
```

### Hash Index Reading

```go
func (ir *IndexReaderImpl) ReadHashIndex(blockIDs []uint64) (*HashIndex, error) {
    // Read all blocks for the index
    var indexData []byte
    for _, blockID := range blockIDs {
        block, err := ir.blockReader.ReadBlock(blockID)
        if err != nil {
            return nil, err
        }
        indexData = append(indexData, block.Data...)
    }

    // Deserialize hash index
    reader := bytes.NewReader(indexData)

    var header HashIndexHeader
    if err := binary.Read(reader, binary.LittleEndian, &header); err != nil {
        return nil, err
    }

    index := &HashIndex{
        ID:          header.IndexID,
        TableID:     header.TableID,
        ColumnIDs:   header.ColumnIDs,
        IsUnique:    header.IsUnique,
        BucketCount: header.BucketCount,
        Buckets:     make([]*HashBucket, header.BucketCount),
    }

    // Read buckets
    for i := uint32(0); i < header.BucketCount; i++ {
        bucket, err := ir.readHashBucket(reader)
        if err != nil {
            return nil, err
        }
        index.Buckets[i] = bucket
    }

    return index, nil
}
```

## Statistics Reading

### Statistics Reader

```go
type StatisticsReader struct {
    blockReader    BlockReader
    cache          *StatisticsCache
}

func (sr *StatisticsReader) ReadTableStatistics(tableID uint64) (*TableStatistics, error) {
    // Check cache first
    if cached := sr.cache.Get(tableID); cached != nil {
        return cached, nil
    }

    // Find statistics block
    blockID := sr.findStatisticsBlock(tableID)
    if blockID == 0 {
        return nil, fmt.Errorf("no statistics found for table %d", tableID)
    }

    // Read statistics block
    block, err := sr.blockReader.ReadBlock(blockID)
    if err != nil {
        return nil, err
    }

    // Deserialize statistics
    stats, err := sr.deserializeTableStatistics(block.Data)
    if err != nil {
        return nil, err
    }

    // Cache for future use
    sr.cache.Put(tableID, stats)

    return stats, nil
}
```

## Corruption Detection

### Integrity Checking

```go
func (fr *FileReader) VerifyFileIntegrity() error {
    // Check file header
    if err := fr.verifyHeaderIntegrity(); err != nil {
        return fmt.Errorf("header integrity check failed: %w", err)
    }

    // Verify all blocks
    for blockID := uint64(0); blockID < fr.header.BlockCount; blockID++ {
        if err := fr.corruptionChecker.VerifyBlock(blockID); err != nil {
            return fmt.Errorf("block %d integrity check failed: %w", blockID, err)
        }
    }

    // Verify catalog consistency
    if err := fr.verifyCatalogConsistency(); err != nil {
        return fmt.Errorf("catalog consistency check failed: %w", err)
    }

    return nil
}
```

### Block-Level Verification

```go
func (cc *CorruptionChecker) VerifyBlock(blockID uint64) error {
    // Read block
    block, err := cc.blockReader.ReadBlock(blockID)
    if err != nil {
        return fmt.Errorf("failed to read block: %w", err)
    }

    // Verify block type is valid
    if !cc.isValidBlockType(block.Header.BlockType) {
        return fmt.Errorf("invalid block type: %d", block.Header.BlockType)
    }

    // Verify block size
    if block.Header.DataSize > MaxBlockDataSize {
        return fmt.Errorf("block data size exceeds maximum: %d > %d",
            block.Header.DataSize, MaxBlockDataSize)
    }

    // Verify checksum
    expectedChecksum := cc.calculateChecksum(block.Data)
    if expectedChecksum != block.Header.Checksum {
        return fmt.Errorf("checksum mismatch: expected %d, got %d",
            expectedChecksum, block.Header.Checksum)
    }

    return nil
}
```

## Error Handling

### Error Types

```go
type FileFormatError struct {
    Type        ErrorType
    BlockID     uint64
    Message     string
    InnerError  error
}

type ErrorType uint8

const (
    ErrInvalidHeader ErrorType = iota
    ErrUnsupportedVersion
    ErrChecksumMismatch
    ErrCorruptedBlock
    ErrInvalidBlockType
    ErrDecompressionFailed
    ErrDeserializationFailed
)
```

### Recovery Strategies

```go
func (fr *FileReader) HandleCorruptedBlock(blockID uint64, err error) error {
    // Log the error
    log.Printf("Corrupted block %d: %v", blockID, err)

    // Try to read backup if available
    if backupID := fr.findBackupBlock(blockID); backupID != 0 {
        log.Printf("Attempting to read backup block %d", backupID)
        return fr.readFromBackup(backupID)
    }

    // Try to skip the block if possible
    if fr.canSkipBlock(blockID) {
        log.Printf("Skipping corrupted block %d", blockID)
        return nil
    }

    // Last resort: attempt partial recovery
    return fr.attemptPartialRecovery(blockID)
}
```

## Performance Optimization

### Parallel Block Reading

```go
func (fr *FileReader) ReadBlocksParallel(blockIDs []uint64) ([]*Block, error) {
    // Create worker pool
    numWorkers := runtime.NumCPU()
    workChan := make(chan uint64, len(blockIDs))
    resultChan := make(chan *Block, len(blockIDs))
    errChan := make(chan error, len(blockIDs))

    // Start workers
    var wg sync.WaitGroup
    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for blockID := range workChan {
                block, err := fr.blockReader.ReadBlock(blockID)
                if err != nil {
                    errChan <- err
                    continue
                }
                resultChan <- block
            }
        }()
    }

    // Queue work
    for _, blockID := range blockIDs {
        workChan <- blockID
    }
    close(workChan)

    // Wait for completion
    wg.Wait()
    close(resultChan)
    close(errChan)

    // Collect results
    blocks := make([]*Block, 0, len(blockIDs))
    for block := range resultChan {
        blocks = append(blocks, block)
    }

    // Check for errors
    if len(errChan) > 0 {
        return blocks, <-errChan
    }

    return blocks, nil
}
```

### Caching Strategy

```go
type ReadCache struct {
    blockCache     *LRUCache
    metadataCache  *TTLCache
    statsCache     *StatisticsCache
}

func (rc *ReadCache) GetBlock(blockID uint64) (*Block, bool) {
    if cached, ok := rc.blockCache.Get(blockID); ok {
        return cached.(*Block), true
    }
    return nil, false
}
```

## Testing

### Unit Tests

```go
func TestFileReader_ReadHeader(t *testing.T) {
    // Create test file
    testFile := createTestFile(t)
    defer os.Remove(testFile)

    // Test header reading
    reader, err := NewFileReader(testFile)
    require.NoError(t, err)

    header, err := reader.ReadHeader()
    require.NoError(t, err)
    assert.Equal(t, uint32(1), header.VersionMajor)
    assert.Equal(t, uint32(4), header.VersionMinor)
    assert.Equal(t, uint32(3), header.VersionPatch)
}
```

### Integration Tests

```go
func TestFileReader_Compatibility(t *testing.T) {
    // Test against actual DuckDB files
    duckdbFiles := []string{
        "testdata/simple.db",
        "testdata/complex_schema.db",
        "testdata/large_data.db",
    }

    for _, file := range duckdbFiles {
        t.Run(filepath.Base(file), func(t *testing.T) {
            reader, err := NewFileReader(file)
            require.NoError(t, err)

            // Verify can read entire file
            err = reader.VerifyFileIntegrity()
            assert.NoError(t, err)

            // Verify catalog
            catalog, err := reader.ReadCatalog()
            require.NoError(t, err)
            assert.NotNil(t, catalog)
        })
    }
}
```

## Conclusion

The file format reading specification provides a comprehensive approach to parsing DuckDB v1.4.3 files. Key features include:

1. **Robust validation** of file headers and blocks
2. **Efficient parsing** of metadata and data structures
3. **Comprehensive error handling** with recovery strategies
4. **Performance optimizations** including parallel reading and caching
5. **Full compatibility** with DuckDB v1.4.3 format specifications

This implementation ensures reliable reading of DuckDB files while maintaining the performance and purity requirements of the dukdb-go project.