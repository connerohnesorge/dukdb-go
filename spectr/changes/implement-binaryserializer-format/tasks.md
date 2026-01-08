# Tasks: Implement DuckDB's BinarySerializer Format

## Phase 1: Foundation

- [ ] 1.1 Implement varint encoding in `internal/storage/duckdb/varint.go` with VarIntEncode, VarIntDecode, ZigZagEncode, ZigZagDecode functions and unit tests
- [ ] 1.2 Implement BinarySerializer writer in `internal/storage/duckdb/binary_serializer.go` with field ID writing, message terminator, and value serialization methods
- [ ] 1.3 Implement property methods (WriteProperty, WritePropertyWithDefault, WriteObject, WriteList) in binary_serializer.go

## Phase 2: Catalog Entry Serialization

- [ ] 2.1 Define catalog constants (CatalogEntryType enum, property IDs, OnCreateConflict enum) in catalog_types.go
- [ ] 2.2 Implement CreateSchemaInfo serialization in `internal/storage/duckdb/catalog_serializer.go`
- [ ] 2.3 Implement LogicalType serialization for all base types and DECIMAL
- [ ] 2.4 Implement ColumnDefinition serialization with name, type, and category properties
- [ ] 2.5 Implement CreateTableInfo serialization with columns list and constraints
- [ ] 2.6 Implement catalog entry serialization with property 99 (catalog_type) and property 100 (entry data)

## Phase 3: Metadata Block System

- [ ] 3.1 Implement metadata block manager in `internal/storage/duckdb/metadata_block.go` with 64 sub-blocks per storage block
- [ ] 3.2 Implement MetaBlockPointer encoding/decoding (56-bit block ID + 8-bit block index) in header.go
- [ ] 3.3 Implement MetadataWriter with automatic block allocation and chaining for large data

## Phase 4: Integration

- [ ] 4.1 Update CatalogWriter to use BinarySerializer instead of custom format
- [ ] 4.2 Update storage.go Checkpoint to use encoded MetaBlockPointer in database header
- [ ] 4.3 Implement BinaryDeserializer for reading serialized catalog data
- [ ] 4.4 Update metadata_reader.go to use BinaryDeserializer for catalog reconstruction

## Phase 5: Verification

- [ ] 5.1 Create hex dump verification tests comparing output with DuckDB reference
- [ ] 5.2 Update TestDuckDBCLIReadsDukdbFile/simple_table to pass without skipping
- [ ] 5.3 Add round-trip tests: create in dukdb-go, read in DuckDB CLI, read back in dukdb-go
- [ ] 5.4 Add edge case tests for empty catalog, Unicode names, many columns, nested types
