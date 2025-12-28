## 1. Build Infrastructure

- [ ] 1.1 Create `arrow.go` with `//go:build duckdb_arrow` tag
- [ ] 1.2 Add `arrow_mapping.go` for type conversions
- [ ] 1.3 Create platform-specific arrow mapping files
- [ ] 1.4 Update go.mod with optional Arrow dependency

## 2. Core Types

- [ ] 2.1 Define Arrow struct with connection reference
- [ ] 2.2 Implement NewArrowFromConn
- [ ] 2.3 Define arrowRecordReader type

## 3. Schema Mapping

- [ ] 3.1 Implement DuckDB Type to Arrow Type conversion
- [ ] 3.2 Handle nested types (LIST, STRUCT, MAP)
- [ ] 3.3 Handle temporal types with correct precision
- [ ] 3.4 Write tests for all type mappings

## 4. Data Conversion

- [ ] 4.1 Implement DataChunk to RecordBatch conversion
- [ ] 4.2 Create type-specific array builders
- [ ] 4.3 Handle NULL values
- [ ] 4.4 Write conversion benchmarks

## 5. Query Execution

- [ ] 5.1 Implement QueryContext with Arrow results
- [ ] 5.2 Implement Query convenience method
- [ ] 5.3 Stream results via channel
- [ ] 5.4 Handle query errors

## 6. RecordReader Implementation

- [ ] 6.1 Implement arrow.RecordReader interface
- [ ] 6.2 Support streaming with Next()/Record()
- [ ] 6.3 Implement Release() for memory management
- [ ] 6.4 Write tests for streaming

## 7. Validation

- [ ] 7.1 Run tests with and without build tag
- [ ] 7.2 Run `golangci-lint`
- [ ] 7.3 Verify API matches duckdb-go
- [ ] 7.4 Document build tag usage
