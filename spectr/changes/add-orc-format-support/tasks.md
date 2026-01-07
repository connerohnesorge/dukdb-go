## 1. Research & Design

- [ ] 1.1 Research pure Go ORC libraries (apache/orc-go, xitongsys/parquet-go)
- [ ] 1.2 Evaluate library maturity and API fit
- [ ] 1.3 Design type mapping between ORC and DuckDB
- [ ] 1.4 Create detailed implementation spec

## 2. ORC Reader Implementation

- [ ] 2.1 Create ORC file header parser (magic number, version)
- [ ] 2.2 Implement file footer parsing (schema, stripes, metadata)
- [ ] 2.3 Implement stripe descriptor parsing
- [ ] 2.4 Implement column reader with vector output
- [ ] 2.5 Implement compression decompression (zlib, snappy, lz4)
- [ ] 2.6 Implement type conversion (ORC → DuckDB vectors)

## 3. Predicate Push-down

- [ ] 3.1 Parse ORC column statistics from footer
- [ ] 3.2 Implement row group-level filtering
- [ ] 3.3 Implement stripe-level filtering
- [ ] 3.4 Add integration with planner for push-down

## 4. Parser Integration

- [ ] 4.1 Add ORC function parsing (read_orc, read_orc_auto)
- [ ] 4.2 Add COPY TO format support (FORMAT ORC)
- [ ] 4.3 Add ORC file extension detection
- [ ] 4.4 Write parser tests

## 5. Planner & Executor Integration

- [ ] 5.1 Create OrcScan logical plan node
- [ ] 5.2 Create PhysicalOrcScan physical operator
- [ ] 5.3 Implement OrcScan execution
- [ ] 5.4 Add column projection support
- [ ] 5.5 Add filter push-down integration

## 6. ORC Writer (Optional - Phase 2)

- [ ] 6.1 Implement ORC writer structure
- [ ] 6.2 Implement stripe generation
- [ ] 6.3 Implement compression encoding
- [ ] 6.4 Implement footer writing

## 7. Testing

- [ ] 7.1 Write unit tests for type mapping
- [ ] 7.2 Write integration tests for reading
- [ ] 7.3 Test with sample ORC files (Hive, Spark generated)
- [ ] 7.4 Test predicate push-down
- [ ] 7.5 Test compression variants

## 8. Documentation

- [ ] 8.1 Document read_orc() function
- [ ] 8.2 Document COPY TO ORC format
- [ ] 8.3 Document supported types and limitations
- [ ] 8.4 Add examples

## 9. Verification

- [ ] 9.1 Run `spectr validate add-orc-format-support`
- [ ] 9.2 Verify with DuckDB ORC compatibility tests
- [ ] 9.3 Ensure all existing tests pass
- [ ] 9.4 Performance benchmark vs DuckDB
