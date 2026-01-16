# Implementation Tasks

## 1. Glob Pattern Matching Engine

- [ ] 1.1 Create `internal/io/filesystem/glob.go` with GlobMatcher type
- [ ] 1.2 Implement pattern parsing (split into segments, identify wildcards)
- [ ] 1.3 Implement single wildcard matching (`*`)
- [ ] 1.4 Implement character class matching (`[a-z]`, `[0-9]`)
- [ ] 1.5 Implement bracket negation (`[!abc]`)
- [ ] 1.6 Implement recursive wildcard matching (`**`)
- [ ] 1.7 Add validation for multiple `**` in one pattern (must error)
- [ ] 1.8 Add prefix extraction optimization (for cloud storage)
- [ ] 1.9 Add file ordering (alphabetical by default)
- [ ] 1.10 Write unit tests for pattern matching (all pattern types)

## 2. FileSystem Interface Extension

- [ ] 2.1 Add `Glob(pattern string) ([]string, error)` to FileSystem interface
- [ ] 2.2 Add `SupportsGlob() bool` to FileSystem interface
- [ ] 2.3 Implement Glob for LocalFileSystem using filepath.Glob + recursive support
- [ ] 2.4 Add fallback glob implementation for filesystems without native support
- [ ] 2.5 Write unit tests for FileSystem glob interface

## 3. Cloud Storage Glob Implementation

- [ ] 3.1 Implement Glob for S3FileSystem using ListObjectsV2 with prefix optimization
- [ ] 3.2 Add pagination handling for S3 ListObjectsV2 (handle >1000 objects)
- [ ] 3.3 Add retry logic with exponential backoff for S3 rate limits
- [ ] 3.4 Implement Glob for GCSFileSystem using storage.Query with prefix
- [ ] 3.5 Add pagination handling for GCS iterator
- [ ] 3.6 Implement Glob for AzureFileSystem using blob list API with prefix
- [ ] 3.7 Add pagination handling for Azure list blobs
- [ ] 3.8 Write integration tests for cloud storage glob (requires test credentials or mocks)

## 4. Multi-File Reading Infrastructure

- [ ] 4.1 Create `readMultipleFiles()` helper function in file reader base
- [ ] 4.2 Implement union-by-name schema merging (`mergeSchemas()`)
- [ ] 4.3 Implement schema alignment (`alignChunkToSchema()`)
- [ ] 4.4 Add type compatibility validation (INTEGER + BIGINT OK, INTEGER + VARCHAR error)
- [ ] 4.5 Add type widening for compatible numeric types
- [ ] 4.6 Implement chunk concatenation across files
- [ ] 4.7 Add max files limit check (default 10,000)
- [ ] 4.8 Write unit tests for schema merging and alignment

## 5. CSV Table Function Integration

- [ ] 5.1 Update `table_function_csv.go` to detect glob patterns
- [ ] 5.2 Add glob expansion logic to CSV table function
- [ ] 5.3 Add array of files support to CSV table function
- [ ] 5.4 Add filename metadata column support (`filename=true`)
- [ ] 5.5 Add file_row_number metadata column support
- [ ] 5.6 Add file_index metadata column support
- [ ] 5.7 Add files_to_sniff option for schema detection with globs
- [ ] 5.8 Add FileGlobOptions support (DISALLOW_EMPTY, ALLOW_EMPTY, FALLBACK_GLOB)
- [ ] 5.9 Add Hive partitioning support (`hive_partitioning=true`)
- [ ] 5.10 Add Hive partitioning auto-detection
- [ ] 5.11 Add hive_types_autocast for type inference
- [ ] 5.12 Add hive_types for explicit type schemas
- [ ] 5.13 Write integration tests for CSV glob patterns
- [ ] 5.14 Write integration tests for CSV array of files
- [ ] 5.15 Write tests for CSV virtual metadata columns
- [ ] 5.16 Write tests for CSV files_to_sniff
- [ ] 5.17 Write tests for CSV Hive partitioning with all options

## 6. JSON Table Function Integration

- [ ] 6.1 Update `table_function_json.go` to detect glob patterns
- [ ] 6.2 Add glob expansion logic to JSON table function
- [ ] 6.3 Add array of files support to JSON table function
- [ ] 6.4 Add virtual metadata columns support (filename, file_row_number, file_index)
- [ ] 6.5 Add FileGlobOptions support
- [ ] 6.6 Add Hive partitioning support with auto-detection and type inference
- [ ] 6.7 Write integration tests for JSON glob patterns
- [ ] 6.8 Write integration tests for JSON array of files
- [ ] 6.9 Write tests for JSON virtual metadata columns

## 7. Parquet Table Function Integration

- [ ] 7.1 Update `table_function_parquet.go` to detect glob patterns
- [ ] 7.2 Add glob expansion logic to Parquet table function
- [ ] 7.3 Add array of files support to Parquet table function
- [ ] 7.4 Add virtual metadata columns support (filename, file_row_number, file_index)
- [ ] 7.5 Add FileGlobOptions support
- [ ] 7.6 Add Hive partitioning support with auto-detection
- [ ] 7.7 Add hive_types_autocast for type inference
- [ ] 7.8 Add hive_types for explicit type schemas
- [ ] 7.9 Write integration tests for Parquet glob patterns
- [ ] 7.10 Write integration tests for Parquet array of files
- [ ] 7.11 Write tests for Parquet virtual metadata columns
- [ ] 7.12 Write tests for Parquet Hive partitioning with all options

## 8. XLSX Table Function Integration

- [ ] 8.1 Update `table_function_xlsx.go` to detect glob patterns
- [ ] 8.2 Add glob expansion logic to XLSX table function
- [ ] 8.3 Add array of files support to XLSX table function
- [ ] 8.4 Add virtual metadata columns support (filename, file_row_number, file_index)
- [ ] 8.5 Add FileGlobOptions support
- [ ] 8.6 Write integration tests for XLSX glob patterns
- [ ] 8.7 Write tests for XLSX virtual metadata columns

## 9. Arrow Table Function Integration

- [ ] 9.1 Update `table_function_arrow.go` to detect glob patterns
- [ ] 9.2 Add glob expansion logic to Arrow table function
- [ ] 9.3 Add array of files support to Arrow table function
- [ ] 9.4 Add virtual metadata columns support (filename, file_row_number, file_index)
- [ ] 9.5 Add FileGlobOptions support
- [ ] 9.6 Write integration tests for Arrow glob patterns
- [ ] 9.7 Write tests for Arrow virtual metadata columns

## 10. Parser Integration for Array Syntax

- [ ] 10.1 Add array literal parsing (`['file1', 'file2']`) to parser
- [ ] 10.2 Add ArrayExpr AST node type
- [ ] 10.3 Add array evaluation to expression evaluator
- [ ] 10.4 Write parser tests for array literals
- [ ] 10.5 Write integration tests for array syntax in table functions

## 11. Error Handling and Edge Cases

- [ ] 11.1 Add "no files match pattern" error handling
- [ ] 11.2 Add "too many files" error with configurable limit
- [ ] 11.3 Add invalid glob pattern syntax error handling
- [ ] 11.4 Add permission denied error handling for directory listing
- [ ] 11.5 Add type incompatibility error for schema alignment
- [ ] 11.6 Add clear error messages for all glob-related failures
- [ ] 11.7 Write tests for all error cases

## 12. Configuration and Settings

- [ ] 12.1 Add `max_files_per_glob` setting (default: 10,000)
- [ ] 12.2 Add `file_glob_timeout` setting for cloud storage operations
- [ ] 12.3 Add configuration tests

## 13. Documentation

- [ ] 13.1 Document glob pattern syntax in table function docs
- [ ] 13.2 Document array of files syntax
- [ ] 13.3 Document filename metadata column option
- [ ] 13.4 Document Hive partitioning support
- [ ] 13.5 Document schema alignment behavior (union-by-name)
- [ ] 13.6 Document cloud storage glob optimization
- [ ] 13.7 Add usage examples for all file formats
- [ ] 13.8 Document max_files_per_glob setting
- [ ] 13.9 Document error handling behavior

## 14. Performance Testing and Optimization

- [ ] 14.1 Benchmark local filesystem glob (1,000 files, 10,000 files)
- [ ] 14.2 Benchmark S3 glob (1,000 objects, 10,000 objects with prefix optimization)
- [ ] 14.3 Benchmark GCS glob (1,000 objects, 10,000 objects with prefix optimization)
- [ ] 14.4 Benchmark schema alignment overhead (10 files with different schemas)
- [ ] 14.5 Optimize prefix extraction algorithm
- [ ] 14.6 Add parallel file reading (if not already in parallel-execution spec)
- [ ] 14.7 Profile memory usage with large file lists
- [ ] 14.8 Add performance regression tests

## 15. End-to-End Integration Tests

- [ ] 15.1 Test CSV glob + union-by-name + filename column
- [ ] 15.2 Test JSON glob + Hive partitioning
- [ ] 15.3 Test Parquet glob + S3 + column projection
- [ ] 15.4 Test mixed storage backends (local + S3 in array)
- [ ] 15.5 Test large-scale scenarios (1,000+ files)
- [ ] 15.6 Test edge cases (empty files, single row files, duplicate schemas)

## 16. Validation and Release

- [ ] 16.1 Run full test suite (unit + integration)
- [ ] 16.2 Validate against DuckDB CLI behavior (compatibility testing)
- [ ] 16.3 Update CHANGELOG with glob pattern support
- [ ] 16.4 Update README with glob pattern examples
- [ ] 16.5 Create migration guide (no breaking changes, additive only)

## Dependencies and Parallelization

**Can be parallelized:**
- Tasks 3.1-3.7 (cloud storage implementations) can be done concurrently
- Tasks 5.x-9.x (table function integrations) can be done concurrently after task 4.x completes
- Documentation tasks 13.x can be done anytime after corresponding features are implemented

**Sequential dependencies:**
- Task 1 (Glob engine) must complete before tasks 3, 5-9
- Task 2 (Interface extension) must complete before task 3
- Task 4 (Multi-file reading) must complete before tasks 5-9
- Task 10 (Parser integration) is independent and can be done in parallel with tasks 5-9

**Critical path:**
Task 1 → Task 2 → Task 3 → Task 4 → Tasks 5-9 (parallel) → Task 15 → Task 16

**Estimated completion:**
- Tasks 1-4: 2 weeks (core infrastructure)
- Tasks 5-9: 2 weeks (table function integrations, can parallelize)
- Tasks 10-12: 1 week (parser, error handling, config)
- Tasks 13-16: 1 week (docs, testing, validation)
- **Total: 6 weeks** (with parallelization, could be 4-5 weeks)
