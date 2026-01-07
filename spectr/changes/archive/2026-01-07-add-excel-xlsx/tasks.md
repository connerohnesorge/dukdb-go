# Tasks: Excel/XLSX Format Support

## Phase 1: XLSX Infrastructure

- [x] 1.1 Add excelize/v2 dependency to go.mod with version pinning
- [x] 1.2 Create `internal/io/xlsx/` package with doc.go, options.go, utils.go
- [x] 1.3 Implement ReaderOptions struct with sheet, range, header, skip, and type options
- [x] 1.4 Implement WriterOptions struct with sheet name, header, and formatting options
- [x] 1.5 Implement A1 notation parsing utilities (cell address parsing, column letter conversion)

## Phase 2: read_xlsx Function

- [x] 2.1 Implement basic Reader struct with file opening and sheet selection
- [x] 2.2 Implement streaming row iteration using excelize Rows() API
- [x] 2.3 Implement header row detection and column name extraction
- [x] 2.4 Implement basic type conversion from Excel cells to dukdb-go values
- [x] 2.5 Implement DataChunk building from streamed rows
- [x] 2.6 Add read_xlsx table function registration in executor

## Phase 3: Sheet/Range Selection

- [x] 3.1 Implement sheet selection by name with error handling
- [x] 3.2 Implement sheet selection by index (0-based)
- [x] 3.3 Implement cell range parsing (A1:D100 notation)
- [x] 3.4 Implement row range filtering during iteration
- [x] 3.5 Implement column range filtering with projection

## Phase 4: Type Inference

- [x] 4.1 Implement Excel cell type detection using excelize GetCellType
- [x] 4.2 Implement date format detection from cell styles
- [x] 4.3 Implement Excel date serial number conversion to time.Time
- [x] 4.4 Implement column type inference from sampling multiple rows
- [x] 4.5 Implement mixed-type column handling with VARCHAR fallback
- [x] 4.6 Add explicit column type override option support

## Phase 5: COPY TO xlsx

- [x] 5.1 Implement basic Writer struct with excelize file creation
- [x] 5.2 Implement header row writing with column names
- [x] 5.3 Implement DataChunk to Excel cell value conversion
- [x] 5.4 Implement time.Time to Excel date serial number conversion
- [x] 5.5 Implement auto-width column calculation
- [x] 5.6 Implement WriteTo for streaming to io.Writer
- [x] 5.7 Add COPY TO FORMAT XLSX support in executor

## Phase 6: Integration

- [x] 6.1 Register xlsx format with format detection system (magic bytes check)
- [x] 6.2 Integrate with FileSystem interface for cloud storage URLs
- [x] 6.3 Add read_xlsx_auto variant for format auto-detection
- [x] 6.4 Create comprehensive integration tests with real Excel files
- [x] 6.5 Add performance benchmarks for large file reading/writing
- [x] 6.6 Document xlsx functions and options in package documentation
