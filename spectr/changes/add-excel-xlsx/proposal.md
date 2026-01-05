# Proposal: Excel/XLSX Format Support (GAP-011)

## Summary

Implement reading and writing of Microsoft Excel XLSX files to enable data import/export from spreadsheet applications, a common interchange format in business environments.

## Motivation

Excel/XLSX is ubiquitous in business and data analysis workflows:
- Analysts commonly export data to Excel for sharing with non-technical stakeholders
- Many data sources (ERP systems, reports, exports) produce XLSX files
- Excel is often the "lingua franca" for business data exchange
- Direct XLSX support eliminates manual CSV conversion steps

Currently, users must:
1. Manually convert XLSX to CSV using external tools
2. Handle multi-sheet workbooks by exporting each sheet separately
3. Lose formatting, formulas, and cell type information in conversion
4. Deal with CSV edge cases (encoding, delimiters, quoting)

## Problem Statement

### Current State
- No native XLSX file reading capability
- No way to export query results to Excel format
- Users must use external tools for XLSX conversion
- Multi-sheet workbooks require manual processing

### Target State
- `read_xlsx()` table function for reading XLSX files
- `COPY TO ... (FORMAT XLSX)` for exporting data
- Sheet selection and cell range support
- Automatic type inference from cell formatting
- Support for large files via streaming

## Scope

### In Scope
- Reading XLSX files with `read_xlsx()` table function
- Writing XLSX files with COPY TO statement
- Sheet selection by name or index
- Cell range selection (e.g., A1:D100)
- Type inference from Excel cell types
- Header row handling
- Large file streaming support
- Cloud storage URL support (S3, GCS, Azure)

### Out of Scope (Future Work)
- XLS (legacy binary format) support
- Formula evaluation (read as text/values)
- Cell formatting preservation on write
- Pivot tables and charts
- Password-protected files
- Macros and VBA
- XLSB (binary XLSX) format

## Approach

### Phase 1: XLSX Infrastructure
1. Add excelize library dependency (pure Go)
2. Create `internal/io/xlsx/` package structure
3. Implement reader options and configuration

### Phase 2: read_xlsx Table Function
1. Implement basic file reading
2. Add column name detection from header row
3. Implement type inference from cell types
4. Register as table function

### Phase 3: Sheet and Range Selection
1. Add sheet selection by name/index
2. Implement cell range parsing (A1 notation)
3. Support skip rows and column selection

### Phase 4: Type Inference
1. Map Excel cell types to dukdb-go types
2. Handle date/time serial numbers
3. Detect numeric precision
4. Handle mixed-type columns

### Phase 5: COPY TO xlsx Support
1. Implement XLSX writer
2. Add header row option
3. Support sheet naming
4. Enable compression options

### Phase 6: Integration
1. Register format with format detector
2. Add COPY statement support
3. End-to-end testing
4. Cloud storage integration

## Success Criteria

1. **Read Compatibility**: Can read any valid XLSX file created by Excel 2007+
2. **Write Compatibility**: Files written by dukdb-go can be opened by Excel
3. **Type Accuracy**: Cell types are correctly inferred (numbers, dates, strings)
4. **Sheet Support**: All sheets accessible by name or index
5. **Range Support**: Arbitrary cell ranges can be selected
6. **Performance**: Can process 1M row files in reasonable time
7. **Streaming**: Memory usage remains bounded for large files

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Complex cell formatting | Medium | Low | Focus on values, not formatting |
| Large file memory | Medium | Medium | Streaming/chunked reading |
| Date serial number edge cases | Medium | Low | Comprehensive date testing |
| excelize library bugs | Low | Medium | Pin version, test thoroughly |
| Type inference ambiguity | Medium | Low | Explicit type hints option |

## Dependencies

- `github.com/xuri/excelize/v2` - Pure Go XLSX library (no CGO)
- Existing internal packages: storage, io, executor

## Affected Specs

- **NEW**: `excel-xlsx` - XLSX file reading and writing
- **MODIFIED**: `file-io` - Add XLSX format detection
- **MODIFIED**: `copy-statement` - Add FORMAT XLSX option
