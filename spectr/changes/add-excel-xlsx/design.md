# Design: Excel/XLSX Format Support

## Context

Microsoft Excel XLSX format (Office Open XML Spreadsheet) is an XML-based format compressed in a ZIP archive. It's the default format for Excel 2007+ and is widely used for data interchange in business environments.

**Stakeholders**:
- Business analysts exchanging data with non-technical teams
- Data engineers importing spreadsheet exports
- Applications generating reports in Excel format

**Constraints**:
- Must remain pure Go (no CGO)
- Must handle large files without excessive memory
- Must correctly interpret Excel date serial numbers
- Must support cloud storage URLs

## Goals / Non-Goals

**Goals**:
1. Read XLSX files with `read_xlsx()` table function
2. Write XLSX files with `COPY TO ... (FORMAT XLSX)`
3. Support sheet selection by name or index
4. Support cell range selection (A1 notation)
5. Infer types from Excel cell types
6. Handle large files via streaming

**Non-Goals**:
1. XLS (legacy binary) format support
2. Formula evaluation (formulas read as calculated values)
3. Cell formatting/styling preservation
4. Pivot tables, charts, macros
5. Password-protected files
6. XLSB format

## Decisions

### Decision 1: XLSX Library Selection

**Options Evaluated**:

| Library | CGO | Active | Features | Performance |
|---------|-----|--------|----------|-------------|
| excelize/v2 | No | Yes | Full | Good |
| tealeg/xlsx | No | Moderate | Basic | Good |
| 360EntSecGroup-Skylar/excelize | No | Merged | - | - |

**Choice**: `github.com/xuri/excelize/v2`

**Rationale**:
- Pure Go implementation (no CGO dependency)
- Actively maintained with regular releases
- Streaming API for large files
- Full read/write support
- Good documentation and community

**Go Implementation**:
```go
import "github.com/xuri/excelize/v2"

// Opening a file
f, err := excelize.OpenFile("data.xlsx")
if err != nil {
    return err
}
defer f.Close()

// Reading cells
value, err := f.GetCellValue("Sheet1", "A1")

// Streaming large files
rows, err := f.Rows("Sheet1")
for rows.Next() {
    row, err := rows.Columns()
    // Process row...
}
```

### Decision 2: read_xlsx Table Function

**Function Signature**:
```sql
-- Basic usage
SELECT * FROM read_xlsx('data.xlsx');

-- With options
SELECT * FROM read_xlsx('data.xlsx',
    sheet := 'Sales',
    range := 'A1:E1000',
    header := true,
    skip := 2,
    columns := {'id': 'INTEGER', 'name': 'VARCHAR', 'date': 'DATE'}
);

-- Auto-detect with cloud URL
SELECT * FROM read_xlsx('s3://bucket/reports/quarterly.xlsx');
```

**Options Structure**:
```go
type ReaderOptions struct {
    // Sheet selection
    Sheet       string   // Sheet name (default: first sheet)
    SheetIndex  int      // Sheet index (0-based, alternative to name)

    // Range selection
    Range       string   // Cell range in A1 notation (e.g., "A1:D100")
    StartRow    int      // First row to read (1-based)
    EndRow      int      // Last row to read (0 = all)
    StartCol    string   // First column (e.g., "A")
    EndCol      string   // Last column (e.g., "Z")

    // Header handling
    Header      bool     // First row contains column names (default: true)
    Skip        int      // Number of rows to skip before header

    // Type handling
    Columns     map[string]string // Explicit column types
    InferTypes  bool     // Auto-detect types (default: true)
    DateFormat  string   // Date format hint

    // Performance
    ChunkSize   int      // Rows per DataChunk (default: 2048)

    // NULL handling
    EmptyAsNull bool     // Treat empty cells as NULL (default: true)
    NullValues  []string // Additional NULL value strings
}

func DefaultReaderOptions() *ReaderOptions {
    return &ReaderOptions{
        Sheet:       "",          // First sheet
        SheetIndex:  -1,          // Not specified
        Header:      true,
        InferTypes:  true,
        ChunkSize:   storage.DefaultVectorSize, // 2048
        EmptyAsNull: true,
    }
}
```

**Reader Implementation**:
```go
type Reader struct {
    file          *excelize.File
    opts          *ReaderOptions
    columns       []string
    columnTypes   []dukdb.Type
    rows          *excelize.Rows  // Streaming row iterator
    currentRow    int
    initialized   bool
    eof           bool
}

func NewReader(r io.Reader, opts *ReaderOptions) (*Reader, error) {
    // excelize needs a file, not just Reader
    // For streaming, use OpenReader with file path or bytes
    f, err := excelize.OpenReader(r)
    if err != nil {
        return nil, fmt.Errorf("xlsx: failed to open file: %w", err)
    }

    reader := &Reader{
        file: f,
        opts: opts,
    }

    return reader, nil
}

func (r *Reader) Read() (*storage.DataChunk, error) {
    if !r.initialized {
        if err := r.initialize(); err != nil {
            return nil, err
        }
    }

    if r.eof {
        return nil, io.EOF
    }

    chunk := storage.NewDataChunk(r.columnTypes, r.opts.ChunkSize)
    rowsRead := 0

    for rowsRead < r.opts.ChunkSize && r.rows.Next() {
        row, err := r.rows.Columns()
        if err != nil {
            return nil, err
        }

        // Skip rows if needed
        r.currentRow++
        if r.currentRow <= r.opts.Skip {
            continue
        }

        // Skip header row (already processed in initialize)
        if r.opts.Header && r.currentRow == r.opts.Skip+1 {
            continue
        }

        // Apply range filter
        if !r.inRange(r.currentRow) {
            continue
        }

        // Convert and append row
        if err := r.appendRow(chunk, row); err != nil {
            return nil, err
        }
        rowsRead++
    }

    if rowsRead == 0 {
        r.eof = true
        return nil, io.EOF
    }

    return chunk, nil
}
```

**Rationale**:
- Streaming API prevents loading entire file into memory
- Options match conventions from read_csv/read_parquet
- Header detection is common Excel use case
- Range selection enables partial file reading

### Decision 3: COPY TO xlsx Support

**SQL Syntax**:
```sql
-- Basic export
COPY table TO 'output.xlsx' (FORMAT XLSX);

-- With options
COPY table TO 'output.xlsx' (
    FORMAT XLSX,
    SHEET 'Results',
    HEADER true
);

-- Query export
COPY (SELECT * FROM sales WHERE year = 2024)
    TO 'sales_2024.xlsx' (FORMAT XLSX);

-- Cloud storage
COPY results TO 's3://bucket/reports/output.xlsx' (FORMAT XLSX);
```

**Writer Options**:
```go
type WriterOptions struct {
    // Sheet configuration
    SheetName   string   // Name for the sheet (default: "Sheet1")

    // Header handling
    Header      bool     // Write column names as first row (default: true)

    // Formatting hints (basic)
    DateFormat  string   // Excel date format string
    TimeFormat  string   // Excel time format string

    // Column widths
    AutoWidth   bool     // Auto-calculate column widths (default: true)

    // Compression (XLSX is always ZIP, this is internal compression level)
    CompressionLevel int // 0-9 (default: 6)
}

func DefaultWriterOptions() *WriterOptions {
    return &WriterOptions{
        SheetName:        "Sheet1",
        Header:           true,
        DateFormat:       "yyyy-mm-dd",
        TimeFormat:       "hh:mm:ss",
        AutoWidth:        true,
        CompressionLevel: 6,
    }
}
```

**Writer Implementation**:
```go
type Writer struct {
    file    *excelize.File
    opts    *WriterOptions
    sheet   string
    row     int
    columns []string
    types   []dukdb.Type
}

func NewWriter(w io.Writer, columns []string, types []dukdb.Type, opts *WriterOptions) (*Writer, error) {
    f := excelize.NewFile()

    // Rename default sheet or create named sheet
    sheetName := opts.SheetName
    if sheetName == "" {
        sheetName = "Sheet1"
    }

    // Get default sheet and rename it
    defaultSheet := f.GetSheetName(0)
    if defaultSheet != sheetName {
        f.SetSheetName(defaultSheet, sheetName)
    }

    writer := &Writer{
        file:    f,
        opts:    opts,
        sheet:   sheetName,
        row:     1,
        columns: columns,
        types:   types,
    }

    // Write header if enabled
    if opts.Header {
        for col, name := range columns {
            cell := cellAddress(col, 1)
            f.SetCellValue(sheetName, cell, name)
        }
        writer.row = 2
    }

    return writer, nil
}

func (w *Writer) Write(chunk *storage.DataChunk) error {
    for rowIdx := 0; rowIdx < chunk.Size(); rowIdx++ {
        for colIdx := 0; colIdx < len(w.columns); colIdx++ {
            cell := cellAddress(colIdx, w.row)
            value := chunk.GetValue(colIdx, rowIdx)

            if value == nil {
                // Leave cell empty for NULL
                continue
            }

            // Convert value based on type
            xlValue := w.convertValue(value, w.types[colIdx])
            w.file.SetCellValue(w.sheet, cell, xlValue)
        }
        w.row++
    }

    return nil
}

func (w *Writer) Close() error {
    if w.opts.AutoWidth {
        w.calculateColumnWidths()
    }
    return nil
}

func (w *Writer) WriteTo(out io.Writer) error {
    return w.file.Write(out)
}
```

**Rationale**:
- Single sheet export keeps implementation simple
- Header option matches CSV/Parquet conventions
- Auto-width improves readability in Excel
- WriteTo enables streaming to cloud storage

### Decision 4: Sheet and Range Selection

**Sheet Selection**:
```go
func (r *Reader) selectSheet() (string, error) {
    sheets := r.file.GetSheetList()

    if len(sheets) == 0 {
        return "", errors.New("xlsx: no sheets in workbook")
    }

    // By name
    if r.opts.Sheet != "" {
        for _, s := range sheets {
            if s == r.opts.Sheet {
                return s, nil
            }
        }
        return "", fmt.Errorf("xlsx: sheet '%s' not found", r.opts.Sheet)
    }

    // By index
    if r.opts.SheetIndex >= 0 {
        if r.opts.SheetIndex >= len(sheets) {
            return "", fmt.Errorf("xlsx: sheet index %d out of range (0-%d)",
                r.opts.SheetIndex, len(sheets)-1)
        }
        return sheets[r.opts.SheetIndex], nil
    }

    // Default: first sheet
    return sheets[0], nil
}
```

**Range Parsing (A1 Notation)**:
```go
// parseRange converts A1:D100 notation to row/column bounds
func parseRange(rangeStr string) (startCol, startRow, endCol, endRow int, err error) {
    parts := strings.Split(rangeStr, ":")
    if len(parts) != 2 {
        return 0, 0, 0, 0, fmt.Errorf("invalid range: %s", rangeStr)
    }

    startCol, startRow, err = parseCell(parts[0])
    if err != nil {
        return 0, 0, 0, 0, err
    }

    endCol, endRow, err = parseCell(parts[1])
    if err != nil {
        return 0, 0, 0, 0, err
    }

    return startCol, startRow, endCol, endRow, nil
}

// parseCell converts A1 to column index (0-based) and row number (1-based)
func parseCell(cell string) (col int, row int, err error) {
    // Separate letters and digits
    var letters, digits string
    for i, c := range cell {
        if c >= '0' && c <= '9' {
            letters = cell[:i]
            digits = cell[i:]
            break
        }
    }

    if letters == "" {
        return 0, 0, fmt.Errorf("invalid cell: %s (no column)", cell)
    }
    if digits == "" {
        return 0, 0, fmt.Errorf("invalid cell: %s (no row)", cell)
    }

    // Convert column letters to index (A=0, B=1, ..., Z=25, AA=26, ...)
    col = columnLettersToIndex(strings.ToUpper(letters))

    // Parse row number
    row, err = strconv.Atoi(digits)
    if err != nil {
        return 0, 0, fmt.Errorf("invalid row: %s", digits)
    }

    return col, row, nil
}

// columnLettersToIndex converts Excel column letters to 0-based index
// A=0, B=1, ..., Z=25, AA=26, AB=27, ...
func columnLettersToIndex(letters string) int {
    result := 0
    for i, c := range letters {
        if i > 0 {
            result = (result + 1) * 26
        }
        result += int(c - 'A')
    }
    return result
}

// indexToColumnLetters converts 0-based index to Excel column letters
func indexToColumnLetters(index int) string {
    var result []byte
    for index >= 0 {
        result = append([]byte{byte('A' + index%26)}, result...)
        index = index/26 - 1
    }
    return string(result)
}

// cellAddress generates cell address from 0-based column and 1-based row
func cellAddress(col, row int) string {
    return fmt.Sprintf("%s%d", indexToColumnLetters(col), row)
}
```

**Rationale**:
- A1 notation is familiar to Excel users
- Supporting both name and index provides flexibility
- Range selection enables efficient partial file reading

### Decision 5: Type Inference from Cells

**Excel Cell Types**:
Excel internally stores values as:
- Numbers (including dates as serial numbers)
- Strings
- Booleans
- Errors
- Formulas (we read the calculated value)

**Type Mapping**:
```go
// inferTypeFromCell determines dukdb-go type from Excel cell
func inferTypeFromCell(f *excelize.File, sheet, cell string) dukdb.Type {
    cellType, err := f.GetCellType(sheet, cell)
    if err != nil {
        return dukdb.TYPE_VARCHAR
    }

    switch cellType {
    case excelize.CellTypeNumber:
        // Check if it's a date by format
        styleID, _ := f.GetCellStyle(sheet, cell)
        if isDateFormat(f, styleID) {
            return dukdb.TYPE_TIMESTAMP
        }
        // Check if integer or float
        value, _ := f.GetCellValue(sheet, cell)
        if !strings.Contains(value, ".") {
            return dukdb.TYPE_BIGINT
        }
        return dukdb.TYPE_DOUBLE

    case excelize.CellTypeBool:
        return dukdb.TYPE_BOOLEAN

    case excelize.CellTypeString, excelize.CellTypeInlineString, excelize.CellTypeSharedString:
        return dukdb.TYPE_VARCHAR

    case excelize.CellTypeFormula:
        // For formulas, check the calculated value type
        value, _ := f.GetCellValue(sheet, cell)
        return inferTypeFromString(value)

    default:
        return dukdb.TYPE_VARCHAR
    }
}

// inferTypesFromColumn samples cells to determine column type
func inferTypesFromColumn(f *excelize.File, sheet string, col int, sampleRows []int) dukdb.Type {
    typeCounts := make(map[dukdb.Type]int)

    for _, row := range sampleRows {
        cell := cellAddress(col, row)
        typ := inferTypeFromCell(f, sheet, cell)
        typeCounts[typ]++
    }

    // Return most common type (with VARCHAR as fallback for mixed)
    maxCount := 0
    resultType := dukdb.TYPE_VARCHAR

    for typ, count := range typeCounts {
        if count > maxCount {
            maxCount = count
            resultType = typ
        }
    }

    // If no clear majority, use VARCHAR
    if maxCount < len(sampleRows)/2 {
        return dukdb.TYPE_VARCHAR
    }

    return resultType
}
```

**Date Handling**:
```go
// Excel stores dates as serial numbers (days since 1899-12-30)
const excelEpoch = -2209161600 // Unix timestamp for 1899-12-30

func excelDateToTime(serial float64) time.Time {
    // Excel has a bug: it thinks 1900 was a leap year
    // Adjust for dates after Feb 28, 1900
    if serial > 60 {
        serial--
    }

    days := int64(serial)
    fraction := serial - float64(days)

    // Convert to Unix timestamp
    unixTime := excelEpoch + days*86400 + int64(fraction*86400)

    return time.Unix(unixTime, 0).UTC()
}

func timeToExcelDate(t time.Time) float64 {
    unixTime := t.Unix()
    serial := float64(unixTime-excelEpoch) / 86400.0

    // Apply Excel 1900 leap year bug
    if serial >= 60 {
        serial++
    }

    return serial
}

// isDateFormat checks if Excel number format is a date format
func isDateFormat(f *excelize.File, styleID int) bool {
    style, err := f.GetStyle(styleID)
    if err != nil {
        return false
    }

    // Check for common date format codes
    numFmt := style.NumFmt
    datePatterns := []string{
        "d", "m", "y",     // Day, month, year
        "h", "s",          // Hour, second (time)
        "AM/PM", "am/pm",  // 12-hour time
    }

    for _, pattern := range datePatterns {
        if strings.Contains(strings.ToLower(numFmt), pattern) {
            return true
        }
    }

    // Check built-in date format IDs
    builtinDateFormats := map[int]bool{
        14: true, 15: true, 16: true, 17: true, 18: true, 19: true,
        20: true, 21: true, 22: true, 45: true, 46: true, 47: true,
    }

    return builtinDateFormats[style.NumFmtID]
}
```

**Rationale**:
- Excel cell types provide strong hints for type inference
- Date detection via format codes handles the serial number ambiguity
- Sampling multiple rows handles mixed-type columns gracefully
- VARCHAR fallback ensures no data loss

### Decision 6: Large File Handling

**Streaming Read Architecture**:
```go
type StreamingReader struct {
    file        *excelize.File
    rows        *excelize.Rows
    sheet       string
    opts        *ReaderOptions
    columns     []string
    columnTypes []dukdb.Type
    currentRow  int
    buffer      [][]string
    bufferIdx   int
}

func (r *StreamingReader) initialize() error {
    // Get sheet name
    sheet, err := r.selectSheet()
    if err != nil {
        return err
    }
    r.sheet = sheet

    // Open streaming row iterator
    rows, err := r.file.Rows(sheet)
    if err != nil {
        return err
    }
    r.rows = rows

    // Skip rows and read header
    for i := 0; i < r.opts.Skip; i++ {
        if !rows.Next() {
            return errors.New("xlsx: not enough rows to skip")
        }
        r.currentRow++
    }

    // Read header row
    if r.opts.Header {
        if !rows.Next() {
            return errors.New("xlsx: no header row found")
        }
        r.currentRow++
        headerRow, err := rows.Columns()
        if err != nil {
            return err
        }
        r.columns = headerRow
    }

    // Infer types from sample rows
    if r.opts.InferTypes {
        r.columnTypes = r.inferTypesFromSample()
    }

    return nil
}

// Read returns the next chunk of data
func (r *StreamingReader) Read() (*storage.DataChunk, error) {
    chunk := storage.NewDataChunk(r.columnTypes, r.opts.ChunkSize)
    rowsRead := 0

    for rowsRead < r.opts.ChunkSize {
        if !r.rows.Next() {
            break
        }
        r.currentRow++

        // Check range bounds
        if r.opts.EndRow > 0 && r.currentRow > r.opts.EndRow {
            break
        }

        row, err := r.rows.Columns()
        if err != nil {
            return nil, err
        }

        // Append row to chunk
        if err := r.appendRow(chunk, row); err != nil {
            return nil, err
        }
        rowsRead++
    }

    if rowsRead == 0 {
        return nil, io.EOF
    }

    chunk.SetSize(rowsRead)
    return chunk, nil
}
```

**Memory Management**:
```go
// estimateMemoryUsage estimates memory for a file
func estimateMemoryUsage(filePath string, opts *ReaderOptions) (int64, error) {
    info, err := os.Stat(filePath)
    if err != nil {
        return 0, err
    }

    // XLSX is compressed; actual data is typically 3-10x larger
    estimatedUncompressed := info.Size() * 5

    // Streaming mode only holds current chunk in memory
    chunkMemory := int64(opts.ChunkSize) * estimatedColumnSize

    // excelize internal buffers
    internalBuffers := int64(10 * 1024 * 1024) // ~10MB for XML parsing

    return chunkMemory + internalBuffers, nil
}

const (
    // Threshold for switching to streaming mode
    streamingThreshold = 100 * 1024 * 1024 // 100MB uncompressed estimate

    // Maximum rows to sample for type inference
    typeSampleSize = 100

    // Estimated average column size
    estimatedColumnSize = 100 // bytes
)
```

**Rationale**:
- excelize Rows() API enables true streaming without loading full sheet
- Chunk-based reading matches existing pattern from CSV/Parquet
- Memory estimation helps users make informed decisions
- Streaming prevents OOM for multi-million row spreadsheets

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| excelize memory usage | Medium | Use streaming API, monitor memory |
| Date format ambiguity | Low | Provide explicit type hints option |
| Formula calculation | Low | Document that formulas read as values |
| Large file performance | Medium | Implement chunked reading |
| Mixed-type columns | Low | VARCHAR fallback, type coercion |

## Migration Plan

### Phase 1: Core Infrastructure (Days 1-2)
1. Add excelize dependency
2. Create `internal/io/xlsx/` package
3. Implement reader options
4. Basic unit tests

### Phase 2: Reader Implementation (Days 3-5)
1. Implement Reader struct
2. Add sheet/range selection
3. Implement type inference
4. Streaming support

### Phase 3: Writer Implementation (Days 6-7)
1. Implement Writer struct
2. Add header/sheet options
3. Type conversion to Excel

### Phase 4: Integration (Days 8-10)
1. Register read_xlsx table function
2. Add COPY TO XLSX support
3. Format detection
4. Cloud storage integration

### Phase 5: Testing & Documentation (Days 11-12)
1. Comprehensive test suite
2. Edge case handling
3. Documentation
4. Performance benchmarks

## Open Questions

1. **Multi-sheet writes?**
   - Answer: Start with single sheet; multi-sheet can be added later via options

2. **Formula handling?**
   - Answer: Read calculated values; writing formulas is out of scope

3. **Password protection?**
   - Answer: Out of scope; return clear error for encrypted files

4. **Maximum file size?**
   - Answer: Streaming handles arbitrarily large files; practical limit is memory/time

5. **XLSB format?**
   - Answer: Out of scope; focus on standard XLSX first
