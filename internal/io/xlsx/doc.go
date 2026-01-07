// Package xlsx provides Excel XLSX file reading and writing capabilities for dukdb-go.
//
// This package implements the io.FileReader and io.FileWriter interfaces for XLSX format,
// enabling reading from and writing to Microsoft Excel files (.xlsx). It uses the
// excelize library (github.com/xuri/excelize/v2) for low-level XLSX operations.
//
// # Format Detection
//
// XLSX files are detected by their magic bytes (ZIP format header: 0x50 0x4B 0x03 0x04)
// or by the .xlsx file extension. The format detection system integrates with the
// main io package's DetectFormat and DetectFormatFromPath functions.
//
// # SQL Functions
//
// Two table functions are provided for reading XLSX files:
//
//	read_xlsx(path, options...) - Read with explicit options
//	read_xlsx_auto(path)        - Read with automatic format detection
//
// Example SQL usage:
//
//	SELECT * FROM read_xlsx('data.xlsx');
//	SELECT * FROM read_xlsx('data.xlsx', sheet='Sales', range='A1:D100');
//	SELECT * FROM read_xlsx_auto('data.xlsx');
//
// # Reader
//
// The Reader implements the io.FileReader interface for reading XLSX files into
// DataChunks. It supports streaming row iteration for memory-efficient processing.
//
// Basic usage:
//
//	reader, err := xlsx.NewReaderFromPath("data.xlsx", nil)
//	if err != nil {
//	    return err
//	}
//	defer reader.Close()
//
//	// Get column names
//	schema, err := reader.Schema()
//
//	// Get column types (inferred or specified)
//	types, err := reader.Types()
//
//	// Read data in chunks
//	for {
//	    chunk, err := reader.ReadChunk()
//	    if err == io.EOF {
//	        break
//	    }
//	    if err != nil {
//	        return err
//	    }
//	    // Process chunk...
//	}
//
// Reading from an io.Reader:
//
//	data, _ := os.ReadFile("data.xlsx")
//	reader, err := xlsx.NewReader(bytes.NewReader(data), nil)
//
// # ReaderOptions
//
// ReaderOptions controls how XLSX files are read. All options are optional;
// sensible defaults are used when not specified.
//
// Sheet Selection:
//
//	opts := xlsx.DefaultReaderOptions()
//	opts.Sheet = "Sales"      // Select sheet by name
//	opts.SheetIndex = 2       // Or by 0-based index (SheetIndex takes precedence if >= 0)
//
// Cell Range:
//
//	opts.Range = "B2:F100"    // Read only cells in range (A1 notation)
//	opts.StartRow = 5         // Or specify start/end rows (1-based)
//	opts.EndRow = 100
//	opts.StartCol = "B"       // And start/end columns
//	opts.EndCol = "F"
//
// Header and Skip:
//
//	opts.Header = true        // First row is header (default)
//	opts.Header = false       // Generate column names: column0, column1, ...
//	opts.Skip = 3             // Skip first 3 rows before header
//
// Type Handling:
//
//	opts.InferTypes = true    // Auto-detect column types (default)
//	opts.InferTypes = false   // All columns as VARCHAR
//	opts.Columns = map[string]string{
//	    "Age": "INTEGER",
//	    "Salary": "DOUBLE",
//	}                         // Explicit column type overrides
//
// NULL Handling:
//
//	opts.EmptyAsNull = true   // Treat empty cells as NULL (default)
//	opts.NullValues = []string{"NA", "N/A", "#N/A"}  // Additional NULL markers
//
// Performance:
//
//	opts.ChunkSize = 2048     // Rows per chunk (default: StandardVectorSize)
//
// # Writer
//
// The Writer implements the io.FileWriter interface for writing DataChunks to
// XLSX files.
//
// Basic usage:
//
//	writer, err := xlsx.NewWriterToPath("output.xlsx", nil)
//	if err != nil {
//	    return err
//	}
//	defer writer.Close()
//
//	// Set schema (column names)
//	writer.SetSchema([]string{"ID", "Name", "Value"})
//
//	// Optionally set types for proper formatting
//	writer.SetTypes([]dukdb.Type{
//	    dukdb.TYPE_INTEGER,
//	    dukdb.TYPE_VARCHAR,
//	    dukdb.TYPE_DOUBLE,
//	})
//
//	// Write chunks
//	for _, chunk := range chunks {
//	    if err := writer.WriteChunk(chunk); err != nil {
//	        return err
//	    }
//	}
//
// Writing to an io.Writer:
//
//	var buf bytes.Buffer
//	writer, err := xlsx.NewWriter(&buf, nil)
//	// ... write data ...
//	writer.Close()
//	// buf now contains the XLSX file
//
// # WriterOptions
//
// WriterOptions controls how XLSX files are written.
//
//	opts := xlsx.DefaultWriterOptions()
//	opts.SheetName = "Report"          // Sheet name (default: "Sheet1")
//	opts.Header = true                  // Write header row (default: true)
//	opts.AutoWidth = true               // Auto-calculate column widths (default: true)
//	opts.DateFormat = "yyyy-mm-dd"      // Date format string
//	opts.TimeFormat = "hh:mm:ss"        // Time format string
//	opts.CompressionLevel = 6           // ZIP compression (0-9, default: 6)
//
// # Type Mapping
//
// The package maps between Excel cell types and dukdb-go types:
//
//	Excel Type          dukdb-go Type
//	----------          -------------
//	Number (integer)    BIGINT
//	Number (float)      DOUBLE
//	Number (date)       TIMESTAMP
//	Boolean             BOOLEAN
//	String              VARCHAR
//	Date                TIMESTAMP
//	Formula             (evaluated result type)
//	Empty               NULL (if EmptyAsNull) or ""
//
// When writing, dukdb-go types are converted to appropriate Excel formats:
//
//	dukdb-go Type       Excel Format
//	-------------       ------------
//	INTEGER/BIGINT      Number (no decimals)
//	DOUBLE/FLOAT        Number (with decimals)
//	BOOLEAN             Boolean (TRUE/FALSE)
//	DATE                Date format (e.g., yyyy-mm-dd)
//	TIMESTAMP           DateTime format
//	TIME                Time format (e.g., hh:mm:ss)
//	VARCHAR             String
//	DECIMAL             String (preserves precision)
//	UUID                String
//	INTERVAL            String (human readable)
//
// # Date/Time Handling
//
// Excel stores dates as serial numbers (days since December 30, 1899).
// The package automatically converts between Excel serial numbers and Go time.Time:
//
//	// Reading: Excel serial -> time.Time
//	// Writing: time.Time -> Excel serial + format style
//
// Date format detection uses both built-in Excel format IDs and custom format
// string patterns to identify date cells.
//
// # A1 Notation Utilities
//
// The package provides utilities for working with Excel A1 notation:
//
//	xlsx.ParseRange("B2:F10")           // Parse range to col/row indices
//	xlsx.ParseCell("AA100")             // Parse cell to col (0-based), row (1-based)
//	xlsx.ColumnLettersToIndex("AA")     // "AA" -> 26 (0-based)
//	xlsx.IndexToColumnLetters(26)       // 26 -> "AA"
//	xlsx.CellAddress(1, 5)              // col=1, row=5 -> "B5"
//
// # Cloud Storage Integration
//
// The XLSX reader integrates with dukdb-go's filesystem abstraction for reading
// from cloud storage:
//
//	SELECT * FROM read_xlsx('s3://bucket/data.xlsx');
//	SELECT * FROM read_xlsx('gs://bucket/data.xlsx');
//	SELECT * FROM read_xlsx('https://example.com/data.xlsx');
//
// Note: XLSX files are read entirely into memory because the format requires
// random access (ZIP archive structure). For very large files, consider
// converting to CSV or Parquet.
//
// # Performance Considerations
//
//   - Memory: XLSX files are loaded into memory for parsing. Use CSV or Parquet
//     for files that don't fit in memory.
//   - Type inference: Disabling InferTypes improves read performance but returns
//     all columns as VARCHAR.
//   - Chunk size: Larger chunks reduce overhead but increase memory per chunk.
//   - Auto-width: Column width calculation adds slight overhead during writing.
//
// # Limitations
//
//   - Maximum rows: Excel limit of 1,048,576 rows per sheet
//   - Maximum columns: Excel limit of 16,384 columns (A to XFD)
//   - Formulas: Read as calculated values, not as formula text
//   - Macros: Not supported (XLSM format not supported)
//   - Comments: Not preserved during read/write
//   - Cell styles: Limited support (date formats only)
//   - Charts/Images: Not supported
package xlsx
