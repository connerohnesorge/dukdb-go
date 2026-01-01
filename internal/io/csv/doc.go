// Package csv provides CSV file reading and writing capabilities for dukdb-go.
// This package implements the io.FileReader and io.FileWriter interfaces for CSV format.
//
// The CSV reader supports:
//   - Auto-detection of delimiters (comma, tab, semicolon, pipe)
//   - Header row detection and parsing
//   - Compressed file reading (gzip, zstd, etc.)
//   - Chunked reading for memory-efficient processing
//   - NULL value handling via configurable null string
//   - Comment line skipping
//   - Automatic type inference for columns
package csv
