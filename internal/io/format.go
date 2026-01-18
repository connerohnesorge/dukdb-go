// Package io provides file I/O interfaces and utilities for reading and writing
// various file formats (CSV, JSON, Parquet) to and from DataChunks.
package io

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"strings"
)

// Magic bytes for file format detection.
var (
	// Parquet magic bytes: "PAR1" (0x50 0x41 0x52 0x31)
	parquetMagic = []byte{0x50, 0x41, 0x52, 0x31}
	// ZIP magic bytes: "PK.." (0x50 0x4B 0x03 0x04) - XLSX files are ZIP archives
	zipMagic = []byte{0x50, 0x4B, 0x03, 0x04}
)

// Errors for format detection operations.
var (
	ErrFormatDetectionFailed = errors.New("failed to detect file format")
)

// Constants for format detection.
const (
	// maxFormatMagicBytes is the maximum number of bytes to peek for format detection.
	// Larger buffer allows for whitespace before JSON content.
	maxFormatMagicBytes = 64
	// parquetMagicLen is the length of the Parquet magic bytes.
	parquetMagicLen = 4
	// zipMagicLen is the length of the ZIP magic bytes.
	zipMagicLen = 4
)

// DetectFormat detects the file format from magic bytes at the start of the reader.
// It returns the detected format and a new reader that includes the peeked bytes.
// The original reader is consumed and should not be used after this call.
//
// Detection priority:
//  1. Parquet: "PAR1" magic bytes at start
//  2. JSON array: starts with '[' (possibly with leading whitespace)
//  3. JSON object: starts with '{' (possibly with leading whitespace)
//  4. NDJSON: detected by heuristic ('{' followed by '}' then newline on first line)
//  5. CSV: fallback if no other format detected
func DetectFormat(r io.Reader) (Format, io.Reader, error) {
	// Use a buffered reader to peek at magic bytes
	bufReader := bufio.NewReader(r)
	header, err := bufReader.Peek(maxFormatMagicBytes)
	if err != nil && !errors.Is(err, io.EOF) {
		return FormatUnknown, bufReader, ErrFormatDetectionFailed
	}

	// Check for each format's magic bytes/patterns
	format := detectFormatFromBytes(header)

	return format, bufReader, nil
}

// detectFormatFromBytes checks the header bytes against known magic
// sequences and patterns, returning the detected format type.
func detectFormatFromBytes(header []byte) Format {
	if len(header) == 0 {
		return FormatCSV // Default to CSV for empty/unreadable files
	}

	// Check for Parquet magic bytes first
	if len(header) >= parquetMagicLen && bytes.HasPrefix(header, parquetMagic) {
		return FormatParquet
	}

	// Check for ZIP magic bytes (XLSX files are ZIP archives)
	// Note: This detects ZIP format which includes XLSX. The caller should
	// verify via file extension or content inspection whether it's XLSX.
	if len(header) >= zipMagicLen && bytes.HasPrefix(header, zipMagic) {
		return FormatXLSX
	}

	// Skip leading whitespace to find the first significant character
	trimmed := bytes.TrimLeft(header, " \t\n\r")
	if len(trimmed) == 0 {
		return FormatCSV // Only whitespace, default to CSV
	}

	firstChar := trimmed[0]

	// JSON array detection
	if firstChar == '[' {
		return FormatJSON
	}

	// JSON object or NDJSON detection
	if firstChar == '{' {
		// Check if this looks like NDJSON (object followed by newline)
		if isLikelyNDJSON(header) {
			return FormatNDJSON
		}
		// Single JSON object or array of objects
		return FormatJSON
	}

	// Fallback to CSV
	return FormatCSV
}

// isLikelyNDJSON attempts to detect if the content is NDJSON format.
// NDJSON has one complete JSON object per line.
// This is a heuristic: if we find a complete object on the first line
// followed by a newline, it is likely NDJSON.
func isLikelyNDJSON(header []byte) bool {
	// Find the first newline
	newlineIdx := bytes.IndexAny(header, "\n\r")
	if newlineIdx == -1 {
		// No newline found in the peeked bytes, can't determine
		// Could still be NDJSON, but we can't tell from this sample
		return false
	}

	firstLine := bytes.TrimSpace(header[:newlineIdx])
	if len(firstLine) == 0 {
		return false
	}

	// Check if the first line starts with '{' and ends with '}'
	if len(firstLine) >= 2 && firstLine[0] == '{' && firstLine[len(firstLine)-1] == '}' {
		// Check if there is more content after the newline that also looks like a JSON object
		remainder := bytes.TrimSpace(header[newlineIdx:])
		if len(remainder) > 0 && remainder[0] == '{' {
			return true
		}
		// Single line with object, but could be part of a JSON array
		// Check if it does not look like an array element
		// NDJSON lines don't have trailing commas
		if !bytes.HasSuffix(bytes.TrimSpace(firstLine), []byte(",")) {
			return true
		}
	}

	return false
}

// DetectFormatFromPath detects the file format from the file extension.
// Extension matching is case-insensitive.
//
// Supported extensions:
//   - .csv, .tsv -> FormatCSV
//   - .parquet, .pq -> FormatParquet
//   - .json -> FormatJSON
//   - .ndjson, .jsonl -> FormatNDJSON
//   - .xlsx -> FormatXLSX
//
// Returns FormatUnknown if the extension is not recognized.
func DetectFormatFromPath(path string) Format {
	// Handle compressed extensions by removing them first
	cleanPath := removeCompressionExtension(path)

	ext := strings.ToLower(filepath.Ext(cleanPath))
	switch ext {
	case ".csv", ".tsv":
		return FormatCSV
	case ".parquet", ".pq":
		return FormatParquet
	case ".json":
		return FormatJSON
	case ".ndjson", ".jsonl":
		return FormatNDJSON
	case ".xlsx":
		return FormatXLSX
	default:
		return FormatUnknown
	}
}

// removeCompressionExtension removes known compression extensions from a path.
// For example, "data.csv.gz" becomes "data.csv".
func removeCompressionExtension(path string) string {
	compressionExts := []string{
		".gz",
		".gzip",
		".zst",
		".zstd",
		".snappy",
		".lz4",
		".br",
		".brotli",
	}
	lower := strings.ToLower(path)
	for _, ext := range compressionExts {
		if strings.HasSuffix(lower, ext) {
			return path[:len(path)-len(ext)]
		}
	}

	return path
}

// ParseFormat parses a format string into a Format value.
// This is useful for parsing user-provided format options.
// Returns FormatUnknown if the string is not recognized.
func ParseFormat(s string) Format {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "csv":
		return FormatCSV
	case "parquet":
		return FormatParquet
	case "json":
		return FormatJSON
	case "ndjson", "jsonl", "newline_delimited":
		return FormatNDJSON
	case "xlsx", "excel":
		return FormatXLSX
	default:
		return FormatUnknown
	}
}
