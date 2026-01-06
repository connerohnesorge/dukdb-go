// Package arrow provides Apache Arrow IPC file reading and writing capabilities for dukdb-go.
// This file contains file format detection utilities for Arrow IPC formats.
package arrow

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
)

// ArrowFormat represents the detected Arrow IPC format type.
type ArrowFormat int

const (
	// FormatUnknown indicates the format could not be determined.
	FormatUnknown ArrowFormat = iota
	// FormatFile indicates an Arrow IPC file format (random access).
	FormatFile
	// FormatStream indicates an Arrow IPC stream format (sequential).
	FormatStream
)

// String returns the string representation of an ArrowFormat.
func (f ArrowFormat) String() string {
	switch f {
	case FormatUnknown:
		return "unknown"
	case FormatFile:
		return "file"
	case FormatStream:
		return "stream"
	default:
		return "unknown"
	}
}

// Magic bytes for Arrow IPC format detection.
var (
	// arrowFileMagicBytes is the magic bytes at the start of Arrow IPC files.
	// The file format starts with "ARROW1" followed by padding to 8 bytes.
	arrowFileMagicBytes = []byte("ARROW1")
)

// Constants for format detection.
const (
	// maxPeekSize is the maximum number of bytes to peek for format detection.
	maxPeekSize = 64
)

// DetectFormat detects the Arrow IPC format from magic bytes at the start of the reader.
// It returns the detected format and a new reader that includes the peeked bytes.
// The original reader is consumed and should not be used after this call.
//
// Detection priority:
//  1. Arrow IPC File: starts with "ARROW1" magic bytes
//  2. Arrow IPC Stream: starts with continuation indicator (0xFFFFFFFF) followed by schema
//  3. Unknown: if neither pattern matches
func DetectFormat(r io.Reader) (ArrowFormat, io.Reader, error) {
	// Use a buffered reader to peek at magic bytes
	bufReader := bufio.NewReader(r)
	header, err := bufReader.Peek(maxPeekSize)
	if err != nil && err != io.EOF {
		return FormatUnknown, bufReader, err
	}

	// Check for each format's magic bytes/patterns
	format := detectFormatFromBytes(header)

	return format, bufReader, nil
}

// detectFormatFromBytes checks the header bytes against known magic
// sequences and patterns, returning the detected format type.
func detectFormatFromBytes(header []byte) ArrowFormat {
	if len(header) == 0 {
		return FormatUnknown
	}

	// Check for Arrow IPC file magic bytes first ("ARROW1")
	if len(header) >= ArrowFileMagicLen && bytes.HasPrefix(header, arrowFileMagicBytes) {
		return FormatFile
	}

	// Check for Arrow IPC stream continuation indicator (0xFFFFFFFF)
	// Stream format starts with the continuation indicator followed by schema message
	if len(header) >= 4 {
		continuation := binary.LittleEndian.Uint32(header[:4])
		if continuation == ArrowStreamContinuation {
			return FormatStream
		}
	}

	return FormatUnknown
}

// IsArrowFile checks if the given bytes represent an Arrow IPC file.
func IsArrowFile(header []byte) bool {
	return len(header) >= ArrowFileMagicLen && bytes.HasPrefix(header, arrowFileMagicBytes)
}

// IsArrowStream checks if the given bytes represent an Arrow IPC stream.
func IsArrowStream(header []byte) bool {
	if len(header) < 4 {
		return false
	}
	continuation := binary.LittleEndian.Uint32(header[:4])
	return continuation == ArrowStreamContinuation
}

// DetectFormatFromPath detects the Arrow IPC format from the file extension.
// Extension matching is case-insensitive.
//
// Supported extensions:
//   - .arrow, .feather, .ipc -> FormatFile (assumed file format)
//   - .arrows -> FormatStream (stream format)
//
// Returns FormatUnknown if the extension is not recognized.
func DetectFormatFromPath(path string) ArrowFormat {
	// Find the extension
	dotIdx := -1
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '.' {
			dotIdx = i
			break
		}
		if path[i] == '/' || path[i] == '\\' {
			break
		}
	}

	if dotIdx < 0 {
		return FormatUnknown
	}

	ext := path[dotIdx:]
	// Convert to lowercase for comparison
	extLower := make([]byte, len(ext))
	for i, c := range ext {
		if c >= 'A' && c <= 'Z' {
			extLower[i] = byte(c + 32) // Convert to lowercase
		} else {
			extLower[i] = byte(c)
		}
	}

	switch string(extLower) {
	case ".arrow", ".feather", ".ipc":
		return FormatFile
	case ".arrows":
		return FormatStream
	default:
		return FormatUnknown
	}
}
