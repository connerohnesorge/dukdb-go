// Package format provides DuckDB binary file format (v64) compatibility for catalog persistence.
package format

import (
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
)

// crc64Table is the CRC64 polynomial table used for checksum calculation.
// DuckDB uses the ECMA polynomial for all checksums.
var crc64Table = crc64.MakeTable(crc64.ECMA)

// CalculateChecksum computes the CRC64 checksum of the given data.
//
// This function uses the ECMA polynomial table, matching DuckDB's checksum algorithm.
// The checksum provides data integrity verification for binary format serialization.
//
// Parameters:
//   - data: The byte slice to checksum
//
// Returns:
//   - The 64-bit CRC64 checksum value
//
// Example:
//
//	data := []byte("hello world")
//	checksum := CalculateChecksum(data)
//	fmt.Printf("Checksum: 0x%x\n", checksum)
func CalculateChecksum(data []byte) uint64 {
	return crc64.Checksum(data, crc64Table)
}

// WriteWithChecksum writes data followed by its CRC64 checksum to the writer.
//
// The checksum is written in little-endian byte order to match DuckDB's binary format.
// This function is used when serializing blocks of data that require integrity verification.
//
// Binary format:
//   - Data: []byte (variable length)
//   - Checksum: uint64 (8 bytes, little-endian)
//
// Parameters:
//   - w: The writer to write to
//   - data: The data to write and checksum
//
// Returns:
//   - error: Any error encountered during writing
//
// Example:
//
//	var buf bytes.Buffer
//	data := []byte("important data")
//	err := WriteWithChecksum(&buf, data)
//	if err != nil {
//	    log.Fatal(err)
//	}
func WriteWithChecksum(
	w io.Writer,
	data []byte,
) error {
	// Write data
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf(
			"failed to write data: %w",
			err,
		)
	}

	// Calculate and write checksum
	checksum := CalculateChecksum(data)
	if err := binary.Write(w, ByteOrder, checksum); err != nil {
		return fmt.Errorf(
			"failed to write checksum: %w",
			err,
		)
	}

	return nil
}

// ReadAndVerifyChecksum reads data and its checksum, verifying data integrity.
//
// This function reads exactly expectedLen bytes of data, then reads an 8-byte checksum.
// It calculates the checksum of the read data and compares it to the stored checksum.
// If the checksums don't match, it returns ErrChecksumMismatch.
//
// Binary format:
//   - Data: []byte (expectedLen bytes)
//   - Checksum: uint64 (8 bytes, little-endian)
//
// Parameters:
//   - r: The reader to read from
//   - expectedLen: The expected number of data bytes (excluding checksum)
//
// Returns:
//   - []byte: The verified data if checksum matches
//   - error: ErrChecksumMismatch if verification fails, or any I/O error
//
// Example:
//
//	data, err := ReadAndVerifyChecksum(reader, 1024)
//	if err != nil {
//	    if errors.Is(err, ErrChecksumMismatch) {
//	        log.Fatal("Data corruption detected!")
//	    }
//	    log.Fatal(err)
//	}
func ReadAndVerifyChecksum(
	r io.Reader,
	expectedLen int,
) ([]byte, error) {
	// Read data
	data := make([]byte, expectedLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf(
			"failed to read data: %w",
			err,
		)
	}

	// Read checksum
	var storedChecksum uint64
	if err := binary.Read(r, ByteOrder, &storedChecksum); err != nil {
		return nil, fmt.Errorf(
			"failed to read checksum: %w",
			err,
		)
	}

	// Verify checksum
	calculatedChecksum := CalculateChecksum(data)
	if calculatedChecksum != storedChecksum {
		return nil, fmt.Errorf(
			"%w: expected 0x%x, got 0x%x",
			ErrChecksumMismatch,
			storedChecksum,
			calculatedChecksum,
		)
	}

	return data, nil
}
