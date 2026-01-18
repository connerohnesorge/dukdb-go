package duckdb

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
)

// DifferenceLocation represents a single difference between two files.
type DifferenceLocation struct {
	// Offset is the byte offset where the difference occurs.
	Offset int64

	// Length is the number of consecutive different bytes.
	Length int64

	// Section is the human-readable name of the file section.
	Section string

	// Byte1 is the byte value from file 1.
	Byte1 byte

	// Byte2 is the byte value from file 2.
	Byte2 byte
}

// String returns a human-readable representation of a difference.
func (d DifferenceLocation) String() string {
	return fmt.Sprintf("Offset %d (0x%x) in %s: file1=%02x file2=%02x",
		d.Offset, d.Offset, d.Section, d.Byte1, d.Byte2)
}

// ComparisonReport contains the results of comparing two DuckDB files.
type ComparisonReport struct {
	// File1Path is the path to the first file.
	File1Path string

	// File2Path is the path to the second file.
	File2Path string

	// Differences is a list of all byte-level differences.
	Differences []DifferenceLocation

	// FirstDifference is the offset of the first difference, or -1 if files are identical.
	FirstDifference int64

	// TotalDifferences is the total number of different bytes.
	TotalDifferences int64

	// FilesIdentical is true if the files are byte-for-byte identical.
	FilesIdentical bool

	// File1Size is the size of file 1 in bytes.
	File1Size int64

	// File2Size is the size of file 2 in bytes.
	File2Size int64
}

// HeaderComparison contains detailed comparison of file and database headers.
type HeaderComparison struct {
	// FileHeaderMatch indicates if the file headers match.
	FileHeaderMatch bool

	// FileHeaderDiffs contains differences in the file header section.
	FileHeaderDiffs []DifferenceLocation

	// DBHeader1Match indicates if database header 1 matches.
	DBHeader1Match bool

	// DBHeader1Diffs contains differences in database header 1.
	DBHeader1Diffs []DifferenceLocation

	// DBHeader2Match indicates if database header 2 matches.
	DBHeader2Match bool

	// DBHeader2Diffs contains differences in database header 2.
	DBHeader2Diffs []DifferenceLocation
}

// String returns a formatted summary of the comparison report.
func (r *ComparisonReport) String() string {
	var buf strings.Builder

	fmt.Fprintf(&buf, "Comparison Report\n")
	fmt.Fprintf(&buf, "=================\n")
	fmt.Fprintf(&buf, "File 1: %s (%d bytes)\n", r.File1Path, r.File1Size)
	fmt.Fprintf(&buf, "File 2: %s (%d bytes)\n", r.File2Path, r.File2Size)
	fmt.Fprintf(&buf, "\n")

	if r.FilesIdentical {
		fmt.Fprintf(&buf, "Files are identical.\n")
		return buf.String()
	}

	fmt.Fprintf(&buf, "Total differences: %d bytes\n", r.TotalDifferences)
	fmt.Fprintf(
		&buf,
		"First difference at offset: %d (0x%x)\n\n",
		r.FirstDifference,
		r.FirstDifference,
	)

	// Group differences by section
	sectionDiffs := make(map[string][]DifferenceLocation)
	for _, diff := range r.Differences {
		sectionDiffs[diff.Section] = append(sectionDiffs[diff.Section], diff)
	}

	for section, diffs := range sectionDiffs {
		fmt.Fprintf(&buf, "%s: %d differences\n", section, len(diffs))
		// Show first few differences in each section
		for i, diff := range diffs {
			if i >= 5 {
				fmt.Fprintf(&buf, "  ... and %d more\n", len(diffs)-5)
				break
			}
			fmt.Fprintf(&buf, "  %s\n", diff.String())
		}
		fmt.Fprintf(&buf, "\n")
	}

	return buf.String()
}

// IdentifySection returns the section name for a given byte offset.
func IdentifySection(offset int64) string {
	switch {
	case offset < FileHeaderSize:
		return "FileHeader"
	case offset >= DatabaseHeader1Offset && offset < DatabaseHeader1Offset+DatabaseHeaderSize:
		return "DatabaseHeader1"
	case offset >= DatabaseHeader2Offset && offset < DatabaseHeader2Offset+DatabaseHeaderSize:
		return "DatabaseHeader2"
	case offset >= DataBlocksOffset:
		// Calculate which block this is in
		blockNum := (offset - DataBlocksOffset) / int64(DefaultBlockSize)
		offsetInBlock := (offset - DataBlocksOffset) % int64(DefaultBlockSize)
		return fmt.Sprintf("DataBlock%d[0x%x]", blockNum, offsetInBlock)
	default:
		return "Unknown"
	}
}

// CompareDBFiles compares two DuckDB files byte-by-byte.
// Returns a detailed comparison report.
func CompareDBFiles(path1, path2 string) (*ComparisonReport, error) {
	// Open both files
	f1, err := os.Open(path1)
	if err != nil {
		return nil, fmt.Errorf("failed to open file1 %s: %w", path1, err)
	}
	defer func() { _ = f1.Close() }()

	f2, err := os.Open(path2)
	if err != nil {
		return nil, fmt.Errorf("failed to open file2 %s: %w", path2, err)
	}
	defer func() { _ = f2.Close() }()

	// Get file sizes
	stat1, err := f1.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file1: %w", err)
	}

	stat2, err := f2.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file2: %w", err)
	}

	report := &ComparisonReport{
		File1Path:       path1,
		File2Path:       path2,
		File1Size:       stat1.Size(),
		File2Size:       stat2.Size(),
		FirstDifference: -1,
		FilesIdentical:  true,
	}

	// Compare files byte-by-byte
	const chunkSize = 4096
	buf1 := make([]byte, chunkSize)
	buf2 := make([]byte, chunkSize)
	offset := int64(0)

	for {
		n1, err1 := f1.Read(buf1)
		n2, err2 := f2.Read(buf2)

		// Process the bytes we read
		minN := n1
		if n2 < minN {
			minN = n2
		}

		for i := 0; i < minN; i++ {
			if buf1[i] != buf2[i] {
				diff := DifferenceLocation{
					Offset:  offset + int64(i),
					Length:  1,
					Section: IdentifySection(offset + int64(i)),
					Byte1:   buf1[i],
					Byte2:   buf2[i],
				}

				if report.FirstDifference == -1 {
					report.FirstDifference = diff.Offset
				}

				report.Differences = append(report.Differences, diff)
				report.TotalDifferences++
				report.FilesIdentical = false
			}
		}

		// Handle size differences
		if n1 > n2 {
			// File1 is longer
			for i := n2; i < n1; i++ {
				diff := DifferenceLocation{
					Offset:  offset + int64(i),
					Length:  1,
					Section: IdentifySection(offset + int64(i)),
					Byte1:   buf1[i],
					Byte2:   0,
				}
				report.Differences = append(report.Differences, diff)
				report.TotalDifferences++
				report.FilesIdentical = false
			}
		} else if n2 > n1 {
			// File2 is longer
			for i := n1; i < n2; i++ {
				diff := DifferenceLocation{
					Offset:  offset + int64(i),
					Length:  1,
					Section: IdentifySection(offset + int64(i)),
					Byte1:   0,
					Byte2:   buf2[i],
				}
				report.Differences = append(report.Differences, diff)
				report.TotalDifferences++
				report.FilesIdentical = false
			}
		}

		offset += int64(minN)

		// Check for EOF
		if (err1 != nil && err1 != io.EOF) || (err2 != nil && err2 != io.EOF) {
			if err1 != nil && err1 != io.EOF {
				return report, fmt.Errorf("error reading file1: %w", err1)
			}
			return report, fmt.Errorf("error reading file2: %w", err2)
		}

		if err1 == io.EOF && err2 == io.EOF {
			break
		}
	}

	return report, nil
}

// HexDumpRange reads a range of bytes from a file and returns a hex dump string.
// The format matches the standard hex dump output with offset, hex bytes, and ASCII.
func HexDumpRange(file string, start, length int64) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %w", file, err)
	}
	defer func() { _ = f.Close() }()

	// Read the data
	data := make([]byte, length)
	n, err := f.ReadAt(data, start)
	if err != nil && err != io.EOF {
		return "", fmt.Errorf("failed to read from offset %d: %w", start, err)
	}
	data = data[:n]

	// Generate hex dump
	var buf strings.Builder
	dumper := hex.Dumper(&buf)
	_, err = dumper.Write(data)
	if err != nil {
		return "", fmt.Errorf("failed to generate hex dump: %w", err)
	}
	_ = dumper.Close()

	return buf.String(), nil
}

// HexDumpSideBySide creates a side-by-side hex comparison of two byte slices.
// This is useful for comparing specific sections where differences occur.
func HexDumpSideBySide(data1, data2 []byte, offset int64) string {
	var buf strings.Builder

	maxLen := len(data1)
	if len(data2) > maxLen {
		maxLen = len(data2)
	}

	// Print header
	fmt.Fprintf(&buf, "Offset    File1                                            File2\n")
	fmt.Fprintf(
		&buf,
		"--------  -----------------------------------------------  -----------------------------------------------\n",
	)

	for i := 0; i < maxLen; i += 16 {
		// Print offset
		fmt.Fprintf(&buf, "%08x  ", offset+int64(i))

		// Print file1 hex bytes
		for j := 0; j < 16; j++ {
			idx := i + j
			if idx < len(data1) {
				fmt.Fprintf(&buf, "%02x ", data1[idx])
			} else {
				fmt.Fprintf(&buf, "   ")
			}
			if j == 7 {
				fmt.Fprintf(&buf, " ")
			}
		}

		fmt.Fprintf(&buf, " ")

		// Print file2 hex bytes
		for j := 0; j < 16; j++ {
			idx := i + j
			if idx < len(data2) {
				fmt.Fprintf(&buf, "%02x ", data2[idx])
			} else {
				fmt.Fprintf(&buf, "   ")
			}
			if j == 7 {
				fmt.Fprintf(&buf, " ")
			}
		}

		fmt.Fprintf(&buf, "\n")
	}

	return buf.String()
}

// CompareHeaders specifically compares the file and database headers of two files.
// This provides a focused comparison of the header sections.
func CompareHeaders(path1, path2 string) (*HeaderComparison, error) {
	f1, err := os.Open(path1)
	if err != nil {
		return nil, fmt.Errorf("failed to open file1 %s: %w", path1, err)
	}
	defer func() { _ = f1.Close() }()

	f2, err := os.Open(path2)
	if err != nil {
		return nil, fmt.Errorf("failed to open file2 %s: %w", path2, err)
	}
	defer func() { _ = f2.Close() }()

	comp := &HeaderComparison{
		FileHeaderMatch: true,
		DBHeader1Match:  true,
		DBHeader2Match:  true,
	}

	// Compare file header
	fileHeader1 := make([]byte, FileHeaderSize)
	fileHeader2 := make([]byte, FileHeaderSize)

	if _, err := f1.ReadAt(fileHeader1, 0); err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read file1 header: %w", err)
	}
	if _, err := f2.ReadAt(fileHeader2, 0); err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read file2 header: %w", err)
	}

	if !bytes.Equal(fileHeader1, fileHeader2) {
		comp.FileHeaderMatch = false
		for i := 0; i < FileHeaderSize; i++ {
			if fileHeader1[i] != fileHeader2[i] {
				comp.FileHeaderDiffs = append(comp.FileHeaderDiffs, DifferenceLocation{
					Offset:  int64(i),
					Length:  1,
					Section: "FileHeader",
					Byte1:   fileHeader1[i],
					Byte2:   fileHeader2[i],
				})
			}
		}
	}

	// Compare database header 1
	dbHeader1_1 := make([]byte, DatabaseHeaderSize)
	dbHeader1_2 := make([]byte, DatabaseHeaderSize)

	if _, err := f1.ReadAt(dbHeader1_1, DatabaseHeader1Offset); err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read file1 db header 1: %w", err)
	}
	if _, err := f2.ReadAt(dbHeader1_2, DatabaseHeader1Offset); err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read file2 db header 1: %w", err)
	}

	if !bytes.Equal(dbHeader1_1, dbHeader1_2) {
		comp.DBHeader1Match = false
		for i := 0; i < DatabaseHeaderSize; i++ {
			if dbHeader1_1[i] != dbHeader1_2[i] {
				comp.DBHeader1Diffs = append(comp.DBHeader1Diffs, DifferenceLocation{
					Offset:  DatabaseHeader1Offset + int64(i),
					Length:  1,
					Section: "DatabaseHeader1",
					Byte1:   dbHeader1_1[i],
					Byte2:   dbHeader1_2[i],
				})
			}
		}
	}

	// Compare database header 2
	dbHeader2_1 := make([]byte, DatabaseHeaderSize)
	dbHeader2_2 := make([]byte, DatabaseHeaderSize)

	if _, err := f1.ReadAt(dbHeader2_1, DatabaseHeader2Offset); err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read file1 db header 2: %w", err)
	}
	if _, err := f2.ReadAt(dbHeader2_2, DatabaseHeader2Offset); err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read file2 db header 2: %w", err)
	}

	if !bytes.Equal(dbHeader2_1, dbHeader2_2) {
		comp.DBHeader2Match = false
		for i := 0; i < DatabaseHeaderSize; i++ {
			if dbHeader2_1[i] != dbHeader2_2[i] {
				comp.DBHeader2Diffs = append(comp.DBHeader2Diffs, DifferenceLocation{
					Offset:  DatabaseHeader2Offset + int64(i),
					Length:  1,
					Section: "DatabaseHeader2",
					Byte1:   dbHeader2_1[i],
					Byte2:   dbHeader2_2[i],
				})
			}
		}
	}

	return comp, nil
}

// String returns a formatted summary of the header comparison.
func (h *HeaderComparison) String() string {
	var buf strings.Builder

	fmt.Fprintf(&buf, "Header Comparison\n")
	fmt.Fprintf(&buf, "=================\n\n")

	fmt.Fprintf(&buf, "File Header: ")
	if h.FileHeaderMatch {
		fmt.Fprintf(&buf, "MATCH\n")
	} else {
		fmt.Fprintf(&buf, "DIFFERENT (%d bytes)\n", len(h.FileHeaderDiffs))
		for i, diff := range h.FileHeaderDiffs {
			if i >= 10 {
				fmt.Fprintf(&buf, "  ... and %d more\n", len(h.FileHeaderDiffs)-10)
				break
			}
			fmt.Fprintf(&buf, "  %s\n", diff.String())
		}
	}
	fmt.Fprintf(&buf, "\n")

	fmt.Fprintf(&buf, "Database Header 1: ")
	if h.DBHeader1Match {
		fmt.Fprintf(&buf, "MATCH\n")
	} else {
		fmt.Fprintf(&buf, "DIFFERENT (%d bytes)\n", len(h.DBHeader1Diffs))
		for i, diff := range h.DBHeader1Diffs {
			if i >= 10 {
				fmt.Fprintf(&buf, "  ... and %d more\n", len(h.DBHeader1Diffs)-10)
				break
			}
			fmt.Fprintf(&buf, "  %s\n", diff.String())
		}
	}
	fmt.Fprintf(&buf, "\n")

	fmt.Fprintf(&buf, "Database Header 2: ")
	if h.DBHeader2Match {
		fmt.Fprintf(&buf, "MATCH\n")
	} else {
		fmt.Fprintf(&buf, "DIFFERENT (%d bytes)\n", len(h.DBHeader2Diffs))
		for i, diff := range h.DBHeader2Diffs {
			if i >= 10 {
				fmt.Fprintf(&buf, "  ... and %d more\n", len(h.DBHeader2Diffs)-10)
				break
			}
			fmt.Fprintf(&buf, "  %s\n", diff.String())
		}
	}

	return buf.String()
}
