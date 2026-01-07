// Package xlsx provides Excel XLSX file reading and writing capabilities for dukdb-go.
// This file contains utility functions for A1 notation parsing and cell address handling.
package xlsx

import (
	"fmt"
	"strings"
	"unicode"
)

// ParseRange parses a cell range in A1 notation (e.g., "A1:D100") and returns
// the start and end column (0-based) and row (1-based) values.
// Returns an error if the range format is invalid.
func ParseRange(rangeStr string) (startCol, startRow, endCol, endRow int, err error) {
	if rangeStr == "" {
		return 0, 0, 0, 0, fmt.Errorf("empty range string")
	}

	// Split on colon to get start and end cells
	parts := strings.Split(rangeStr, ":")
	if len(parts) != 2 {
		return 0, 0, 0, 0, fmt.Errorf("invalid range format: expected 'A1:B2', got %q", rangeStr)
	}

	startCol, startRow, err = ParseCell(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("invalid start cell in range: %w", err)
	}

	endCol, endRow, err = ParseCell(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("invalid end cell in range: %w", err)
	}

	// Validate that end is after or equal to start
	if endCol < startCol {
		return 0, 0, 0, 0, fmt.Errorf("end column (%s) is before start column (%s)",
			IndexToColumnLetters(endCol), IndexToColumnLetters(startCol))
	}
	if endRow < startRow {
		return 0, 0, 0, 0, fmt.Errorf("end row (%d) is before start row (%d)", endRow, startRow)
	}

	return startCol, startRow, endCol, endRow, nil
}

// ParseCell parses a cell reference in A1 notation (e.g., "A1", "AA100") and returns
// the 0-based column index and 1-based row number.
// Returns an error if the cell format is invalid.
func ParseCell(cell string) (col int, row int, err error) {
	if cell == "" {
		return 0, 0, fmt.Errorf("empty cell reference")
	}

	cell = strings.TrimSpace(strings.ToUpper(cell))

	// Find where letters end and digits begin
	letterEnd := 0
	for i, r := range cell {
		if unicode.IsLetter(r) {
			letterEnd = i + 1
		} else {
			break
		}
	}

	if letterEnd == 0 {
		return 0, 0, fmt.Errorf("invalid cell reference %q: no column letters", cell)
	}

	if letterEnd >= len(cell) {
		return 0, 0, fmt.Errorf("invalid cell reference %q: no row number", cell)
	}

	letters := cell[:letterEnd]
	rowStr := cell[letterEnd:]

	col = ColumnLettersToIndex(letters)
	if col < 0 {
		return 0, 0, fmt.Errorf("invalid column letters %q", letters)
	}

	// Parse row number
	row = 0
	for _, r := range rowStr {
		if r < '0' || r > '9' {
			return 0, 0, fmt.Errorf("invalid row number in cell reference %q: non-digit character %q", cell, r)
		}
		row = row*10 + int(r-'0')
	}

	if row <= 0 {
		return 0, 0, fmt.Errorf("invalid row number in cell reference %q: row must be >= 1", cell)
	}

	return col, row, nil
}

// ColumnLettersToIndex converts Excel column letters to a 0-based index.
// For example: "A" -> 0, "Z" -> 25, "AA" -> 26, "AZ" -> 51, "BA" -> 52.
// Returns -1 if the input is invalid.
func ColumnLettersToIndex(letters string) int {
	if letters == "" {
		return -1
	}

	letters = strings.ToUpper(letters)
	index := 0

	for _, r := range letters {
		if r < 'A' || r > 'Z' {
			return -1
		}
		index = index*26 + int(r-'A') + 1
	}

	return index - 1
}

// IndexToColumnLetters converts a 0-based column index to Excel column letters.
// For example: 0 -> "A", 25 -> "Z", 26 -> "AA", 51 -> "AZ", 52 -> "BA".
// Returns an empty string if the index is negative.
func IndexToColumnLetters(index int) string {
	if index < 0 {
		return ""
	}

	var result strings.Builder

	// Convert to 1-based for easier calculation
	n := index + 1

	for n > 0 {
		// Adjust for 1-based calculation
		n-- // Make it 0-based for this iteration
		remainder := n % 26
		result.WriteByte(byte('A' + remainder))
		n = n / 26
	}

	// Reverse the string since we built it backwards
	bytes := []byte(result.String())
	for i, j := 0, len(bytes)-1; i < j; i, j = i+1, j-1 {
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}

	return string(bytes)
}

// CellAddress generates a cell reference in A1 notation from a 0-based column index
// and 1-based row number.
// For example: (0, 1) -> "A1", (26, 100) -> "AA100".
func CellAddress(col, row int) string {
	colLetters := IndexToColumnLetters(col)
	if colLetters == "" {
		return ""
	}

	if row <= 0 {
		return ""
	}

	return fmt.Sprintf("%s%d", colLetters, row)
}
