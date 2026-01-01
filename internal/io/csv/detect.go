// Package csv provides CSV file reading and writing capabilities for dukdb-go.
// This file contains utilities for auto-detecting CSV format properties
// such as delimiter characters and header rows.
package csv

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// headerNumericThreshold is the ratio of numeric values in data rows
// required to consider a non-numeric first row as a header.
// If more than 50% of data values in a column are numeric but the
// first row value is not numeric, that column likely has a header.
const headerNumericThreshold = 0.5

// maxPeekSize is the maximum number of bytes to peek for delimiter detection.
// 64KB provides enough sample data for reliable detection.
const maxPeekSize = 64 * 1024

// minLinesForConsistency is the minimum number of lines needed
// to check delimiter consistency across multiple rows.
const minLinesForConsistency = 2

// commonDelimiters lists the delimiters to try during auto-detection.
// These are tried in order: comma, tab, semicolon, and pipe.
var commonDelimiters = []rune{',', '\t', ';', '|'}

// detectDelimiter tries to auto-detect the delimiter from sample lines.
// It peeks at the beginning of the file and analyzes which common delimiter
// appears most consistently across lines. Consistency is preferred over
// raw count because a delimiter should appear the same number of times
// on each line (representing column separators).
func detectDelimiter(r *bufio.Reader, sampleSize int) (rune, error) {
	data, err := r.Peek(maxPeekSize)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, bufio.ErrBufferFull) {
		return DefaultDelimiter, nil
	}

	if len(data) == 0 {
		return DefaultDelimiter, nil
	}

	lines := strings.Split(string(data), "\n")
	if len(lines) > sampleSize {
		lines = lines[:sampleSize]
	}

	scores, consistency := analyzeDelimiters(lines)

	return pickBestDelimiter(scores, consistency)
}

// analyzeDelimiters counts delimiter occurrences and tracks consistency across lines.
// Returns two maps: total scores (sum of occurrences) and consistency data
// (per-line counts for each delimiter).
func analyzeDelimiters(lines []string) (map[rune]int, map[rune][]int) {
	scores := make(map[rune]int)
	consistency := make(map[rune][]int)

	for _, delim := range commonDelimiters {
		consistency[delim] = make([]int, 0, len(lines))
	}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		for _, delim := range commonDelimiters {
			count := strings.Count(trimmed, string(delim))
			consistency[delim] = append(consistency[delim], count)

			if count > 0 {
				scores[delim] += count
			}
		}
	}

	return scores, consistency
}

// pickBestDelimiter selects the best delimiter based on scores and consistency.
// Priority is given to delimiters that appear consistently (same count on each line)
// since that indicates proper column separation. Falls back to highest raw count
// if no consistent delimiter is found.
func pickBestDelimiter(scores map[rune]int, consistency map[rune][]int) (rune, error) {
	bestDelimiter := DefaultDelimiter
	bestScore := 0

	// First, look for a consistent delimiter (preferred)
	for _, delim := range commonDelimiters {
		counts := consistency[delim]
		if len(counts) < minLinesForConsistency {
			continue
		}

		firstCount := counts[0]
		if firstCount == 0 {
			continue
		}

		// Check score first (cheaper) before consistency check
		if scores[delim] > bestScore && isConsistent(counts) {
			bestScore = scores[delim]
			bestDelimiter = delim
		}
	}

	// If no consistent delimiter, pick highest count
	if bestScore == 0 {
		for _, delim := range commonDelimiters {
			if scores[delim] > bestScore {
				bestScore = scores[delim]
				bestDelimiter = delim
			}
		}
	}

	return bestDelimiter, nil
}

// isConsistent checks if all values in the slice are equal.
// For delimiter detection, this means the delimiter appears the same
// number of times on each line, which is expected for proper CSV format.
func isConsistent(counts []int) bool {
	if len(counts) == 0 {
		return true
	}

	first := counts[0]
	for _, c := range counts[1:] {
		if c != first {
			return false
		}
	}

	return true
}

// DetectHeader attempts to detect if the first row is a header by comparing types.
// This is a heuristic: if the first row contains different "types" than subsequent rows,
// it is likely a header.
func DetectHeader(lines [][]string) bool {
	if len(lines) < 2 {
		return true
	}

	firstRow := lines[0]
	dataRows := lines[1:]
	headerScore := 0

	for colIdx := range firstRow {
		firstVal := firstRow[colIdx]
		isFirstNumeric := looksNumeric(firstVal)

		dataNumericCount := countNumericValues(dataRows, colIdx)
		dataNumericRatio := float64(dataNumericCount) / float64(len(dataRows))

		if !isFirstNumeric && dataNumericRatio > headerNumericThreshold {
			headerScore++
		}
	}

	return headerScore > len(firstRow)/2
}

// countNumericValues counts how many rows have a numeric value in the given column.
// This is used for header detection - if data rows are mostly numeric but
// the first row is not, the first row is likely a header.
func countNumericValues(rows [][]string, colIdx int) int {
	count := 0

	for _, row := range rows {
		if colIdx < len(row) && looksNumeric(row[colIdx]) {
			count++
		}
	}

	return count
}

// looksNumeric returns true if the string appears to be a numeric value.
// It checks for integer and decimal number patterns, allowing for
// thousands separators (comma or space) and optional sign prefix.
func looksNumeric(str string) bool {
	s := strings.TrimSpace(str)
	if s == "" {
		return false
	}

	hasDigit := false
	hasDecimal := false
	hasSign := false

	for i, r := range s {
		switch {
		case r >= '0' && r <= '9':
			hasDigit = true
		case r == '.':
			if hasDecimal {
				return false
			}
			hasDecimal = true
		case r == '-' || r == '+':
			if i != 0 || hasSign {
				return false
			}
			hasSign = true
		case r == ',' || r == ' ':
			// Thousands separator - ignore
		default:
			return false
		}
	}

	return hasDigit
}
