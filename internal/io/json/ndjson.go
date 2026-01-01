// Package json provides JSON and NDJSON file reading and writing capabilities for dukdb-go.
// This file contains NDJSON-specific and format detection reading functionality.
package json

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// initNDJSONFormat initializes reading from NDJSON format.
func (r *Reader) initNDJSONFormat() {
	r.scanner = bufio.NewScanner(r.bufReader)
	// Set a reasonable max line size (default is 64KB, increase to MaxObjectSize).
	maxLineSize := r.opts.MaxObjectSize
	if maxLineSize < bufio.MaxScanTokenSize {
		maxLineSize = bufio.MaxScanTokenSize
	}

	r.scanner.Buffer(make([]byte, 0, maxLineSize), maxLineSize)
}

// readNextNDJSONObject reads the next JSON object from NDJSON format.
func (r *Reader) readNextNDJSONObject() (map[string]any, error) {
	for r.scanner.Scan() {
		line := r.scanner.Bytes()

		// Skip empty lines and whitespace-only lines.
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}

		// Parse the line as a JSON object.
		var obj map[string]any

		decoder := json.NewDecoder(bytes.NewReader(trimmed))
		decoder.UseNumber()

		if err := decoder.Decode(&obj); err != nil {
			if r.opts.IgnoreErrors {
				continue
			}

			return nil, fmt.Errorf("json: failed to decode NDJSON line: %w", err)
		}

		return obj, nil
	}

	// Check for scanner error.
	if err := r.scanner.Err(); err != nil {
		return nil, fmt.Errorf("json: scanner error: %w", err)
	}

	// End of file.
	return nil, nil
}

// detectFormat auto-detects the JSON format by peeking at the first non-whitespace character.
// Returns FormatArray if the file starts with '[', FormatNDJSON if it starts with '{'.
func (r *Reader) detectFormat() (string, error) {
	// Peek at bytes to find the first non-whitespace character.
	for {
		b, err := r.bufReader.Peek(1)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Empty file - default to array format.
				return FormatArray, nil
			}

			return "", fmt.Errorf("json: failed to detect format: %w", err)
		}

		c := b[0]

		// Skip whitespace.
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			// Consume the whitespace character.
			_, _ = r.bufReader.ReadByte()

			continue
		}

		// Check the first non-whitespace character.
		if c == '[' {
			return FormatArray, nil
		}

		if c == '{' {
			return FormatNDJSON, nil
		}

		// Unknown format - default to array and let it fail with proper error.
		return FormatArray, nil
	}
}

// initArrayFormat initializes reading from a JSON array.
func (r *Reader) initArrayFormat() error {
	// Create decoder for array format.
	r.decoder = json.NewDecoder(r.bufReader)
	r.decoder.UseNumber()

	// Read the opening bracket.
	token, err := r.decoder.Token()
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Empty file.
			r.eof = true
			r.inArray = false

			return nil
		}

		return fmt.Errorf("json: failed to read opening token: %w", err)
	}

	// Expect an array.
	delim, ok := token.(json.Delim)
	if !ok || delim != '[' {
		return fmt.Errorf("json: expected '[' but got %v", token)
	}

	r.inArray = true

	return nil
}
