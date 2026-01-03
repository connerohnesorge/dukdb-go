package compression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

// FSSTCodec implements Fast Static Symbol Table compression for string/blob columns.
// FSST replaces frequently occurring byte sequences with single-byte codes, using a symbol table
// that is trained on sample data and stored with the compressed output.
//
// This is a simplified implementation suitable for pure Go:
// - Uses single-byte symbols (codes 0-255)
// - Symbol table maps byte sequences (1-8 bytes) to codes
// - Greedy longest-match encoding
//
// Format:
//   [num_symbols:uint8][symbol_table][compressed_data]
//
// Symbol table format (for each symbol):
//   [code:uint8][length:uint8][bytes:length]
//
// The compressed data consists of symbol codes that reference the table.
type FSSTCodec struct {
	symbolTable map[string]byte // symbol string -> code
	codeTable   []string        // code -> symbol string
	trained     bool            // whether the codec has been trained
}

// NewFSSTCodec creates a new FSST codec.
// The codec must be trained with sample data before use.
func NewFSSTCodec() *FSSTCodec {
	return &FSSTCodec{
		symbolTable: make(map[string]byte),
		codeTable:   make([]string, 0, 256),
		trained:     false,
	}
}

// symbolCandidate represents a candidate for the symbol table during training.
type symbolCandidate struct {
	symbol string
	count  int
	saving int // bytes saved = count * (len(symbol) - 1)
}

// Train builds the symbol table from sample data.
// This analyzes the input data to find frequently occurring byte sequences
// and creates a mapping to single-byte codes.
func (c *FSSTCodec) Train(samples [][]byte) error {
	if len(samples) == 0 {
		return fmt.Errorf("cannot train on empty sample set")
	}

	// Combine all samples for analysis
	var allData []byte
	for _, sample := range samples {
		allData = append(allData, sample...)
	}

	if len(allData) == 0 {
		return fmt.Errorf("cannot train on empty data")
	}

	// Count byte sequence frequencies (1-8 byte sequences)
	frequencies := make(map[string]int)

	maxSymbolLen := 8
	if len(allData) < maxSymbolLen {
		maxSymbolLen = len(allData)
	}

	// Count all possible substrings
	for length := 1; length <= maxSymbolLen; length++ {
		for i := 0; i <= len(allData)-length; i++ {
			symbol := string(allData[i : i+length])
			frequencies[symbol]++
		}
	}

	// Build candidates and calculate savings
	candidates := make([]symbolCandidate, 0, len(frequencies))
	for symbol, count := range frequencies {
		// Only consider sequences that appear more than once
		if count > 1 && len(symbol) > 1 {
			saving := count * (len(symbol) - 1) // bytes saved vs single-byte encoding
			candidates = append(candidates, symbolCandidate{
				symbol: symbol,
				count:  count,
				saving: saving,
			})
		}
	}

	// Sort by saving (descending), then by count, then by length
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].saving != candidates[j].saving {
			return candidates[i].saving > candidates[j].saving
		}
		if candidates[i].count != candidates[j].count {
			return candidates[i].count > candidates[j].count
		}
		return len(candidates[i].symbol) > len(candidates[j].symbol)
	})

	// Select top candidates for the symbol table
	// Reserve code 0 for escape sequences, use codes 1-255 for symbols
	// We limit to 128 symbols to leave room for single-byte literals
	maxSymbols := 128
	if len(candidates) > maxSymbols {
		candidates = candidates[:maxSymbols]
	}

	// Build the symbol table
	c.symbolTable = make(map[string]byte)
	c.codeTable = make([]string, len(candidates))

	for i, candidate := range candidates {
		code := byte(i + 1) // codes 1-128
		c.symbolTable[candidate.symbol] = code
		c.codeTable[i] = candidate.symbol
	}

	c.trained = true
	return nil
}

// Compress compresses the input data using the trained symbol table.
func (c *FSSTCodec) Compress(data []byte) ([]byte, error) {
	if !c.trained {
		// If not trained, train on the data itself
		if err := c.Train([][]byte{data}); err != nil {
			return nil, fmt.Errorf("auto-training failed: %w", err)
		}
	}

	buf := new(bytes.Buffer)

	// Write symbol table header
	numSymbols := len(c.codeTable)
	if numSymbols > 255 {
		return nil, fmt.Errorf("symbol table too large: %d symbols", numSymbols)
	}

	if err := buf.WriteByte(byte(numSymbols)); err != nil {
		return nil, fmt.Errorf("failed to write symbol count: %w", err)
	}

	// Write symbol table
	for i, symbol := range c.codeTable {
		code := byte(i + 1)
		length := len(symbol)
		if length > 255 {
			return nil, fmt.Errorf("symbol too long: %d bytes", length)
		}

		// Write: [code:uint8][length:uint8][bytes]
		if err := buf.WriteByte(code); err != nil {
			return nil, fmt.Errorf("failed to write symbol code: %w", err)
		}
		if err := buf.WriteByte(byte(length)); err != nil {
			return nil, fmt.Errorf("failed to write symbol length: %w", err)
		}
		if _, err := buf.WriteString(symbol); err != nil {
			return nil, fmt.Errorf("failed to write symbol data: %w", err)
		}
	}

	// Compress data using greedy longest-match encoding
	pos := 0
	for pos < len(data) {
		// Try to find the longest matching symbol
		bestMatchLen := 0

		// Check all possible lengths from longest to shortest
		maxLen := 8
		if pos+maxLen > len(data) {
			maxLen = len(data) - pos
		}

		for length := maxLen; length >= 1; length-- {
			candidate := string(data[pos : pos+length])
			if code, exists := c.symbolTable[candidate]; exists {
				bestMatchLen = length
				// Write the symbol code
				if err := buf.WriteByte(code); err != nil {
					return nil, fmt.Errorf("failed to write symbol code: %w", err)
				}
				break
			}
		}

		if bestMatchLen > 0 {
			// Found a match, advance position
			pos += bestMatchLen
		} else {
			// No match found, write literal byte with escape code (0)
			if err := buf.WriteByte(0); err != nil {
				return nil, fmt.Errorf("failed to write escape code: %w", err)
			}
			if err := buf.WriteByte(data[pos]); err != nil {
				return nil, fmt.Errorf("failed to write literal byte: %w", err)
			}
			pos++
		}
	}

	return buf.Bytes(), nil
}

// Decompress decompresses FSST-encoded data.
func (c *FSSTCodec) Decompress(data []byte, destSize int) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("compressed data is empty")
	}

	r := bytes.NewReader(data)
	result := new(bytes.Buffer)

	// Read symbol table header
	numSymbols, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read symbol count: %w", err)
	}

	// Read symbol table
	decodeTable := make(map[byte]string)
	for i := 0; i < int(numSymbols); i++ {
		code, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read symbol code %d: %w", i, err)
		}

		length, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read symbol length %d: %w", i, err)
		}

		symbolBytes := make([]byte, length)
		if _, err := r.Read(symbolBytes); err != nil {
			return nil, fmt.Errorf("failed to read symbol data %d: %w", i, err)
		}

		decodeTable[code] = string(symbolBytes)
	}

	// Decompress data
	for {
		code, err := r.ReadByte()
		if err != nil {
			break // End of data
		}

		if code == 0 {
			// Escape code: next byte is a literal
			literal, err := r.ReadByte()
			if err != nil {
				return nil, fmt.Errorf("failed to read literal after escape: %w", err)
			}
			if err := result.WriteByte(literal); err != nil {
				return nil, fmt.Errorf("failed to write literal: %w", err)
			}
		} else {
			// Symbol code: look up in table
			symbol, exists := decodeTable[code]
			if !exists {
				return nil, fmt.Errorf("invalid symbol code: %d", code)
			}
			if _, err := result.WriteString(symbol); err != nil {
				return nil, fmt.Errorf("failed to write symbol: %w", err)
			}
		}
	}

	decompressed := result.Bytes()

	// Handle empty data case - ensure we return []byte{} not nil
	if destSize == 0 && decompressed == nil {
		return []byte{}, nil
	}

	// Validate decompressed size
	if destSize > 0 && len(decompressed) != destSize {
		return nil, fmt.Errorf("decompressed size mismatch: got %d, expected %d", len(decompressed), destSize)
	}

	return decompressed, nil
}

// Type returns CompressionFSST.
func (c *FSSTCodec) Type() CompressionType {
	return CompressionFSST
}

// CompressWithTraining trains on samples and compresses data.
// This is a convenience method for one-shot compression.
func (c *FSSTCodec) CompressWithTraining(data []byte, samples [][]byte) ([]byte, error) {
	if err := c.Train(samples); err != nil {
		return nil, fmt.Errorf("training failed: %w", err)
	}
	return c.Compress(data)
}

// GetSymbolTableSize returns the size of the symbol table in bytes.
func (c *FSSTCodec) GetSymbolTableSize() int {
	size := 1 // num_symbols byte
	for _, symbol := range c.codeTable {
		size += 1 // code byte
		size += 1 // length byte
		size += len(symbol)
	}
	return size
}

// GetCompressionRatio estimates the compression ratio for the given data.
// Returns the ratio as compressed_size / original_size.
func (c *FSSTCodec) GetCompressionRatio(data []byte) (float64, error) {
	compressed, err := c.Compress(data)
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}
	return float64(len(compressed)) / float64(len(data)), nil
}

// MarshalSymbolTable serializes the symbol table for storage.
func (c *FSSTCodec) MarshalSymbolTable() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write number of symbols
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(c.codeTable))); err != nil {
		return nil, err
	}

	// Write each symbol
	for i, symbol := range c.codeTable {
		code := byte(i + 1)
		if err := buf.WriteByte(code); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(symbol))); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(symbol); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// UnmarshalSymbolTable deserializes a symbol table.
func (c *FSSTCodec) UnmarshalSymbolTable(data []byte) error {
	r := bytes.NewReader(data)

	var numSymbols uint16
	if err := binary.Read(r, binary.LittleEndian, &numSymbols); err != nil {
		return err
	}

	c.codeTable = make([]string, numSymbols)
	c.symbolTable = make(map[string]byte)

	for i := 0; i < int(numSymbols); i++ {
		code, err := r.ReadByte()
		if err != nil {
			return err
		}

		var length uint16
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return err
		}

		symbolBytes := make([]byte, length)
		if _, err := r.Read(symbolBytes); err != nil {
			return err
		}

		symbol := string(symbolBytes)
		c.codeTable[i] = symbol
		c.symbolTable[symbol] = code
	}

	c.trained = true
	return nil
}
