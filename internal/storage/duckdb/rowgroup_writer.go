// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file implements RowGroupWriter for writing row
// groups with automatic compression selection and block allocation.
package duckdb

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
)

// RowGroupWriter error definitions.
var (
	// ErrRowGroupFull indicates the row group has reached its maximum size.
	ErrRowGroupFull = errors.New("row group is full")

	// ErrColumnCountMismatch indicates the row has wrong number of columns.
	ErrColumnCountMismatch = errors.New("column count mismatch")

	// ErrRowGroupWriterClosed indicates the writer has been closed.
	ErrRowGroupWriterClosed = errors.New("row group writer is closed")

	// ErrNothingToFlush indicates there is no data to flush.
	ErrNothingToFlush = errors.New("nothing to flush")
)

// RowGroupWriter writes row groups with automatic compression selection
// and block allocation. It accumulates rows and flushes them as row groups
// when full or when explicitly requested.
type RowGroupWriter struct {
	// blockManager handles block allocation and I/O.
	blockManager *BlockManager

	// tableOID is the table object ID this writer belongs to.
	tableOID uint64

	// columnTypes contains the logical type ID for each column.
	columnTypes []LogicalTypeID

	// columnMods contains type modifiers for each column.
	columnMods []*TypeModifiers

	// rowStart is the starting row index for the next row group.
	rowStart uint64

	// columns holds buffered column data (one slice per column).
	columns [][]any

	// rowCount is the number of rows currently buffered.
	rowCount uint64

	// closed indicates whether the writer has been closed.
	closed bool

	// mu protects concurrent access to the writer.
	mu sync.Mutex
}

// NewRowGroupWriter creates a new RowGroupWriter for writing row groups.
//
// Parameters:
//   - bm: BlockManager for block allocation and I/O
//   - tableOID: Table object ID this writer belongs to
//   - types: LogicalTypeID for each column
//   - mods: TypeModifiers for each column (may contain nil entries)
//   - rowStart: Starting row index for the first row group
func NewRowGroupWriter(
	bm *BlockManager,
	tableOID uint64,
	types []LogicalTypeID,
	mods []*TypeModifiers,
	rowStart uint64,
) *RowGroupWriter {
	// Initialize column buffers
	columnCount := len(types)
	columns := make([][]any, columnCount)
	for i := range columns {
		columns[i] = make([]any, 0, DefaultRowGroupSize)
	}

	// Ensure mods slice has same length as types
	if len(mods) < columnCount {
		extendedMods := make([]*TypeModifiers, columnCount)
		copy(extendedMods, mods)
		mods = extendedMods
	}

	return &RowGroupWriter{
		blockManager: bm,
		tableOID:     tableOID,
		columnTypes:  types,
		columnMods:   mods,
		rowStart:     rowStart,
		columns:      columns,
		rowCount:     0,
		closed:       false,
	}
}

// AppendRow adds a row to the current row group.
// If the row group becomes full after adding this row, it does NOT
// automatically flush - the caller must check IsFull() and call Flush().
//
// Parameters:
//   - row: Slice of values, one per column. Use nil for NULL values.
//
// Returns ErrRowGroupFull if the row group is already full,
// ErrColumnCountMismatch if the row has wrong number of columns.
func (w *RowGroupWriter) AppendRow(row []any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrRowGroupWriterClosed
	}

	if w.rowCount >= DefaultRowGroupSize {
		return ErrRowGroupFull
	}

	if len(row) != len(w.columnTypes) {
		return fmt.Errorf("%w: expected %d columns, got %d",
			ErrColumnCountMismatch, len(w.columnTypes), len(row))
	}

	// Append each value to its column
	for i, val := range row {
		w.columns[i] = append(w.columns[i], val)
	}
	w.rowCount++

	return nil
}

// AppendRows adds multiple rows to the current row group.
// Stops and returns error if row group becomes full or a row is invalid.
//
// Parameters:
//   - rows: Slice of rows, each row is a slice of values
//
// Returns the number of rows successfully added and any error.
func (w *RowGroupWriter) AppendRows(rows [][]any) (int, error) {
	for i, row := range rows {
		if err := w.AppendRow(row); err != nil {
			return i, err
		}
	}
	return len(rows), nil
}

// RowCount returns the number of rows currently buffered.
func (w *RowGroupWriter) RowCount() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.rowCount
}

// IsFull returns true if the row group has reached DefaultRowGroupSize.
func (w *RowGroupWriter) IsFull() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.rowCount >= DefaultRowGroupSize
}

// Flush writes the buffered row group to disk.
// Returns a RowGroupPointer for catalog integration, or nil if nothing to flush.
//
// After flushing:
//   - rowStart is advanced by the number of rows flushed
//   - column buffers are reset for the next row group
//   - rowCount is reset to 0
func (w *RowGroupWriter) Flush() (*RowGroupPointer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, ErrRowGroupWriterClosed
	}

	if w.rowCount == 0 {
		return nil, nil // Nothing to flush
	}

	// Write each column and collect data pointers
	dataPointers := make([]MetaBlockPointer, len(w.columns))
	for i, colData := range w.columns {
		mbp, err := w.writeColumn(i, colData)
		if err != nil {
			return nil, fmt.Errorf("failed to write column %d: %w", i, err)
		}
		dataPointers[i] = mbp
	}

	// Build row group pointer
	rgp := &RowGroupPointer{
		TableOID:     w.tableOID,
		RowStart:     w.rowStart,
		TupleCount:   w.rowCount,
		DataPointers: dataPointers,
	}

	// Reset for next row group
	w.rowStart += w.rowCount
	for i := range w.columns {
		w.columns[i] = make([]any, 0, DefaultRowGroupSize)
	}
	w.rowCount = 0

	return rgp, nil
}

// writeColumn encodes, compresses, and writes a column to disk.
// Returns a MetaBlockPointer to the serialized DataPointer.
//
//nolint:revive // function-length: complex write operation requires multiple steps
func (w *RowGroupWriter) writeColumn(colIdx int, data []any) (MetaBlockPointer, error) {
	typeID := w.columnTypes[colIdx]
	mods := w.columnMods[colIdx]

	// Encode values to bytes and track validity
	encoded, validity, err := encodeColumnData(data, typeID, mods)
	if err != nil {
		return MetaBlockPointer{}, fmt.Errorf("failed to encode column data: %w", err)
	}

	// Get value size for compression
	valueSize := GetValueSize(typeID, mods)
	if valueSize <= 0 {
		// Variable-size type - use 0 to indicate variable
		valueSize = 0
	}

	// Select best compression algorithm
	compression := SelectCompression(data, typeID)

	// Compress data
	compressed, actualCompression, err := TryCompress(compression, encoded, valueSize)
	if err != nil {
		// Fallback to uncompressed
		compressed = encoded
		actualCompression = CompressionUncompressed
	}

	// Allocate block for column data
	dataBlockID, err := w.blockManager.AllocateBlock()
	if err != nil {
		return MetaBlockPointer{}, fmt.Errorf("failed to allocate data block: %w", err)
	}

	// Write compressed data to block
	dataBlock := &Block{
		ID:   dataBlockID,
		Type: BlockRowGroup,
		Data: make([]byte, w.blockManager.BlockSize()-BlockChecksumSize),
	}
	copy(dataBlock.Data, compressed)

	if err := w.blockManager.WriteBlock(dataBlock); err != nil {
		return MetaBlockPointer{}, fmt.Errorf("failed to write data block: %w", err)
	}

	// Compute statistics
	stats := computeStatistics(data, typeID)

	// Build DataPointer
	dp := &DataPointer{
		RowStart:    0,
		TupleCount:  uint64(len(data)),
		Block:       BlockPointer{BlockID: dataBlockID, Offset: 0},
		Compression: actualCompression,
		Statistics:  stats,
		SegmentState: ColumnSegmentState{
			HasValidityMask: validity != nil && !validity.AllValid(),
		},
	}

	// If there's a validity mask with NULLs, serialize and store it
	if dp.SegmentState.HasValidityMask {
		validityBytes, err := validity.SerializeToBytes()
		if err != nil {
			return MetaBlockPointer{}, fmt.Errorf("failed to serialize validity mask: %w", err)
		}
		dp.SegmentState.StateData = validityBytes
	}

	// Serialize DataPointer to metadata block
	dpBytes, err := dp.SerializeToBytes()
	if err != nil {
		return MetaBlockPointer{}, fmt.Errorf("failed to serialize data pointer: %w", err)
	}

	// Allocate block for metadata
	metaBlockID, err := w.blockManager.AllocateBlock()
	if err != nil {
		return MetaBlockPointer{}, fmt.Errorf("failed to allocate metadata block: %w", err)
	}

	// Write serialized DataPointer to metadata block
	metaBlock := &Block{
		ID:   metaBlockID,
		Type: BlockMetaData,
		Data: make([]byte, w.blockManager.BlockSize()-BlockChecksumSize),
	}
	copy(metaBlock.Data, dpBytes)

	if err := w.blockManager.WriteBlock(metaBlock); err != nil {
		return MetaBlockPointer{}, fmt.Errorf("failed to write metadata block: %w", err)
	}

	return MetaBlockPointer{BlockID: metaBlockID, BlockIndex: 0, Offset: 0}, nil
}

// Close closes the writer, flushing any remaining data.
// Returns the final RowGroupPointer if there was data to flush, nil otherwise.
func (w *RowGroupWriter) Close() (*RowGroupPointer, error) {
	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return nil, nil
	}

	// Check if there's data to flush
	hasData := w.rowCount > 0

	if !hasData {
		w.closed = true
		w.mu.Unlock()
		return nil, nil
	}

	// Unlock before calling Flush (which will lock)
	w.mu.Unlock()

	// Flush any remaining data
	rgp, err := w.Flush()
	if err != nil {
		return nil, err
	}

	// Mark as closed after successful flush
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()

	return rgp, nil
}

// TableOID returns the table object ID for this writer.
func (w *RowGroupWriter) TableOID() uint64 {
	return w.tableOID
}

// ColumnCount returns the number of columns.
func (w *RowGroupWriter) ColumnCount() int {
	return len(w.columnTypes)
}

// CurrentRowStart returns the starting row index for the current row group.
func (w *RowGroupWriter) CurrentRowStart() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.rowStart
}

// encodeColumnData converts a slice of any values to raw bytes with validity tracking.
// Returns the encoded bytes, validity mask, and any error.
func encodeColumnData(data []any, typeID LogicalTypeID, mods *TypeModifiers) ([]byte, *ValidityMask, error) {
	if len(data) == 0 {
		return []byte{}, nil, nil
	}

	validity := NewValidityMask(uint64(len(data)))
	valueSize := GetValueSize(typeID, mods)
	isVariableSize := valueSize <= 0

	var buf bytes.Buffer

	for i, val := range data {
		if val == nil {
			validity.SetInvalid(uint64(i))
			if !isVariableSize {
				// Write placeholder for fixed-size type
				buf.Write(make([]byte, valueSize))
			} else {
				// For variable size, write empty length-prefixed entry
				buf.Write([]byte{0, 0, 0, 0}) // uint32 length = 0
			}
			continue
		}

		encoded, err := EncodeValue(val, typeID, mods)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode value at index %d: %w", i, err)
		}

		if !isVariableSize {
			// Fixed size - ensure correct size
			if len(encoded) != valueSize {
				return nil, nil, fmt.Errorf("unexpected encoded size at index %d: got %d, expected %d",
					i, len(encoded), valueSize)
			}
		}

		buf.Write(encoded)
	}

	return buf.Bytes(), validity, nil
}

// computeStatistics generates column statistics from the data.
func computeStatistics(data []any, typeID LogicalTypeID) BaseStatistics {
	stats := BaseStatistics{
		HasStats: true,
		HasNull:  false,
	}

	if len(data) == 0 {
		return stats
	}

	// Count NULLs and distinct values
	var nullCount uint64
	uniqueSet := make(map[any]struct{})

	for _, val := range data {
		if val == nil {
			nullCount++
			stats.HasNull = true
			continue
		}

		// Track unique values (for simple types)
		switch v := val.(type) {
		case []byte:
			uniqueSet[string(v)] = struct{}{}
		default:
			uniqueSet[val] = struct{}{}
		}
	}

	stats.NullCount = nullCount
	stats.DistinctCount = uint64(len(uniqueSet))

	// Generate type-specific statistics
	stats.StatData = computeTypeSpecificStats(data, typeID)

	return stats
}

// computeTypeSpecificStats generates statistics specific to the data type.
func computeTypeSpecificStats(data []any, typeID LogicalTypeID) []byte {
	if len(data) == 0 {
		return nil
	}

	// For numeric types, compute min/max
	if isNumericType(typeID) {
		var buf bytes.Buffer
		w := NewBinaryWriter(&buf)

		ns := computeNumericStats(data, typeID)

		// Write numeric statistics
		w.WriteBool(ns.HasMin)
		if ns.HasMin {
			writeStatValue(w, ns.Min)
		}
		w.WriteBool(ns.HasMax)
		if ns.HasMax {
			writeStatValue(w, ns.Max)
		}

		return buf.Bytes()
	}

	// For string types, compute length statistics
	if typeID == TypeVarchar || typeID == TypeChar || typeID == TypeBlob {
		var buf bytes.Buffer
		w := NewBinaryWriter(&buf)

		ss := computeStringStats(data)
		w.WriteBool(ss.HasStats)
		if ss.HasStats {
			w.WriteUint32(ss.MinLen)
			w.WriteUint32(ss.MaxLen)
			w.WriteBool(ss.HasMaxLen)
		}

		return buf.Bytes()
	}

	return nil
}

// computeNumericStats computes min/max for numeric data.
func computeNumericStats(data []any, typeID LogicalTypeID) NumericStatistics {
	ns := NumericStatistics{}

	for _, val := range data {
		if val == nil {
			continue
		}

		if !ns.HasMin {
			ns.Min = val
			ns.Max = val
			ns.HasMin = true
			ns.HasMax = true
			continue
		}

		ns.Min = minValue(ns.Min, val, typeID)
		ns.Max = maxValue(ns.Max, val, typeID)
	}

	return ns
}

// computeStringStats computes length statistics for string data.
func computeStringStats(data []any) StringStatistics {
	ss := StringStatistics{}

	for _, val := range data {
		if val == nil {
			continue
		}

		var length uint32
		switch v := val.(type) {
		case string:
			length = uint32(len(v))
		case []byte:
			length = uint32(len(v))
		default:
			continue
		}

		if !ss.HasStats {
			ss.MinLen = length
			ss.MaxLen = length
			ss.HasStats = true
		} else {
			if length < ss.MinLen {
				ss.MinLen = length
			}
			if length > ss.MaxLen {
				ss.MaxLen = length
			}
		}
	}

	return ss
}

// writeStatValue writes a statistic value to the binary writer.
// Uses type assertions to write values in their native format.
func writeStatValue(w *BinaryWriter, val any) {
	switch v := val.(type) {
	case int8:
		w.WriteInt8(v)
	case int16:
		w.WriteInt16(v)
	case int32:
		w.WriteInt32(v)
	case int64:
		w.WriteInt64(v)
	case uint8:
		w.WriteUint8(v)
	case uint16:
		w.WriteUint16(v)
	case uint32:
		w.WriteUint32(v)
	case uint64:
		w.WriteUint64(v)
	case float32:
		w.WriteFloat32(v)
	case float64:
		w.WriteFloat64(v)
	default:
		// Fallback for other types - attempt conversion
		w.WriteInt64(toInt64(val))
	}
}
