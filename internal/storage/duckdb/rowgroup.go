package duckdb

import (
	"bytes"
	"errors"
	"fmt"
)

// Row group error definitions.
var (
	// ErrInvalidRowGroup indicates the row group data is invalid or corrupted.
	ErrInvalidRowGroup = errors.New("invalid row group")

	// ErrInvalidDataPointer indicates the data pointer is invalid or corrupted.
	ErrInvalidDataPointer = errors.New("invalid data pointer")

	// ErrInvalidValidityMask indicates the validity mask data is invalid.
	ErrInvalidValidityMask = errors.New("invalid validity mask")

	// ErrRowIndexOutOfRange indicates the row index is outside valid bounds.
	ErrRowIndexOutOfRange = errors.New("row index out of range")
)

// errWrapFormat is the format string for wrapping errors with context.
const errWrapFormat = "%w: %v"

// RowGroupPointer points to a row group's metadata in the DuckDB file.
// It is stored in table metadata and references column DataPointers
// through MetaBlockPointers (one per column).
type RowGroupPointer struct {
	// TableOID is the table object ID this row group belongs to.
	TableOID uint64

	// RowStart is the starting row index within the table.
	RowStart uint64

	// TupleCount is the number of rows in this row group.
	TupleCount uint64

	// DataPointers contains one MetaBlockPointer per column.
	// Each MetaBlockPointer points to a metadata block where the
	// actual DataPointer for that column is serialized.
	DataPointers []MetaBlockPointer
}

// DataPointer contains the actual column data location.
// It is stored IN metadata blocks (referenced by MetaBlockPointer)
// and points to where the compressed column data resides.
type DataPointer struct {
	// RowStart is the starting row within this segment.
	RowStart uint64

	// TupleCount is the number of tuples in this segment.
	TupleCount uint64

	// Block points to where actual column data is stored.
	Block BlockPointer

	// Compression indicates how the data is compressed.
	Compression CompressionType

	// Statistics contains per-segment statistics.
	Statistics BaseStatistics

	// SegmentState contains segment-specific state including validity info.
	SegmentState ColumnSegmentState

	// ValidityPointer points to the validity mask data for this column.
	// This is populated when the ColumnData has a nested validity child (field 101).
	// The validity mask indicates which rows are NULL.
	ValidityPointer *DataPointer
}

// ColumnSegmentState contains segment metadata including validity mask info.
type ColumnSegmentState struct {
	// HasValidityMask indicates whether there are NULLs in this segment.
	HasValidityMask bool

	// ValidityBlock points to where the validity mask is stored (if separate).
	// If HasValidityMask is true and ValidityBlock is invalid, the validity
	// mask is inlined in StateData.
	ValidityBlock BlockPointer

	// ValidityCompression is the compression type for the validity column.
	// Relevant when HasValidityMask is true and validity is stored separately.
	ValidityCompression CompressionType

	// ValidityHasNull indicates whether the validity column's statistics
	// report HasNull=true (meaning all values in this column are NULL).
	// This is used when validity uses CONSTANT compression with block ID 127.
	ValidityHasNull bool

	// StateData contains additional compression-specific state.
	// For segments with inlined validity masks, this contains the mask data.
	StateData []byte
}

// BaseStatistics contains per-segment statistics for a column.
type BaseStatistics struct {
	// HasStats indicates whether statistics are available.
	HasStats bool

	// HasNull indicates whether NULL values exist in this segment.
	HasNull bool

	// NullCount is the count of NULL values (if tracked).
	NullCount uint64

	// DistinctCount is the approximate count of distinct values.
	DistinctCount uint64

	// StatData contains type-specific statistics stored as bytes.
	// Use NumericStatistics or StringStatistics to interpret.
	StatData []byte
}

// NumericStatistics contains statistics for numeric column types.
type NumericStatistics struct {
	// HasMin indicates whether minimum value is available.
	HasMin bool

	// HasMax indicates whether maximum value is available.
	HasMax bool

	// Min is the minimum value (type depends on column type).
	Min any

	// Max is the maximum value (type depends on column type).
	Max any
}

// StringStatistics contains statistics for string/varchar column types.
type StringStatistics struct {
	// HasStats indicates whether string statistics are available.
	HasStats bool

	// MinLen is the minimum string length in this segment.
	MinLen uint32

	// MaxLen is the maximum string length in this segment.
	MaxLen uint32

	// HasMaxLen indicates whether MaxLen is a hard limit (CHAR type).
	HasMaxLen bool
}

// ValidityMask tracks NULL values for a column segment.
// Uses a bit array where 1 = valid (not NULL) and 0 = null.
type ValidityMask struct {
	// data is the bit array for tracking validity.
	// Each uint64 holds validity for 64 rows.
	data []uint64

	// allValid is an optimization flag - if true, no NULLs exist.
	allValid bool

	// rowCount is the number of rows this mask covers.
	rowCount uint64
}

// validityMaskConstants for bit manipulation.
const (
	validityBitsPerWord = 64
	validityWordMask    = validityBitsPerWord - 1
	allValid            = ^uint64(0) // All 64 bits set
)

// NewValidityMask creates a new ValidityMask for the given row count.
// By default, all rows are marked as valid (not NULL).
func NewValidityMask(rowCount uint64) *ValidityMask {
	wordCount := (rowCount + validityBitsPerWord - 1) / validityBitsPerWord
	data := make([]uint64, wordCount)

	// Initialize all bits to 1 (valid)
	for i := range data {
		data[i] = ^uint64(0) // All bits set to 1
	}

	return &ValidityMask{
		data:     data,
		allValid: true,
		rowCount: rowCount,
	}
}

// NewValidityMaskAllNull creates a new ValidityMask where all rows are NULL.
// All bits are initialized to 0 (invalid/NULL).
func NewValidityMaskAllNull(rowCount uint64) *ValidityMask {
	wordCount := (rowCount + validityBitsPerWord - 1) / validityBitsPerWord
	data := make([]uint64, wordCount) // All zeros by default in Go

	return &ValidityMask{
		data:     data,
		allValid: false,
		rowCount: rowCount,
	}
}

// NewValidityMaskFromData creates a ValidityMask from existing bit data.
func NewValidityMaskFromData(data []uint64, rowCount uint64) *ValidityMask {
	// Check if all values are valid
	allValid := true
	fullWords := rowCount / validityBitsPerWord
	remainingBits := rowCount % validityBitsPerWord

	for i := uint64(0); i < fullWords && allValid; i++ {
		if data[i] != ^uint64(0) {
			allValid = false
		}
	}

	// Check partial last word if exists
	if allValid && remainingBits > 0 && len(data) > int(fullWords) {
		mask := (uint64(1) << remainingBits) - 1
		if data[fullWords]&mask != mask {
			allValid = false
		}
	}

	return &ValidityMask{
		data:     data,
		allValid: allValid,
		rowCount: rowCount,
	}
}

// IsValid returns true if the row at the given index is valid (not NULL).
func (v *ValidityMask) IsValid(rowIdx uint64) bool {
	if v.allValid {
		return true
	}

	if rowIdx >= v.rowCount {
		return false
	}

	wordIdx := rowIdx / validityBitsPerWord
	bitIdx := rowIdx % validityBitsPerWord

	return (v.data[wordIdx] & (1 << bitIdx)) != 0
}

// SetInvalid marks the row at the given index as invalid (NULL).
func (v *ValidityMask) SetInvalid(rowIdx uint64) {
	if rowIdx >= v.rowCount {
		return
	}

	wordIdx := rowIdx / validityBitsPerWord
	bitIdx := rowIdx % validityBitsPerWord

	v.data[wordIdx] &^= (1 << bitIdx)
	v.allValid = false
}

// SetValid marks the row at the given index as valid (not NULL).
func (v *ValidityMask) SetValid(rowIdx uint64) {
	if rowIdx >= v.rowCount {
		return
	}

	wordIdx := rowIdx / validityBitsPerWord
	bitIdx := rowIdx % validityBitsPerWord

	v.data[wordIdx] |= (1 << bitIdx)
}

// AllValid returns true if all rows are valid (no NULLs).
func (v *ValidityMask) AllValid() bool {
	return v.allValid
}

// RowCount returns the number of rows this mask covers.
func (v *ValidityMask) RowCount() uint64 {
	return v.rowCount
}

// Data returns the underlying bit array.
func (v *ValidityMask) Data() []uint64 {
	return v.data
}

// NullCount returns the count of NULL values in the mask.
func (v *ValidityMask) NullCount() uint64 {
	if v.allValid {
		return 0
	}

	var count uint64
	for i := uint64(0); i < v.rowCount; i++ {
		if !v.IsValid(i) {
			count++
		}
	}

	return count
}

// CountValid counts the number of valid (non-NULL) rows in the range [start, end).
// Uses popcount optimization for whole words.
func (v *ValidityMask) CountValid(start, end uint64) uint64 {
	if v.allValid {
		return end - start
	}

	if end > v.rowCount {
		end = v.rowCount
	}
	if start >= end {
		return 0
	}

	var count uint64

	// Process bit by bit for partial first word
	startWord := start / validityBitsPerWord
	endWord := end / validityBitsPerWord

	if startWord == endWord {
		// All bits in the same word
		for i := start; i < end; i++ {
			if v.IsValid(i) {
				count++
			}
		}
		return count
	}

	// Handle partial first word
	for i := start; i < (startWord+1)*validityBitsPerWord; i++ {
		if v.IsValid(i) {
			count++
		}
	}

	// Handle full words using popcount
	for w := startWord + 1; w < endWord; w++ {
		count += popcount64(v.data[w])
	}

	// Handle partial last word
	for i := endWord * validityBitsPerWord; i < end; i++ {
		if v.IsValid(i) {
			count++
		}
	}

	return count
}

// popcount64 counts the number of set bits in a uint64.
// This is the Hamming weight algorithm.
func popcount64(x uint64) uint64 {
	// See https://en.wikipedia.org/wiki/Hamming_weight
	const m1 = 0x5555555555555555
	const m2 = 0x3333333333333333
	const m4 = 0x0f0f0f0f0f0f0f0f
	const h01 = 0x0101010101010101

	x -= (x >> 1) & m1
	x = (x & m2) + ((x >> 2) & m2)
	x = (x + (x >> 4)) & m4
	return (x * h01) >> 56
}

// SetAllValid sets the allValid flag and fills all bits with 1.
// Use this when you know there are no NULLs.
func (v *ValidityMask) SetAllValid() {
	v.allValid = true
	for i := range v.data {
		v.data[i] = ^uint64(0)
	}
}

// Clone creates a deep copy of the validity mask.
func (v *ValidityMask) Clone() *ValidityMask {
	dataCopy := make([]uint64, len(v.data))
	copy(dataCopy, v.data)

	return &ValidityMask{
		data:     dataCopy,
		allValid: v.allValid,
		rowCount: v.rowCount,
	}
}

// Serialization methods for RowGroupPointer

// Serialize writes the RowGroupPointer to a BinaryWriter.
func (rg *RowGroupPointer) Serialize(w *BinaryWriter) error {
	w.WriteUint64(rg.TableOID)
	w.WriteUint64(rg.RowStart)
	w.WriteUint64(rg.TupleCount)

	// Write number of data pointers (columns)
	w.WriteUint32(uint32(len(rg.DataPointers)))

	// Write each MetaBlockPointer
	for _, mbp := range rg.DataPointers {
		w.WriteUint64(mbp.BlockID)
		w.WriteUint64(mbp.Offset)
	}

	return w.Err()
}

// Deserialize reads a RowGroupPointer from a BinaryReader.
func (rg *RowGroupPointer) Deserialize(r *BinaryReader) error {
	rg.TableOID = r.ReadUint64()
	rg.RowStart = r.ReadUint64()
	rg.TupleCount = r.ReadUint64()

	// Read number of data pointers (columns)
	count := r.ReadUint32()

	if r.Err() != nil {
		return fmt.Errorf(errWrapFormat, ErrInvalidRowGroup, r.Err())
	}

	// Read each MetaBlockPointer
	rg.DataPointers = make([]MetaBlockPointer, count)
	for i := uint32(0); i < count; i++ {
		rg.DataPointers[i].BlockID = r.ReadUint64()
		rg.DataPointers[i].Offset = r.ReadUint64()
	}

	if r.Err() != nil {
		return fmt.Errorf(errWrapFormat, ErrInvalidRowGroup, r.Err())
	}

	return nil
}

// SerializeToBytes serializes the RowGroupPointer to bytes.
func (rg *RowGroupPointer) SerializeToBytes() ([]byte, error) {
	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	if err := rg.Serialize(w); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeRowGroupPointer deserializes a RowGroupPointer from bytes.
func DeserializeRowGroupPointer(data []byte) (*RowGroupPointer, error) {
	r := NewBinaryReader(bytes.NewReader(data))
	rg := &RowGroupPointer{}

	if err := rg.Deserialize(r); err != nil {
		return nil, err
	}

	return rg, nil
}

// Serialization methods for DataPointer

// Serialize writes the DataPointer to a BinaryWriter.
func (dp *DataPointer) Serialize(w *BinaryWriter) error {
	w.WriteUint64(dp.RowStart)
	w.WriteUint64(dp.TupleCount)

	// Write BlockPointer
	w.WriteUint64(dp.Block.BlockID)
	w.WriteUint32(dp.Block.Offset)

	// Write compression type
	w.WriteUint8(uint8(dp.Compression))

	// Write statistics
	if err := dp.Statistics.Serialize(w); err != nil {
		return err
	}

	// Write segment state
	if err := dp.SegmentState.Serialize(w); err != nil {
		return err
	}

	return w.Err()
}

// Deserialize reads a DataPointer from a BinaryReader.
func (dp *DataPointer) Deserialize(r *BinaryReader) error {
	dp.RowStart = r.ReadUint64()
	dp.TupleCount = r.ReadUint64()

	// Read BlockPointer
	dp.Block.BlockID = r.ReadUint64()
	dp.Block.Offset = r.ReadUint32()

	// Read compression type
	dp.Compression = CompressionType(r.ReadUint8())

	if r.Err() != nil {
		return fmt.Errorf(errWrapFormat, ErrInvalidDataPointer, r.Err())
	}

	// Read statistics
	if err := dp.Statistics.Deserialize(r); err != nil {
		return err
	}

	// Read segment state
	if err := dp.SegmentState.Deserialize(r); err != nil {
		return err
	}

	return r.Err()
}

// SerializeToBytes serializes the DataPointer to bytes.
func (dp *DataPointer) SerializeToBytes() ([]byte, error) {
	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	if err := dp.Serialize(w); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeDataPointer deserializes a DataPointer from bytes.
func DeserializeDataPointer(data []byte) (*DataPointer, error) {
	r := NewBinaryReader(bytes.NewReader(data))
	dp := &DataPointer{}

	if err := dp.Deserialize(r); err != nil {
		return nil, err
	}

	return dp, nil
}

// Serialization methods for BaseStatistics

// Serialize writes the BaseStatistics to a BinaryWriter.
func (s *BaseStatistics) Serialize(w *BinaryWriter) error {
	w.WriteBool(s.HasStats)

	if !s.HasStats {
		return w.Err()
	}

	w.WriteBool(s.HasNull)
	w.WriteUint64(s.NullCount)
	w.WriteUint64(s.DistinctCount)

	// Write type-specific stat data with length prefix
	w.WriteUint32(uint32(len(s.StatData)))
	if len(s.StatData) > 0 {
		w.WriteBytes(s.StatData)
	}

	return w.Err()
}

// Deserialize reads BaseStatistics from a BinaryReader.
func (s *BaseStatistics) Deserialize(r *BinaryReader) error {
	s.HasStats = r.ReadBool()

	if !s.HasStats {
		return r.Err()
	}

	s.HasNull = r.ReadBool()
	s.NullCount = r.ReadUint64()
	s.DistinctCount = r.ReadUint64()

	// Read type-specific stat data
	statDataLen := r.ReadUint32()
	if statDataLen > 0 {
		s.StatData = r.ReadBytes(int(statDataLen))
	}

	return r.Err()
}

// Serialization methods for ColumnSegmentState

// Serialize writes the ColumnSegmentState to a BinaryWriter.
func (s *ColumnSegmentState) Serialize(w *BinaryWriter) error {
	w.WriteBool(s.HasValidityMask)

	if s.HasValidityMask {
		// Write validity block pointer
		w.WriteUint64(s.ValidityBlock.BlockID)
		w.WriteUint32(s.ValidityBlock.Offset)
	}

	// Write state data with length prefix
	w.WriteUint32(uint32(len(s.StateData)))
	if len(s.StateData) > 0 {
		w.WriteBytes(s.StateData)
	}

	return w.Err()
}

// Deserialize reads ColumnSegmentState from a BinaryReader.
func (s *ColumnSegmentState) Deserialize(r *BinaryReader) error {
	s.HasValidityMask = r.ReadBool()

	if s.HasValidityMask {
		// Read validity block pointer
		s.ValidityBlock.BlockID = r.ReadUint64()
		s.ValidityBlock.Offset = r.ReadUint32()
	}

	// Read state data
	stateDataLen := r.ReadUint32()
	if stateDataLen > 0 {
		s.StateData = r.ReadBytes(int(stateDataLen))
	}

	return r.Err()
}

// Serialization methods for ValidityMask

// Serialize writes the ValidityMask to a BinaryWriter.
func (v *ValidityMask) Serialize(w *BinaryWriter) error {
	w.WriteUint64(v.rowCount)
	w.WriteBool(v.allValid)

	// If all valid, no need to write the data array
	if v.allValid {
		return w.Err()
	}

	// Write the data array
	w.WriteUint32(uint32(len(v.data)))
	for _, word := range v.data {
		w.WriteUint64(word)
	}

	return w.Err()
}

// Deserialize reads a ValidityMask from a BinaryReader.
func (v *ValidityMask) Deserialize(r *BinaryReader) error {
	v.rowCount = r.ReadUint64()
	v.allValid = r.ReadBool()

	if r.Err() != nil {
		return fmt.Errorf(errWrapFormat, ErrInvalidValidityMask, r.Err())
	}

	if v.allValid {
		// Create data array with all bits set
		wordCount := (v.rowCount + validityBitsPerWord - 1) / validityBitsPerWord
		v.data = make([]uint64, wordCount)
		for i := range v.data {
			v.data[i] = ^uint64(0)
		}

		return nil
	}

	// Read the data array
	wordCount := r.ReadUint32()
	v.data = make([]uint64, wordCount)

	for i := uint32(0); i < wordCount; i++ {
		v.data[i] = r.ReadUint64()
	}

	if r.Err() != nil {
		return fmt.Errorf(errWrapFormat, ErrInvalidValidityMask, r.Err())
	}

	return nil
}

// SerializeToBytes serializes the ValidityMask to bytes.
func (v *ValidityMask) SerializeToBytes() ([]byte, error) {
	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	if err := v.Serialize(w); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeValidityMask deserializes a ValidityMask from bytes.
func DeserializeValidityMask(data []byte) (*ValidityMask, error) {
	r := NewBinaryReader(bytes.NewReader(data))
	v := &ValidityMask{}

	if err := v.Deserialize(r); err != nil {
		return nil, err
	}

	return v, nil
}

// Serialization methods for NumericStatistics

// Serialize writes NumericStatistics to a BinaryWriter.
// Note: The caller must handle type-specific min/max serialization.
func (s *NumericStatistics) Serialize(w *BinaryWriter, writeValue func(*BinaryWriter, any) error) error {
	w.WriteBool(s.HasMin)
	if s.HasMin {
		if err := writeValue(w, s.Min); err != nil {
			return err
		}
	}

	w.WriteBool(s.HasMax)
	if s.HasMax {
		if err := writeValue(w, s.Max); err != nil {
			return err
		}
	}

	return w.Err()
}

// Deserialize reads NumericStatistics from a BinaryReader.
// Note: The caller must handle type-specific min/max deserialization.
func (s *NumericStatistics) Deserialize(r *BinaryReader, readValue func(*BinaryReader) (any, error)) error {
	s.HasMin = r.ReadBool()
	if s.HasMin {
		var err error
		s.Min, err = readValue(r)
		if err != nil {
			return err
		}
	}

	s.HasMax = r.ReadBool()
	if s.HasMax {
		var err error
		s.Max, err = readValue(r)
		if err != nil {
			return err
		}
	}

	return r.Err()
}

// Serialization methods for StringStatistics

// Serialize writes StringStatistics to a BinaryWriter.
func (s *StringStatistics) Serialize(w *BinaryWriter) error {
	w.WriteBool(s.HasStats)

	if !s.HasStats {
		return w.Err()
	}

	w.WriteUint32(s.MinLen)
	w.WriteUint32(s.MaxLen)
	w.WriteBool(s.HasMaxLen)

	return w.Err()
}

// Deserialize reads StringStatistics from a BinaryReader.
func (s *StringStatistics) Deserialize(r *BinaryReader) error {
	s.HasStats = r.ReadBool()

	if !s.HasStats {
		return r.Err()
	}

	s.MinLen = r.ReadUint32()
	s.MaxLen = r.ReadUint32()
	s.HasMaxLen = r.ReadBool()

	return r.Err()
}

// SerializeToBytes serializes StringStatistics to bytes.
func (s *StringStatistics) SerializeToBytes() ([]byte, error) {
	var buf bytes.Buffer
	w := NewBinaryWriter(&buf)

	if err := s.Serialize(w); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeStringStatistics deserializes StringStatistics from bytes.
func DeserializeStringStatistics(data []byte) (*StringStatistics, error) {
	r := NewBinaryReader(bytes.NewReader(data))
	s := &StringStatistics{}

	if err := s.Deserialize(r); err != nil {
		return nil, err
	}

	return s, nil
}

// Helper functions

// NewRowGroupPointer creates a new RowGroupPointer.
func NewRowGroupPointer(tableOID, rowStart, tupleCount uint64, columnCount int) *RowGroupPointer {
	return &RowGroupPointer{
		TableOID:     tableOID,
		RowStart:     rowStart,
		TupleCount:   tupleCount,
		DataPointers: make([]MetaBlockPointer, columnCount),
	}
}

// NewDataPointer creates a new DataPointer with default values.
func NewDataPointer(rowStart, tupleCount uint64, block BlockPointer, compression CompressionType) *DataPointer {
	return &DataPointer{
		RowStart:    rowStart,
		TupleCount:  tupleCount,
		Block:       block,
		Compression: compression,
		Statistics: BaseStatistics{
			HasStats: false,
		},
		SegmentState: ColumnSegmentState{
			HasValidityMask: false,
		},
	}
}

// NewBaseStatistics creates new BaseStatistics with the given values.
func NewBaseStatistics(hasNull bool, nullCount, distinctCount uint64) *BaseStatistics {
	return &BaseStatistics{
		HasStats:      true,
		HasNull:       hasNull,
		NullCount:     nullCount,
		DistinctCount: distinctCount,
	}
}

// NewColumnSegmentState creates a new ColumnSegmentState.
func NewColumnSegmentState(hasValidityMask bool, validityBlock BlockPointer) *ColumnSegmentState {
	return &ColumnSegmentState{
		HasValidityMask: hasValidityMask,
		ValidityBlock:   validityBlock,
	}
}

// NewNumericStatistics creates new NumericStatistics.
func NewNumericStatistics(minVal, maxVal any) *NumericStatistics {
	return &NumericStatistics{
		HasMin: minVal != nil,
		HasMax: maxVal != nil,
		Min:    minVal,
		Max:    maxVal,
	}
}

// NewStringStatistics creates new StringStatistics.
func NewStringStatistics(minLen, maxLen uint32, hasMaxLen bool) *StringStatistics {
	return &StringStatistics{
		HasStats:  true,
		MinLen:    minLen,
		MaxLen:    maxLen,
		HasMaxLen: hasMaxLen,
	}
}

// ColumnCount returns the number of columns in the row group.
func (rg *RowGroupPointer) ColumnCount() int {
	return len(rg.DataPointers)
}

// IsEmpty returns true if the row group has no rows.
func (rg *RowGroupPointer) IsEmpty() bool {
	return rg.TupleCount == 0
}

// RowEnd returns the ending row index (exclusive) for this row group.
func (rg *RowGroupPointer) RowEnd() uint64 {
	return rg.RowStart + rg.TupleCount
}

// ContainsRow returns true if the given row index is within this row group.
func (rg *RowGroupPointer) ContainsRow(rowIdx uint64) bool {
	return rowIdx >= rg.RowStart && rowIdx < rg.RowEnd()
}

// IsEmpty returns true if the data pointer represents no data.
func (dp *DataPointer) IsEmpty() bool {
	return dp.TupleCount == 0
}

// HasStatistics returns true if statistics are available.
func (dp *DataPointer) HasStatistics() bool {
	return dp.Statistics.HasStats
}

// HasNulls returns true if the segment may contain NULL values.
func (dp *DataPointer) HasNulls() bool {
	return dp.SegmentState.HasValidityMask || dp.Statistics.HasNull
}
