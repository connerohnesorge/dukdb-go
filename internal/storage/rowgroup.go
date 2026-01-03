package storage

import (
	"fmt"

	"github.com/dukdb/dukdb-go/internal/compression"
)

const (
	// bitsPerValidityEntry is the number of bits in each validity bitmap entry
	bitsPerValidityEntry = 64
)

// DuckDBRowGroup represents a DuckDB row group - a collection of column segments
// that together form a batch of rows. This is the fundamental unit of data
// storage in DuckDB's columnar format.
//
// DuckDB uses row groups to organize data in a columnar fashion, where each
// column is stored in its own segment with appropriate compression. This
// enables efficient analytical queries that only need to read specific columns.
//
// # Index Storage
//
// Row groups can optionally store index data for indexed columns. The index
// data is stored as serialized ART (Adaptive Radix Tree) indexes and is
// automatically included in the row group serialization format.
//
// When a column has an index:
//   - The index is serialized using the ART serialization format
//   - The serialized index data is stored in the IndexData map
//   - During checkpoint/persistence, the index data is automatically saved
//   - During recovery/load, the index data is automatically restored
//
// Index data can be accessed using:
//   - SetIndex(columnIdx, indexData) - Store serialized index for a column
//   - GetIndex(columnIdx) - Retrieve serialized index for a column
//   - HasIndex(columnIdx) - Check if a column has an index
//
// Note: This is different from the internal RowGroup type used for table storage.
// DuckDBRowGroup follows the DuckDB file format specification.
type DuckDBRowGroup struct {
	// MetaData contains row group metadata (row count, flags, etc.)
	MetaData DuckDBRowGroupMetadata
	// ColumnData contains the actual column segments
	ColumnData []*DuckDBColumnSegment
	// IndexData contains optional index data for this row group.
	// Maps column index (uint16) to serialized ART index data ([]byte).
	// The serialized data follows the DuckDB ART index format for compatibility.
	IndexData map[uint16][]byte
}

// DuckDBRowGroupMetadata contains metadata about a DuckDB row group.
// This structure matches DuckDB's row group metadata format.
type DuckDBRowGroupMetadata struct {
	// RowCount is the number of rows in this row group
	RowCount uint64
	// ColumnCount is the number of columns in this row group
	ColumnCount uint16
	// Flags contains row group flags (e.g., sorted, compressed)
	Flags uint32
	// StartId is the starting row ID for this row group
	StartId uint64
}

// DuckDBRowGroupFlags defines flags for row group metadata.
type DuckDBRowGroupFlags uint32

const (
	// DuckDBRowGroupFlagNone indicates no special flags
	DuckDBRowGroupFlagNone DuckDBRowGroupFlags = 0
	// DuckDBRowGroupFlagSorted indicates the row group is sorted
	DuckDBRowGroupFlagSorted DuckDBRowGroupFlags = 1 << 0
	// DuckDBRowGroupFlagCompressed indicates the row group uses compression
	DuckDBRowGroupFlagCompressed DuckDBRowGroupFlags = 1 << 1
)

// DuckDBColumnSegment represents a single column's data within a DuckDB row group.
// Each segment contains the actual data, compression metadata, and a validity
// bitmap for NULL tracking.
type DuckDBColumnSegment struct {
	// MetaData contains segment metadata (type, size, location)
	MetaData DuckDBSegmentMetadata
	// Compression is the compression algorithm used for this segment
	Compression compression.CompressionType
	// Data contains the actual (possibly compressed) column data
	Data []byte
	// Validity is the NULL bitmap (1 = valid, 0 = NULL)
	// Each uint64 holds 64 validity bits
	Validity []uint64
}

// DuckDBSegmentMetadata contains metadata about a column segment.
// This structure matches DuckDB's segment metadata format.
type DuckDBSegmentMetadata struct {
	// Type is the logical type of the column
	Type compression.LogicalTypeID
	// SegmentSize is the size of the segment in bytes (uncompressed)
	SegmentSize uint64
	// BlockId is the block ID where this segment is stored
	BlockId uint64
	// Offset is the offset within the block
	Offset uint64
	// Length is the length of the segment data (compressed)
	Length uint64
}

// NewDuckDBRowGroup creates a new empty DuckDB row group.
func NewDuckDBRowGroup(columnCount uint16) *DuckDBRowGroup {
	return &DuckDBRowGroup{
		MetaData: DuckDBRowGroupMetadata{
			RowCount:    0,
			ColumnCount: columnCount,
			Flags:       uint32(DuckDBRowGroupFlagNone),
			StartId:     0,
		},
		ColumnData: make([]*DuckDBColumnSegment, columnCount),
		IndexData:  make(map[uint16][]byte),
	}
}

// NewDuckDBColumnSegment creates a new DuckDB column segment.
func NewDuckDBColumnSegment(typeID compression.LogicalTypeID) *DuckDBColumnSegment {
	return &DuckDBColumnSegment{
		MetaData: DuckDBSegmentMetadata{
			Type:        typeID,
			SegmentSize: 0,
			BlockId:     0,
			Offset:      0,
			Length:      0,
		},
		Compression: compression.CompressionNone,
		Data:        nil,
		Validity:    nil,
	}
}

// SetData sets the data for a DuckDB column segment and updates metadata.
// If isCompressed is true, SegmentSize should be set separately to the uncompressed size.
func (cs *DuckDBColumnSegment) SetData(data []byte, isCompressed bool) {
	cs.Data = data
	cs.MetaData.Length = uint64(len(data))

	if !isCompressed {
		cs.MetaData.SegmentSize = uint64(len(data))
	}
}

// SetValidity sets the validity bitmap for a DuckDB column segment.
// The validity bitmap uses one bit per row, packed into uint64 values.
func (cs *DuckDBColumnSegment) SetValidity(validity []uint64) {
	cs.Validity = validity
}

// IsValid checks if a value at the given index is valid (not NULL).
func (cs *DuckDBColumnSegment) IsValid(idx int) bool {
	if cs.Validity == nil {
		// No validity bitmap means all values are valid
		return true
	}

	entry := idx / bitsPerValidityEntry
	if entry >= len(cs.Validity) {
		return false
	}

	bit := uint64(1) << (idx % bitsPerValidityEntry)

	return cs.Validity[entry]&bit != 0
}

// SetValid marks a value at the given index as valid.
func (cs *DuckDBColumnSegment) SetValid(idx int) {
	if cs.Validity == nil {
		// Initialize validity bitmap if needed
		numEntries := (idx/bitsPerValidityEntry + 1)
		cs.Validity = make([]uint64, numEntries)
		// Initialize all bits to 1 (valid)
		for i := range cs.Validity {
			cs.Validity[i] = ^uint64(0)
		}
	}

	entry := idx / bitsPerValidityEntry
	if entry >= len(cs.Validity) {
		// Expand validity bitmap
		oldLen := len(cs.Validity)
		newValidity := make([]uint64, entry+1)
		copy(newValidity, cs.Validity)
		// Initialize new entries to all valid
		for i := oldLen; i < len(newValidity); i++ {
			newValidity[i] = ^uint64(0)
		}
		cs.Validity = newValidity
	}

	bit := uint64(1) << (idx % bitsPerValidityEntry)
	cs.Validity[entry] |= bit
}

// SetInvalid marks a value at the given index as invalid (NULL).
func (cs *DuckDBColumnSegment) SetInvalid(idx int) {
	if cs.Validity == nil {
		// Initialize validity bitmap if needed
		numEntries := (idx/bitsPerValidityEntry + 1)
		cs.Validity = make([]uint64, numEntries)
		// Initialize all bits to 1 (valid)
		for i := range cs.Validity {
			cs.Validity[i] = ^uint64(0)
		}
	}

	entry := idx / bitsPerValidityEntry
	if entry >= len(cs.Validity) {
		// Expand validity bitmap
		oldLen := len(cs.Validity)
		newValidity := make([]uint64, entry+1)
		copy(newValidity, cs.Validity)
		// Initialize new entries to all valid
		for i := oldLen; i < len(newValidity); i++ {
			newValidity[i] = ^uint64(0)
		}
		cs.Validity = newValidity
	}

	bit := uint64(1) << (idx % bitsPerValidityEntry)
	cs.Validity[entry] &^= bit
}

// AddColumn adds a column segment to the DuckDB row group.
func (rg *DuckDBRowGroup) AddColumn(idx int, segment *DuckDBColumnSegment) {
	if idx >= 0 && idx < len(rg.ColumnData) {
		rg.ColumnData[idx] = segment
	}
}

// GetColumn returns the column segment at the given index.
func (rg *DuckDBRowGroup) GetColumn(idx int) *DuckDBColumnSegment {
	if idx < 0 || idx >= len(rg.ColumnData) {
		return nil
	}

	return rg.ColumnData[idx]
}

// SetRowCount sets the number of rows in this DuckDB row group.
func (rg *DuckDBRowGroup) SetRowCount(count uint64) {
	rg.MetaData.RowCount = count
}

// SetStartId sets the starting row ID for this DuckDB row group.
func (rg *DuckDBRowGroup) SetStartId(startId uint64) {
	rg.MetaData.StartId = startId
}

// SetFlags sets the DuckDB row group flags.
func (rg *DuckDBRowGroup) SetFlags(flags DuckDBRowGroupFlags) {
	rg.MetaData.Flags = uint32(flags)
}

// HasFlag checks if a specific flag is set.
func (rg *DuckDBRowGroup) HasFlag(flag DuckDBRowGroupFlags) bool {
	return (DuckDBRowGroupFlags(rg.MetaData.Flags) & flag) != 0
}

// SetIndex sets the serialized index data for a specific column.
// The index data should be the serialized ART index for the column.
func (rg *DuckDBRowGroup) SetIndex(columnIdx uint16, indexData []byte) {
	if rg.IndexData == nil {
		rg.IndexData = make(map[uint16][]byte)
	}
	rg.IndexData[columnIdx] = indexData
}

// GetIndex returns the serialized index data for a specific column.
// Returns nil if no index exists for the column.
func (rg *DuckDBRowGroup) GetIndex(columnIdx uint16) []byte {
	if rg.IndexData == nil {
		return nil
	}
	return rg.IndexData[columnIdx]
}

// HasIndex returns true if an index exists for the specified column.
func (rg *DuckDBRowGroup) HasIndex(columnIdx uint16) bool {
	if rg.IndexData == nil {
		return false
	}
	_, exists := rg.IndexData[columnIdx]
	return exists
}

// SerializeRowGroup serializes a DuckDB row group to binary format.
// Format:
//   [RowGroupMetadata]
//     - RowCount: uint64 (8 bytes)
//     - ColumnCount: uint16 (2 bytes)
//     - Flags: uint32 (4 bytes)
//     - StartId: uint64 (8 bytes)
//   [For each column segment]
//     - TypeID: uint32 (4 bytes)
//     - CompressionType: uint8 (1 byte)
//     - DataLength: uint64 (8 bytes)
//     - ValidityLength: uint32 (4 bytes)
//     - Data: [DataLength bytes]
//     - Validity: [ValidityLength bytes]
//   [Index Data]
//     - IndexCount: uint16 (2 bytes) - number of indexed columns
//     - For each indexed column:
//       - ColumnIndex: uint16 (2 bytes)
//       - IndexDataLength: uint32 (4 bytes)
//       - IndexData: [IndexDataLength bytes]
func SerializeRowGroup(rg *DuckDBRowGroup) ([]byte, error) {
	if rg == nil {
		return nil, fmt.Errorf("cannot serialize nil row group")
	}

	// Calculate total size needed
	size := 8 + 2 + 4 + 8 // Metadata: RowCount + ColumnCount + Flags + StartId

	// Add size for each column segment
	for _, col := range rg.ColumnData {
		// TypeID (4) + CompressionType (1) + DataLength (8) + ValidityLength (4)
		size += 4 + 1 + 8 + 4

		if col == nil {
			// Nil columns still need header space
			continue
		}

		// Add data and validity sizes
		size += len(col.Data)
		size += len(col.Validity) * 8 // Each validity entry is uint64 (8 bytes)
	}

	// Add size for index data
	size += 2 // IndexCount (uint16)
	if rg.IndexData != nil {
		for _, indexData := range rg.IndexData {
			size += 2 + 4 + len(indexData) // ColumnIndex (2) + IndexDataLength (4) + data
		}
	}

	// Allocate buffer
	buf := make([]byte, size)
	offset := 0

	// Write metadata
	writeUint64(buf[offset:], rg.MetaData.RowCount)
	offset += 8
	writeUint16(buf[offset:], rg.MetaData.ColumnCount)
	offset += 2
	writeUint32(buf[offset:], rg.MetaData.Flags)
	offset += 4
	writeUint64(buf[offset:], rg.MetaData.StartId)
	offset += 8

	// Write each column segment
	for _, col := range rg.ColumnData {
		if col == nil {
			// Write empty segment
			writeUint32(buf[offset:], uint32(compression.LogicalTypeInvalid))
			offset += 4
			buf[offset] = byte(compression.CompressionNone)
			offset += 1
			writeUint64(buf[offset:], 0) // DataLength
			offset += 8
			writeUint32(buf[offset:], 0) // ValidityLength
			offset += 4
			continue
		}

		// Write TypeID
		writeUint32(buf[offset:], uint32(col.MetaData.Type))
		offset += 4

		// Write CompressionType
		buf[offset] = byte(col.Compression)
		offset += 1

		// Write DataLength
		writeUint64(buf[offset:], uint64(len(col.Data)))
		offset += 8

		// Write ValidityLength (number of uint64 entries)
		writeUint32(buf[offset:], uint32(len(col.Validity)))
		offset += 4

		// Write Data
		copy(buf[offset:], col.Data)
		offset += len(col.Data)

		// Write Validity bitmap (each entry is uint64)
		for _, v := range col.Validity {
			writeUint64(buf[offset:], v)
			offset += 8
		}
	}

	// Write index data
	indexCount := uint16(0)
	if rg.IndexData != nil {
		indexCount = uint16(len(rg.IndexData))
	}
	writeUint16(buf[offset:], indexCount)
	offset += 2

	if indexCount > 0 {
		// Write each index
		for colIdx, indexData := range rg.IndexData {
			writeUint16(buf[offset:], colIdx)
			offset += 2
			writeUint32(buf[offset:], uint32(len(indexData)))
			offset += 4
			copy(buf[offset:], indexData)
			offset += len(indexData)
		}
	}

	return buf, nil
}

// DeserializeRowGroup deserializes a DuckDB row group from binary format.
func DeserializeRowGroup(data []byte) (*DuckDBRowGroup, error) {
	if len(data) < 22 { // Minimum size: RowCount + ColumnCount + Flags + StartId
		return nil, fmt.Errorf("data too short for row group metadata: %d bytes", len(data))
	}

	offset := 0

	// Read metadata
	rowCount := readUint64(data[offset:])
	offset += 8
	columnCount := readUint16(data[offset:])
	offset += 2
	flags := readUint32(data[offset:])
	offset += 4
	startId := readUint64(data[offset:])
	offset += 8

	// Create row group
	rg := &DuckDBRowGroup{
		MetaData: DuckDBRowGroupMetadata{
			RowCount:    rowCount,
			ColumnCount: columnCount,
			Flags:       flags,
			StartId:     startId,
		},
		ColumnData: make([]*DuckDBColumnSegment, columnCount),
		IndexData:  make(map[uint16][]byte),
	}

	// Read each column segment
	for i := uint16(0); i < columnCount; i++ {
		if offset+17 > len(data) { // TypeID + CompressionType + DataLength + ValidityLength
			return nil, fmt.Errorf("data too short for column %d metadata at offset %d", i, offset)
		}

		// Read TypeID
		typeID := readUint32(data[offset:])
		offset += 4

		// Read CompressionType
		compressionType := compression.CompressionType(data[offset])
		offset += 1

		// Read DataLength
		dataLength := readUint64(data[offset:])
		offset += 8

		// Read ValidityLength
		validityLength := readUint32(data[offset:])
		offset += 4

		// Check if this is an empty segment
		if typeID == uint32(compression.LogicalTypeInvalid) && dataLength == 0 && validityLength == 0 {
			rg.ColumnData[i] = nil
			continue
		}

		// Create column segment
		col := &DuckDBColumnSegment{
			MetaData: DuckDBSegmentMetadata{
				Type:   compression.LogicalTypeID(typeID),
				Length: dataLength,
			},
			Compression: compressionType,
		}

		// Read Data
		if dataLength > 0 {
			if offset+int(dataLength) > len(data) {
				return nil, fmt.Errorf("data too short for column %d data at offset %d (need %d bytes)", i, offset, dataLength)
			}
			col.Data = make([]byte, dataLength)
			copy(col.Data, data[offset:offset+int(dataLength)])
			offset += int(dataLength)
		}

		// Read Validity bitmap
		if validityLength > 0 {
			if offset+int(validityLength)*8 > len(data) {
				return nil, fmt.Errorf("data too short for column %d validity at offset %d (need %d bytes)", i, offset, validityLength*8)
			}
			col.Validity = make([]uint64, validityLength)
			for j := uint32(0); j < validityLength; j++ {
				col.Validity[j] = readUint64(data[offset:])
				offset += 8
			}
		}

		rg.ColumnData[i] = col
	}

	// Read index data (if present)
	// Check if there are at least 2 bytes remaining for IndexCount
	if offset+2 <= len(data) {
		indexCount := readUint16(data[offset:])
		offset += 2

		// Read each index
		for i := uint16(0); i < indexCount; i++ {
			if offset+6 > len(data) { // ColumnIndex (2) + IndexDataLength (4)
				return nil, fmt.Errorf("data too short for index %d metadata at offset %d", i, offset)
			}

			colIdx := readUint16(data[offset:])
			offset += 2

			indexDataLength := readUint32(data[offset:])
			offset += 4

			if offset+int(indexDataLength) > len(data) {
				return nil, fmt.Errorf("data too short for index %d data at offset %d (need %d bytes)", i, offset, indexDataLength)
			}

			indexData := make([]byte, indexDataLength)
			copy(indexData, data[offset:offset+int(indexDataLength)])
			offset += int(indexDataLength)

			rg.IndexData[colIdx] = indexData
		}
	}

	return rg, nil
}

// Helper functions for binary encoding/decoding

func writeUint64(buf []byte, v uint64) {
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	buf[4] = byte(v >> 32)
	buf[5] = byte(v >> 40)
	buf[6] = byte(v >> 48)
	buf[7] = byte(v >> 56)
}

func readUint64(buf []byte) uint64 {
	return uint64(buf[0]) |
		uint64(buf[1])<<8 |
		uint64(buf[2])<<16 |
		uint64(buf[3])<<24 |
		uint64(buf[4])<<32 |
		uint64(buf[5])<<40 |
		uint64(buf[6])<<48 |
		uint64(buf[7])<<56
}

func writeUint32(buf []byte, v uint32) {
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
}

func readUint32(buf []byte) uint32 {
	return uint32(buf[0]) |
		uint32(buf[1])<<8 |
		uint32(buf[2])<<16 |
		uint32(buf[3])<<24
}

func writeUint16(buf []byte, v uint16) {
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
}

func readUint16(buf []byte) uint16 {
	return uint16(buf[0]) |
		uint16(buf[1])<<8
}
