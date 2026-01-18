package optimizer

import (
	"bytes"
	"encoding/binary"
	"math"

	dukdb "github.com/dukdb/dukdb-go"
)

// Binary Format Documentation for DuckDB v1.4.3 Statistics
// ========================================================
//
// This serializer matches DuckDB's property-based statistics serialization format.
// Format reference: src/storage/statistics/column_statistics.cpp (lines 59-69)
//
// Overall Structure:
//
//   ColumnStatistics:
//     [PropertyBegin: 100] [BaseStatistics] [PropertyEnd]
//     [PropertyBegin: 101] [DistinctStatistics] [PropertyEnd]
//     [MessageTerminator: 0xFFFF]
//
// Property-based serialization uses field IDs for forward compatibility:
//   - PropertyBegin writes: uint16(fieldID) in little-endian
//   - Property value follows in type-specific encoding
//   - Multiple properties can be added in future versions without breaking readers
//
// BaseStatistics Format (Property ID 100):
//   Byte Layout:
//   [0-0]   uint8       LogicalType ID (0=Unknown, 1=Boolean, 2=TinyInt, ...)
//   [1-1]   uint8       has_null flag (0 or 1)
//   [2-2]   uint8       has_no_null flag (0 or 1)
//   [3-10]  int64       distinct_count (varint-encoded)
//   [11+]   Variable    Type-specific min/max values
//
// DistinctStatistics Format (Property ID 101):
//   Only present if column has distinct value tracking.
//   Byte Layout:
//   [0-7]   int64       sample_count (varint-encoded)
//   [8-15]  int64       total_count (varint-encoded)
//   [16+]   Variable    HyperLogLog state (if available)
//
// Type-Specific Value Storage:
//
//   Numeric types (INT8-64, UINT8-64, FLOAT, DOUBLE):
//     min: stored as type-specific bytes (little-endian for integers, IEEE754 for floats)
//     max: stored as type-specific bytes
//
//   String types (VARCHAR):
//     min: [length varint] [utf8 bytes]
//     max: [length varint] [utf8 bytes]
//
//   Temporal types (DATE, TIME, TIMESTAMP):
//     Stored as int32 (DATE) or int64 (TIME/TIMESTAMP) values
//
// NULL Handling:
//   has_null=true, has_no_null=true   => May have NULL and non-NULL
//   has_null=true, has_no_null=false  => All NULL (no data rows)
//   has_null=false, has_no_null=true  => No NULL values
//   has_null=false, has_no_null=false => Empty result set

// StatsSerializer provides methods to serialize table and column statistics
// in DuckDB v1.4.3 binary format.
type StatsSerializer struct {
	buf *bytes.Buffer
	err error
}

// NewStatsSerializer creates a new statistics serializer.
func NewStatsSerializer() *StatsSerializer {
	return &StatsSerializer{
		buf: new(bytes.Buffer),
	}
}

// Bytes returns the serialized bytes. Only call after all serialization is complete.
func (s *StatsSerializer) Bytes() []byte {
	return s.buf.Bytes()
}

// Err returns any error encountered during serialization.
func (s *StatsSerializer) Err() error {
	return s.err
}

// SerializeTableStatistics serializes a table's statistics to DuckDB binary format.
// Returns the serialized bytes and any error encountered.
func (s *StatsSerializer) SerializeTableStatistics(stats *TableStatistics) ([]byte, error) {
	// Write version header (1 byte) - dukdb-go version 1
	s.writeByte(1)

	// Write table-level fields
	// RowCount as varint (variable-length integer encoding)
	s.writeVarint(uint64(stats.RowCount))

	// Write column count
	s.writeVarint(uint64(len(stats.Columns)))

	// Serialize each column's statistics
	for i := range stats.Columns {
		// Write column name
		s.writeString(stats.Columns[i].ColumnName)
		s.serializeColumnStatisticsInner(&stats.Columns[i])
		if s.err != nil {
			return nil, s.err
		}
	}

	return s.buf.Bytes(), s.err
}

// serializeColumnStatisticsInner serializes a single column's statistics.
// This is the core column statistics serialization following DuckDB format.
//
// Format (DuckDB ColumnStatistics::Serialize):
//   - Property 100: BaseStatistics
//   - Property 101: DistinctStatistics (optional)
//   - Message terminator: 0xFFFF
func (s *StatsSerializer) serializeColumnStatisticsInner(colStats *ColumnStatistics) {
	if s.err != nil {
		return
	}

	// Property 100: BaseStatistics
	// Reference: column_statistics.cpp line 40
	s.writePropertyBegin(100)
	s.serializeBaseStatistics(colStats)

	// Property 101: DistinctStatistics (optional)
	// Reference: column_statistics.cpp line 42-44
	// Only write if we have distinct count information
	if colStats.DistinctCount > 0 {
		s.writePropertyBegin(101)
		s.serializeDistinctStatistics(colStats)
	}

	// Message terminator
	s.writeMessageTerminator()
}

// serializeBaseStatistics writes the BaseStatistics component.
// Format: LogicalType + has_null + has_no_null + distinct_count + type-specific min/max
//
// Reference: base_statistics.cpp, lines 10-25
func (s *StatsSerializer) serializeBaseStatistics(colStats *ColumnStatistics) {
	if s.err != nil {
		return
	}

	// Write logical type ID (1 byte)
	// Map duckdb.Type to internal type IDs matching DuckDB's LogicalType system
	typeID := s.mapTypeToID(colStats.ColumnType)
	s.writeByte(typeID)

	// NULL handling flags (1 byte each)
	// has_null: true if column may contain NULL values
	// has_no_null: true if column may contain non-NULL values
	hasNull := colStats.NullFraction > 0
	hasNoNull := colStats.NullFraction < 1.0
	s.writeBool(hasNull)
	s.writeBool(hasNoNull)

	// Distinct count as varint
	// Reference: base_statistics.cpp line 22
	s.writeVarint(uint64(colStats.DistinctCount))

	// Type-specific min/max values
	// These are stored after the base fields and vary by type
	s.serializeTypeSpecificStats(colStats)
}

// serializeDistinctStatistics writes the DistinctStatistics component.
// This includes HyperLogLog-based cardinality estimates.
//
// Reference: distinct_statistics.cpp, lines 10-84
// This component tracks:
//   - sample_count: number of rows sampled
//   - total_count: total rows processed
//   - HyperLogLog state: for distinct value approximation
func (s *StatsSerializer) serializeDistinctStatistics(colStats *ColumnStatistics) {
	if s.err != nil {
		return
	}

	// Sample count and total count
	// In dukdb-go, these are derived from RowCount
	// For now, use DistinctCount as proxy for estimation
	sampleCount := int64(colStats.DistinctCount)
	if sampleCount == 0 {
		sampleCount = 1
	}

	s.writeVarint(uint64(sampleCount))
	s.writeVarint(uint64(sampleCount)) // Both set to sampleCount for simplicity

	// HyperLogLog serialization would go here
	// For now, minimal implementation (no HyperLogLog)
	// Future: Implement proper HyperLogLog serialization if needed
}

// serializeTypeSpecificStats writes type-specific min/max values.
// Format depends on the column's logical type.
//
//nolint:exhaustive // We only handle types that have min/max values
func (s *StatsSerializer) serializeTypeSpecificStats(colStats *ColumnStatistics) {
	if s.err != nil {
		return
	}

	// Only serialize min/max if both exist
	if colStats.MinValue == nil || colStats.MaxValue == nil {
		return
	}

	// Dispatch by type
	switch colStats.ColumnType {
	case dukdb.TYPE_TINYINT:
		s.serializeInt8MinMax(colStats)
	case dukdb.TYPE_UTINYINT:
		s.serializeUint8MinMax(colStats)
	case dukdb.TYPE_SMALLINT:
		s.serializeInt16MinMax(colStats)
	case dukdb.TYPE_USMALLINT:
		s.serializeUint16MinMax(colStats)
	case dukdb.TYPE_INTEGER:
		s.serializeInt32MinMax(colStats)
	case dukdb.TYPE_UINTEGER:
		s.serializeUint32MinMax(colStats)
	case dukdb.TYPE_BIGINT:
		s.serializeInt64MinMax(colStats)
	case dukdb.TYPE_UBIGINT:
		s.serializeUint64MinMax(colStats)
	case dukdb.TYPE_FLOAT:
		s.serializeFloatMinMax(colStats)
	case dukdb.TYPE_DOUBLE:
		s.serializeDoubleMinMax(colStats)
	case dukdb.TYPE_VARCHAR:
		s.serializeStringMinMax(colStats)
	case dukdb.TYPE_DATE:
		s.serializeDateMinMax(colStats)
	case dukdb.TYPE_TIMESTAMP:
		s.serializeTimestampMinMax(colStats)
		// Other types: no min/max serialization
	}
}

// serializeInt8MinMax writes int8 min/max values
func (s *StatsSerializer) serializeInt8MinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(int8); ok {
		s.writeByte(uint8(minVal))
	}
	if maxVal, ok := colStats.MaxValue.(int8); ok {
		s.writeByte(uint8(maxVal))
	}
}

// serializeUint8MinMax writes uint8 min/max values
func (s *StatsSerializer) serializeUint8MinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(uint8); ok {
		s.writeByte(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(uint8); ok {
		s.writeByte(maxVal)
	}
}

// serializeInt16MinMax writes int16 min/max values (little-endian)
func (s *StatsSerializer) serializeInt16MinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(int16); ok {
		s.writeInt16(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(int16); ok {
		s.writeInt16(maxVal)
	}
}

// serializeUint16MinMax writes uint16 min/max values (little-endian)
func (s *StatsSerializer) serializeUint16MinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(uint16); ok {
		s.writeUint16(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(uint16); ok {
		s.writeUint16(maxVal)
	}
}

// serializeInt32MinMax writes int32 min/max values (little-endian)
func (s *StatsSerializer) serializeInt32MinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(int32); ok {
		s.writeInt32(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(int32); ok {
		s.writeInt32(maxVal)
	}
}

// serializeUint32MinMax writes uint32 min/max values (little-endian)
func (s *StatsSerializer) serializeUint32MinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(uint32); ok {
		s.writeUint32(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(uint32); ok {
		s.writeUint32(maxVal)
	}
}

// serializeInt64MinMax writes int64 min/max values (little-endian)
func (s *StatsSerializer) serializeInt64MinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(int64); ok {
		s.writeInt64(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(int64); ok {
		s.writeInt64(maxVal)
	}
}

// serializeUint64MinMax writes uint64 min/max values (little-endian)
func (s *StatsSerializer) serializeUint64MinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(uint64); ok {
		s.writeUint64(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(uint64); ok {
		s.writeUint64(maxVal)
	}
}

// serializeFloatMinMax writes float32 min/max values (IEEE754, little-endian)
func (s *StatsSerializer) serializeFloatMinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(float32); ok {
		s.writeFloat32(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(float32); ok {
		s.writeFloat32(maxVal)
	}
}

// serializeDoubleMinMax writes float64 min/max values (IEEE754, little-endian)
func (s *StatsSerializer) serializeDoubleMinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(float64); ok {
		s.writeFloat64(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(float64); ok {
		s.writeFloat64(maxVal)
	}
}

// serializeStringMinMax writes string min/max values with length prefixes
func (s *StatsSerializer) serializeStringMinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(string); ok {
		s.writeString(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(string); ok {
		s.writeString(maxVal)
	}
}

// serializeDateMinMax writes DATE values as int32 (days since epoch)
func (s *StatsSerializer) serializeDateMinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(int32); ok {
		s.writeInt32(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(int32); ok {
		s.writeInt32(maxVal)
	}
}

// serializeTimestampMinMax writes TIMESTAMP values as int64 (microseconds)
func (s *StatsSerializer) serializeTimestampMinMax(colStats *ColumnStatistics) {
	if minVal, ok := colStats.MinValue.(int64); ok {
		s.writeInt64(minVal)
	}
	if maxVal, ok := colStats.MaxValue.(int64); ok {
		s.writeInt64(maxVal)
	}
}

// mapTypeToID converts a duckdb.Type to DuckDB's internal LogicalType ID.
// These IDs are used in serialized statistics for type identification.
// Reference: DuckDB's LogicalType ID assignments
func (s *StatsSerializer) mapTypeToID(t dukdb.Type) uint8 {
	switch t {
	case dukdb.TYPE_INVALID:
		return 0
	case dukdb.TYPE_BOOLEAN:
		return 1
	case dukdb.TYPE_TINYINT:
		return 2
	case dukdb.TYPE_SMALLINT:
		return 3
	case dukdb.TYPE_INTEGER:
		return 4
	case dukdb.TYPE_BIGINT:
		return 5
	case dukdb.TYPE_UTINYINT:
		return 6
	case dukdb.TYPE_USMALLINT:
		return 7
	case dukdb.TYPE_UINTEGER:
		return 8
	case dukdb.TYPE_UBIGINT:
		return 9
	case dukdb.TYPE_FLOAT:
		return 10
	case dukdb.TYPE_DOUBLE:
		return 11
	case dukdb.TYPE_DECIMAL:
		return 12
	case dukdb.TYPE_VARCHAR:
		return 13
	case dukdb.TYPE_BLOB:
		return 14
	case dukdb.TYPE_DATE:
		return 15
	case dukdb.TYPE_TIME:
		return 16
	case dukdb.TYPE_TIMESTAMP:
		return 17
	case dukdb.TYPE_LIST:
		return 18
	case dukdb.TYPE_STRUCT:
		return 19
	case dukdb.TYPE_SQLNULL:
		return 20
	case dukdb.TYPE_INTERVAL:
		return 21
	default:
		return 0 // Default to unknown
	}
}

// Helper methods for writing primitive types

func (s *StatsSerializer) writeByte(b uint8) {
	if s.err != nil {
		return
	}
	s.err = s.buf.WriteByte(b)
}

func (s *StatsSerializer) writeBool(b bool) {
	if b {
		s.writeByte(1)
	} else {
		s.writeByte(0)
	}
}

func (s *StatsSerializer) writeInt16(v int16) {
	if s.err != nil {
		return
	}
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(v))
	_, s.err = s.buf.Write(buf[:])
}

func (s *StatsSerializer) writeUint16(v uint16) {
	if s.err != nil {
		return
	}
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	_, s.err = s.buf.Write(buf[:])
}

func (s *StatsSerializer) writeInt32(v int32) {
	if s.err != nil {
		return
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(v))
	_, s.err = s.buf.Write(buf[:])
}

func (s *StatsSerializer) writeUint32(v uint32) {
	if s.err != nil {
		return
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	_, s.err = s.buf.Write(buf[:])
}

func (s *StatsSerializer) writeInt64(v int64) {
	if s.err != nil {
		return
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	_, s.err = s.buf.Write(buf[:])
}

func (s *StatsSerializer) writeUint64(v uint64) {
	if s.err != nil {
		return
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	_, s.err = s.buf.Write(buf[:])
}

func (s *StatsSerializer) writeFloat32(v float32) {
	if s.err != nil {
		return
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], math.Float32bits(v))
	_, s.err = s.buf.Write(buf[:])
}

func (s *StatsSerializer) writeFloat64(v float64) {
	if s.err != nil {
		return
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
	_, s.err = s.buf.Write(buf[:])
}

// writeString writes a string with length prefix (varint-encoded length)
func (s *StatsSerializer) writeString(str string) {
	if s.err != nil {
		return
	}
	s.writeVarint(uint64(len(str)))
	_, s.err = s.buf.WriteString(str)
}

// writeVarint writes an unsigned integer using varint encoding
func (s *StatsSerializer) writeVarint(v uint64) {
	if s.err != nil {
		return
	}
	for v >= 0x80 {
		s.writeByte(uint8(v&0x7F | 0x80))
		if s.err != nil {
			return
		}
		v >>= 7
	}
	s.writeByte(uint8(v))
}

// writePropertyBegin writes a property ID marker
func (s *StatsSerializer) writePropertyBegin(propertyID uint16) {
	if s.err != nil {
		return
	}
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], propertyID)
	_, s.err = s.buf.Write(buf[:])
}

// writeMessageTerminator writes the end-of-object marker
func (s *StatsSerializer) writeMessageTerminator() {
	if s.err != nil {
		return
	}
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], 0xFFFF)
	_, s.err = s.buf.Write(buf[:])
}
