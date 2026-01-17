package optimizer

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	dukdb "github.com/dukdb/dukdb-go"
)

// StatsDeserializer provides methods to deserialize table and column statistics
// from DuckDB v1.4.3 binary format.
//
// This deserializer is the inverse of StatsSerializer and must be able to read
// statistics created by DuckDB v1.4.3 as well as statistics created by dukdb-go.
// See stats_serializer.go for detailed format documentation.
type StatsDeserializer struct {
	r           io.Reader
	err         error
	readBytes   int64
	maxReadSize int64 // Prevent DoS attacks on corrupted data
}

// NewStatsDeserializer creates a new statistics deserializer.
// maxReadSize limits the maximum amount of data that can be read (prevents memory exhaustion).
// If maxReadSize <= 0, no limit is enforced.
func NewStatsDeserializer(r io.Reader, maxReadSize int64) *StatsDeserializer {
	return &StatsDeserializer{
		r:           r,
		maxReadSize: maxReadSize,
	}
}

// Err returns any error encountered during deserialization.
func (d *StatsDeserializer) Err() error {
	return d.err
}

// DeserializeTableStatistics deserializes table statistics from DuckDB binary format.
// Returns the deserialized statistics and any error encountered.
// On error, returns conservative default statistics.
func (d *StatsDeserializer) DeserializeTableStatistics() (*TableStatistics, error) {
	if d.err != nil {
		return nil, d.err
	}

	// Read and validate version
	version, err := d.readByte()
	if err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}

	// Version 0 = DuckDB format, Version 1 = dukdb-go format
	if version != 0 && version != 1 {
		return nil, fmt.Errorf("unsupported statistics version: %d", version)
	}

	// Read row count
	rowCount, err := d.readVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read row count: %w", err)
	}

	// Read column count
	colCount, err := d.readVarint()
	if err != nil {
		return nil, fmt.Errorf("failed to read column count: %w", err)
	}

	// Sanity check: column count shouldn't be unreasonably large
	if colCount > 10000 {
		return nil, fmt.Errorf("column count exceeds reasonable limit: %d", colCount)
	}

	stats := &TableStatistics{
		RowCount: int64(rowCount),
		Columns:  make([]ColumnStatistics, colCount),
	}

	// Deserialize each column's statistics
	for i := 0; i < int(colCount); i++ {
		// Read column name
		colName, err := d.readString()
		if err != nil {
			// Log error but continue with defaults for this column
			stats.Columns[i] = ColumnStatistics{}
			continue
		}

		colStats, err := d.deserializeColumnStatisticsInner()
		if err != nil {
			// Log error but continue with defaults for this column
			stats.Columns[i] = ColumnStatistics{ColumnName: colName}
			continue
		}
		colStats.ColumnName = colName
		stats.Columns[i] = colStats
	}

	return stats, nil
}

// deserializeColumnStatisticsInner deserializes a single column's statistics.
// This reads Property 100 (BaseStatistics), Property 101 (DistinctStatistics), and terminator.
func (d *StatsDeserializer) deserializeColumnStatisticsInner() (ColumnStatistics, error) {
	colStats := ColumnStatistics{
		DistinctCount: DefaultDistinctCount,
	}

	// Read properties until we hit message terminator
	for {
		// Read property ID
		propertyID, err := d.readUint16()
		if err != nil {
			return colStats, fmt.Errorf("failed to read property ID: %w", err)
		}

		// Check for message terminator (0xFFFF)
		if propertyID == 0xFFFF {
			break
		}

		switch propertyID {
		case 100:
			// BaseStatistics
			stats, err := d.deserializeBaseStatistics()
			if err != nil {
				return colStats, fmt.Errorf("failed to deserialize base statistics: %w", err)
			}
			colStats = stats

		case 101:
			// DistinctStatistics
			err := d.deserializeDistinctStatistics(&colStats)
			if err != nil {
				// Non-fatal: continue without distinct statistics
				_ = err
			}

		default:
			// Unknown property: skip it
			// In property-based serialization, unknown properties are safely skipped
			// This allows forward compatibility with future DuckDB versions
		}
	}

	return colStats, nil
}

// deserializeBaseStatistics reads a BaseStatistics component.
// Format: LogicalType + has_null + has_no_null + distinct_count + type-specific min/max
func (d *StatsDeserializer) deserializeBaseStatistics() (ColumnStatistics, error) {
	colStats := ColumnStatistics{
		DistinctCount: DefaultDistinctCount,
	}

	// Read logical type ID
	typeID, err := d.readByte()
	if err != nil {
		return colStats, fmt.Errorf("failed to read type ID: %w", err)
	}

	// Map type ID back to duckdb.Type
	colStats.ColumnType = d.mapIDToType(typeID)

	// Read NULL handling flags
	hasNull, err := d.readBool()
	if err != nil {
		return colStats, fmt.Errorf("failed to read has_null: %w", err)
	}

	hasNoNull, err := d.readBool()
	if err != nil {
		return colStats, fmt.Errorf("failed to read has_no_null: %w", err)
	}

	// Compute null fraction from flags
	if !hasNull && !hasNoNull {
		// Empty result
		colStats.NullFraction = 0.0
	} else if hasNull && !hasNoNull {
		// All NULL
		colStats.NullFraction = 1.0
	} else if !hasNull && hasNoNull {
		// No NULL
		colStats.NullFraction = 0.0
	} else {
		// Has both NULL and non-NULL: estimate ~10% NULL
		colStats.NullFraction = 0.1
	}

	// Read distinct count
	distinctCount, err := d.readVarint()
	if err != nil {
		return colStats, fmt.Errorf("failed to read distinct count: %w", err)
	}
	colStats.DistinctCount = int64(distinctCount)

	// Deserialize type-specific min/max values
	err = d.deserializeTypeSpecificStats(&colStats)
	if err != nil {
		return colStats, fmt.Errorf("failed to deserialize type-specific stats: %w", err)
	}

	return colStats, nil
}

// deserializeDistinctStatistics reads a DistinctStatistics component.
// This updates the column statistics with distinct value estimation information.
func (d *StatsDeserializer) deserializeDistinctStatistics(colStats *ColumnStatistics) error {
	// Read sample count
	sampleCount, err := d.readVarint()
	if err != nil {
		return fmt.Errorf("failed to read sample count: %w", err)
	}
	_ = sampleCount // Currently unused in dukdb-go

	// Read total count
	totalCount, err := d.readVarint()
	if err != nil {
		return fmt.Errorf("failed to read total count: %w", err)
	}
	_ = totalCount // Currently unused in dukdb-go

	// HyperLogLog state would be read here if implemented
	// For now, skip to next property

	return nil
}

// deserializeTypeSpecificStats reads type-specific min/max values.
//
//nolint:exhaustive // We only handle types that have min/max values
func (d *StatsDeserializer) deserializeTypeSpecificStats(colStats *ColumnStatistics) error {
	switch colStats.ColumnType {
	case dukdb.TYPE_TINYINT:
		return d.deserializeInt8MinMax(colStats)
	case dukdb.TYPE_UTINYINT:
		return d.deserializeUint8MinMax(colStats)
	case dukdb.TYPE_SMALLINT:
		return d.deserializeInt16MinMax(colStats)
	case dukdb.TYPE_USMALLINT:
		return d.deserializeUint16MinMax(colStats)
	case dukdb.TYPE_INTEGER:
		return d.deserializeInt32MinMax(colStats)
	case dukdb.TYPE_UINTEGER:
		return d.deserializeUint32MinMax(colStats)
	case dukdb.TYPE_BIGINT:
		return d.deserializeInt64MinMax(colStats)
	case dukdb.TYPE_UBIGINT:
		return d.deserializeUint64MinMax(colStats)
	case dukdb.TYPE_FLOAT:
		return d.deserializeFloatMinMax(colStats)
	case dukdb.TYPE_DOUBLE:
		return d.deserializeDoubleMinMax(colStats)
	case dukdb.TYPE_VARCHAR:
		return d.deserializeStringMinMax(colStats)
	case dukdb.TYPE_DATE:
		return d.deserializeDateMinMax(colStats)
	case dukdb.TYPE_TIMESTAMP:
		return d.deserializeTimestampMinMax(colStats)
	default:
		// No type-specific stats for this type
		return nil
	}
}

// deserializeInt8MinMax reads int8 min/max values
func (d *StatsDeserializer) deserializeInt8MinMax(colStats *ColumnStatistics) error {
	minByte, err := d.readByte()
	if err != nil {
		return err
	}
	colStats.MinValue = int8(minByte)

	maxByte, err := d.readByte()
	if err != nil {
		return err
	}
	colStats.MaxValue = int8(maxByte)

	return nil
}

// deserializeUint8MinMax reads uint8 min/max values
func (d *StatsDeserializer) deserializeUint8MinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readByte()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readByte()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeInt16MinMax reads int16 min/max values (little-endian)
func (d *StatsDeserializer) deserializeInt16MinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readInt16()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readInt16()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeUint16MinMax reads uint16 min/max values (little-endian)
func (d *StatsDeserializer) deserializeUint16MinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readUint16()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readUint16()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeInt32MinMax reads int32 min/max values (little-endian)
func (d *StatsDeserializer) deserializeInt32MinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readInt32()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readInt32()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeUint32MinMax reads uint32 min/max values (little-endian)
func (d *StatsDeserializer) deserializeUint32MinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readUint32()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readUint32()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeInt64MinMax reads int64 min/max values (little-endian)
func (d *StatsDeserializer) deserializeInt64MinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readInt64()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readInt64()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeUint64MinMax reads uint64 min/max values (little-endian)
func (d *StatsDeserializer) deserializeUint64MinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readUint64()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readUint64()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeFloatMinMax reads float32 min/max values (IEEE754, little-endian)
func (d *StatsDeserializer) deserializeFloatMinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readFloat32()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readFloat32()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeDoubleMinMax reads float64 min/max values (IEEE754, little-endian)
func (d *StatsDeserializer) deserializeDoubleMinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readFloat64()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readFloat64()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeStringMinMax reads string min/max values with length prefixes
func (d *StatsDeserializer) deserializeStringMinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readString()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readString()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeDateMinMax reads DATE values as int32 (days since epoch)
func (d *StatsDeserializer) deserializeDateMinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readInt32()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readInt32()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// deserializeTimestampMinMax reads TIMESTAMP values as int64 (microseconds)
func (d *StatsDeserializer) deserializeTimestampMinMax(colStats *ColumnStatistics) error {
	minVal, err := d.readInt64()
	if err != nil {
		return err
	}
	colStats.MinValue = minVal

	maxVal, err := d.readInt64()
	if err != nil {
		return err
	}
	colStats.MaxValue = maxVal

	return nil
}

// mapIDToType converts DuckDB's internal LogicalType ID back to dukdb.Type.
func (d *StatsDeserializer) mapIDToType(typeID uint8) dukdb.Type {
	switch typeID {
	case 0:
		return dukdb.TYPE_INVALID
	case 1:
		return dukdb.TYPE_BOOLEAN
	case 2:
		return dukdb.TYPE_TINYINT
	case 3:
		return dukdb.TYPE_SMALLINT
	case 4:
		return dukdb.TYPE_INTEGER
	case 5:
		return dukdb.TYPE_BIGINT
	case 6:
		return dukdb.TYPE_UTINYINT
	case 7:
		return dukdb.TYPE_USMALLINT
	case 8:
		return dukdb.TYPE_UINTEGER
	case 9:
		return dukdb.TYPE_UBIGINT
	case 10:
		return dukdb.TYPE_FLOAT
	case 11:
		return dukdb.TYPE_DOUBLE
	case 12:
		return dukdb.TYPE_DECIMAL
	case 13:
		return dukdb.TYPE_VARCHAR
	case 14:
		return dukdb.TYPE_BLOB
	case 15:
		return dukdb.TYPE_DATE
	case 16:
		return dukdb.TYPE_TIME
	case 17:
		return dukdb.TYPE_TIMESTAMP
	case 18:
		return dukdb.TYPE_LIST
	case 19:
		return dukdb.TYPE_STRUCT
	case 20:
		return dukdb.TYPE_SQLNULL
	case 21:
		return dukdb.TYPE_INTERVAL
	default:
		return dukdb.TYPE_INVALID
	}
}

// Helper methods for reading primitive types

func (d *StatsDeserializer) checkReadSize(n int64) error {
	if d.maxReadSize > 0 {
		d.readBytes += n
		if d.readBytes > d.maxReadSize {
			return fmt.Errorf("read size exceeded: %d > %d", d.readBytes, d.maxReadSize)
		}
	}
	return nil
}

func (d *StatsDeserializer) readByte() (uint8, error) {
	if d.err != nil {
		return 0, d.err
	}
	if err := d.checkReadSize(1); err != nil {
		d.err = err
		return 0, err
	}

	var buf [1]byte
	_, err := io.ReadFull(d.r, buf[:])
	if err != nil {
		d.err = err
		return 0, err
	}
	return buf[0], nil
}

func (d *StatsDeserializer) readBool() (bool, error) {
	b, err := d.readByte()
	return b != 0, err
}

func (d *StatsDeserializer) readInt16() (int16, error) {
	v, err := d.readUint16()
	return int16(v), err
}

func (d *StatsDeserializer) readUint16() (uint16, error) {
	if d.err != nil {
		return 0, d.err
	}
	if err := d.checkReadSize(2); err != nil {
		d.err = err
		return 0, err
	}

	var buf [2]byte
	_, err := io.ReadFull(d.r, buf[:])
	if err != nil {
		d.err = err
		return 0, err
	}
	return binary.LittleEndian.Uint16(buf[:]), nil
}

func (d *StatsDeserializer) readInt32() (int32, error) {
	v, err := d.readUint32()
	return int32(v), err
}

func (d *StatsDeserializer) readUint32() (uint32, error) {
	if d.err != nil {
		return 0, d.err
	}
	if err := d.checkReadSize(4); err != nil {
		d.err = err
		return 0, err
	}

	var buf [4]byte
	_, err := io.ReadFull(d.r, buf[:])
	if err != nil {
		d.err = err
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func (d *StatsDeserializer) readInt64() (int64, error) {
	v, err := d.readUint64()
	return int64(v), err
}

func (d *StatsDeserializer) readUint64() (uint64, error) {
	if d.err != nil {
		return 0, d.err
	}
	if err := d.checkReadSize(8); err != nil {
		d.err = err
		return 0, err
	}

	var buf [8]byte
	_, err := io.ReadFull(d.r, buf[:])
	if err != nil {
		d.err = err
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[:]), nil
}

func (d *StatsDeserializer) readFloat32() (float32, error) {
	v, err := d.readUint32()
	return math.Float32frombits(uint32(v)), err
}

func (d *StatsDeserializer) readFloat64() (float64, error) {
	v, err := d.readUint64()
	return math.Float64frombits(v), err
}

// readString reads a string with length prefix (varint-encoded length)
func (d *StatsDeserializer) readString() (string, error) {
	length, err := d.readVarint()
	if err != nil {
		return "", err
	}

	if length > 1024*1024 { // Sanity check: no string > 1MB
		return "", fmt.Errorf("string length exceeds limit: %d", length)
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(d.r, buf)
	if err != nil {
		d.err = err
		return "", err
	}

	d.readBytes += int64(length)
	if d.maxReadSize > 0 && d.readBytes > d.maxReadSize {
		d.err = fmt.Errorf("read size exceeded: %d > %d", d.readBytes, d.maxReadSize)
		return "", d.err
	}

	return string(buf), nil
}

// readVarint reads an unsigned integer using varint encoding
func (d *StatsDeserializer) readVarint() (uint64, error) {
	var result uint64
	var shift uint

	for {
		b, err := d.readByte()
		if err != nil {
			return 0, err
		}

		result |= uint64(b&0x7F) << shift

		if b&0x80 == 0 {
			return result, nil
		}

		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint overflow")
		}
	}
}
