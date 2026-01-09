// Package arrow provides Apache Arrow IPC file reading and writing capabilities for dukdb-go.
// This file contains type conversion utilities between Arrow and DuckDB types.
package arrow

import (
	"fmt"
	"math/big"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Constants for time conversion.
const (
	// secondsPerDay is the number of seconds in a day.
	secondsPerDay = 86400
	// nanosecondsPerMicrosecond is the number of nanoseconds in a microsecond.
	nanosecondsPerMicrosecond = 1000
	// uuidByteLength is the byte length of a UUID value.
	uuidByteLength = 16
)

// DuckDBTypeToArrow converts a DuckDB Type to an Arrow DataType.
// This handles the basic type mapping without TypeInfo details.
// For complex types like LIST, STRUCT, MAP, this returns generic Arrow types.
//
//nolint:cyclop // Type dispatch function - complexity is inherent.
func DuckDBTypeToArrow(t dukdb.Type) (arrow.DataType, error) {
	switch t {
	case dukdb.TYPE_BOOLEAN:
		return arrow.FixedWidthTypes.Boolean, nil
	case dukdb.TYPE_TINYINT:
		return arrow.PrimitiveTypes.Int8, nil
	case dukdb.TYPE_SMALLINT:
		return arrow.PrimitiveTypes.Int16, nil
	case dukdb.TYPE_INTEGER:
		return arrow.PrimitiveTypes.Int32, nil
	case dukdb.TYPE_BIGINT:
		return arrow.PrimitiveTypes.Int64, nil
	case dukdb.TYPE_UTINYINT:
		return arrow.PrimitiveTypes.Uint8, nil
	case dukdb.TYPE_USMALLINT:
		return arrow.PrimitiveTypes.Uint16, nil
	case dukdb.TYPE_UINTEGER:
		return arrow.PrimitiveTypes.Uint32, nil
	case dukdb.TYPE_UBIGINT:
		return arrow.PrimitiveTypes.Uint64, nil
	case dukdb.TYPE_FLOAT:
		return arrow.PrimitiveTypes.Float32, nil
	case dukdb.TYPE_DOUBLE:
		return arrow.PrimitiveTypes.Float64, nil
	case dukdb.TYPE_VARCHAR:
		return arrow.BinaryTypes.String, nil
	case dukdb.TYPE_BLOB:
		return arrow.BinaryTypes.Binary, nil
	case dukdb.TYPE_DATE:
		return arrow.FixedWidthTypes.Date32, nil
	case dukdb.TYPE_TIME, dukdb.TYPE_TIME_TZ:
		return arrow.FixedWidthTypes.Time64us, nil
	case dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_TZ:
		return &arrow.TimestampType{
			Unit:     arrow.Microsecond,
			TimeZone: "UTC",
		}, nil
	case dukdb.TYPE_TIMESTAMP_S:
		return &arrow.TimestampType{
			Unit:     arrow.Second,
			TimeZone: "UTC",
		}, nil
	case dukdb.TYPE_TIMESTAMP_MS:
		return &arrow.TimestampType{
			Unit:     arrow.Millisecond,
			TimeZone: "UTC",
		}, nil
	case dukdb.TYPE_TIMESTAMP_NS:
		return &arrow.TimestampType{
			Unit:     arrow.Nanosecond,
			TimeZone: "UTC",
		}, nil
	case dukdb.TYPE_INTERVAL:
		return arrow.FixedWidthTypes.MonthDayNanoInterval, nil
	case dukdb.TYPE_UUID:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: uuidByteLength,
		}, nil
	case dukdb.TYPE_HUGEINT, dukdb.TYPE_UHUGEINT:
		// 128-bit integers represented as Decimal128 with scale 0
		return &arrow.Decimal128Type{
			Precision: 38,
			Scale:     0,
		}, nil
	case dukdb.TYPE_DECIMAL:
		// Default decimal with typical precision/scale
		return &arrow.Decimal128Type{
			Precision: 18,
			Scale:     3,
		}, nil
	case dukdb.TYPE_LIST:
		// Default to list of strings for generic list type
		return arrow.ListOf(arrow.BinaryTypes.String), nil
	case dukdb.TYPE_ARRAY:
		// Default to fixed size list of strings
		return arrow.FixedSizeListOf(1, arrow.BinaryTypes.String), nil
	case dukdb.TYPE_STRUCT:
		// Empty struct - caller should provide proper schema
		return arrow.StructOf(), nil
	case dukdb.TYPE_MAP:
		// Default to map of string -> string
		return arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), nil
	case dukdb.TYPE_ENUM:
		// ENUM represented as dictionary-encoded string
		return &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint32,
			ValueType: arrow.BinaryTypes.String,
		}, nil
	case dukdb.TYPE_BIT:
		return arrow.BinaryTypes.Binary, nil
	case dukdb.TYPE_SQLNULL:
		return arrow.Null, nil
	case dukdb.TYPE_INVALID, dukdb.TYPE_ANY:
		return nil, fmt.Errorf("unsupported type for Arrow conversion: %s", t.String())
	default:
		return nil, fmt.Errorf("unsupported type: %s", t.String())
	}
}

// ArrowTypeToDuckDB converts an Arrow DataType to a DuckDB Type.
// This leverages the existing arrowTypeToDuckDB function from the dukdb package.
func ArrowTypeToDuckDB(dt arrow.DataType) (dukdb.Type, error) {
	switch dt.ID() {
	case arrow.BOOL:
		return dukdb.TYPE_BOOLEAN, nil
	case arrow.INT8:
		return dukdb.TYPE_TINYINT, nil
	case arrow.INT16:
		return dukdb.TYPE_SMALLINT, nil
	case arrow.INT32:
		return dukdb.TYPE_INTEGER, nil
	case arrow.INT64:
		return dukdb.TYPE_BIGINT, nil
	case arrow.UINT8:
		return dukdb.TYPE_UTINYINT, nil
	case arrow.UINT16:
		return dukdb.TYPE_USMALLINT, nil
	case arrow.UINT32:
		return dukdb.TYPE_UINTEGER, nil
	case arrow.UINT64:
		return dukdb.TYPE_UBIGINT, nil
	case arrow.FLOAT32:
		return dukdb.TYPE_FLOAT, nil
	case arrow.FLOAT64:
		return dukdb.TYPE_DOUBLE, nil
	case arrow.STRING, arrow.LARGE_STRING:
		return dukdb.TYPE_VARCHAR, nil
	case arrow.BINARY, arrow.LARGE_BINARY:
		return dukdb.TYPE_BLOB, nil
	case arrow.DATE32, arrow.DATE64:
		return dukdb.TYPE_DATE, nil
	case arrow.TIME32, arrow.TIME64:
		return dukdb.TYPE_TIME, nil
	case arrow.TIMESTAMP:
		tsType, ok := dt.(*arrow.TimestampType)
		if !ok {
			return dukdb.TYPE_TIMESTAMP, nil
		}
		switch tsType.Unit {
		case arrow.Second:
			return dukdb.TYPE_TIMESTAMP_S, nil
		case arrow.Millisecond:
			return dukdb.TYPE_TIMESTAMP_MS, nil
		case arrow.Microsecond:
			return dukdb.TYPE_TIMESTAMP, nil
		case arrow.Nanosecond:
			return dukdb.TYPE_TIMESTAMP_NS, nil
		}
		return dukdb.TYPE_TIMESTAMP, nil
	case arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO:
		return dukdb.TYPE_INTERVAL, nil
	case arrow.FIXED_SIZE_BINARY:
		fsb, ok := dt.(*arrow.FixedSizeBinaryType)
		if !ok {
			return dukdb.TYPE_BLOB, nil
		}
		if fsb.ByteWidth == uuidByteLength {
			return dukdb.TYPE_UUID, nil
		}
		return dukdb.TYPE_BLOB, nil
	case arrow.DECIMAL128:
		return dukdb.TYPE_DECIMAL, nil
	case arrow.LIST, arrow.LARGE_LIST:
		return dukdb.TYPE_LIST, nil
	case arrow.FIXED_SIZE_LIST:
		return dukdb.TYPE_ARRAY, nil
	case arrow.STRUCT:
		return dukdb.TYPE_STRUCT, nil
	case arrow.MAP:
		return dukdb.TYPE_MAP, nil
	case arrow.DICTIONARY:
		// Dictionary-encoded types are typically ENUMs in DuckDB
		return dukdb.TYPE_VARCHAR, nil
	case arrow.NULL:
		return dukdb.TYPE_SQLNULL, nil
	default:
		return dukdb.TYPE_VARCHAR, fmt.Errorf("unsupported Arrow type: %s", dt.Name())
	}
}

// SchemaToColumnTypes converts an Arrow schema to DuckDB column types.
func SchemaToColumnTypes(schema *arrow.Schema) ([]string, []dukdb.Type, error) {
	fields := schema.Fields()
	names := make([]string, len(fields))
	types := make([]dukdb.Type, len(fields))

	for i, field := range fields {
		names[i] = field.Name
		typ, err := ArrowTypeToDuckDB(field.Type)
		if err != nil {
			return nil, nil, fmt.Errorf("column %s: %w", field.Name, err)
		}
		types[i] = typ
	}

	return names, types, nil
}

// RecordBatchToDataChunk converts an Arrow RecordBatch to a storage.DataChunk.
// Uses copy semantics for memory safety.
func RecordBatchToDataChunk(record arrow.RecordBatch) (*storage.DataChunk, error) {
	if record == nil {
		return nil, fmt.Errorf("record cannot be nil")
	}

	numRows := int(record.NumRows())
	numCols := int(record.NumCols())

	// Get types from schema
	types := make([]dukdb.Type, numCols)
	for i := 0; i < numCols; i++ {
		field := record.Schema().Field(i)
		typ, err := ArrowTypeToDuckDB(field.Type)
		if err != nil {
			return nil, fmt.Errorf("column %d (%s): %w", i, field.Name, err)
		}
		types[i] = typ
	}

	chunk := storage.NewDataChunkWithCapacity(types, numRows)
	chunk.SetCount(numRows)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		arr := record.Column(colIdx)
		vec := chunk.GetVector(colIdx)
		if err := arrowArrayToVector(arr, vec, numRows); err != nil {
			return nil, fmt.Errorf("column %d: %w", colIdx, err)
		}
	}

	return chunk, nil
}

// arrowArrayToVector copies values from an Arrow Array to a Vector.
func arrowArrayToVector(arr arrow.Array, vec *storage.Vector, numRows int) error {
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		if arr.IsNull(rowIdx) {
			vec.Validity().SetInvalid(rowIdx)
			continue
		}

		val := extractArrowValue(arr, rowIdx)
		vec.SetValue(rowIdx, val)
	}

	return nil
}

// extractArrowValue extracts a Go value from an Arrow array at the given index.
// Returns nil for NULL values.
//
//nolint:cyclop // Type dispatch function - complexity is inherent.
func extractArrowValue(arr arrow.Array, idx int) any {
	if arr.IsNull(idx) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(idx)
	case *array.Int8:
		return a.Value(idx)
	case *array.Int16:
		return a.Value(idx)
	case *array.Int32:
		return a.Value(idx)
	case *array.Int64:
		return a.Value(idx)
	case *array.Uint8:
		return a.Value(idx)
	case *array.Uint16:
		return a.Value(idx)
	case *array.Uint32:
		return a.Value(idx)
	case *array.Uint64:
		return a.Value(idx)
	case *array.Float32:
		return a.Value(idx)
	case *array.Float64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	case *array.LargeString:
		return a.Value(idx)
	case *array.Binary:
		return a.Value(idx)
	case *array.LargeBinary:
		return a.Value(idx)
	case *array.Date32:
		days := int64(a.Value(idx))
		return time.Unix(days*secondsPerDay, 0).UTC()
	case *array.Date64:
		millis := int64(a.Value(idx))
		return time.UnixMilli(millis).UTC()
	case *array.Time32:
		// Time32 is in seconds or milliseconds
		val := int64(a.Value(idx))
		return time.UnixMicro(val * 1000).UTC()
	case *array.Time64:
		// Time64 is in microseconds or nanoseconds
		val := int64(a.Value(idx))
		return time.UnixMicro(val).UTC()
	case *array.Timestamp:
		ts := a.Value(idx)
		tsType, ok := a.DataType().(*arrow.TimestampType)
		if !ok {
			return time.UnixMicro(int64(ts)).UTC()
		}
		switch tsType.Unit {
		case arrow.Second:
			return time.Unix(int64(ts), 0).UTC()
		case arrow.Millisecond:
			return time.UnixMilli(int64(ts)).UTC()
		case arrow.Microsecond:
			return time.UnixMicro(int64(ts)).UTC()
		case arrow.Nanosecond:
			return time.Unix(0, int64(ts)).UTC()
		}
		return time.UnixMicro(int64(ts)).UTC()
	case *array.MonthDayNanoInterval:
		val := a.Value(idx)
		return dukdb.Interval{
			Months: val.Months,
			Days:   val.Days,
			Micros: val.Nanoseconds / nanosecondsPerMicrosecond,
		}
	case *array.Decimal128:
		val := a.Value(idx)
		return val.BigInt()
	case *array.FixedSizeBinary:
		data := a.Value(idx)
		if len(data) == uuidByteLength {
			var u dukdb.UUID
			copy(u[:], data)
			return u
		}
		return data
	case *array.List:
		return extractListValue(a, idx)
	case *array.LargeList:
		return extractLargeListValue(a, idx)
	case *array.FixedSizeList:
		return extractFixedSizeListValue(a, idx)
	case *array.Struct:
		return extractStructValue(a, idx)
	case *array.Map:
		return extractMapValue(a, idx)
	default:
		// Fallback: try to get string representation
		return arr.ValueStr(idx)
	}
}

// extractListValue extracts a list value from an Arrow List array.
func extractListValue(a *array.List, idx int) []any {
	start, end := a.ValueOffsets(idx)
	child := a.ListValues()
	result := make([]any, end-start)
	for i := start; i < end; i++ {
		result[i-start] = extractArrowValue(child, int(i))
	}
	return result
}

// extractLargeListValue extracts a list value from an Arrow LargeList array.
func extractLargeListValue(a *array.LargeList, idx int) []any {
	start, end := a.ValueOffsets(idx)
	child := a.ListValues()
	result := make([]any, end-start)
	for i := start; i < end; i++ {
		result[i-start] = extractArrowValue(child, int(i))
	}
	return result
}

// extractFixedSizeListValue extracts a list value from an Arrow FixedSizeList array.
func extractFixedSizeListValue(a *array.FixedSizeList, idx int) []any {
	listSize := a.DataType().(*arrow.FixedSizeListType).Len()
	start := int64(idx) * int64(listSize)
	child := a.ListValues()
	result := make([]any, listSize)
	for i := int32(0); i < listSize; i++ {
		result[i] = extractArrowValue(child, int(start)+int(i))
	}
	return result
}

// extractStructValue extracts a struct value from an Arrow Struct array.
func extractStructValue(a *array.Struct, idx int) map[string]any {
	structType, ok := a.DataType().(*arrow.StructType)
	if !ok {
		return nil
	}
	result := make(map[string]any)
	for i, field := range structType.Fields() {
		fieldArr := a.Field(i)
		result[field.Name] = extractArrowValue(fieldArr, idx)
	}
	return result
}

// extractMapValue extracts a map value from an Arrow Map array.
func extractMapValue(a *array.Map, idx int) dukdb.Map {
	keys := a.Keys()
	items := a.Items()
	start, end := a.ValueOffsets(idx)
	result := make(dukdb.Map)
	for i := start; i < end; i++ {
		key := extractArrowValue(keys, int(i))
		value := extractArrowValue(items, int(i))
		result[key] = value
	}
	return result
}

// DataChunkToRecordBatch converts a storage.DataChunk to an Arrow RecordBatch.
// This function creates a new RecordBatch with copy semantics.
func DataChunkToRecordBatch(
	chunk *storage.DataChunk,
	schema *arrow.Schema,
	alloc memory.Allocator,
) (arrow.RecordBatch, error) {
	if chunk == nil {
		return nil, fmt.Errorf("chunk cannot be nil")
	}
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}
	if alloc == nil {
		return nil, fmt.Errorf("allocator cannot be nil")
	}

	numCols := len(schema.Fields())
	if chunk.ColumnCount() != numCols {
		return nil, fmt.Errorf(
			"column count mismatch: chunk has %d, schema has %d",
			chunk.ColumnCount(),
			numCols,
		)
	}

	arrays := make([]arrow.Array, numCols)
	defer func() {
		// Release any arrays that were created if we return an error
		for _, arr := range arrays {
			if arr != nil {
				arr.Release()
			}
		}
	}()

	for i := 0; i < numCols; i++ {
		arr, err := vectorToArrowArray(chunk, i, schema.Field(i).Type, alloc)
		if err != nil {
			return nil, fmt.Errorf("column %d (%s): %w", i, schema.Field(i).Name, err)
		}
		arrays[i] = arr
	}

	// Create record - takes ownership of arrays
	record := array.NewRecordBatch(schema, arrays, int64(chunk.Count()))

	// Clear the defer cleanup since record now owns the arrays
	for i := range arrays {
		arrays[i] = nil
	}

	return record, nil
}

// vectorToArrowArray converts a Vector column to an Arrow Array.
func vectorToArrowArray(
	chunk *storage.DataChunk,
	colIdx int,
	dt arrow.DataType,
	alloc memory.Allocator,
) (arrow.Array, error) {
	vec := chunk.GetVector(colIdx)
	size := chunk.Count()
	builder := array.NewBuilder(alloc, dt)
	defer builder.Release()

	builder.Reserve(size)

	for rowIdx := 0; rowIdx < size; rowIdx++ {
		val := vec.GetValue(rowIdx)
		if err := appendValueToBuilder(builder, val, dt); err != nil {
			return nil, fmt.Errorf("row %d: %w", rowIdx, err)
		}
	}

	return builder.NewArray(), nil
}

// appendValueToBuilder appends a Go value to an Arrow builder.
//
//nolint:cyclop // Type dispatch function - complexity is inherent.
func appendValueToBuilder(builder array.Builder, val any, dt arrow.DataType) error {
	if val == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		v, ok := val.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", val)
		}
		b.Append(v)

	case *array.Int8Builder:
		v, ok := val.(int8)
		if !ok {
			return fmt.Errorf("expected int8, got %T", val)
		}
		b.Append(v)

	case *array.Int16Builder:
		v, ok := val.(int16)
		if !ok {
			return fmt.Errorf("expected int16, got %T", val)
		}
		b.Append(v)

	case *array.Int32Builder:
		v, ok := val.(int32)
		if !ok {
			return fmt.Errorf("expected int32, got %T", val)
		}
		b.Append(v)

	case *array.Int64Builder:
		v, ok := val.(int64)
		if !ok {
			return fmt.Errorf("expected int64, got %T", val)
		}
		b.Append(v)

	case *array.Uint8Builder:
		v, ok := val.(uint8)
		if !ok {
			return fmt.Errorf("expected uint8, got %T", val)
		}
		b.Append(v)

	case *array.Uint16Builder:
		v, ok := val.(uint16)
		if !ok {
			return fmt.Errorf("expected uint16, got %T", val)
		}
		b.Append(v)

	case *array.Uint32Builder:
		v, ok := val.(uint32)
		if !ok {
			return fmt.Errorf("expected uint32, got %T", val)
		}
		b.Append(v)

	case *array.Uint64Builder:
		v, ok := val.(uint64)
		if !ok {
			return fmt.Errorf("expected uint64, got %T", val)
		}
		b.Append(v)

	case *array.Float32Builder:
		v, ok := val.(float32)
		if !ok {
			return fmt.Errorf("expected float32, got %T", val)
		}
		b.Append(v)

	case *array.Float64Builder:
		v, ok := val.(float64)
		if !ok {
			return fmt.Errorf("expected float64, got %T", val)
		}
		b.Append(v)

	case *array.StringBuilder:
		v, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", val)
		}
		b.Append(v)

	case *array.BinaryBuilder:
		v, ok := val.([]byte)
		if !ok {
			return fmt.Errorf("expected []byte, got %T", val)
		}
		b.Append(v)

	case *array.Date32Builder:
		t, ok := val.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", val)
		}
		days := int32(t.Unix() / secondsPerDay)
		b.Append(arrow.Date32(days))

	case *array.Date64Builder:
		t, ok := val.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", val)
		}
		b.Append(arrow.Date64(t.UnixMilli()))

	case *array.Time64Builder:
		t, ok := val.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", val)
		}
		micros := t.UnixMicro()
		b.Append(arrow.Time64(micros))

	case *array.TimestampBuilder:
		t, ok := val.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", val)
		}
		tsType, ok := dt.(*arrow.TimestampType)
		if !ok {
			return fmt.Errorf("expected *arrow.TimestampType, got %T", dt)
		}
		var ts arrow.Timestamp
		switch tsType.Unit {
		case arrow.Second:
			ts = arrow.Timestamp(t.Unix())
		case arrow.Millisecond:
			ts = arrow.Timestamp(t.UnixMilli())
		case arrow.Microsecond:
			ts = arrow.Timestamp(t.UnixMicro())
		case arrow.Nanosecond:
			ts = arrow.Timestamp(t.UnixNano())
		}
		b.Append(ts)

	case *array.MonthDayNanoIntervalBuilder:
		iv, ok := val.(dukdb.Interval)
		if !ok {
			return fmt.Errorf("expected Interval, got %T", val)
		}
		b.Append(arrow.MonthDayNanoInterval{
			Months:      iv.Months,
			Days:        iv.Days,
			Nanoseconds: iv.Micros * nanosecondsPerMicrosecond,
		})

	case *array.Decimal128Builder:
		switch v := val.(type) {
		case *big.Int:
			b.Append(decimal128.FromBigInt(v))
		case dukdb.Decimal:
			b.Append(decimal128.FromBigInt(v.Value))
		default:
			return fmt.Errorf("expected *big.Int or Decimal, got %T", val)
		}

	case *array.FixedSizeBinaryBuilder:
		switch v := val.(type) {
		case dukdb.UUID:
			b.Append(v[:])
		case [16]byte:
			b.Append(v[:])
		case []byte:
			b.Append(v)
		default:
			return fmt.Errorf("expected UUID or []byte, got %T", val)
		}

	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}
