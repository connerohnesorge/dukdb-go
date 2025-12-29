package dukdb

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// DataChunkToRecordBatch converts a DataChunk to an Arrow RecordBatch.
// Uses copy semantics for memory safety (Arrow and Go have different buffer management).
// The caller is responsible for releasing the returned Record.
func DataChunkToRecordBatch(chunk *DataChunk, schema *arrow.Schema, alloc memory.Allocator) (arrow.Record, error) {
	if chunk == nil {
		return nil, errors.New("chunk cannot be nil")
	}
	if schema == nil {
		return nil, errors.New("schema cannot be nil")
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}

	numCols := len(schema.Fields())
	if chunk.GetColumnCount() != numCols {
		return nil, fmt.Errorf("column count mismatch: chunk has %d, schema has %d", chunk.GetColumnCount(), numCols)
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
	record := array.NewRecord(schema, arrays, int64(chunk.GetSize()))

	// Clear the defer cleanup since record now owns the arrays
	for i := range arrays {
		arrays[i] = nil
	}

	return record, nil
}

// vectorToArrowArray converts a column from a DataChunk to an Arrow Array.
// Uses copy semantics for safety.
func vectorToArrowArray(chunk *DataChunk, colIdx int, dt arrow.DataType, alloc memory.Allocator) (arrow.Array, error) {
	size := chunk.GetSize()
	builder := array.NewBuilder(alloc, dt)
	defer builder.Release()

	builder.Reserve(size)

	for rowIdx := 0; rowIdx < size; rowIdx++ {
		val, err := chunk.GetValue(colIdx, rowIdx)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", rowIdx, err)
		}

		if err := appendValueToBuilder(builder, val, dt); err != nil {
			return nil, fmt.Errorf("row %d: %w", rowIdx, err)
		}
	}

	return builder.NewArray(), nil
}

// appendValueToBuilder appends a Go value to an Arrow builder.
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

	case *array.Time32Builder:
		t, ok := val.(time.Time)
		if !ok {
			return fmt.Errorf("expected time.Time, got %T", val)
		}
		// Convert to seconds or milliseconds since midnight
		secs := int32(t.Hour()*3600 + t.Minute()*60 + t.Second())
		b.Append(arrow.Time32(secs))

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
		tsType := dt.(*arrow.TimestampType)
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
		iv, ok := val.(Interval)
		if !ok {
			return fmt.Errorf("expected Interval, got %T", val)
		}
		b.Append(arrow.MonthDayNanoInterval{
			Months:      iv.Months,
			Days:        iv.Days,
			Nanoseconds: iv.Micros * 1000,
		})

	case *array.Decimal128Builder:
		switch v := val.(type) {
		case *big.Int:
			dec := decimal128.FromBigInt(v)
			b.Append(dec)
		case Decimal:
			dec := decimal128.FromBigInt(v.Value)
			b.Append(dec)
		case Uhugeint:
			dec := decimal128.FromBigInt(v.ToBigInt())
			b.Append(dec)
		default:
			return fmt.Errorf("expected *big.Int or Decimal, got %T", val)
		}

	case *array.FixedSizeBinaryBuilder:
		switch v := val.(type) {
		case UUID:
			b.Append(v[:])
		case [16]byte:
			b.Append(v[:])
		case []byte:
			b.Append(v)
		default:
			return fmt.Errorf("expected UUID or []byte, got %T", val)
		}

	case *array.ListBuilder:
		slice, ok := val.([]any)
		if !ok {
			return fmt.Errorf("expected []any, got %T", val)
		}
		b.Append(true)
		valueBuilder := b.ValueBuilder()
		listType := dt.(*arrow.ListType)
		for _, elem := range slice {
			if err := appendValueToBuilder(valueBuilder, elem, listType.Elem()); err != nil {
				return err
			}
		}

	case *array.FixedSizeListBuilder:
		slice, ok := val.([]any)
		if !ok {
			return fmt.Errorf("expected []any, got %T", val)
		}
		b.Append(true)
		valueBuilder := b.ValueBuilder()
		fslType := dt.(*arrow.FixedSizeListType)
		for _, elem := range slice {
			if err := appendValueToBuilder(valueBuilder, elem, fslType.Elem()); err != nil {
				return err
			}
		}

	case *array.StructBuilder:
		m, ok := val.(map[string]any)
		if !ok {
			return fmt.Errorf("expected map[string]any, got %T", val)
		}
		b.Append(true)
		structType := dt.(*arrow.StructType)
		for i, field := range structType.Fields() {
			fieldBuilder := b.FieldBuilder(i)
			if err := appendValueToBuilder(fieldBuilder, m[field.Name], field.Type); err != nil {
				return fmt.Errorf("field %s: %w", field.Name, err)
			}
		}

	case *array.MapBuilder:
		m, ok := val.(Map)
		if !ok {
			return fmt.Errorf("expected Map, got %T", val)
		}
		b.Append(true)
		keyBuilder := b.KeyBuilder()
		itemBuilder := b.ItemBuilder()
		mapType := dt.(*arrow.MapType)
		for k, v := range m {
			if err := appendValueToBuilder(keyBuilder, k, mapType.KeyType()); err != nil {
				return fmt.Errorf("map key: %w", err)
			}
			if err := appendValueToBuilder(itemBuilder, v, mapType.ItemType()); err != nil {
				return fmt.Errorf("map value: %w", err)
			}
		}

	case *array.BinaryDictionaryBuilder:
		s, ok := val.(string)
		if !ok {
			return fmt.Errorf("expected string for dictionary, got %T", val)
		}
		if err := b.AppendString(s); err != nil {
			return err
		}

	case *array.DenseUnionBuilder:
		u, ok := val.(Union)
		if !ok {
			return fmt.Errorf("expected Union, got %T", val)
		}
		unionType := dt.(*arrow.DenseUnionType)
		// Find type code for tag
		var typeCode int8 = -1
		var childType arrow.DataType
		for i, field := range unionType.Fields() {
			if field.Name == u.Tag {
				typeCode = int8(i)
				childType = field.Type
				break
			}
		}
		if typeCode < 0 {
			return fmt.Errorf("unknown union tag: %s", u.Tag)
		}
		b.Append(typeCode)
		childBuilder := b.Child(int(typeCode))
		if err := appendValueToBuilder(childBuilder, u.Value, childType); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}

// RecordBatchToDataChunk converts an Arrow RecordBatch to a DataChunk.
// The chunk must be pre-initialized with the correct column types.
// Uses copy semantics for memory safety.
func RecordBatchToDataChunk(record arrow.Record, chunk *DataChunk) error {
	if record == nil {
		return errors.New("record cannot be nil")
	}
	if chunk == nil {
		return errors.New("chunk cannot be nil")
	}

	numRows := int(record.NumRows())
	numCols := int(record.NumCols())

	if chunk.GetColumnCount() != numCols {
		return fmt.Errorf("column count mismatch: chunk has %d, record has %d", chunk.GetColumnCount(), numCols)
	}

	if numRows > GetDataChunkCapacity() {
		return fmt.Errorf("record has %d rows, exceeds chunk capacity %d", numRows, GetDataChunkCapacity())
	}

	if err := chunk.SetSize(numRows); err != nil {
		return err
	}

	for colIdx := 0; colIdx < numCols; colIdx++ {
		arr := record.Column(colIdx)
		if err := arrowArrayToChunkColumn(arr, chunk, colIdx); err != nil {
			return fmt.Errorf("column %d: %w", colIdx, err)
		}
	}

	return nil
}

// arrowArrayToChunkColumn copies values from an Arrow Array to a DataChunk column.
func arrowArrayToChunkColumn(arr arrow.Array, chunk *DataChunk, colIdx int) error {
	for rowIdx := 0; rowIdx < arr.Len(); rowIdx++ {
		val := extractArrowValueForConversion(arr, rowIdx)
		if err := chunk.SetValue(colIdx, rowIdx, val); err != nil {
			return fmt.Errorf("row %d: %w", rowIdx, err)
		}
	}
	return nil
}

// extractArrowValueForConversion extracts a Go value from an Arrow array at the given index.
// Returns nil for NULL values.
func extractArrowValueForConversion(arr arrow.Array, idx int) any {
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
		val := int64(a.Value(idx))
		return time.UnixMicro(val * 1000).UTC()
	case *array.Time64:
		val := int64(a.Value(idx))
		return time.UnixMicro(val).UTC()
	case *array.Timestamp:
		ts := a.Value(idx)
		tsType := a.DataType().(*arrow.TimestampType)
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
		return Interval{
			Months: val.Months,
			Days:   val.Days,
			Micros: val.Nanoseconds / 1000,
		}
	case *array.Decimal128:
		val := a.Value(idx)
		return val.BigInt()
	case *array.FixedSizeBinary:
		data := a.Value(idx)
		if len(data) == 16 {
			var u UUID
			copy(u[:], data)
			return u
		}
		return data
	case *array.List:
		start, end := a.ValueOffsets(idx)
		child := a.ListValues()
		result := make([]any, end-start)
		for i := start; i < end; i++ {
			result[i-start] = extractArrowValueForConversion(child, int(i))
		}
		return result
	case *array.LargeList:
		start, end := a.ValueOffsets(idx)
		child := a.ListValues()
		result := make([]any, end-start)
		for i := start; i < end; i++ {
			result[i-start] = extractArrowValueForConversion(child, int(i))
		}
		return result
	case *array.FixedSizeList:
		listSize := a.DataType().(*arrow.FixedSizeListType).Len()
		start := int64(idx) * int64(listSize)
		child := a.ListValues()
		result := make([]any, listSize)
		for i := int32(0); i < listSize; i++ {
			result[i] = extractArrowValueForConversion(child, int(start)+int(i))
		}
		return result
	case *array.Struct:
		structType := a.DataType().(*arrow.StructType)
		result := make(map[string]any)
		for i, field := range structType.Fields() {
			fieldArr := a.Field(i)
			result[field.Name] = extractArrowValueForConversion(fieldArr, idx)
		}
		return result
	case *array.Map:
		keys := a.Keys()
		items := a.Items()
		start, end := a.ValueOffsets(idx)
		result := make(Map)
		for i := start; i < end; i++ {
			key := extractArrowValueForConversion(keys, int(i))
			value := extractArrowValueForConversion(items, int(i))
			result[key] = value
		}
		return result
	case *array.DenseUnion:
		typeCode := a.TypeCode(idx)
		childArr := a.Field(int(typeCode))
		// Get the value offset for this type
		childIdx := a.ValueOffset(idx)
		unionType := a.DataType().(*arrow.DenseUnionType)
		tagName := unionType.Fields()[typeCode].Name
		value := extractArrowValueForConversion(childArr, int(childIdx))
		return Union{Tag: tagName, Value: value}
	default:
		// Fallback: try to get string representation
		return arr.ValueStr(idx)
	}
}

// SchemaFromTypes creates an Arrow schema from DuckDB type information.
func SchemaFromTypes(names []string, types []TypeInfo) (*arrow.Schema, error) {
	if len(names) != len(types) {
		return nil, fmt.Errorf("names and types length mismatch: %d vs %d", len(names), len(types))
	}

	fields := make([]arrow.Field, len(types))
	for i, typeInfo := range types {
		arrowType, err := duckdbTypeToArrow(typeInfo)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", names[i], err)
		}
		fields[i] = arrow.Field{
			Name:     names[i],
			Type:     arrowType,
			Nullable: true,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// TypesFromSchema extracts DuckDB type information from an Arrow schema.
func TypesFromSchema(schema *arrow.Schema) ([]TypeInfo, error) {
	fields := schema.Fields()
	types := make([]TypeInfo, len(fields))

	for i, field := range fields {
		typeInfo, err := arrowTypeToDuckDB(field.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", field.Name, err)
		}
		types[i] = typeInfo
	}

	return types, nil
}
