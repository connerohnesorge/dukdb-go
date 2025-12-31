package dukdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/coder/quartz"
)

// Arrow provides Arrow-format query results from a DuckDB connection.
// Arrow connections are NOT safe for concurrent use and do NOT benefit
// from database/sql connection pooling. Use dedicated connections for streaming.
type Arrow struct {
	conn  *Conn
	clock quartz.Clock
	alloc memory.Allocator
}

// NewArrowFromConn creates an Arrow interface from a driver connection.
// The driverConn must be a *dukdb.Conn obtained from sql.Conn.Raw().
func NewArrowFromConn(
	driverConn driver.Conn,
) (*Arrow, error) {
	conn, ok := driverConn.(*Conn)
	if !ok {
		return nil, errors.New(
			"driverConn must be a *dukdb.Conn",
		)
	}

	return &Arrow{
		conn:  conn,
		clock: quartz.NewReal(),
		alloc: memory.DefaultAllocator,
	}, nil
}

// WithClock returns a new Arrow instance with the given clock for deterministic testing.
func (a *Arrow) WithClock(
	clock quartz.Clock,
) *Arrow {
	return &Arrow{
		conn:  a.conn,
		clock: clock,
		alloc: a.alloc,
	}
}

// WithAllocator returns a new Arrow instance with the given memory allocator.
func (a *Arrow) WithAllocator(
	alloc memory.Allocator,
) *Arrow {
	return &Arrow{
		conn:  a.conn,
		clock: a.clock,
		alloc: alloc,
	}
}

// Clock returns the clock used for temporal type conversions.
func (a *Arrow) Clock() quartz.Clock {
	return a.clock
}

// QueryContext executes a query and returns an Arrow RecordReader for streaming results.
// The caller is responsible for calling Release() on the returned RecordReader.
func (a *Arrow) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (array.RecordReader, error) {
	// Check context deadline using clock
	if deadline, ok := ctx.Deadline(); ok {
		if a.clock.Until(deadline) <= 0 {
			return nil, context.DeadlineExceeded
		}
	}

	// Convert args to driver.NamedValue
	namedArgs := make(
		[]driver.NamedValue,
		len(args),
	)
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}

	// Execute query
	rows, err := a.conn.QueryContext(
		ctx,
		query,
		namedArgs,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"arrow query failed: %w",
			err,
		)
	}

	// Get column types
	driverRows, ok := rows.(*Rows)
	if !ok {
		return nil, errors.New(
			"unexpected rows type",
		)
	}

	// Build Arrow schema from column info
	schema, err := a.buildSchema(driverRows)
	if err != nil {
		_ = driverRows.Close()

		return nil, fmt.Errorf(
			"failed to build Arrow schema: %w",
			err,
		)
	}

	return newArrowRecordReader(
		a,
		driverRows,
		schema,
	), nil
}

// Query is a convenience method that calls QueryContext with context.Background().
func (a *Arrow) Query(
	query string,
	args ...any,
) (array.RecordReader, error) {
	return a.QueryContext(
		context.Background(),
		query,
		args...)
}

// buildSchema builds an Arrow schema from DuckDB row information.
func (a *Arrow) buildSchema(
	rows *Rows,
) (*arrow.Schema, error) {
	colNames := rows.Columns()
	colTypes := rows.columnTypes()

	fields := make([]arrow.Field, len(colNames))
	for i, name := range colNames {
		arrowType, err := duckdbTypeToArrow(
			colTypes[i],
		)
		if err != nil {
			return nil, fmt.Errorf(
				"column %s: %w",
				name,
				err,
			)
		}
		fields[i] = arrow.Field{
			Name:     name,
			Type:     arrowType,
			Nullable: true, // DuckDB columns are nullable by default
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// arrowRecordReader implements array.RecordReader for streaming Arrow results.
type arrowRecordReader struct {
	arrow       *Arrow
	rows        *Rows
	schema      *arrow.Schema
	current     arrow.RecordBatch
	err         error
	closed      bool
	mu          sync.Mutex
	refCount    int64
	chunkSize   int
	colTypes    []TypeInfo
	colBuilders []array.Builder
}

// newArrowRecordReader creates a new record reader.
func newArrowRecordReader(
	a *Arrow,
	rows *Rows,
	schema *arrow.Schema,
) *arrowRecordReader {
	return &arrowRecordReader{
		arrow:     a,
		rows:      rows,
		schema:    schema,
		refCount:  1,
		chunkSize: GetDataChunkCapacity(),
		colTypes:  rows.columnTypes(),
	}
}

// Schema returns the Arrow schema for the result set.
func (r *arrowRecordReader) Schema() *arrow.Schema {
	return r.schema
}

// Next advances to the next record batch.
// Returns true if a record is available, false otherwise.
func (r *arrowRecordReader) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed || r.err != nil {
		return false
	}

	// Release previous record
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}

	// Build next record batch
	record, err := r.buildNextBatch()
	if err != nil {
		r.err = err

		return false
	}

	if record == nil {
		return false
	}

	r.current = record

	return true
}

// RecordBatch returns the current record batch.
// Returns nil if Next() has not been called or returned false.
func (r *arrowRecordReader) RecordBatch() arrow.RecordBatch {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.current
}

// Record returns the current record batch (deprecated, use RecordBatch).
func (r *arrowRecordReader) Record() arrow.RecordBatch {
	return r.RecordBatch()
}

// Err returns any error that occurred during iteration.
func (r *arrowRecordReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.err
}

// Retain increases the reference count.
func (r *arrowRecordReader) Retain() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refCount++
}

// Release decreases the reference count and releases resources when it reaches zero.
func (r *arrowRecordReader) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.refCount--
	if r.refCount <= 0 {
		r.close()
	}
}

// close releases all resources.
func (r *arrowRecordReader) close() {
	if r.closed {
		return
	}
	r.closed = true

	if r.current != nil {
		r.current.Release()
		r.current = nil
	}

	if r.rows != nil {
		_ = r.rows.Close()
		r.rows = nil
	}

	// Release builders
	for _, builder := range r.colBuilders {
		if builder != nil {
			builder.Release()
		}
	}
	r.colBuilders = nil
}

// buildNextBatch builds the next record batch from rows.
func (r *arrowRecordReader) buildNextBatch() (arrow.RecordBatch, error) {
	// Initialize builders if needed
	if r.colBuilders == nil {
		r.colBuilders = make(
			[]array.Builder,
			len(r.schema.Fields()),
		)
		for i, field := range r.schema.Fields() {
			r.colBuilders[i] = array.NewBuilder(
				r.arrow.alloc,
				field.Type,
			)
		}
	}

	// Reset builders
	for _, builder := range r.colBuilders {
		builder.Reserve(r.chunkSize)
	}

	// Read rows into builders
	rowCount := 0
	dest := make(
		[]driver.Value,
		len(r.schema.Fields()),
	)

	for rowCount < r.chunkSize {
		err := r.rows.Next(dest)
		if err != nil {
			// Check for EOF
			if rowCount == 0 {
				return nil, nil // No more data
			}

			break
		}

		// Append values to builders
		for i, val := range dest {
			if err := appendToBuilder(r.colBuilders[i], val, r.colTypes[i], r.arrow.clock); err != nil {
				return nil, fmt.Errorf(
					"column %d: %w",
					i,
					err,
				)
			}
		}
		rowCount++
	}

	if rowCount == 0 {
		return nil, nil
	}

	// Build arrays from builders
	arrays := make(
		[]arrow.Array,
		len(r.colBuilders),
	)
	for i, builder := range r.colBuilders {
		arrays[i] = builder.NewArray()
	}

	// Create record batch
	return array.NewRecordBatch(
		r.schema,
		arrays,
		int64(rowCount),
	), nil
}

// duckdbTypeToArrow converts a DuckDB TypeInfo to an Arrow DataType.
func duckdbTypeToArrow(
	typeInfo TypeInfo,
) (arrow.DataType, error) {
	t := typeInfo.InternalType()

	switch t {
	case TYPE_BOOLEAN:
		return arrow.FixedWidthTypes.Boolean, nil
	case TYPE_TINYINT:
		return arrow.PrimitiveTypes.Int8, nil
	case TYPE_SMALLINT:
		return arrow.PrimitiveTypes.Int16, nil
	case TYPE_INTEGER:
		return arrow.PrimitiveTypes.Int32, nil
	case TYPE_BIGINT:
		return arrow.PrimitiveTypes.Int64, nil
	case TYPE_UTINYINT:
		return arrow.PrimitiveTypes.Uint8, nil
	case TYPE_USMALLINT:
		return arrow.PrimitiveTypes.Uint16, nil
	case TYPE_UINTEGER:
		return arrow.PrimitiveTypes.Uint32, nil
	case TYPE_UBIGINT:
		return arrow.PrimitiveTypes.Uint64, nil
	case TYPE_FLOAT:
		return arrow.PrimitiveTypes.Float32, nil
	case TYPE_DOUBLE:
		return arrow.PrimitiveTypes.Float64, nil
	case TYPE_VARCHAR:
		return arrow.BinaryTypes.String, nil
	case TYPE_BLOB:
		return arrow.BinaryTypes.Binary, nil
	case TYPE_DATE:
		return arrow.FixedWidthTypes.Date32, nil
	case TYPE_TIME, TYPE_TIME_TZ:
		return arrow.FixedWidthTypes.Time64us, nil
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		return &arrow.TimestampType{
			Unit:     arrow.Microsecond,
			TimeZone: "UTC",
		}, nil
	case TYPE_TIMESTAMP_S:
		return &arrow.TimestampType{
			Unit:     arrow.Second,
			TimeZone: "UTC",
		}, nil
	case TYPE_TIMESTAMP_MS:
		return &arrow.TimestampType{
			Unit:     arrow.Millisecond,
			TimeZone: "UTC",
		}, nil
	case TYPE_TIMESTAMP_NS:
		return &arrow.TimestampType{
			Unit:     arrow.Nanosecond,
			TimeZone: "UTC",
		}, nil
	case TYPE_INTERVAL:
		return arrow.FixedWidthTypes.MonthDayNanoInterval, nil
	case TYPE_UUID:
		return &arrow.FixedSizeBinaryType{
			ByteWidth: 16,
		}, nil
	case TYPE_HUGEINT, TYPE_UHUGEINT:
		// 128-bit integers represented as Decimal128 with scale 0
		return &arrow.Decimal128Type{
			Precision: 38,
			Scale:     0,
		}, nil
	case TYPE_DECIMAL:
		details, ok := typeInfo.Details().(*DecimalDetails)
		if !ok {
			return nil, errors.New(
				"expected DecimalDetails for DECIMAL type",
			)
		}

		return &arrow.Decimal128Type{
			Precision: int32(details.Width),
			Scale:     int32(details.Scale),
		}, nil
	case TYPE_LIST:
		details, ok := typeInfo.Details().(*ListDetails)
		if !ok {
			return nil, errors.New(
				"expected ListDetails for LIST type",
			)
		}
		childType, err := duckdbTypeToArrow(
			details.Child,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"list child: %w",
				err,
			)
		}

		return arrow.ListOf(childType), nil
	case TYPE_ARRAY:
		details, ok := typeInfo.Details().(*ArrayDetails)
		if !ok {
			return nil, errors.New(
				"expected ArrayDetails for ARRAY type",
			)
		}
		childType, err := duckdbTypeToArrow(
			details.Child,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"array child: %w",
				err,
			)
		}

		return arrow.FixedSizeListOf(
			int32(details.Size),
			childType,
		), nil
	case TYPE_STRUCT:
		details, ok := typeInfo.Details().(*StructDetails)
		if !ok {
			return nil, errors.New(
				"expected StructDetails for STRUCT type",
			)
		}
		fields := make(
			[]arrow.Field,
			len(details.Entries),
		)
		for i, entry := range details.Entries {
			childType, err := duckdbTypeToArrow(
				entry.Info(),
			)
			if err != nil {
				return nil, fmt.Errorf(
					"struct field %s: %w",
					entry.Name(),
					err,
				)
			}
			fields[i] = arrow.Field{
				Name:     entry.Name(),
				Type:     childType,
				Nullable: true,
			}
		}

		return arrow.StructOf(fields...), nil
	case TYPE_MAP:
		details, ok := typeInfo.Details().(*MapDetails)
		if !ok {
			return nil, errors.New(
				"expected MapDetails for MAP type",
			)
		}
		keyType, err := duckdbTypeToArrow(
			details.Key,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"map key: %w",
				err,
			)
		}
		valueType, err := duckdbTypeToArrow(
			details.Value,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"map value: %w",
				err,
			)
		}

		return arrow.MapOf(
			keyType,
			valueType,
		), nil
	case TYPE_ENUM:
		// ENUM represented as dictionary-encoded string
		return &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint32,
			ValueType: arrow.BinaryTypes.String,
		}, nil
	case TYPE_BIT:
		return arrow.BinaryTypes.Binary, nil
	case TYPE_UNION:
		details, ok := typeInfo.Details().(*UnionDetails)
		if !ok {
			return nil, errors.New(
				"expected UnionDetails for UNION type",
			)
		}
		fields := make(
			[]arrow.Field,
			len(details.Members),
		)
		typeCodes := make(
			[]arrow.UnionTypeCode,
			len(details.Members),
		)
		for i, member := range details.Members {
			memberType, err := duckdbTypeToArrow(
				member.Type,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"union member %s: %w",
					member.Name,
					err,
				)
			}
			fields[i] = arrow.Field{
				Name:     member.Name,
				Type:     memberType,
				Nullable: true,
			}
			typeCodes[i] = arrow.UnionTypeCode(i)
		}

		return arrow.DenseUnionOf(
			fields,
			typeCodes,
		), nil
	case TYPE_SQLNULL:
		return arrow.Null, nil
	default:
		return nil, fmt.Errorf(
			"unsupported type: %s",
			t.String(),
		)
	}
}

// arrowTypeToDuckDB converts an Arrow DataType to a DuckDB TypeInfo.
// This is the reverse of duckdbTypeToArrow.
func arrowTypeToDuckDB(
	dt arrow.DataType,
) (TypeInfo, error) {
	switch dt.ID() {
	case arrow.BOOL:
		return NewTypeInfo(TYPE_BOOLEAN)
	case arrow.INT8:
		return NewTypeInfo(TYPE_TINYINT)
	case arrow.INT16:
		return NewTypeInfo(TYPE_SMALLINT)
	case arrow.INT32:
		return NewTypeInfo(TYPE_INTEGER)
	case arrow.INT64:
		return NewTypeInfo(TYPE_BIGINT)
	case arrow.UINT8:
		return NewTypeInfo(TYPE_UTINYINT)
	case arrow.UINT16:
		return NewTypeInfo(TYPE_USMALLINT)
	case arrow.UINT32:
		return NewTypeInfo(TYPE_UINTEGER)
	case arrow.UINT64:
		return NewTypeInfo(TYPE_UBIGINT)
	case arrow.FLOAT32:
		return NewTypeInfo(TYPE_FLOAT)
	case arrow.FLOAT64:
		return NewTypeInfo(TYPE_DOUBLE)
	case arrow.STRING, arrow.LARGE_STRING:
		return NewTypeInfo(TYPE_VARCHAR)
	case arrow.BINARY, arrow.LARGE_BINARY:
		return NewTypeInfo(TYPE_BLOB)
	case arrow.DATE32, arrow.DATE64:
		return NewTypeInfo(TYPE_DATE)
	case arrow.TIME32, arrow.TIME64:
		return NewTypeInfo(TYPE_TIME)
	case arrow.TIMESTAMP:
		tsType := dt.(*arrow.TimestampType)
		switch tsType.Unit {
		case arrow.Second:
			return NewTypeInfo(TYPE_TIMESTAMP_S)
		case arrow.Millisecond:
			return NewTypeInfo(TYPE_TIMESTAMP_MS)
		case arrow.Microsecond:
			return NewTypeInfo(TYPE_TIMESTAMP)
		case arrow.Nanosecond:
			return NewTypeInfo(TYPE_TIMESTAMP_NS)
		}

		return NewTypeInfo(TYPE_TIMESTAMP)
	case arrow.INTERVAL_MONTHS,
		arrow.INTERVAL_DAY_TIME,
		arrow.INTERVAL_MONTH_DAY_NANO:
		return NewTypeInfo(TYPE_INTERVAL)
	case arrow.FIXED_SIZE_BINARY:
		fsb := dt.(*arrow.FixedSizeBinaryType)
		if fsb.ByteWidth == 16 {
			return NewTypeInfo(TYPE_UUID)
		}

		return NewTypeInfo(TYPE_BLOB)
	case arrow.DECIMAL128:
		dec := dt.(*arrow.Decimal128Type)

		return NewDecimalInfo(
			uint8(dec.Precision),
			uint8(dec.Scale),
		)
	case arrow.LIST, arrow.LARGE_LIST:
		var elemType arrow.DataType
		switch lt := dt.(type) {
		case *arrow.ListType:
			elemType = lt.Elem()
		case *arrow.LargeListType:
			elemType = lt.Elem()
		}
		childInfo, err := arrowTypeToDuckDB(
			elemType,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"list element: %w",
				err,
			)
		}

		return NewListInfo(childInfo)
	case arrow.FIXED_SIZE_LIST:
		fsl := dt.(*arrow.FixedSizeListType)
		childInfo, err := arrowTypeToDuckDB(
			fsl.Elem(),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"array element: %w",
				err,
			)
		}

		return NewArrayInfo(
			childInfo,
			uint64(fsl.Len()),
		)
	case arrow.STRUCT:
		st := dt.(*arrow.StructType)
		if len(st.Fields()) == 0 {
			return nil, errors.New(
				"empty struct type not supported",
			)
		}
		entries := make(
			[]StructEntry,
			len(st.Fields()),
		)
		for i, field := range st.Fields() {
			fieldInfo, err := arrowTypeToDuckDB(
				field.Type,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"struct field %s: %w",
					field.Name,
					err,
				)
			}
			entry, err := NewStructEntry(
				fieldInfo,
				field.Name,
			)
			if err != nil {
				return nil, err
			}
			entries[i] = entry
		}

		return NewStructInfo(
			entries[0],
			entries[1:]...)
	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		keyInfo, err := arrowTypeToDuckDB(
			mt.KeyType(),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"map key: %w",
				err,
			)
		}
		valueInfo, err := arrowTypeToDuckDB(
			mt.ItemType(),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"map value: %w",
				err,
			)
		}

		return NewMapInfo(keyInfo, valueInfo)
	case arrow.DICTIONARY:
		// Dictionary-encoded types are typically ENUMs in DuckDB
		// For simplicity, treat as VARCHAR
		return NewTypeInfo(TYPE_VARCHAR)
	case arrow.NULL:
		// NULL type doesn't have a direct NewTypeInfo, use VARCHAR as fallback
		return NewTypeInfo(TYPE_VARCHAR)
	default:
		return nil, fmt.Errorf(
			"unsupported Arrow type: %s",
			dt.Name(),
		)
	}
}

// appendToBuilder appends a value to an Arrow array builder.
func appendToBuilder(
	builder array.Builder,
	val any,
	typeInfo TypeInfo,
	clock quartz.Clock,
) error {
	if val == nil {
		builder.AppendNull()

		return nil
	}

	t := typeInfo.InternalType()

	switch t {
	case TYPE_BOOLEAN:
		b := builder.(*array.BooleanBuilder)
		b.Append(val.(bool))
	case TYPE_TINYINT:
		b := builder.(*array.Int8Builder)
		b.Append(val.(int8))
	case TYPE_SMALLINT:
		b := builder.(*array.Int16Builder)
		b.Append(val.(int16))
	case TYPE_INTEGER:
		b := builder.(*array.Int32Builder)
		b.Append(val.(int32))
	case TYPE_BIGINT:
		b := builder.(*array.Int64Builder)
		b.Append(val.(int64))
	case TYPE_UTINYINT:
		b := builder.(*array.Uint8Builder)
		b.Append(val.(uint8))
	case TYPE_USMALLINT:
		b := builder.(*array.Uint16Builder)
		b.Append(val.(uint16))
	case TYPE_UINTEGER:
		b := builder.(*array.Uint32Builder)
		b.Append(val.(uint32))
	case TYPE_UBIGINT:
		b := builder.(*array.Uint64Builder)
		b.Append(val.(uint64))
	case TYPE_FLOAT:
		b := builder.(*array.Float32Builder)
		b.Append(val.(float32))
	case TYPE_DOUBLE:
		b := builder.(*array.Float64Builder)
		b.Append(val.(float64))
	case TYPE_VARCHAR:
		b := builder.(*array.StringBuilder)
		b.Append(val.(string))
	case TYPE_BLOB:
		b := builder.(*array.BinaryBuilder)
		b.Append(val.([]byte))
	case TYPE_DATE:
		b := builder.(*array.Date32Builder)
		t := val.(time.Time)
		days := int32(t.Unix() / secondsPerDay)
		b.Append(arrow.Date32(days))
	case TYPE_TIME, TYPE_TIME_TZ:
		b := builder.(*array.Time64Builder)
		t := val.(time.Time)
		micros := t.UnixMicro()
		b.Append(arrow.Time64(micros))
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		b := builder.(*array.TimestampBuilder)
		t := val.(time.Time)
		b.Append(arrow.Timestamp(t.UnixMicro()))
	case TYPE_TIMESTAMP_S:
		b := builder.(*array.TimestampBuilder)
		t := val.(time.Time)
		b.Append(arrow.Timestamp(t.Unix()))
	case TYPE_TIMESTAMP_MS:
		b := builder.(*array.TimestampBuilder)
		t := val.(time.Time)
		b.Append(arrow.Timestamp(t.UnixMilli()))
	case TYPE_TIMESTAMP_NS:
		b := builder.(*array.TimestampBuilder)
		t := val.(time.Time)
		b.Append(arrow.Timestamp(t.UnixNano()))
	case TYPE_INTERVAL:
		b := builder.(*array.MonthDayNanoIntervalBuilder)
		iv := val.(Interval)
		b.Append(arrow.MonthDayNanoInterval{
			Months:      iv.Months,
			Days:        iv.Days,
			Nanoseconds: iv.Micros * 1000, // Convert micros to nanos
		})
	case TYPE_UUID:
		b := builder.(*array.FixedSizeBinaryBuilder)
		u := val.(UUID)
		b.Append(u[:])
	case TYPE_HUGEINT:
		b := builder.(*array.Decimal128Builder)
		bigInt := val.(*big.Int)
		dec := decimal128.FromBigInt(bigInt)
		b.Append(dec)
	case TYPE_UHUGEINT:
		b := builder.(*array.Decimal128Builder)
		u := val.(Uhugeint)
		bigInt := u.ToBigInt()
		dec := decimal128.FromBigInt(bigInt)
		b.Append(dec)
	case TYPE_DECIMAL:
		b := builder.(*array.Decimal128Builder)
		d := val.(Decimal)
		dec := decimal128.FromBigInt(d.Value)
		b.Append(dec)
	case TYPE_LIST:
		b := builder.(*array.ListBuilder)
		slice := val.([]any)
		b.Append(true)
		childBuilder := b.ValueBuilder()
		childTypeInfo := typeInfo.Details().(*ListDetails).Child
		for _, elem := range slice {
			if err := appendToBuilder(childBuilder, elem, childTypeInfo, clock); err != nil {
				return err
			}
		}
	case TYPE_ARRAY:
		b := builder.(*array.FixedSizeListBuilder)
		slice := val.([]any)
		b.Append(true)
		childBuilder := b.ValueBuilder()
		childTypeInfo := typeInfo.Details().(*ArrayDetails).Child
		for _, elem := range slice {
			if err := appendToBuilder(childBuilder, elem, childTypeInfo, clock); err != nil {
				return err
			}
		}
	case TYPE_STRUCT:
		b := builder.(*array.StructBuilder)
		m := val.(map[string]any)
		b.Append(true)
		details := typeInfo.Details().(*StructDetails)
		for i, entry := range details.Entries {
			fieldBuilder := b.FieldBuilder(i)
			if err := appendToBuilder(fieldBuilder, m[entry.Name()], entry.Info(), clock); err != nil {
				return err
			}
		}
	case TYPE_MAP:
		b := builder.(*array.MapBuilder)
		m := val.(Map)
		b.Append(true)
		keyBuilder := b.KeyBuilder()
		itemBuilder := b.ItemBuilder()
		details := typeInfo.Details().(*MapDetails)
		for k, v := range m {
			if err := appendToBuilder(keyBuilder, k, details.Key, clock); err != nil {
				return err
			}
			if err := appendToBuilder(itemBuilder, v, details.Value, clock); err != nil {
				return err
			}
		}
	case TYPE_ENUM:
		b := builder.(*array.BinaryDictionaryBuilder)
		s := val.(string)
		if err := b.AppendString(s); err != nil {
			return err
		}
	case TYPE_BIT:
		b := builder.(*array.BinaryBuilder)
		bit := val.(Bit)
		b.Append(bit.data)
	case TYPE_UNION:
		b := builder.(*array.DenseUnionBuilder)
		u := val.(Union)
		details := typeInfo.Details().(*UnionDetails)
		// Find the type code for this tag
		var typeCode int8 = -1
		var memberType TypeInfo
		for i, member := range details.Members {
			if member.Name == u.Tag {
				typeCode = int8(i)
				memberType = member.Type

				break
			}
		}
		if typeCode < 0 {
			return fmt.Errorf(
				"unknown union tag: %s",
				u.Tag,
			)
		}
		b.Append(typeCode)
		childBuilder := b.Child(int(typeCode))
		if err := appendToBuilder(childBuilder, u.Value, memberType, clock); err != nil {
			return err
		}
	case TYPE_SQLNULL:
		builder.AppendNull()
	default:
		return fmt.Errorf(
			"unsupported type for Arrow conversion: %s",
			t.String(),
		)
	}

	return nil
}

// arrowSchemaToColumns converts an Arrow schema to a slice of VirtualColumnDef.
func arrowSchemaToColumns(
	schema *arrow.Schema,
) ([]VirtualColumnDef, error) {
	fields := schema.Fields()
	columns := make(
		[]VirtualColumnDef,
		len(fields),
	)

	for i, field := range fields {
		typeInfo, err := arrowTypeToDuckDB(
			field.Type,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"column %s: %w",
				field.Name,
				err,
			)
		}

		columns[i] = VirtualColumnDef{
			Name:     field.Name,
			Type:     typeInfo.InternalType(),
			TypeInfo: typeInfo,
			Nullable: field.Nullable,
		}
	}

	return columns, nil
}

// arrowVirtualTable implements VirtualTable for Arrow RecordReaders.
type arrowVirtualTable struct {
	name    string
	reader  array.RecordReader
	schema  *arrow.Schema
	columns []VirtualColumnDef
	clock   quartz.Clock
	mu      sync.Mutex
}

// Name returns the table name.
func (t *arrowVirtualTable) Name() string {
	return t.name
}

// Schema returns the table columns.
func (t *arrowVirtualTable) Schema() []VirtualColumnDef {
	return t.columns
}

// Scan returns an iterator over the table rows.
func (t *arrowVirtualTable) Scan() (RowIterator, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return &arrowRowIterator{
		reader:  t.reader,
		columns: t.columns,
		clock:   t.clock,
	}, nil
}

// arrowRowIterator implements RowIterator for Arrow RecordReaders.
type arrowRowIterator struct {
	reader  array.RecordReader
	columns []VirtualColumnDef
	clock   quartz.Clock

	batch   arrow.Record
	rowIdx  int64
	numRows int64
	values  []any
	err     error
	closed  bool
}

// Next advances to the next row. Returns false when done.
func (it *arrowRowIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	// Move to next row in current batch
	it.rowIdx++

	// If we've exhausted the current batch, get the next one
	for it.batch == nil || it.rowIdx >= it.numRows {
		// Release previous batch
		if it.batch != nil {
			it.batch.Release()
			it.batch = nil
		}

		// Get next batch
		if !it.reader.Next() {
			if err := it.reader.Err(); err != nil {
				it.err = err
			}

			return false
		}

		it.batch = it.reader.Record()
		if it.batch != nil {
			it.batch.Retain()
			it.numRows = it.batch.NumRows()
			it.rowIdx = 0
		}
	}

	// Extract values for current row
	it.values = it.extractRowValues()

	return true
}

// extractRowValues extracts values from the current row in the batch.
func (it *arrowRowIterator) extractRowValues() []any {
	if it.batch == nil {
		return nil
	}

	numCols := int(it.batch.NumCols())
	values := make([]any, numCols)

	for col := range numCols {
		arr := it.batch.Column(col)
		if arr.IsNull(int(it.rowIdx)) {
			values[col] = nil

			continue
		}

		values[col] = extractArrowValue(
			arr,
			int(it.rowIdx),
			it.columns[col].TypeInfo,
			it.clock,
		)
	}

	return values
}

// extractArrowValue extracts a value from an Arrow array at the given index.
func extractArrowValue(
	arr arrow.Array,
	idx int,
	typeInfo TypeInfo,
	clock quartz.Clock,
) any {
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
		childTypeInfo := typeInfo.Details().(*ListDetails).Child
		for i := start; i < end; i++ {
			result[i-start] = extractArrowValue(child, int(i), childTypeInfo, clock)
		}

		return result
	case *array.LargeList:
		start, end := a.ValueOffsets(idx)
		child := a.ListValues()
		result := make([]any, end-start)
		childTypeInfo := typeInfo.Details().(*ListDetails).Child
		for i := start; i < end; i++ {
			result[i-start] = extractArrowValue(child, int(i), childTypeInfo, clock)
		}

		return result
	case *array.FixedSizeList:
		listSize := a.DataType().(*arrow.FixedSizeListType).Len()
		start := int64(idx) * int64(listSize)
		child := a.ListValues()
		result := make([]any, listSize)
		childTypeInfo := typeInfo.Details().(*ArrayDetails).Child
		for i := range listSize {
			result[i] = extractArrowValue(child, int(start)+int(i), childTypeInfo, clock)
		}

		return result
	case *array.Struct:
		result := make(map[string]any)
		structDetails := typeInfo.Details().(*StructDetails)
		for i, entry := range structDetails.Entries {
			field := a.Field(i)
			result[entry.Name()] = extractArrowValue(field, idx, entry.Info(), clock)
		}

		return result
	case *array.Map:
		keys := a.Keys()
		items := a.Items()
		start, end := a.ValueOffsets(idx)
		result := make(Map)
		mapDetails := typeInfo.Details().(*MapDetails)
		for i := start; i < end; i++ {
			key := extractArrowValue(keys, int(i), mapDetails.Key, clock)
			value := extractArrowValue(items, int(i), mapDetails.Value, clock)
			result[key] = value
		}

		return result
	default:
		// Fallback: try to get string representation
		return arr.ValueStr(idx)
	}
}

// Values returns the current row values.
func (it *arrowRowIterator) Values() []any {
	return it.values
}

// Err returns any error that occurred during iteration.
func (it *arrowRowIterator) Err() error {
	return it.err
}

// Close releases resources held by the iterator.
func (it *arrowRowIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true

	if it.batch != nil {
		it.batch.Release()
		it.batch = nil
	}

	return nil
}

// RegisterView registers an Arrow RecordReader as a queryable view.
// The view can be queried with SQL until the returned release function is called.
// The caller must call the release function when done to free Arrow resources.
func (a *Arrow) RegisterView(
	reader array.RecordReader,
	name string,
) (release func(), err error) {
	if name == "" {
		return nil, errors.New(
			"view name cannot be empty",
		)
	}

	// Convert Arrow schema to DuckDB columns
	columns, err := arrowSchemaToColumns(
		reader.Schema(),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to convert Arrow schema: %w",
			err,
		)
	}

	// Create virtual table
	vt := &arrowVirtualTable{
		name:    name,
		reader:  reader,
		schema:  reader.Schema(),
		columns: columns,
		clock:   a.clock,
	}

	// Get virtual table registry from connection
	registry := a.conn.getVirtualTableRegistry()
	if registry == nil {
		return nil, errors.New(
			"connection does not support virtual tables",
		)
	}

	// Register in catalog
	if err := registry.RegisterVirtualTable(vt); err != nil {
		return nil, err
	}

	// Return release function
	release = func() {
		_ = registry.UnregisterVirtualTable(name)
		reader.Release()
	}

	return release, nil
}

// Compile-time assertions
var (
	_ array.RecordReader = (*arrowRecordReader)(
		nil,
	)
	_ VirtualTable = (*arrowVirtualTable)(
		nil,
	)
	_ RowIterator = (*arrowRowIterator)(
		nil,
	)
)
