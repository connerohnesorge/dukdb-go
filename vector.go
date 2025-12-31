package dukdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"
	"unsafe"
)

// VectorSize is the default number of values in a vector (matching DuckDB's VECTOR_SIZE).
const VectorSize = 2048

// bitsPerWord is the number of bits in a uint64 (used for validity bitmaps).
const bitsPerWord = 64

// fnGetVectorValue is a callback function type for getting values from a vector.
type fnGetVectorValue func(vec *vector, rowIdx int) any

// fnSetVectorValue is a callback function type for setting values in a vector.
type fnSetVectorValue func(vec *vector, rowIdx int, val any) error

// vectorTypeInfo holds type-specific metadata for a vector.
type vectorTypeInfo struct {
	// Type is the DuckDB type of this vector.
	Type Type

	// structEntries holds field metadata for STRUCT types.
	structEntries []StructEntry

	// decimalWidth is the precision for DECIMAL types.
	decimalWidth uint8

	// decimalScale is the scale for DECIMAL types.
	decimalScale uint8

	// arrayLength is the fixed size for ARRAY types.
	arrayLength int

	// internalType is the internal storage type for ENUM and DECIMAL.
	internalType Type

	// namesDict maps enum/union names to their indices.
	namesDict map[string]uint32

	// tagDict maps union tag indices to names.
	tagDict map[uint32]string
}

// vector is the internal columnar storage of a DuckDB column.
// It stores data in a type-specific slice with a validity bitmap for NULL handling.
type vector struct {
	// Type metadata.
	vectorTypeInfo

	// dataSlice holds the actual typed data (e.g., []int64, []float64, []string).
	dataSlice any

	// maskBits is the validity bitmap. Bit = 1 means valid, bit = 0 means NULL.
	maskBits []uint64

	// getFn is a callback to get a value from this vector.
	getFn fnGetVectorValue

	// setFn is a callback to set a value in this vector.
	setFn fnSetVectorValue

	// childVectors holds child vectors for nested types (LIST, STRUCT, MAP, ARRAY, UNION).
	childVectors []vector

	// listOffsets stores offsets for LIST type entries.
	listOffsets []uint64

	// capacity is the maximum number of values this vector can hold.
	capacity int
}

// newVector creates a new vector with the specified capacity.
func newVector(capacity int) *vector {
	maskWords := (capacity + bitsPerWord - 1) / bitsPerWord
	mask := make([]uint64, maskWords)
	// Initialize all bits to 1 (valid) by default.
	for i := range mask {
		mask[i] = ^uint64(0)
	}

	return &vector{
		capacity: capacity,
		maskBits: mask,
	}
}

// isNull checks if the value at rowIdx is NULL.
func (vec *vector) isNull(rowIdx int) bool {
	wordIdx := rowIdx / bitsPerWord
	bitIdx := rowIdx % bitsPerWord

	return (vec.maskBits[wordIdx] & (1 << bitIdx)) == 0
}

// setNull marks the value at rowIdx as NULL.
func (vec *vector) setNull(rowIdx int) {
	wordIdx := rowIdx / bitsPerWord
	bitIdx := rowIdx % bitsPerWord
	vec.maskBits[wordIdx] &^= (1 << bitIdx)
}

// setValid marks the value at rowIdx as valid (not NULL).
func (vec *vector) setValid(rowIdx int) {
	wordIdx := rowIdx / bitsPerWord
	bitIdx := rowIdx % bitsPerWord
	vec.maskBits[wordIdx] |= (1 << bitIdx)
}

// getNull checks if the value at rowIdx is NULL (alias for isNull).
func (vec *vector) getNull(rowIdx int) bool {
	return vec.isNull(rowIdx)
}

// clearMask resets all values to NULL.
// Currently unused but kept for future use in batch operations.
func (vec *vector) clearMask() {
	for i := range vec.maskBits {
		vec.maskBits[i] = 0
	}
}

var _ = (*vector).clearMask // Suppress unused warning.

// fillMask sets all values to valid (not NULL).
func (vec *vector) fillMask() {
	for i := range vec.maskBits {
		vec.maskBits[i] = ^uint64(0)
	}
}

// init initializes the vector based on a TypeInfo.
func (vec *vector) init(typeInfo TypeInfo, colIdx int) error {
	t := typeInfo.InternalType()

	// Check for unsupported types.
	switch t {
	case TYPE_INVALID, TYPE_ANY, TYPE_BIGNUM:
		return fmt.Errorf("column index %d: unsupported type %s", colIdx, t.String())
	}

	vec.Type = t

	switch t {
	case TYPE_BOOLEAN:
		initBoolVec(vec)
	case TYPE_TINYINT:
		initNumericVec[int8](vec, t)
	case TYPE_SMALLINT:
		initNumericVec[int16](vec, t)
	case TYPE_INTEGER:
		initNumericVec[int32](vec, t)
	case TYPE_BIGINT:
		initNumericVec[int64](vec, t)
	case TYPE_UTINYINT:
		initNumericVec[uint8](vec, t)
	case TYPE_USMALLINT:
		initNumericVec[uint16](vec, t)
	case TYPE_UINTEGER:
		initNumericVec[uint32](vec, t)
	case TYPE_UBIGINT:
		initNumericVec[uint64](vec, t)
	case TYPE_FLOAT:
		initNumericVec[float32](vec, t)
	case TYPE_DOUBLE:
		initNumericVec[float64](vec, t)
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ:
		vec.initTimestamp(t)
	case TYPE_DATE:
		vec.initDate()
	case TYPE_TIME, TYPE_TIME_TZ:
		vec.initTime(t)
	case TYPE_INTERVAL:
		vec.initInterval()
	case TYPE_HUGEINT:
		vec.initHugeint()
	case TYPE_UHUGEINT:
		vec.initUhugeint()
	case TYPE_VARCHAR:
		vec.initVarchar()
	case TYPE_BLOB:
		vec.initBlob()
	case TYPE_DECIMAL:
		return vec.initDecimal(typeInfo, colIdx)
	case TYPE_ENUM:
		return vec.initEnum(typeInfo, colIdx)
	case TYPE_LIST:
		return vec.initList(typeInfo, colIdx)
	case TYPE_STRUCT:
		return vec.initStruct(typeInfo, colIdx)
	case TYPE_MAP:
		return vec.initMap(typeInfo, colIdx)
	case TYPE_ARRAY:
		return vec.initArray(typeInfo, colIdx)
	case TYPE_UNION:
		return vec.initUnion(typeInfo, colIdx)
	case TYPE_BIT:
		vec.initBit()
	case TYPE_UUID:
		vec.initUUID()
	case TYPE_SQLNULL:
		vec.initSQLNull()
	default:
		return fmt.Errorf("column index %d: unknown type %s", colIdx, t.String())
	}

	return nil
}

// Primitive type helpers using generics and unsafe pointers.

// numericType is a constraint for numeric types.
type numericType interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

// getPrimitive gets a primitive value from the vector at the given index.
func getPrimitive[T any](vec *vector, rowIdx int) T {
	slice := vec.dataSlice.([]T)

	return slice[rowIdx]
}

// setPrimitive sets a primitive value in the vector at the given index.
func setPrimitive[T any](vec *vector, rowIdx int, val T) {
	slice := vec.dataSlice.([]T)
	slice[rowIdx] = val
}

// initBoolVec initializes a boolean vector.
func initBoolVec(vec *vector) {
	vec.dataSlice = make([]bool, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return getPrimitive[bool](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setBool(vec, rowIdx, val)
	}
	vec.Type = TYPE_BOOLEAN
}

// setBool sets a boolean value with type coercion.
func setBool(vec *vector, rowIdx int, val any) error {
	switch v := val.(type) {
	case bool:
		vec.setValid(rowIdx)
		setPrimitive(vec, rowIdx, v)

		return nil
	default:
		return fmt.Errorf("cannot convert %T to bool", val)
	}
}

// initNumericVec initializes a numeric vector of type T.
func initNumericVec[T numericType](vec *vector, t Type) {
	vec.dataSlice = make([]T, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return getPrimitive[T](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setNumeric[T](vec, rowIdx, val)
	}
	vec.Type = t
}

// setNumeric sets a numeric value with type coercion.
func setNumeric[T numericType](vec *vector, rowIdx int, val any) error {
	var result T
	var err error

	switch v := val.(type) {
	case int:
		result = T(v)
	case int8:
		result = T(v)
	case int16:
		result = T(v)
	case int32:
		result = T(v)
	case int64:
		result = T(v)
	case uint:
		result = T(v)
	case uint8:
		result = T(v)
	case uint16:
		result = T(v)
	case uint32:
		result = T(v)
	case uint64:
		result = T(v)
	case float32:
		result = T(v)
	case float64:
		result = T(v)
	default:
		err = fmt.Errorf("cannot convert %T to numeric type", val)
	}

	if err != nil {
		return err
	}
	vec.setValid(rowIdx)
	setPrimitive(vec, rowIdx, result)

	return nil
}

// Timestamp type initialization.
func (vec *vector) initTimestamp(t Type) {
	vec.dataSlice = make([]int64, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return vec.getTimestamp(t, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setTimestamp(vec, t, rowIdx, val)
	}
	vec.Type = t
}

func (vec *vector) getTimestamp(t Type, rowIdx int) time.Time {
	micros := getPrimitive[int64](vec, rowIdx)
	switch t {
	case TYPE_TIMESTAMP_S:
		return time.Unix(micros, 0).UTC()
	case TYPE_TIMESTAMP_MS:
		return time.UnixMilli(micros).UTC()
	case TYPE_TIMESTAMP_NS:
		return time.Unix(0, micros).UTC()
	default: // TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ
		return time.UnixMicro(micros).UTC()
	}
}

func setTimestamp(vec *vector, t Type, rowIdx int, val any) error {
	ti, ok := val.(time.Time)
	if !ok {
		return fmt.Errorf("cannot convert %T to time.Time", val)
	}

	var micros int64
	switch t {
	case TYPE_TIMESTAMP_S:
		micros = ti.Unix()
	case TYPE_TIMESTAMP_MS:
		micros = ti.UnixMilli()
	case TYPE_TIMESTAMP_NS:
		micros = ti.UnixNano()
	default:
		micros = ti.UnixMicro()
	}

	vec.setValid(rowIdx)
	setPrimitive(vec, rowIdx, micros)

	return nil
}

// Date type initialization.
func (vec *vector) initDate() {
	vec.dataSlice = make([]int32, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return vec.getDate(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setDate(vec, rowIdx, val)
	}
	vec.Type = TYPE_DATE
}

func (vec *vector) getDate(rowIdx int) time.Time {
	days := getPrimitive[int32](vec, rowIdx)

	return time.Unix(int64(days)*secondsPerDay, 0).UTC()
}

func setDate(vec *vector, rowIdx int, val any) error {
	ti, ok := val.(time.Time)
	if !ok {
		return fmt.Errorf("cannot convert %T to time.Time for DATE", val)
	}
	days := int32(ti.Unix() / secondsPerDay)
	vec.setValid(rowIdx)
	setPrimitive(vec, rowIdx, days)

	return nil
}

// Time type initialization.
func (vec *vector) initTime(t Type) {
	vec.dataSlice = make([]int64, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return vec.getTime(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setTime(vec, rowIdx, val)
	}
	vec.Type = t
}

func (vec *vector) getTime(rowIdx int) time.Time {
	micros := getPrimitive[int64](vec, rowIdx)
	// Convert microseconds since midnight to time.Time.
	return time.UnixMicro(micros).UTC()
}

func setTime(vec *vector, rowIdx int, val any) error {
	ti, ok := val.(time.Time)
	if !ok {
		return fmt.Errorf("cannot convert %T to time.Time for TIME", val)
	}
	// Store as microseconds since midnight.
	base := time.Date(1970, time.January, 1, ti.Hour(), ti.Minute(), ti.Second(), ti.Nanosecond(), time.UTC)
	micros := base.UnixMicro()
	vec.setValid(rowIdx)
	setPrimitive(vec, rowIdx, micros)

	return nil
}

// Interval type initialization.
func (vec *vector) initInterval() {
	vec.dataSlice = make([]Interval, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return getPrimitive[Interval](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setInterval(vec, rowIdx, val)
	}
	vec.Type = TYPE_INTERVAL
}

func setInterval(vec *vector, rowIdx int, val any) error {
	interval, ok := val.(Interval)
	if !ok {
		return fmt.Errorf("cannot convert %T to Interval", val)
	}
	vec.setValid(rowIdx)
	setPrimitive(vec, rowIdx, interval)

	return nil
}

// Hugeint type initialization.
func (vec *vector) initHugeint() {
	vec.dataSlice = make([]hugeInt, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		h := getPrimitive[hugeInt](vec, rowIdx)

		return hugeIntToBigInt(h)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setHugeint(vec, rowIdx, val)
	}
	vec.Type = TYPE_HUGEINT
}

func setHugeint(vec *vector, rowIdx int, val any) error {
	var b *big.Int
	switch v := val.(type) {
	case *big.Int:
		b = v
	case int64:
		b = big.NewInt(v)
	case int:
		b = big.NewInt(int64(v))
	default:
		return fmt.Errorf("cannot convert %T to *big.Int for HUGEINT", val)
	}

	h, err := bigIntToHugeInt(b)
	if err != nil {
		return err
	}
	vec.setValid(rowIdx)
	setPrimitive(vec, rowIdx, h)

	return nil
}

// Uhugeint (128-bit unsigned) type initialization.
func (vec *vector) initUhugeint() {
	vec.dataSlice = make([]Uhugeint, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return getPrimitive[Uhugeint](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setUhugeint(vec, rowIdx, val)
	}
	vec.Type = TYPE_UHUGEINT
}

func setUhugeint(vec *vector, rowIdx int, val any) error {
	var u Uhugeint
	var err error

	switch v := val.(type) {
	case Uhugeint:
		u = v
	case *Uhugeint:
		u = *v
	case *big.Int:
		u, err = NewUhugeint(v)
		if err != nil {
			return err
		}
	case uint64:
		u = NewUhugeintFromUint64(v)
	case int64:
		if v < 0 {
			return fmt.Errorf("cannot convert negative int64 to UHUGEINT")
		}
		u = NewUhugeintFromUint64(uint64(v))
	case int:
		if v < 0 {
			return fmt.Errorf("cannot convert negative int to UHUGEINT")
		}
		u = NewUhugeintFromUint64(uint64(v))
	default:
		return fmt.Errorf("cannot convert %T to Uhugeint for UHUGEINT", val)
	}

	vec.setValid(rowIdx)
	setPrimitive(vec, rowIdx, u)

	return nil
}

// Bit (variable-length bit string) type initialization.
func (vec *vector) initBit() {
	vec.dataSlice = make([]Bit, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return getPrimitive[Bit](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setBit(vec, rowIdx, val)
	}
	vec.Type = TYPE_BIT
}

func setBit(vec *vector, rowIdx int, val any) error {
	var b Bit
	var err error

	switch v := val.(type) {
	case Bit:
		b = v
	case *Bit:
		b = *v
	case string:
		b, err = NewBit(v)
		if err != nil {
			return err
		}
	case []byte:
		// Interpret as a bit string.
		b, err = NewBit(string(v))
		if err != nil {
			// If parsing fails, treat as raw bytes.
			b = NewBitFromBytes(v, len(v)*8)
		}
	default:
		return fmt.Errorf("cannot convert %T to Bit for BIT", val)
	}

	vec.setValid(rowIdx)
	setPrimitive(vec, rowIdx, b)

	return nil
}

// VARCHAR type initialization.
func (vec *vector) initVarchar() {
	vec.dataSlice = make([]string, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return getPrimitive[string](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setVarchar(vec, rowIdx, val)
	}
	vec.Type = TYPE_VARCHAR
}

func setVarchar(vec *vector, rowIdx int, val any) error {
	switch v := val.(type) {
	case string:
		vec.setValid(rowIdx)
		setPrimitive(vec, rowIdx, v)

		return nil
	case []byte:
		vec.setValid(rowIdx)
		setPrimitive(vec, rowIdx, string(v))

		return nil
	default:
		return fmt.Errorf("cannot convert %T to string", val)
	}
}

// BLOB type initialization.
func (vec *vector) initBlob() {
	vec.dataSlice = make([][]byte, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return getPrimitive[[]byte](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setBlob(vec, rowIdx, val)
	}
	vec.Type = TYPE_BLOB
}

func setBlob(vec *vector, rowIdx int, val any) error {
	switch v := val.(type) {
	case []byte:
		vec.setValid(rowIdx)
		setPrimitive(vec, rowIdx, v)

		return nil
	case string:
		vec.setValid(rowIdx)
		setPrimitive(vec, rowIdx, []byte(v))

		return nil
	default:
		return fmt.Errorf("cannot convert %T to []byte", val)
	}
}

// initJSON initializes a JSON vector (VARCHAR with JSON parsing).
// Currently unused but reserved for JSON alias type support.
func (vec *vector) initJSON() {
	vec.dataSlice = make([]string, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		s := getPrimitive[string](vec, rowIdx)
		var result any
		if err := json.Unmarshal([]byte(s), &result); err != nil {
			return s // Return raw string on parse failure.
		}

		return result
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setJSON(vec, rowIdx, val)
	}
	vec.Type = TYPE_VARCHAR
}

// Suppress unused warnings for JSON support functions.
var (
	_ = (*vector).initJSON
	_ = setJSON
)

func setJSON(vec *vector, rowIdx int, val any) error {
	var s string
	switch v := val.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		// Marshal to JSON.
		b, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("cannot marshal %T to JSON: %w", val, err)
		}
		s = string(b)
	}
	vec.setValid(rowIdx)
	setPrimitive(vec, rowIdx, s)

	return nil
}

// DECIMAL type initialization.
func (vec *vector) initDecimal(typeInfo TypeInfo, colIdx int) error {
	details, ok := typeInfo.Details().(*DecimalDetails)
	if !ok {
		return fmt.Errorf("column index %d: expected DecimalDetails for DECIMAL type", colIdx)
	}

	vec.decimalWidth = details.Width
	vec.decimalScale = details.Scale

	// Determine internal type based on width.
	switch {
	case details.Width <= 4:
		vec.internalType = TYPE_SMALLINT
		vec.dataSlice = make([]int16, vec.capacity)
	case details.Width <= 9:
		vec.internalType = TYPE_INTEGER
		vec.dataSlice = make([]int32, vec.capacity)
	case details.Width <= 18:
		vec.internalType = TYPE_BIGINT
		vec.dataSlice = make([]int64, vec.capacity)
	default:
		vec.internalType = TYPE_HUGEINT
		vec.dataSlice = make([]hugeInt, vec.capacity)
	}

	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return vec.getDecimal(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setDecimal(vec, rowIdx, val)
	}
	vec.Type = TYPE_DECIMAL

	return nil
}

func (vec *vector) getDecimal(rowIdx int) Decimal {
	var value *big.Int
	switch vec.internalType {
	case TYPE_SMALLINT:
		v := getPrimitive[int16](vec, rowIdx)
		value = big.NewInt(int64(v))
	case TYPE_INTEGER:
		v := getPrimitive[int32](vec, rowIdx)
		value = big.NewInt(int64(v))
	case TYPE_BIGINT:
		v := getPrimitive[int64](vec, rowIdx)
		value = big.NewInt(v)
	case TYPE_HUGEINT:
		h := getPrimitive[hugeInt](vec, rowIdx)
		value = hugeIntToBigInt(h)
	}

	return Decimal{
		Width: vec.decimalWidth,
		Scale: vec.decimalScale,
		Value: value,
	}
}

func setDecimal(vec *vector, rowIdx int, val any) error {
	d, ok := val.(Decimal)
	if !ok {
		return fmt.Errorf("cannot convert %T to Decimal", val)
	}

	vec.setValid(rowIdx)
	switch vec.internalType {
	case TYPE_SMALLINT:
		slice := vec.dataSlice.([]int16)
		slice[rowIdx] = int16(d.Value.Int64())
	case TYPE_INTEGER:
		slice := vec.dataSlice.([]int32)
		slice[rowIdx] = int32(d.Value.Int64())
	case TYPE_BIGINT:
		slice := vec.dataSlice.([]int64)
		slice[rowIdx] = d.Value.Int64()
	case TYPE_HUGEINT:
		h, err := bigIntToHugeInt(d.Value)
		if err != nil {
			return err
		}
		slice := vec.dataSlice.([]hugeInt)
		slice[rowIdx] = h
	}

	return nil
}

// ENUM type initialization.
func (vec *vector) initEnum(typeInfo TypeInfo, colIdx int) error {
	details, ok := typeInfo.Details().(*EnumDetails)
	if !ok {
		return fmt.Errorf("column index %d: expected EnumDetails for ENUM type", colIdx)
	}

	// Build name dictionary.
	vec.namesDict = make(map[string]uint32, len(details.Values))
	for i, name := range details.Values {
		vec.namesDict[name] = uint32(i)
	}

	// Build reverse dictionary.
	vec.tagDict = make(map[uint32]string, len(details.Values))
	for i, name := range details.Values {
		vec.tagDict[uint32(i)] = name
	}

	// Determine internal type based on dictionary size.
	dictSize := len(details.Values)
	switch {
	case dictSize <= 256:
		vec.internalType = TYPE_UTINYINT
		vec.dataSlice = make([]uint8, vec.capacity)
	case dictSize <= 65536:
		vec.internalType = TYPE_USMALLINT
		vec.dataSlice = make([]uint16, vec.capacity)
	case dictSize <= math.MaxUint32:
		vec.internalType = TYPE_UINTEGER
		vec.dataSlice = make([]uint32, vec.capacity)
	default:
		vec.internalType = TYPE_UBIGINT
		vec.dataSlice = make([]uint64, vec.capacity)
	}

	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return vec.getEnum(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setEnum(vec, rowIdx, val)
	}
	vec.Type = TYPE_ENUM

	return nil
}

func (vec *vector) getEnum(rowIdx int) string {
	var idx uint32
	switch vec.internalType {
	case TYPE_UTINYINT:
		idx = uint32(getPrimitive[uint8](vec, rowIdx))
	case TYPE_USMALLINT:
		idx = uint32(getPrimitive[uint16](vec, rowIdx))
	case TYPE_UINTEGER:
		idx = getPrimitive[uint32](vec, rowIdx)
	case TYPE_UBIGINT:
		idx = uint32(getPrimitive[uint64](vec, rowIdx))
	}

	return vec.tagDict[idx]
}

func setEnum(vec *vector, rowIdx int, val any) error {
	name, ok := val.(string)
	if !ok {
		return fmt.Errorf("cannot convert %T to string for ENUM", val)
	}

	idx, found := vec.namesDict[name]
	if !found {
		return fmt.Errorf("invalid enum value: %q", name)
	}

	vec.setValid(rowIdx)
	switch vec.internalType {
	case TYPE_UTINYINT:
		slice := vec.dataSlice.([]uint8)
		slice[rowIdx] = uint8(idx)
	case TYPE_USMALLINT:
		slice := vec.dataSlice.([]uint16)
		slice[rowIdx] = uint16(idx)
	case TYPE_UINTEGER:
		slice := vec.dataSlice.([]uint32)
		slice[rowIdx] = idx
	case TYPE_UBIGINT:
		slice := vec.dataSlice.([]uint64)
		slice[rowIdx] = uint64(idx)
	}

	return nil
}

// LIST type initialization.
func (vec *vector) initList(typeInfo TypeInfo, colIdx int) error {
	details, ok := typeInfo.Details().(*ListDetails)
	if !ok {
		return fmt.Errorf("column index %d: expected ListDetails for LIST type", colIdx)
	}

	// Initialize child vector.
	vec.childVectors = make([]vector, 1)
	childVec := newVector(vec.capacity * 4) // Allow for some growth.
	vec.childVectors[0] = *childVec
	if err := vec.childVectors[0].init(details.Child, colIdx); err != nil {
		return err
	}

	// Initialize list offsets.
	vec.listOffsets = make([]uint64, vec.capacity+1)

	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return vec.getList(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setList(vec, rowIdx, val)
	}
	vec.Type = TYPE_LIST

	return nil
}

func (vec *vector) getList(rowIdx int) []any {
	start := vec.listOffsets[rowIdx]
	end := vec.listOffsets[rowIdx+1]

	result := make([]any, end-start)
	child := &vec.childVectors[0]
	for i := start; i < end; i++ {
		result[i-start] = child.getFn(child, int(i))
	}

	return result
}

func setList(vec *vector, rowIdx int, val any) error {
	slice, ok := val.([]any)
	if !ok {
		return fmt.Errorf("cannot convert %T to []any for LIST", val)
	}

	child := &vec.childVectors[0]
	start := vec.listOffsets[rowIdx]

	for i, elem := range slice {
		if err := child.setFn(child, int(start)+i, elem); err != nil {
			return err
		}
	}

	vec.setValid(rowIdx)
	vec.listOffsets[rowIdx+1] = start + uint64(len(slice))

	return nil
}

// STRUCT type initialization.
func (vec *vector) initStruct(typeInfo TypeInfo, colIdx int) error {
	details, ok := typeInfo.Details().(*StructDetails)
	if !ok {
		return fmt.Errorf("column index %d: expected StructDetails for STRUCT type", colIdx)
	}

	vec.structEntries = details.Entries
	vec.childVectors = make([]vector, len(details.Entries))

	for i, entry := range details.Entries {
		childVec := newVector(vec.capacity)
		vec.childVectors[i] = *childVec
		if err := vec.childVectors[i].init(entry.Info(), colIdx); err != nil {
			return err
		}
	}

	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return vec.getStruct(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setStruct(vec, rowIdx, val)
	}
	vec.Type = TYPE_STRUCT

	return nil
}

func (vec *vector) getStruct(rowIdx int) map[string]any {
	result := make(map[string]any, len(vec.structEntries))
	for i, entry := range vec.structEntries {
		child := &vec.childVectors[i]
		result[entry.Name()] = child.getFn(child, rowIdx)
	}

	return result
}

func setStruct(vec *vector, rowIdx int, val any) error {
	m, ok := val.(map[string]any)
	if !ok {
		return fmt.Errorf("cannot convert %T to map[string]any for STRUCT", val)
	}

	for i, entry := range vec.structEntries {
		child := &vec.childVectors[i]
		fieldVal := m[entry.Name()]
		if err := child.setFn(child, rowIdx, fieldVal); err != nil {
			return err
		}
	}

	vec.setValid(rowIdx)

	return nil
}

// MAP type initialization.
func (vec *vector) initMap(typeInfo TypeInfo, colIdx int) error {
	details, ok := typeInfo.Details().(*MapDetails)
	if !ok {
		return fmt.Errorf("column index %d: expected MapDetails for MAP type", colIdx)
	}

	// Check for unsupported key types.
	keyType := details.Key.InternalType()
	switch keyType {
	case TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY, TYPE_UNION:
		return fmt.Errorf("column index %d: unsupported map key type: %s", colIdx, keyType.String())
	}

	// A MAP is stored as a LIST of STRUCT{key, value}.
	// Create a synthetic struct type for the entries.
	keyEntry, _ := NewStructEntry(details.Key, "key")
	valueEntry, _ := NewStructEntry(details.Value, "value")
	entryInfo, _ := NewStructInfo(keyEntry, valueEntry)
	listInfo, _ := NewListInfo(entryInfo)

	// Initialize as LIST.
	if err := vec.initList(listInfo, colIdx); err != nil {
		return err
	}

	// Override type and getters/setters for MAP behavior.
	vec.Type = TYPE_MAP

	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return vec.getMap(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setMap(vec, rowIdx, val)
	}

	return nil
}

func (vec *vector) getMap(rowIdx int) Map {
	entries := vec.getList(rowIdx)
	result := make(Map, len(entries))
	for _, entry := range entries {
		m, ok := entry.(map[string]any)
		if ok {
			result[m["key"]] = m["value"]
		}
	}

	return result
}

func setMap(vec *vector, rowIdx int, val any) error {
	m, ok := val.(Map)
	if !ok {
		return fmt.Errorf("cannot convert %T to Map", val)
	}

	entries := make([]any, 0, len(m))
	for k, v := range m {
		entries = append(entries, map[string]any{"key": k, "value": v})
	}

	return setList(vec, rowIdx, entries)
}

// ARRAY type initialization (fixed-size).
func (vec *vector) initArray(typeInfo TypeInfo, colIdx int) error {
	details, ok := typeInfo.Details().(*ArrayDetails)
	if !ok {
		return fmt.Errorf("column index %d: expected ArrayDetails for ARRAY type", colIdx)
	}

	vec.arrayLength = int(details.Size)

	// Initialize child vector.
	vec.childVectors = make([]vector, 1)
	childVec := newVector(vec.capacity * vec.arrayLength)
	vec.childVectors[0] = *childVec
	if err := vec.childVectors[0].init(details.Child, colIdx); err != nil {
		return err
	}

	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return vec.getArray(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setArray(vec, rowIdx, val)
	}
	vec.Type = TYPE_ARRAY

	return nil
}

func (vec *vector) getArray(rowIdx int) []any {
	result := make([]any, vec.arrayLength)
	child := &vec.childVectors[0]
	start := rowIdx * vec.arrayLength
	for i := range vec.arrayLength {
		result[i] = child.getFn(child, start+i)
	}

	return result
}

func setArray(vec *vector, rowIdx int, val any) error {
	slice, ok := val.([]any)
	if !ok {
		return fmt.Errorf("cannot convert %T to []any for ARRAY", val)
	}

	if len(slice) != vec.arrayLength {
		return fmt.Errorf("array size mismatch: expected %d, got %d", vec.arrayLength, len(slice))
	}

	child := &vec.childVectors[0]
	start := rowIdx * vec.arrayLength
	for i, elem := range slice {
		if err := child.setFn(child, start+i, elem); err != nil {
			return err
		}
	}

	vec.setValid(rowIdx)

	return nil
}

// UNION type initialization.
func (vec *vector) initUnion(typeInfo TypeInfo, colIdx int) error {
	details, ok := typeInfo.Details().(*UnionDetails)
	if !ok {
		return fmt.Errorf("column index %d: expected UnionDetails for UNION type", colIdx)
	}

	memberCount := len(details.Members)

	// Child 0 is the tag vector (uint8).
	vec.childVectors = make([]vector, memberCount+1)

	// Initialize tag vector.
	tagVec := newVector(vec.capacity)
	initNumericVec[uint8](tagVec, TYPE_UTINYINT)
	vec.childVectors[0] = *tagVec

	// Initialize member vectors.
	vec.namesDict = make(map[string]uint32, memberCount)
	vec.tagDict = make(map[uint32]string, memberCount)
	for i, member := range details.Members {
		vec.namesDict[member.Name] = uint32(i)
		vec.tagDict[uint32(i)] = member.Name

		memberVec := newVector(vec.capacity)
		vec.childVectors[i+1] = *memberVec
		if err := vec.childVectors[i+1].init(member.Type, colIdx); err != nil {
			return err
		}
	}

	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}

		return vec.getUnion(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setUnion(vec, rowIdx, val)
	}
	vec.Type = TYPE_UNION

	return nil
}

func (vec *vector) getUnion(rowIdx int) Union {
	tagVec := &vec.childVectors[0]
	tag := getPrimitive[uint8](tagVec, rowIdx)
	tagName := vec.tagDict[uint32(tag)]

	memberVec := &vec.childVectors[tag+1]
	value := memberVec.getFn(memberVec, rowIdx)

	return Union{Tag: tagName, Value: value}
}

func setUnion(vec *vector, rowIdx int, val any) error {
	u, ok := val.(Union)
	if !ok {
		return fmt.Errorf("cannot convert %T to Union", val)
	}

	tagIdx, found := vec.namesDict[u.Tag]
	if !found {
		return fmt.Errorf("invalid union tag: %q", u.Tag)
	}

	// Set tag.
	tagVec := &vec.childVectors[0]
	slice := tagVec.dataSlice.([]uint8)
	slice[rowIdx] = uint8(tagIdx)
	tagVec.setValid(rowIdx)

	// Set value in the appropriate member vector.
	memberVec := &vec.childVectors[tagIdx+1]
	if err := memberVec.setFn(memberVec, rowIdx, u.Value); err != nil {
		return err
	}

	vec.setValid(rowIdx)

	return nil
}

// UUID type initialization.
func (vec *vector) initUUID() {
	vec.dataSlice = make([]hugeInt, vec.capacity)
	vec.getFn = func(vec *vector, rowIdx int) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		h := getPrimitive[hugeInt](vec, rowIdx)

		return hugeIntToUUID(&h)
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		if val == nil {
			vec.setNull(rowIdx)

			return nil
		}

		return setUUID(vec, rowIdx, val)
	}
	vec.Type = TYPE_UUID
}

func hugeIntToUUID(h *hugeInt) UUID {
	var u UUID
	// Upper 64 bits (XOR with sign bit for UUID format).
	upper := uint64(h.upper) ^ (1 << 63)
	for i := range 8 {
		u[i] = byte(upper >> (56 - i*8))
	}
	// Lower 64 bits.
	for i := range 8 {
		u[8+i] = byte(h.lower >> (56 - i*8))
	}

	return u
}

func uuidToHugeInt(u *UUID) hugeInt {
	var upper uint64
	for i := range 8 {
		upper |= uint64(u[i]) << (56 - i*8)
	}
	// XOR with sign bit.
	upper ^= (1 << 63)

	var lower uint64
	for i := range 8 {
		lower |= uint64(u[8+i]) << (56 - i*8)
	}

	return hugeInt{lower: lower, upper: int64(upper)}
}

func setUUID(vec *vector, rowIdx int, val any) error {
	var u *UUID
	switch v := val.(type) {
	case UUID:
		u = &v
	case *UUID:
		u = v
	case [16]byte:
		tmp := UUID(v)
		u = &tmp
	case []byte:
		if len(v) != 16 {
			return fmt.Errorf("UUID must be 16 bytes, got %d", len(v))
		}
		var tmp UUID
		copy(tmp[:], v)
		u = &tmp
	case string:
		var tmp UUID
		if err := tmp.Scan(v); err != nil {
			return err
		}
		u = &tmp
	default:
		return fmt.Errorf("cannot convert %T to UUID", val)
	}

	vec.setValid(rowIdx)
	h := uuidToHugeInt(u)
	setPrimitive(vec, rowIdx, h)

	return nil
}

// SQLNULL type initialization.
func (vec *vector) initSQLNull() {
	vec.getFn = func(vec *vector, rowIdx int) any {
		return nil
	}
	vec.setFn = func(vec *vector, rowIdx int, val any) error {
		return errors.New("cannot set value for SQLNULL type")
	}
	vec.Type = TYPE_SQLNULL
}

// Reset clears the vector's data to zero values and marks all entries as valid.
// It does not reallocate slices, making it suitable for reuse from a pool.
func (vec *vector) Reset() {
	// Mark all entries as valid
	vec.fillMask()

	// Zero out data slices based on type
	switch data := vec.dataSlice.(type) {
	case []bool:
		for i := range data {
			data[i] = false
		}
	case []int8:
		for i := range data {
			data[i] = 0
		}
	case []int16:
		for i := range data {
			data[i] = 0
		}
	case []int32:
		for i := range data {
			data[i] = 0
		}
	case []int64:
		for i := range data {
			data[i] = 0
		}
	case []uint8:
		for i := range data {
			data[i] = 0
		}
	case []uint16:
		for i := range data {
			data[i] = 0
		}
	case []uint32:
		for i := range data {
			data[i] = 0
		}
	case []uint64:
		for i := range data {
			data[i] = 0
		}
	case []float32:
		for i := range data {
			data[i] = 0
		}
	case []float64:
		for i := range data {
			data[i] = 0
		}
	case []string:
		for i := range data {
			data[i] = ""
		}
	case [][]byte:
		for i := range data {
			data[i] = nil
		}
	case []Interval:
		for i := range data {
			data[i] = Interval{}
		}
	case []hugeInt:
		for i := range data {
			data[i] = hugeInt{}
		}
	case []Uhugeint:
		for i := range data {
			data[i] = Uhugeint{}
		}
	case []Bit:
		for i := range data {
			data[i] = Bit{}
		}
	}

	// Reset list offsets if applicable
	if vec.listOffsets != nil {
		for i := range vec.listOffsets {
			vec.listOffsets[i] = 0
		}
	}

	// Reset child vectors recursively for nested types
	for i := range vec.childVectors {
		vec.childVectors[i].Reset()
	}
}

// Close nils out all slices to allow GC and sets capacity to 0.
// This prepares the vector for return to a pool.
func (vec *vector) Close() {
	// Nil out data slice
	vec.dataSlice = nil

	// Nil out mask bits
	vec.maskBits = nil

	// Nil out list offsets
	vec.listOffsets = nil

	// Close and nil out child vectors
	for i := range vec.childVectors {
		vec.childVectors[i].Close()
	}
	vec.childVectors = nil

	// Clear dictionaries
	vec.namesDict = nil
	vec.tagDict = nil

	// Reset capacity
	vec.capacity = 0
}

// setVectorVal is a helper for type-safe value setting.
func setVectorVal[T any](vec *vector, rowIdx int, val T) error {
	return vec.setFn(vec, rowIdx, any(val))
}

// ptrAdd is a helper for unsafe pointer arithmetic (reserved for optimization).
// Suppress unused warning.
var _ = ptrAdd

func ptrAdd(ptr unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Add(ptr, offset)
}
