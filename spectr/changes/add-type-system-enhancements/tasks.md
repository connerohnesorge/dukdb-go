# Tasks: Add Type System Enhancements

## Phase 1: Core Infrastructure

- [ ] **1.1** Create `convert.go` with type conversion utilities
  - Implement `convertToType[T any](src any) (T, error)`
  - Implement `setFieldValue(field reflect.Value, val any) error`
  - Add numeric widening support (int8→int64, etc.)
  - Add tests for all numeric conversion paths

- [ ] **1.2** Add type conversion test matrix
  - Test int8→int16→int32→int64 conversions
  - Test uint8→uint16→uint32→uint64 conversions
  - Test float32↔float64 conversions
  - Test nil/NULL handling
  - Verify error messages are descriptive

## Phase 2: List Scanning

- [ ] **2.1** Implement `ListScanner[T any]`
  - Create struct with Result *[]T field
  - Implement Scan(src any) error method
  - Handle []any source type
  - Handle direct []T source type (fast path)
  - Handle nil source → nil slice

- [ ] **2.2** Implement `ScanList[T any](dest *[]T) sql.Scanner` helper
  - Factory function returning ListScanner
  - Add documentation with usage example

- [ ] **2.3** Add ListScanner tests
  - Test []int64 from []any{int64, int64}
  - Test []string from []any{string, string}
  - Test nested [][]int64 from nested slices
  - Test NULL elements → zero values
  - Test *T elements for nullable lists
  - Test type mismatch error messages

## Phase 3: Struct Scanning

- [ ] **3.1** Implement `StructScanner[T any]`
  - Create struct with Result *T field
  - Implement Scan(src any) error method
  - Handle map[string]any source type
  - Support `duckdb:"field_name"` struct tags
  - Support case-insensitive field matching
  - Handle nil source

- [ ] **3.2** Implement `ScanStruct[T any](dest *T) sql.Scanner` helper
  - Factory function returning StructScanner
  - Add documentation with usage example

- [ ] **3.3** Add StructScanner tests
  - Test simple struct with matching fields
  - Test struct with duckdb tags
  - Test embedded struct fields
  - Test unexported fields (should skip)
  - Test missing fields (should ignore)
  - Test extra map keys (should ignore)
  - Test NULL values → zero values

## Phase 4: Map Scanning

- [ ] **4.1** Implement `MapScanner[K comparable, V any]`
  - Create struct with Result *map[K]V field
  - Implement Scan(src any) error method
  - Handle map[any]any source type
  - Convert keys and values to typed versions
  - Handle nil source → nil map

- [ ] **4.2** Implement `ScanMap[K comparable, V any](dest *map[K]V) sql.Scanner` helper
  - Factory function returning MapScanner
  - Add documentation with usage example

- [ ] **4.3** Add MapScanner tests
  - Test map[string]int64 from map[any]any
  - Test map[int64]string from map[any]any
  - Test NULL keys (error)
  - Test NULL values → zero values
  - Test type mismatch error messages

## Phase 5: Union Scanning

- [ ] **5.1** Implement `UnionValue` type
  - Tag string field (active member name)
  - Index int field (0-based member index)
  - Value any field (the actual value)
  - Implement `As(dest any) error` method for typed extraction

- [ ] **5.2** Implement `UnionScanner`
  - Create struct with Result *UnionValue field
  - Implement Scan(src any) error method
  - Handle direct UnionValue source
  - Handle map[string]any with tag/index/value keys
  - Handle nil source → empty UnionValue with Index -1

- [ ] **5.3** Implement `ScanUnion(dest *UnionValue) sql.Scanner` helper
  - Factory function returning UnionScanner

- [ ] **5.4** Add UnionScanner tests
  - Test union with int member active
  - Test union with string member active
  - Test NULL union
  - Test As() with correct type
  - Test As() with incorrect type (error)

## Phase 6: Enum Scanning

- [ ] **6.1** Implement `EnumScanner[T ~string]`
  - Create struct with Result *T field
  - Implement Scan(src any) error method
  - Handle string source type
  - Convert to user's string-based type
  - Handle nil source → zero value

- [ ] **6.2** Implement `ScanEnum[T ~string](dest *T) sql.Scanner` helper
  - Factory function returning EnumScanner

- [ ] **6.3** Add EnumScanner tests
  - Define custom enum type: type Status string
  - Test scanning to custom type
  - Test NULL enum → empty string
  - Test non-string source (error)

## Phase 7: JSON Scanning

- [ ] **7.1** Implement `JSONScanner[T any]`
  - Create struct with Result *T field
  - Implement Scan(src any) error method
  - Handle string source → json.Unmarshal
  - Handle []byte source → json.Unmarshal
  - Handle nil source (no-op)

- [ ] **7.2** Implement `ScanJSON[T any](dest *T) sql.Scanner` helper
  - Factory function returning JSONScanner

- [ ] **7.3** Add JSONScanner tests
  - Test struct with json tags
  - Test nested structs
  - Test slice unmarshaling
  - Test map unmarshaling
  - Test invalid JSON (error)
  - Test NULL → no change to dest

## Phase 8: Parameter Binding

- [ ] **8.1** Implement `ListValue[T any]`
  - Type alias for []T
  - Implement driver.Valuer interface
  - Convert to []any for driver

- [ ] **8.2** Implement `StructValue[T any]`
  - Struct with V T field
  - Implement driver.Valuer interface
  - Convert to map[string]any using reflection
  - Support duckdb struct tags

- [ ] **8.3** Implement `MapValue[K comparable, V any]`
  - Type alias for map[K]V
  - Implement driver.Valuer interface
  - Convert to map[any]any for driver

- [ ] **8.4** Add parameter binding tests
  - Test ListValue[int] as parameter
  - Test StructValue with tagged struct
  - Test MapValue[string, int] as parameter
  - Test round-trip: insert then select

## Phase 9: Extended Type Clock Integration

- [ ] **9.1** Add clock parameter to TimeNS.NowNS()
  - Modify to accept quartz.Clock parameter
  - Default to quartz.NewReal() if nil
  - Use clock.Now() instead of time.Now()

- [ ] **9.2** Add CurrentTimeNS(clock) utility
  - Return current nanosecond timestamp
  - Use injected clock for deterministic testing

- [ ] **9.3** Add deterministic time tests
  - Use quartz.NewMock(t) in tests
  - Verify NowNS returns mock time
  - Verify CurrentTimeNS returns mock time

## Phase 10: Integration Tests

- [ ] **10.1** Create comprehensive type matrix test
  - All 37 DuckDB types × relevant scanners
  - Insert via standard path, scan via typed scanner
  - Verify exact round-trip

- [ ] **10.2** Create complex nested type tests
  - LIST of STRUCT
  - STRUCT with LIST field
  - MAP with STRUCT value
  - LIST of LIST
  - Verify deep nesting works

- [ ] **10.3** Create NULL handling test suite
  - Every scanner with NULL input
  - Every scanner with NULL elements
  - Verify zero values vs nil pointers

- [ ] **10.4** Create error message verification tests
  - Trigger each error path
  - Verify message is descriptive
  - Verify element indices in list errors
  - Verify field names in struct errors

## Validation Criteria

- [ ] All typed scanners compile with type safety
- [ ] All scanners pass round-trip tests
- [ ] NULL handling consistent across all scanners
- [ ] Error messages include context (index, field name)
- [ ] Parameter binding works for all complex types
- [ ] Clock integration works in deterministic tests
- [ ] No performance regression (fast path verified)
- [ ] All tests pass with `go test ./...`
