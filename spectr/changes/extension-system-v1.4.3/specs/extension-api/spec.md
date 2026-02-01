# Extension API Specification

**Specification ID:** `extension-api-v1.0`
**Version:** 1.0
**Status:** Draft
**Last Updated:** 2024-01-20

## Overview

This specification defines the Extension API for dukdb-go, providing a comprehensive set of interfaces and abstractions for developing extensions. The API maintains DuckDB v1.4.3 compatibility while adhering to pure Go constraints and enabling secure, performant extension development.

## Design Principles

1. **Minimal Interface**: Simple, focused interfaces that are easy to implement
2. **Type Safety**: Strong typing with compile-time checks
3. **Performance**: Zero-allocation hot paths where possible
4. **Extensibility**: Plugin architecture supporting diverse extension types
5. **Security**: Safe abstractions that prevent common vulnerabilities
6. **Compatibility**: DuckDB v1.4.3 API compatibility

## Core Interfaces

### Extension Interface

```go
// Extension is the main interface that all extensions must implement
type Extension interface {
    // Metadata returns extension metadata
    Metadata() *ExtensionMetadata

    // Initialize is called when the extension is loaded
    Initialize(ctx context.Context, api ExtensionAPI) error

    // Cleanup is called when the extension is unloaded
    Cleanup() error

    // Version returns the extension version
    Version() string
}

// ExtensionMetadata contains extension information
type ExtensionMetadata struct {
    Name        string
    Version     string
    Description string
    Author      string
    License     string
    Homepage    string
    Repository  string
}
```

### Extension API

```go
// ExtensionAPI provides access to database services
type ExtensionAPI interface {
    // Function registry
    Functions() FunctionRegistry

    // Type system
    Types() TypeRegistry

    // Operators
    Operators() OperatorRegistry

    // File formats
    FileFormats() FileFormatRegistry

    // Storage
    Storage() StorageAPI

    // Memory management
    Memory() MemoryManager

    // Logging
    Logger() Logger

    // Configuration
    Config() Config

    // Metrics
    Metrics() MetricsCollector
}
```

## Function API

### Scalar Functions

```go
// ScalarFunction represents a scalar function implementation
type ScalarFunction interface {
    // Call executes the function with given arguments
    Call(ctx *FunctionContext, args []Value) (Value, error)
}

// ScalarFunctionInfo contains function metadata
type ScalarFunctionInfo struct {
    Name       string
    Arguments  []Type
    ReturnType Type
    Volatility Volatility
    Variadic   bool
}

// FunctionContext provides execution context
type FunctionContext struct {
    Context context.Context
    Memory  MemoryManager
    Logger  Logger
    State   interface{} // Function-specific state
}

// Example: Custom scalar function
type StringReverse struct{}

func (f *StringReverse) Call(ctx *FunctionContext, args []Value) (Value, error) {
    if len(args) != 1 {
        return nil, fmt.Errorf("string_reverse expects 1 argument, got %d", len(args))
    }

    str, err := args[0].AsString()
    if err != nil {
        return nil, err
    }

    // Reverse string
    runes := []rune(str)
    for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
        runes[i], runes[j] = runes[j], runes[i]
    }

    return types.NewVarcharValue(string(runes)), nil
}
```

### Aggregate Functions

```go
// AggregateFunction represents an aggregate function
type AggregateFunction interface {
    // CreateState creates a new aggregate state
    CreateState() (AggregateState, error)

    // Update updates the aggregate state with new values
    Update(state AggregateState, args []Value) error

    // Finalize produces the final result
    Finalize(state AggregateState) (Value, error)

    // Combine combines two aggregate states
    Combine(state1, state2 AggregateState) (AggregateState, error)
}

// AggregateState represents the state of an aggregate
type AggregateState interface {
    // Clone creates a copy of the state
    Clone() AggregateState

    // Serialize converts state to bytes
    Serialize() ([]byte, error)

    // Deserialize restores state from bytes
    Deserialize(data []byte) error
}

// Example: Custom aggregate function
type StringConcat struct {
    Separator string
}

func (f *StringConcat) CreateState() (AggregateState, error) {
    return &StringConcatState{
        values: []string{},
    }, nil
}

func (f *StringConcat) Update(state AggregateState, args []Value) error {
    concatState := state.(*StringConcatState)

    for _, arg := range args {
        str, err := arg.AsString()
        if err != nil {
            return err
        }
        concatState.values = append(concatState.values, str)
    }

    return nil
}

func (f *StringConcat) Finalize(state AggregateState) (Value, error) {
    concatState := state.(*StringConcatState)
    return types.NewVarcharValue(strings.Join(concatState.values, f.Separator)), nil
}

type StringConcatState struct {
    values []string
}

func (s *StringConcatState) Clone() AggregateState {
    return &StringConcatState{
        values: append([]string{}, s.values...),
    }
}
```

### Table Functions

```go
// TableFunction represents a table-producing function
type TableFunction interface {
    // Bind validates arguments and returns the result type
    Bind(ctx *BindContext, args []Value) (*TableFunctionType, error)

    // Init initializes the function execution
    Init(ctx *ExecutionContext, args []Value) (TableFunctionState, error)

    // Execute produces the next batch of results
    Execute(state TableFunctionState, output *DataChunk) error
}

// TableFunctionState holds execution state
type TableFunctionState interface {
    // GetProgress returns execution progress (0-100)
    GetProgress() float64

    // GetCardinality returns estimated total rows
    GetCardinality() int64
}

// Example: Custom table function
type ReadCSV struct{}

func (f *ReadCSV) Bind(ctx *BindContext, args []Value) (*TableFunctionType, error) {
    if len(args) < 1 {
        return nil, fmt.Errorf("read_csv requires at least 1 argument")
    }

    filename, err := args[0].AsString()
    if err != nil {
        return nil, err
    }

    // Detect schema from file
    schema, err := f.detectSchema(filename)
    if err != nil {
        return nil, err
    }

    return &TableFunctionType{
        Columns: schema,
    }, nil
}

func (f *ReadCSV) Init(ctx *ExecutionContext, args []Value) (TableFunctionState, error) {
    filename, _ := args[0].AsString()

    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }

    reader := csv.NewReader(file)

    return &ReadCSVState{
        file:   file,
        reader: reader,
    }, nil
}

type ReadCSVState struct {
    file   *os.File
    reader *csv.Reader
    row    int64
}

func (s *ReadCSVState) GetProgress() float64 {
    // Implementation based on file size
    return 0.0
}

func (s *ReadCSVState) GetCardinality() int64 {
    return -1 // Unknown
}
```

## Type System API

### Custom Types

```go
// ExtensionType represents a custom type
type ExtensionType interface {
    Type

    // Extension info
    ExtensionName() string

    // Serialization
    Serialize(value Value) ([]byte, error)
    Deserialize(data []byte) (Value, error)

    // Comparison
    Compare(a, b Value) (int, error)

    // Hash
    Hash(value Value) (uint64, error)

    // String representation
    Format(value Value) (string, error)
    Parse(input string) (Value, error)
}

// TypeDefinition defines a custom type
type TypeDefinition struct {
    Name        string
    Size        int
    Alignment   int
    StorageType Type

    // Optional methods
    Serialize   SerializeFunc
    Deserialize DeserializeFunc
    Compare     CompareFunc
    Hash        HashFunc
    Format      FormatFunc
    Parse       ParseFunc
}

// Example: Custom type
type UUIDType struct{}

func (t *UUIDType) Name() string {
    return "UUID"
}

func (t *UUIDType) Size() int {
    return 16
}

func (t *UUIDType) Serialize(value Value) ([]byte, error) {
    uuid, err := value.AsUUID()
    if err != nil {
        return nil, err
    }
    return uuid[:], nil
}

func (t *UUIDType) Deserialize(data []byte) (Value, error) {
    if len(data) != 16 {
        return nil, fmt.Errorf("invalid UUID size: %d", len(data))
    }

    var uuid UUID
    copy(uuid[:], data)

    return types.NewUUIDValue(uuid), nil
}

func (t *UUIDType) Compare(a, b Value) (int, error) {
    uuidA, err := a.AsUUID()
    if err != nil {
        return 0, err
    }

    uuidB, err := b.AsUUID()
    if err != nil {
        return 0, err
    }

    return bytes.Compare(uuidA[:], uuidB[:]), nil
}
```

### Type Casts

```go
// CastFunction converts values between types
type CastFunction interface {
    Cast(ctx *CastContext, value Value) (Value, error)
}

// CastContext provides casting context
type CastContext struct {
    Context    context.Context
    FromType   Type
    ToType     Type
    Implicit   bool
}

// Example: UUID to string cast
type UUIDToStringCast struct{}

func (c *UUIDToStringCast) Cast(ctx *CastContext, value Value) (Value, error) {
    uuid, err := value.AsUUID()
    if err != nil {
        return nil, err
    }

    return types.NewVarcharValue(uuid.String()), nil
}
```

## Operator API

### Custom Operators

```go
// OperatorFunction represents a custom operator
type OperatorFunction interface {
    // Execute runs the operator
    Execute(ctx *OperatorContext, left, right Value) (Value, error)
}

// OperatorContext provides operator execution context
type OperatorContext struct {
    Context  context.Context
    Operator string
    LeftType Type
    RightType Type
}

// OperatorDefinition defines an operator
type OperatorDefinition struct {
    Name       string
    Symbol     string
    LeftType   Type
    RightType  Type
    ReturnType Type
    Function   OperatorFunction
}

// Example: Custom operator
type UUIDEqual struct{}

func (o *UUIDEqual) Execute(ctx *OperatorContext, left, right Value) (Value, error) {
    uuidLeft, err := left.AsUUID()
    if err != nil {
        return nil, err
    }

    uuidRight, err := right.AsUUID()
    if err != nil {
        return nil, err
    }

    return types.NewBooleanValue(uuidLeft == uuidRight), nil
}
```

## File Format API

### Custom File Formats

```go
// FileFormat represents a custom file format
type FileFormat interface {
    // GetInfo returns format information
    GetInfo() *FileFormatInfo

    // CreateReader creates a reader for the format
    CreateReader(source io.Reader, options map[string]interface{}) (FileReader, error)

    // CreateWriter creates a writer for the format
    CreateWriter(sink io.Writer, options map[string]interface{}) (FileWriter, error)
}

// FileFormatInfo contains format metadata
type FileFormatInfo struct {
    Name        string
    Extensions  []string
    MimeTypes   []string
    Options     []OptionDefinition
}

// FileReader reads data in the format
type FileReader interface {
    // Read reads the next batch of data
    Read() (*DataChunk, error)

    // GetSchema returns the schema of the data
    GetSchema() *Schema

    // Close closes the reader
    Close() error
}

// FileWriter writes data in the format
type FileWriter interface {
    // Write writes a batch of data
    Write(chunk *DataChunk) error

    // Flush flushes any buffered data
    Flush() error

    // Close closes the writer
    Close() error
}

// Example: Custom file format
type MyFormat struct{}

func (f *MyFormat) GetInfo() *FileFormatInfo {
    return &FileFormatInfo{
        Name:       "MYFORMAT",
        Extensions: []string{".myf", ".myformat"},
        MimeTypes:  []string{"application/x-myformat"},
        Options: []OptionDefinition{
            {
                Name:        "compression",
                Type:        "string",
                Description: "Compression algorithm",
                Default:     "none",
                Choices:     []string{"none", "gzip", "lz4"},
            },
        },
    }
}

type MyFormatReader struct {
    source io.Reader
    schema *Schema
}

func (r *MyFormatReader) Read() (*DataChunk, error) {
    // Read and parse data
    return nil, nil
}

func (r *MyFormatReader) GetSchema() *Schema {
    return r.schema
}

func (r *MyFormatReader) Close() error {
    return nil
}
```

## Storage API

### Storage Extensions

```go
// StorageEngine represents a custom storage backend
type StorageEngine interface {
    // GetName returns the engine name
    GetName() string

    // CreateTable creates a table in the storage
    CreateTable(name string, schema *Schema) (Table, error)

    // GetTable retrieves a table
    GetTable(name string) (Table, error)

    // ListTables lists all tables
    ListTables() ([]string, error)

    // DropTable drops a table
    DropTable(name string) error
}

// Table represents a table in storage
type Table interface {
    // GetSchema returns the table schema
    GetSchema() *Schema

    // Scan creates a scanner for reading data
    Scan(columns []string, predicate *Predicate) (Scanner, error)

    // Writer creates a writer for writing data
    Writer() (Writer, error)

    // GetInfo returns table information
    GetInfo() *TableInfo
}

// Scanner reads data from a table
type Scanner interface {
    // Scan reads the next batch
    Scan() (*DataChunk, error)

    // GetProgress returns scan progress
    GetProgress() float64

    // Close closes the scanner
    Close() error
}
```

## Memory Management

### Memory Manager

```go
// MemoryManager manages memory allocation
type MemoryManager interface {
    // Allocate allocates memory
    Allocate(size int) ([]byte, error)

    // Free frees memory
    Free(data []byte) error

    // GetUsage returns current memory usage
    GetUsage() MemoryUsage

    // SetLimit sets memory limit
    SetLimit(limit int64) error
}

// MemoryUsage contains memory statistics
type MemoryUsage struct {
    Allocated int64
    Used      int64
    Peak      int64
}
```

## Configuration

### Extension Configuration

```go
// Config provides configuration access
type Config interface {
    // Get returns a configuration value
    Get(key string) (interface{}, error)

    // GetString returns a string value
    GetString(key string) (string, error)

    // GetInt returns an integer value
    GetInt(key string) (int, error)

    // GetBool returns a boolean value
    GetBool(key string) (bool, error)

    // Set sets a configuration value
    Set(key string, value interface{}) error

    // List returns all keys
    List() []string
}

// Configuration keys
const (
    ConfigExtensionDir     = "extension.directory"
    ConfigMaxMemory        = "extension.max_memory"
    ConfigVerifySignature  = "extension.verify_signature"
    ConfigCacheSize        = "extension.cache_size"
)
```

## Logging

### Logger Interface

```go
// Logger provides logging facilities
type Logger interface {
    // Debug logs a debug message
    Debug(msg string, args ...interface{})

    // Info logs an info message
    Info(msg string, args ...interface{})

    // Warn logs a warning message
    Warn(msg string, args ...interface{})

    // Error logs an error message
    Error(msg string, args ...interface{})

    // WithFields returns a logger with fields
    WithFields(fields map[string]interface{}) Logger
}

// Log levels
const (
    LogLevelDebug = "debug"
    LogLevelInfo  = "info"
    LogLevelWarn  = "warn"
    LogLevelError = "error"
)
```

## Metrics

### Metrics Collection

```go
// MetricsCollector collects metrics
type MetricsCollector interface {
    // Counter creates a counter metric
    Counter(name string, tags map[string]string) Counter

    // Gauge creates a gauge metric
    Gauge(name string, tags map[string]string) Gauge

    // Histogram creates a histogram metric
    Histogram(name string, tags map[string]string) Histogram

    // Timer creates a timer metric
    Timer(name string, tags map[string]string) Timer
}

// Counter counts events
type Counter interface {
    Inc()
    Add(delta int64)
    Value() int64
}

// Gauge measures a value
type Gauge interface {
    Set(value int64)
    Value() int64
}

// Histogram measures distributions
type Histogram interface {
    Record(value int64)
    Count() int64
    Sum() int64
}

// Timer measures durations
type Timer interface {
    Start() TimerHandle
}

type TimerHandle interface {
    Stop()
}
```

## Error Handling

### Extension Errors

```go
// ExtensionError represents an extension error
type ExtensionError struct {
    Extension string
    Operation string
    Cause     error
    Context   map[string]interface{}
}

func (e *ExtensionError) Error() string {
    return fmt.Sprintf("extension %s: %s failed: %v",
        e.Extension, e.Operation, e.Cause)
}

func (e *ExtensionError) Unwrap() error {
    return e.Cause
}

// Common errors
var (
    ErrInvalidArgument = errors.New("invalid argument")
    ErrTypeMismatch    = errors.New("type mismatch")
    ErrOutOfMemory     = errors.New("out of memory")
    ErrPermissionDenied = errors.New("permission denied")
    ErrNotImplemented  = errors.New("not implemented")
)
```

## Best Practices

### Function Implementation

1. **Validate Arguments**: Always validate argument count and types
2. **Handle Nulls**: Properly handle NULL input values
3. **Memory Management**: Use the provided memory manager
4. **Error Handling**: Return descriptive errors
5. **Performance**: Minimize allocations in hot paths

### Type Implementation

1. **Immutable Values**: Types should be immutable
2. **Thread Safety**: Implementation must be thread-safe
3. **Serialization**: Ensure consistent serialization
4. **Comparison**: Implement consistent comparison
5. **Validation**: Validate input during parsing

### Extension Development

1. **Minimal Dependencies**: Keep dependencies minimal
2. **Configuration**: Make behavior configurable
3. **Logging**: Log important events and errors
4. **Metrics**: Expose relevant metrics
5. **Testing**: Write comprehensive tests

### Example Extension

```go
package myextension

import (
    "context"
    "fmt"
    "github.com/dukdb-go/dukdb-go/extension"
)

type MyExtension struct {
    api extension.ExtensionAPI
}

func (e *MyExtension) Metadata() *extension.ExtensionMetadata {
    return &extension.ExtensionMetadata{
        Name:        "my_extension",
        Version:     "1.0.0",
        Description: "My custom extension",
        Author:      "John Doe",
        License:     "MIT",
    }
}

func (e *MyExtension) Initialize(ctx context.Context, api extension.ExtensionAPI) error {
    e.api = api

    // Register scalar function
    err := api.Functions().RegisterScalarFunction(
        &extension.ScalarFunctionInfo{
            Name:       "my_function",
            Arguments:  []types.Type{types.VARCHAR},
            ReturnType: types.INTEGER,
            Volatility: extension.Immutable,
        },
        &MyFunction{},
    )
    if err != nil {
        return err
    }

    // Register custom type
    err = api.Types().RegisterType(
        &extension.TypeDefinition{
            Name:      "MY_TYPE",
            Size:      16,
            Alignment: 8,
        },
        &MyType{},
    )
    if err != nil {
        return err
    }

    return nil
}

func (e *MyExtension) Cleanup() error {
    // Cleanup resources
    return nil
}

func (e *MyExtension) Version() string {
    return "1.0.0"
}

// Extension entry point
func init() {
    extension.Register("my_extension", func() extension.Extension {
        return &MyExtension{}
    })
}
```

## Version History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | 2024-01-20 | Initial specification | dukdb-go Team |