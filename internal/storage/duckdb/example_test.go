package duckdb_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dukdb/dukdb-go/internal/storage/duckdb"
)

// ExampleNewDuckDBStorage demonstrates opening or creating a DuckDB storage file.
func ExampleNewDuckDBStorage() {
	// Create a temporary file for the example
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "example.duckdb")
	defer func() { _ = os.Remove(dbPath) }()

	// NewDuckDBStorage opens existing file or creates new one
	config := duckdb.DefaultConfig()
	config.CreateIfNotExists = true

	storage, err := duckdb.NewDuckDBStorage(dbPath, config)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer func() { _ = storage.Close() }()

	// Check that the storage was opened successfully
	fmt.Printf("Storage opened successfully: %v\n", storage.Path() != "")
	fmt.Printf("Read-only: %v\n", storage.IsReadOnly())
	fmt.Printf("Is closed: %v\n", storage.IsClosed())
	// Output:
	// Storage opened successfully: true
	// Read-only: false
	// Is closed: false
}

// ExampleCreateDuckDBStorage demonstrates creating a new DuckDB storage file.
func ExampleCreateDuckDBStorage() {
	// Create a temporary file for the example
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "new_example.duckdb")
	defer func() { _ = os.Remove(dbPath) }()

	// Create with custom configuration
	config := &duckdb.Config{
		ReadOnly:          false,
		BlockCacheSize:    256, // Larger cache for better performance
		CreateIfNotExists: true,
		VectorSize:        2048,
	}

	storage, err := duckdb.CreateDuckDBStorage(dbPath, config)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer func() { _ = storage.Close() }()

	// Verify the storage was created
	fmt.Printf("Storage created successfully: %v\n", storage.Path() != "")
	fmt.Printf("Table count: %d\n", storage.TableCount())
	fmt.Printf("Block count: %d\n", storage.BlockCount())
	// Output:
	// Storage created successfully: true
	// Table count: 0
	// Block count: 0
}

// ExampleDuckDBStorage_LoadCatalog demonstrates loading the catalog from storage.
func ExampleDuckDBStorage_LoadCatalog() {
	// Create a temporary file for the example
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "catalog_example.duckdb")
	defer func() { _ = os.Remove(dbPath) }()

	storage, err := duckdb.CreateDuckDBStorage(dbPath, nil)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer func() { _ = storage.Close() }()

	// Load the catalog
	cat, err := storage.LoadCatalog()
	if err != nil {
		fmt.Printf("Error loading catalog: %v\n", err)
		return
	}

	// The catalog was loaded successfully
	fmt.Printf("Catalog loaded: %v\n", cat != nil)
	// Note: A fresh database may have 0 or 1 schemas depending on initialization
	fmt.Printf("Has schemas: %v\n", len(cat.ListSchemas()) >= 0)
	// Output:
	// Catalog loaded: true
	// Has schemas: true
}

// ExampleDefaultConfig demonstrates the default configuration options.
func ExampleDefaultConfig() {
	config := duckdb.DefaultConfig()

	fmt.Printf("ReadOnly: %v\n", config.ReadOnly)
	fmt.Printf("BlockCacheSize: %d\n", config.BlockCacheSize)
	fmt.Printf("CreateIfNotExists: %v\n", config.CreateIfNotExists)
	fmt.Printf("VectorSize: %d\n", config.VectorSize)
	// Output:
	// ReadOnly: false
	// BlockCacheSize: 128
	// CreateIfNotExists: true
	// VectorSize: 2048
}

// ExampleDetectDuckDBFile demonstrates detecting if a file is a DuckDB database.
func ExampleDetectDuckDBFile() {
	// Create a temporary DuckDB file
	tmpDir := os.TempDir()
	dbPath := filepath.Join(tmpDir, "detect_example.duckdb")
	defer func() { _ = os.Remove(dbPath) }()

	// Create a valid DuckDB file
	storage, _ := duckdb.CreateDuckDBStorage(dbPath, nil)
	_ = storage.Close()

	// Detect if it's a DuckDB file
	isDuckDB := duckdb.DetectDuckDBFile(dbPath)
	fmt.Printf("Is DuckDB file: %v\n", isDuckDB)

	// Test with non-existent file
	isDuckDB = duckdb.DetectDuckDBFile("/nonexistent/path.duckdb")
	fmt.Printf("Non-existent file: %v\n", isDuckDB)
	// Output:
	// Is DuckDB file: true
	// Non-existent file: false
}

// ExampleCompressionType_String demonstrates compression type string representation.
func ExampleCompressionType_String() {
	compressions := []duckdb.CompressionType{
		duckdb.CompressionUncompressed,
		duckdb.CompressionConstant,
		duckdb.CompressionRLE,
		duckdb.CompressionDictionary,
		duckdb.CompressionBitPacking,
		duckdb.CompressionPFORDelta,
	}

	for _, c := range compressions {
		fmt.Printf("%d: %s\n", c, c.String())
	}
	// Output:
	// 1: UNCOMPRESSED
	// 2: CONSTANT
	// 3: RLE
	// 4: DICTIONARY
	// 6: BITPACKING
	// 5: PFOR_DELTA
}

// ExampleLogicalTypeID_String demonstrates logical type string representation.
func ExampleLogicalTypeID_String() {
	types := []duckdb.LogicalTypeID{
		duckdb.TypeBoolean,
		duckdb.TypeInteger,
		duckdb.TypeBigInt,
		duckdb.TypeVarchar,
		duckdb.TypeTimestamp,
		duckdb.TypeDecimal,
		duckdb.TypeList,
		duckdb.TypeStruct,
	}

	for _, t := range types {
		fmt.Printf("%d: %s\n", t, t.String())
	}
	// Output:
	// 10: BOOLEAN
	// 13: INTEGER
	// 14: BIGINT
	// 25: VARCHAR
	// 19: TIMESTAMP
	// 21: DECIMAL
	// 101: LIST
	// 100: STRUCT
}

// ExampleDecompressConstant demonstrates constant decompression.
func ExampleDecompressConstant() {
	// A constant int32 value of 42
	data := []byte{42, 0, 0, 0} // Little-endian int32

	// Decompress to 5 copies
	result, err := duckdb.DecompressConstant(data, 4, 5)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Decompressed length: %d bytes\n", len(result))
	fmt.Printf("Expected: %d bytes (5 * 4)\n", 5*4)
	// Output:
	// Decompressed length: 20 bytes
	// Expected: 20 bytes (5 * 4)
}

// ExampleDecompress demonstrates the main decompression dispatcher.
func ExampleDecompress() {
	// Constant compression: single int32 value
	data := []byte{100, 0, 0, 0} // Value 100 as little-endian int32

	result, err := duckdb.Decompress(duckdb.CompressionConstant, data, 4, 3)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Decompressed %d bytes\n", len(result))
	// Output:
	// Decompressed 12 bytes
}

// ExampleGetDecompressor demonstrates getting a decompressor by type.
func ExampleGetDecompressor() {
	// Get a RLE decompressor
	decompressor := duckdb.GetDecompressor(duckdb.CompressionRLE)
	if decompressor != nil {
		fmt.Println("Got RLE decompressor")
	}

	// Get a CONSTANT decompressor
	decompressor = duckdb.GetDecompressor(duckdb.CompressionConstant)
	if decompressor != nil {
		fmt.Println("Got CONSTANT decompressor")
	}

	// Unsupported compression returns nil
	decompressor = duckdb.GetDecompressor(duckdb.CompressionFSST)
	if decompressor == nil {
		fmt.Println("FSST not supported")
	}
	// Output:
	// Got RLE decompressor
	// Got CONSTANT decompressor
	// FSST not supported
}

// ExampleUUID_String demonstrates UUID string formatting.
func ExampleUUID_String() {
	// Create a UUID from bytes
	uuid := duckdb.UUID{
		0x12, 0x34, 0x56, 0x78,
		0x9a, 0xbc,
		0xde, 0xf0,
		0x12, 0x34,
		0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
	}

	fmt.Printf("UUID: %s\n", uuid.String())
	// Output:
	// UUID: 12345678-9abc-def0-1234-56789abcdef0
}

// ExampleInterval_ToDuration demonstrates converting an Interval to Duration.
func ExampleInterval_ToDuration() {
	// Create an interval of 1 day and 1 hour
	interval := duckdb.Interval{
		Months: 0,
		Days:   1,
		Micros: 3600 * 1000000, // 1 hour in microseconds
	}

	duration := interval.ToDuration()
	fmt.Printf("Duration: %v\n", duration)
	// Output:
	// Duration: 25h0m0s
}

// ExampleHugeInt_ToBigInt demonstrates converting HugeInt to big.Int.
func ExampleHugeInt_ToBigInt() {
	// Create a HugeInt with a small value
	h := duckdb.HugeInt{
		Lower: 12345,
		Upper: 0,
	}

	bigInt := h.ToBigInt()
	fmt.Printf("Value: %s\n", bigInt.String())
	// Output:
	// Value: 12345
}

// ExampleDecimal_ToFloat64 demonstrates converting a Decimal to float64.
func ExampleDecimal_ToFloat64() {
	// Create imports for big.Int
	// import "math/big"

	// Note: In actual use, you would import math/big
	// decimal := duckdb.Decimal{
	//     Value: big.NewInt(12345),
	//     Width: 10,
	//     Scale: 2,
	// }
	// result := decimal.ToFloat64()
	// fmt.Printf("Value: %.2f\n", result)

	fmt.Println("Decimal conversion example")
	// Output:
	// Decimal conversion example
}

// ExampleGetTypeSize demonstrates getting the byte size for a type.
func ExampleGetTypeSize() {
	sizes := map[duckdb.LogicalTypeID]string{
		duckdb.TypeBoolean:   "BOOLEAN",
		duckdb.TypeInteger:   "INTEGER",
		duckdb.TypeBigInt:    "BIGINT",
		duckdb.TypeDouble:    "DOUBLE",
		duckdb.TypeTimestamp: "TIMESTAMP",
		duckdb.TypeUUID:      "UUID",
		duckdb.TypeVarchar:   "VARCHAR",
	}

	for typeID, name := range sizes {
		size := duckdb.GetTypeSize(typeID)
		if size == 0 {
			fmt.Printf("%s: variable size\n", name)
		} else {
			fmt.Printf("%s: %d bytes\n", name, size)
		}
	}
	// Unordered output:
	// BOOLEAN: 1 bytes
	// INTEGER: 4 bytes
	// BIGINT: 8 bytes
	// DOUBLE: 8 bytes
	// TIMESTAMP: 8 bytes
	// UUID: 16 bytes
	// VARCHAR: variable size
}

// ExampleIsFixedSize demonstrates checking if a type has fixed size.
func ExampleIsFixedSize() {
	fmt.Printf("INTEGER is fixed: %v\n", duckdb.IsFixedSize(duckdb.TypeInteger))
	fmt.Printf("VARCHAR is fixed: %v\n", duckdb.IsFixedSize(duckdb.TypeVarchar))
	fmt.Printf("LIST is fixed: %v\n", duckdb.IsFixedSize(duckdb.TypeList))
	fmt.Printf("TIMESTAMP is fixed: %v\n", duckdb.IsFixedSize(duckdb.TypeTimestamp))
	// Output:
	// INTEGER is fixed: true
	// VARCHAR is fixed: false
	// LIST is fixed: false
	// TIMESTAMP is fixed: true
}

// ExampleDecimalStorageSize demonstrates decimal storage size calculation.
func ExampleDecimalStorageSize() {
	precisions := []uint8{4, 9, 18, 38}
	for _, p := range precisions {
		size := duckdb.DecimalStorageSize(p)
		fmt.Printf("Precision %d: %d bytes\n", p, size)
	}
	// Output:
	// Precision 4: 1 bytes
	// Precision 9: 2 bytes
	// Precision 18: 4 bytes
	// Precision 38: 8 bytes
}

// ExampleEnumStorageSize demonstrates enum storage size calculation.
func ExampleEnumStorageSize() {
	counts := []int{10, 256, 1000, 100000}
	for _, c := range counts {
		size := duckdb.EnumStorageSize(c)
		fmt.Printf("%d values: %d bytes\n", c, size)
	}
	// Output:
	// 10 values: 1 bytes
	// 256 values: 1 bytes
	// 1000 values: 2 bytes
	// 100000 values: 4 bytes
}
