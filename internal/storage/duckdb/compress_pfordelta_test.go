package duckdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPFORDelta_Sequential tests PFOR-DELTA compression with sequential integers.
// These are ideal candidates for PFOR-DELTA because deltas are constant (1).
func TestPFORDelta_Sequential(t *testing.T) {
	tests := []struct {
		name      string
		values    []int64
		expectOK  bool
		expectBW  uint8 // expected bit width for deltas
	}{
		{
			name:     "sequential 0-99",
			values:   makeSequence(0, 100),
			expectOK: true,
			expectBW: 1, // delta of 1 needs 1 bit
		},
		{
			name:     "sequential 1000-1099",
			values:   makeSequence(1000, 100),
			expectOK: true,
			expectBW: 1,
		},
		{
			name:     "sequential with step 2",
			values:   makeSequenceStep(0, 100, 2),
			expectOK: true,
			expectBW: 2, // delta of 2 needs 2 bits
		},
		{
			name:     "sequential with step 5",
			values:   makeSequenceStep(0, 100, 5),
			expectOK: true,
			expectBW: 3, // delta of 5 needs 3 bits
		},
		{
			name:     "too few values",
			values:   []int64{1},
			expectOK: false,
		},
		{
			name:     "highly variable (not beneficial)",
			values:   []int64{1, 1000000, 2, 2000000, 3},
			expectOK: false, // Large deltas won't compress well
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, ok := CompressPFORDeltaFromInt64(tt.values)
			assert.Equal(t, tt.expectOK, ok, "compression result mismatch")

			if ok {
				// Verify format: [int64 ref][uint8 bitWidth][uint64 count][packed deltas]
				require.NotNil(t, compressed)
				require.GreaterOrEqual(t, len(compressed), 17, "compressed data too short")

				// Read reference value
				ref := int64(binary.LittleEndian.Uint64(compressed[0:8]))
				assert.Equal(t, tt.values[0], ref, "reference value mismatch")

				// Read bit width
				bitWidth := compressed[8]
				if tt.expectBW > 0 {
					assert.Equal(t, tt.expectBW, bitWidth, "bit width mismatch")
				}

				// Read count
				count := binary.LittleEndian.Uint64(compressed[9:17])
				assert.Equal(t, uint64(len(tt.values)), count, "count mismatch")

				// Verify compression is beneficial
				originalSize := len(tt.values) * 8
				assert.Less(t, len(compressed), originalSize, "compression not beneficial")

				t.Logf("Compressed %d values from %d bytes to %d bytes (%.1f%% savings)",
					len(tt.values), originalSize, len(compressed),
					100.0*(1.0-float64(len(compressed))/float64(originalSize)))
			}
		})
	}
}

// TestPFORDelta_Timestamps tests PFOR-DELTA with timestamp sequences.
// Timestamps with regular intervals should compress very well.
func TestPFORDelta_Timestamps(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		interval time.Duration
		count    int
		expectOK bool
	}{
		{
			name:     "hourly timestamps",
			interval: time.Hour,
			count:    100,
			expectOK: true,
		},
		{
			name:     "minute timestamps",
			interval: time.Minute,
			count:    100,
			expectOK: true,
		},
		{
			name:     "second timestamps",
			interval: time.Second,
			count:    100,
			expectOK: true,
		},
		{
			name:     "daily timestamps",
			interval: 24 * time.Hour,
			count:    100,
			expectOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create timestamp sequence
			values := make([]int64, tt.count)
			for i := 0; i < tt.count; i++ {
				ts := baseTime.Add(time.Duration(i) * tt.interval)
				values[i] = ts.Unix()
			}

			compressed, ok := CompressPFORDeltaFromInt64(values)
			assert.Equal(t, tt.expectOK, ok, "compression result mismatch")

			if ok {
				originalSize := len(values) * 8
				compressionRatio := float64(len(compressed)) / float64(originalSize)
				t.Logf("Timestamp compression: %d bytes -> %d bytes (%.1f%% of original)",
					originalSize, len(compressed), compressionRatio*100)
				assert.Less(t, compressionRatio, 0.5, "expected at least 50% compression")
			}
		})
	}
}

// TestPFORDelta_Dates tests PFOR-DELTA with date values (small deltas).
// Days since epoch with regular intervals should compress excellently.
func TestPFORDelta_Dates(t *testing.T) {
	// Days since Unix epoch (1970-01-01)
	startDay := int64(19723) // 2024-01-01

	tests := []struct {
		name     string
		step     int64 // days between values
		count    int
		expectOK bool
	}{
		{
			name:     "consecutive days",
			step:     1,
			count:    365,
			expectOK: true,
		},
		{
			name:     "weekly (every 7 days)",
			step:     7,
			count:    52,
			expectOK: true,
		},
		{
			name:     "monthly (every 30 days)",
			step:     30,
			count:    12,
			expectOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := make([]int64, tt.count)
			for i := 0; i < tt.count; i++ {
				values[i] = startDay + int64(i)*tt.step
			}

			compressed, ok := CompressPFORDeltaFromInt64(values)
			assert.Equal(t, tt.expectOK, ok, "compression result mismatch")

			if ok {
				originalSize := len(values) * 8
				t.Logf("Date compression: %d bytes -> %d bytes",
					originalSize, len(compressed))
				assert.Less(t, len(compressed), originalSize, "compression not beneficial")
			}
		})
	}
}

// TestPFORDelta_RoundTrip tests compression and decompression roundtrip.
func TestPFORDelta_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{
			name:   "sequential",
			values: makeSequence(0, 1000),
		},
		{
			name:   "timestamps",
			values: makeTimestamps(100, time.Hour),
		},
		{
			name:   "dates",
			values: makeSequence(19723, 365), // Year 2024
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Compress
			compressed, ok := CompressPFORDeltaFromInt64(tt.values)
			require.True(t, ok, "compression should succeed")

			// Decompress
			decompressed, err := DecompressPFORDelta(compressed, 8, uint64(len(tt.values)))
			require.NoError(t, err, "decompression should succeed")

			// Verify
			require.Equal(t, len(tt.values)*8, len(decompressed), "decompressed size mismatch")

			// Convert back to int64 and compare
			for i := 0; i < len(tt.values); i++ {
				offset := i * 8
				got := int64(binary.LittleEndian.Uint64(decompressed[offset:]))
				assert.Equal(t, tt.values[i], got, "value mismatch at index %d", i)
			}
		})
	}
}

// TestPFORDelta_DuckDBInterop creates a database with DuckDB CLI using data
// that should trigger PFOR-DELTA compression, then reads it back to verify.
func TestPFORDelta_DuckDBInterop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping DuckDB interop test in short mode")
	}

	// Check if duckdb CLI is available
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb CLI not found, skipping interop test")
	}

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "pfordelta_test.db")

	tests := []struct {
		name       string
		createSQL  string
		insertSQL  string
		rowCount   int
		expectComp CompressionType
	}{
		{
			name: "sequential integers",
			createSQL: `CREATE TABLE seq_test (
				id INTEGER,
				value INTEGER
			);`,
			insertSQL:  "INSERT INTO seq_test SELECT i, i FROM generate_series(0, 999) AS t(i);",
			rowCount:   1000,
			expectComp: CompressionPFORDelta, // Or CompressionBitPacking - DuckDB chooses
		},
		{
			name: "timestamp sequence",
			createSQL: `CREATE TABLE ts_test (
				id INTEGER,
				ts TIMESTAMP
			);`,
			insertSQL:  "INSERT INTO ts_test SELECT i, '2024-01-01 00:00:00'::TIMESTAMP + INTERVAL (i) HOUR FROM generate_series(0, 999) AS t(i);",
			rowCount:   1000,
			expectComp: CompressionPFORDelta,
		},
		{
			name: "date sequence",
			createSQL: `CREATE TABLE date_test (
				id INTEGER,
				dt DATE
			);`,
			insertSQL:  "INSERT INTO date_test SELECT i, '2024-01-01'::DATE + INTERVAL (i) DAY FROM generate_series(0, 365) AS t(i);",
			rowCount:   366,
			expectComp: CompressionPFORDelta,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Remove previous test database
			_ = os.Remove(dbPath)

			// Create database with DuckDB CLI
			createCmd := exec.Command("duckdb", dbPath)
			createCmd.Stdin = bytes.NewBufferString(fmt.Sprintf("%s\n%s\n", tt.createSQL, tt.insertSQL))
			output, err := createCmd.CombinedOutput()
			require.NoError(t, err, "failed to create database: %s", string(output))

			// Open and read the database file
			db, err := OpenDuckDBStorage(dbPath, nil)
			require.NoError(t, err, "failed to open database")
			defer func() { _ = db.Close() }()

			// Read catalog to verify compression type
			require.NotNil(t, db.catalog, "catalog should be loaded")
			require.NotEmpty(t, db.catalog.Tables, "should have tables")

			// Find the test table
			var foundTable bool
			var tableName string
			switch tt.name {
			case "sequential integers":
				tableName = "seq_test"
			case "timestamp sequence":
				tableName = "ts_test"
			case "date sequence":
				tableName = "date_test"
			}

			for _, table := range db.catalog.Tables {
				if table.Name == tableName {
					foundTable = true
					t.Logf("Found table: %s with %d columns", table.Name, len(table.Columns))

					// Check column compression
					for _, col := range table.Columns {
						t.Logf("  Column %s: type=%s, compression=%s",
							col.Name, col.Type.String(), col.CompressionType.String())

						// DuckDB may choose PFOR_DELTA, BITPACKING, or other compression
						// We just verify it's not UNCOMPRESSED for sequential data
						if col.Name != "id" { // id column might be compressed differently
							assert.NotEqual(t, CompressionUncompressed, col.CompressionType,
								"expected compression for column %s", col.Name)

							// Log what compression was actually used
							t.Logf("  DuckDB chose %s compression for %s",
								col.CompressionType.String(), col.Name)
						}
					}
					break
				}
			}

			require.True(t, foundTable, "table %s not found in catalog", tableName)

			// Try to read the data back (use "main" schema and nil projection to read all columns)
			scanner, err := db.ScanTable("main", tableName, nil)
			require.NoError(t, err, "failed to create scanner")

			rowsRead := 0
			for scanner.Next() {
				rowsRead++
			}
			require.NoError(t, scanner.Err(), "scan error")

			assert.Equal(t, tt.rowCount, rowsRead,
				"should read all %d rows from %s", tt.rowCount, tableName)
		})
	}
}

// TestPFORDelta_Format verifies the exact format of PFOR-DELTA compressed data.
func TestPFORDelta_Format(t *testing.T) {
	// Test with known values to verify exact format
	values := []int64{100, 101, 102, 103, 104}
	compressed, ok := CompressPFORDeltaFromInt64(values)
	require.True(t, ok, "compression should succeed")

	// Expected format: [int64 ref][uint8 bitWidth][uint64 count][packed deltas]
	// ref = 100, bitWidth = 1 (delta is always 1), count = 5
	// deltas = [1, 1, 1, 1] packed in 1 bit each = 1 byte (0b00001111)

	require.GreaterOrEqual(t, len(compressed), 17, "minimum header size")

	// Verify reference
	ref := int64(binary.LittleEndian.Uint64(compressed[0:8]))
	assert.Equal(t, int64(100), ref, "reference value")

	// Verify bit width
	bitWidth := compressed[8]
	assert.Equal(t, uint8(1), bitWidth, "bit width for delta of 1")

	// Verify count
	count := binary.LittleEndian.Uint64(compressed[9:17])
	assert.Equal(t, uint64(5), count, "value count")

	// Verify packed deltas (4 deltas of value 1, each 1 bit)
	// Total: 4 bits = 1 byte
	deltasStart := 17
	assert.GreaterOrEqual(t, len(compressed), deltasStart+1, "should have delta bytes")

	// The exact bit pattern depends on the packing implementation
	// We just verify the delta section exists and has reasonable size
	deltaBytes := len(compressed) - deltasStart
	expectedDeltaBytes := ((len(values)-1)*int(bitWidth) + 7) / 8
	assert.Equal(t, expectedDeltaBytes, deltaBytes, "delta bytes count")

	t.Logf("Compressed format verified: ref=%d, bitWidth=%d, count=%d, deltaBytes=%d",
		ref, bitWidth, count, deltaBytes)
}

// TestPFORDelta_EdgeCases tests edge cases for PFOR-DELTA compression.
func TestPFORDelta_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		values   []int64
		expectOK bool
		reason   string
	}{
		{
			name:     "empty",
			values:   []int64{},
			expectOK: false,
			reason:   "no values to compress",
		},
		{
			name:     "single value",
			values:   []int64{42},
			expectOK: false,
			reason:   "need at least 2 values for delta encoding",
		},
		{
			name:     "two values",
			values:   []int64{1, 2},
			expectOK: false,
			reason:   "too few values - overhead not worth it",
		},
		{
			name:     "constant deltas",
			values:   []int64{0, 10, 20, 30, 40, 50},
			expectOK: true,
			reason:   "constant delta of 10",
		},
		{
			name:     "negative values with positive deltas",
			values:   []int64{-100, -99, -98, -97},
			expectOK: true,
			reason:   "negative values but positive deltas",
		},
		{
			name:     "large deltas (not beneficial)",
			values:   []int64{0, 1000000000, 2000000000},
			expectOK: false,
			reason:   "large deltas won't compress well",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, ok := CompressPFORDeltaFromInt64(tt.values)
			assert.Equal(t, tt.expectOK, ok, "compression result: %s", tt.reason)

			if ok && len(compressed) > 0 {
				// Verify decompression works
				decompressed, err := DecompressPFORDelta(compressed, 8, uint64(len(tt.values)))
				require.NoError(t, err, "decompression failed")

				// Verify values
				for i := 0; i < len(tt.values); i++ {
					offset := i * 8
					got := int64(binary.LittleEndian.Uint64(decompressed[offset:]))
					assert.Equal(t, tt.values[i], got, "value mismatch at index %d", i)
				}
			}
		})
	}
}

// TestPFORDelta_ValueSizes tests PFOR-DELTA with different value sizes.
func TestPFORDelta_ValueSizes(t *testing.T) {
	tests := []struct {
		name      string
		valueSize int
		values    []int64
		expectOK  bool
	}{
		{
			name:      "int8 range",
			valueSize: 1,
			values:    makeSequence(0, 100),
			expectOK:  true,
		},
		{
			name:      "int16 range",
			valueSize: 2,
			values:    makeSequence(0, 1000),
			expectOK:  true,
		},
		{
			name:      "int32 range",
			valueSize: 4,
			values:    makeSequence(0, 10000),
			expectOK:  true,
		},
		{
			name:      "int64 full",
			valueSize: 8,
			values:    makeSequence(0, 1000),
			expectOK:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert values to bytes based on valueSize
			data := make([]byte, len(tt.values)*tt.valueSize)
			for i, val := range tt.values {
				offset := i * tt.valueSize
				switch tt.valueSize {
				case 1:
					data[offset] = byte(val)
				case 2:
					binary.LittleEndian.PutUint16(data[offset:], uint16(val))
				case 4:
					binary.LittleEndian.PutUint32(data[offset:], uint32(val))
				case 8:
					binary.LittleEndian.PutUint64(data[offset:], uint64(val))
				}
			}

			// Try compression from bytes
			compressed, ok := TryCompressPFORDeltaFromBytes(data, tt.valueSize)
			assert.Equal(t, tt.expectOK, ok, "compression result mismatch")

			if ok {
				originalSize := len(data)
				t.Logf("Compressed %d-byte values: %d bytes -> %d bytes",
					tt.valueSize, originalSize, len(compressed))
				assert.Less(t, len(compressed), originalSize, "compression not beneficial")
			}
		})
	}
}

// Helper functions

func makeSequence(start int64, count int) []int64 {
	result := make([]int64, count)
	for i := 0; i < count; i++ {
		result[i] = start + int64(i)
	}
	return result
}

func makeSequenceStep(start int64, count int, step int64) []int64 {
	result := make([]int64, count)
	for i := 0; i < count; i++ {
		result[i] = start + int64(i)*step
	}
	return result
}

func makeTimestamps(count int, interval time.Duration) []int64 {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	result := make([]int64, count)
	for i := 0; i < count; i++ {
		ts := baseTime.Add(time.Duration(i) * interval)
		result[i] = ts.Unix()
	}
	return result
}
