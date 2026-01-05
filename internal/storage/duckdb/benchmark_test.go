package duckdb

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// ============================================================================
// Block Cache Benchmarks
// ============================================================================

// BenchmarkBlockCacheHitRate measures cache hit rates under different access patterns.
func BenchmarkBlockCacheHitRate(b *testing.B) {
	b.Run("sequential", func(b *testing.B) {
		cache := NewBlockCache(100)

		// Pre-populate cache
		for i := uint64(0); i < 100; i++ {
			cache.Put(&Block{ID: i, Data: make([]byte, 1024)})
		}

		cache.ResetStats()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			cache.Get(uint64(i % 100))
		}

		stats := cache.Stats()
		b.ReportMetric(stats.HitRate(), "hit_rate_%")
		b.ReportMetric(float64(stats.Hits), "hits")
		b.ReportMetric(float64(stats.Misses), "misses")
	})

	b.Run("random", func(b *testing.B) {
		cache := NewBlockCache(100)

		// Pre-populate cache with 50 blocks
		for i := uint64(0); i < 50; i++ {
			cache.Put(&Block{ID: i, Data: make([]byte, 1024)})
		}

		// Generate random access pattern
		rng := rand.New(rand.NewSource(42))
		accesses := make([]uint64, b.N)
		for i := range accesses {
			accesses[i] = uint64(rng.Intn(200)) // 50% in cache, 50% not
		}

		cache.ResetStats()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			cache.Get(accesses[i])
		}

		stats := cache.Stats()
		b.ReportMetric(stats.HitRate(), "hit_rate_%")
	})

	b.Run("with_eviction", func(b *testing.B) {
		cache := NewBlockCache(50)

		// Pre-populate cache
		for i := uint64(0); i < 50; i++ {
			cache.Put(&Block{ID: i, Data: make([]byte, 1024)})
		}

		cache.ResetStats()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Access pattern that causes evictions
			cache.Put(&Block{ID: uint64(50 + i), Data: make([]byte, 1024)})
			cache.Get(uint64(i % 50))
		}

		stats := cache.Stats()
		b.ReportMetric(stats.HitRate(), "hit_rate_%")
		b.ReportMetric(float64(stats.Evictions), "evictions")
	})
}

// BenchmarkBlockCacheOperations measures individual cache operations.
func BenchmarkBlockCacheOperations(b *testing.B) {
	b.Run("Get/hit", func(b *testing.B) {
		cache := NewBlockCache(1000)
		for i := uint64(0); i < 100; i++ {
			cache.Put(&Block{ID: i, Data: make([]byte, 4096)})
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Get(uint64(i % 100))
		}
	})

	b.Run("Get/miss", func(b *testing.B) {
		cache := NewBlockCache(1000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Get(uint64(i))
		}
	})

	b.Run("Put/no_eviction", func(b *testing.B) {
		cache := NewBlockCache(b.N + 1)
		block := &Block{Data: make([]byte, 4096)}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			block.ID = uint64(i)
			cache.Put(block)
		}
	})

	b.Run("Put/with_eviction", func(b *testing.B) {
		cache := NewBlockCache(100)
		block := &Block{Data: make([]byte, 4096)}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			block.ID = uint64(i)
			cache.Put(block)
		}
	})
}

// ============================================================================
// Decompression Benchmarks
// ============================================================================

// BenchmarkDecompressConstant measures CONSTANT decompression throughput.
func BenchmarkDecompressConstant(b *testing.B) {
	sizes := []struct {
		valueSize int
		count     uint64
	}{
		{4, 1000},
		{4, 10000},
		{8, 1000},
		{8, 10000},
	}

	for _, s := range sizes {
		name := fmt.Sprintf("size%d_count%d", s.valueSize, s.count)
		b.Run(name, func(b *testing.B) {
			data := make([]byte, s.valueSize)
			for i := range data {
				data[i] = byte(i + 1)
			}

			b.ResetTimer()
			b.SetBytes(int64(s.valueSize) * int64(s.count))

			for i := 0; i < b.N; i++ {
				_, err := DecompressConstant(data, s.valueSize, s.count)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecompressRLE measures RLE decompression throughput.
func BenchmarkDecompressRLE(b *testing.B) {
	// Create RLE data with various run lengths
	createRLEData := func(valueSize int, runs int, avgRunLen int) []byte {
		var data []byte
		for r := 0; r < runs; r++ {
			// Add varint run length
			runLen := uint64(avgRunLen)
			for runLen >= 0x80 {
				data = append(data, byte(runLen)|0x80)
				runLen >>= 7
			}
			data = append(data, byte(runLen))

			// Add value
			value := make([]byte, valueSize)
			binary.LittleEndian.PutUint32(value, uint32(r))
			data = append(data, value...)
		}
		return data
	}

	cases := []struct {
		valueSize  int
		runs       int
		avgRunLen  int
		totalCount uint64
	}{
		{4, 100, 10, 1000},
		{4, 100, 100, 10000},
		{8, 100, 10, 1000},
		{8, 100, 100, 10000},
	}

	for _, c := range cases {
		name := fmt.Sprintf("size%d_runs%d_runlen%d", c.valueSize, c.runs, c.avgRunLen)
		b.Run(name, func(b *testing.B) {
			data := createRLEData(c.valueSize, c.runs, c.avgRunLen)

			b.ResetTimer()
			b.SetBytes(int64(c.valueSize) * int64(c.totalCount))

			for i := 0; i < b.N; i++ {
				_, err := DecompressRLE(data, c.valueSize, c.totalCount)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecompressBitPacking measures bit-packing decompression throughput.
func BenchmarkDecompressBitPacking(b *testing.B) {
	// Create bit-packed data
	createBitPackedData := func(bitWidth uint8, count uint64) []byte {
		// Header: bitWidth (1) + count (8) + packed data
		totalBits := uint64(bitWidth) * count
		dataBytes := (totalBits + 7) / 8

		data := make([]byte, 9+dataBytes)
		data[0] = bitWidth
		binary.LittleEndian.PutUint64(data[1:9], count)

		// Fill with random bits
		rng := rand.New(rand.NewSource(42))
		for i := uint64(0); i < dataBytes; i++ {
			data[9+i] = byte(rng.Intn(256))
		}

		return data
	}

	bitWidths := []uint8{1, 4, 8, 12, 16, 24, 32}
	counts := []uint64{1000, 10000}

	for _, bw := range bitWidths {
		for _, count := range counts {
			name := fmt.Sprintf("bits%d_count%d", bw, count)
			b.Run(name, func(b *testing.B) {
				data := createBitPackedData(bw, count)

				b.ResetTimer()
				b.SetBytes(int64(count) * 8) // Output is uint64

				for i := 0; i < b.N; i++ {
					_, err := DecompressBitPackingToUint64(data)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkDecompressBitPackingFast compares fast vs standard bit unpacking.
func BenchmarkDecompressBitPackingFast(b *testing.B) {
	// Test aligned bit widths (fast path)
	alignedWidths := []uint8{8, 16, 32, 64}

	for _, bw := range alignedWidths {
		count := uint64(10000)
		bytesPer := int(bw / 8)
		data := make([]byte, int(count)*bytesPer)

		// Fill with data
		rng := rand.New(rand.NewSource(42))
		for i := range data {
			data[i] = byte(rng.Intn(256))
		}

		b.Run(fmt.Sprintf("standard_bits%d", bw), func(b *testing.B) {
			b.SetBytes(int64(count) * 8)
			for i := 0; i < b.N; i++ {
				_, _ = DecompressBitPackingWithParams(data, bw, count)
			}
		})

		b.Run(fmt.Sprintf("fast_bits%d", bw), func(b *testing.B) {
			b.SetBytes(int64(count) * 8)
			for i := 0; i < b.N; i++ {
				_, _ = DecompressBitPackingFast(data, bw, count)
			}
		})
	}
}

// BenchmarkDecompressPFORDelta measures PFOR_DELTA decompression throughput.
func BenchmarkDecompressPFORDelta(b *testing.B) {
	// Create PFOR_DELTA data
	createPFORData := func(count uint64, bitWidth uint8) []byte {
		// Header: reference (8) + bitWidth (1) + count (8)
		deltaCount := count - 1
		totalBits := uint64(bitWidth) * deltaCount
		dataBytes := (totalBits + 7) / 8

		data := make([]byte, 17+dataBytes)
		binary.LittleEndian.PutUint64(data[0:8], 1000) // reference
		data[8] = bitWidth
		binary.LittleEndian.PutUint64(data[9:17], count)

		// Fill deltas with random bits
		rng := rand.New(rand.NewSource(42))
		for i := uint64(0); i < dataBytes; i++ {
			data[17+i] = byte(rng.Intn(256))
		}

		return data
	}

	cases := []struct {
		count    uint64
		bitWidth uint8
	}{
		{1000, 4},
		{1000, 8},
		{10000, 4},
		{10000, 8},
	}

	for _, c := range cases {
		name := fmt.Sprintf("count%d_bits%d", c.count, c.bitWidth)
		b.Run(name, func(b *testing.B) {
			data := createPFORData(c.count, c.bitWidth)

			b.ResetTimer()
			b.SetBytes(int64(c.count) * 8)

			for i := 0; i < b.N; i++ {
				_, err := DecompressPFORDelta(data, 8, c.count)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ============================================================================
// Column Scan Benchmarks
// ============================================================================

// BenchmarkColumnScan measures column scanning performance.
func BenchmarkColumnScan(b *testing.B) {
	// Create column data
	createColumnData := func(typeID LogicalTypeID, count uint64) *ColumnData {
		size := GetTypeSizeFast(typeID)
		if size <= 0 {
			size = 8
		}

		data := make([]byte, count*uint64(size))
		rng := rand.New(rand.NewSource(42))
		for i := range data {
			data[i] = byte(rng.Intn(256))
		}

		return &ColumnData{
			Data:       data,
			Validity:   nil, // All valid
			TupleCount: count,
			TypeID:     typeID,
		}
	}

	types := []LogicalTypeID{
		TypeInteger,
		TypeBigInt,
		TypeDouble,
	}

	counts := []uint64{1000, 10000, 100000}

	for _, typeID := range types {
		for _, count := range counts {
			name := fmt.Sprintf("%s_count%d", typeID.String(), count)
			b.Run(name, func(b *testing.B) {
				col := createColumnData(typeID, count)

				b.ResetTimer()
				b.SetBytes(int64(count) * int64(GetTypeSizeFast(typeID)))

				for i := 0; i < b.N; i++ {
					for j := uint64(0); j < count; j++ {
						_, _ = col.GetValue(j)
					}
				}
			})
		}
	}
}

// BenchmarkColumnScanBatch compares standard vs batch column access.
func BenchmarkColumnScanBatch(b *testing.B) {
	count := uint64(10000)

	// Create column data
	data := make([]byte, count*8)
	rng := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = byte(rng.Intn(256))
	}

	col := &ColumnData{
		Data:       data,
		Validity:   nil,
		TupleCount: count,
		TypeID:     TypeBigInt,
	}

	b.Run("standard", func(b *testing.B) {
		b.SetBytes(int64(count) * 8)
		for i := 0; i < b.N; i++ {
			for j := uint64(0); j < count; j++ {
				_, _ = col.GetValue(j)
			}
		}
	})

	b.Run("batch", func(b *testing.B) {
		batch := NewColumnDataBatch(col)
		b.SetBytes(int64(count) * 8)
		for i := 0; i < b.N; i++ {
			for j := uint64(0); j < count; j++ {
				_, _ = batch.GetInt64(j)
			}
		}
	})
}

// ============================================================================
// Type Conversion Benchmarks
// ============================================================================

// BenchmarkTypeConversion measures type conversion overhead.
func BenchmarkTypeConversion(b *testing.B) {
	types := []LogicalTypeID{
		TypeInteger,
		TypeBigInt,
		TypeDouble,
		TypeTimestamp,
	}

	for _, typeID := range types {
		name := typeID.String()
		b.Run(name, func(b *testing.B) {
			size := GetTypeSizeFast(typeID)
			data := make([]byte, size)

			// Fill with sample data
			switch typeID {
			case TypeInteger:
				binary.LittleEndian.PutUint32(data, 12345)
			case TypeBigInt:
				binary.LittleEndian.PutUint64(data, 1234567890)
			case TypeDouble:
				binary.LittleEndian.PutUint64(data, 0x4059000000000000) // 100.0
			case TypeTimestamp:
				binary.LittleEndian.PutUint64(data, 1704067200000000) // 2024-01-01
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = DecodeValue(data, typeID, nil)
			}
		})
	}
}

// BenchmarkGetTypeSize compares type size lookup methods.
func BenchmarkGetTypeSize(b *testing.B) {
	types := []LogicalTypeID{
		TypeInteger,
		TypeBigInt,
		TypeDouble,
		TypeTimestamp,
		TypeVarchar,
	}

	b.Run("standard", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, t := range types {
				_ = GetTypeSize(t)
			}
		}
	})

	b.Run("fast", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, t := range types {
				_ = GetTypeSizeFast(t)
			}
		}
	})
}

// ============================================================================
// Buffer Pool Benchmarks
// ============================================================================

// BenchmarkDecompressBuffer measures buffer pool efficiency.
func BenchmarkDecompressBuffer(b *testing.B) {
	b.Run("with_pool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := GetDecompressBuffer()
			_ = buf.Grow(65536)
			PutDecompressBuffer(buf)
		}
	})

	b.Run("without_pool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = make([]byte, 65536)
		}
	})
}

// ============================================================================
// Full Integration Benchmarks
// ============================================================================

// BenchmarkBlockManagerRead measures end-to-end block read performance.
func BenchmarkBlockManagerRead(b *testing.B) {
	// Create test file
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "bench.duckdb")

	f, err := os.Create(tmpFile)
	require.NoError(b, err)
	defer func() { _ = f.Close() }()

	// Write headers
	fileHeader := NewFileHeader()
	require.NoError(b, WriteFileHeader(f, fileHeader))

	dbHeader := NewDatabaseHeader()
	require.NoError(b, WriteDatabaseHeader(f, dbHeader, DatabaseHeader1Offset))
	require.NoError(b, WriteDatabaseHeader(f, dbHeader, DatabaseHeader2Offset))

	bm := NewBlockManager(f, DefaultBlockSize, 128)

	// Pre-write blocks
	numBlocks := 100
	for i := 0; i < numBlocks; i++ {
		data := make([]byte, DefaultBlockSize-BlockChecksumSize)
		rng := rand.New(rand.NewSource(int64(i)))
		for j := range data {
			data[j] = byte(rng.Intn(256))
		}
		block := &Block{ID: uint64(i), Data: data}
		require.NoError(b, bm.WriteBlock(block))
	}

	b.Run("cached", func(b *testing.B) {
		// Warm cache
		for i := 0; i < numBlocks; i++ {
			_, _ = bm.ReadBlock(uint64(i))
		}
		bm.ResetCacheStats()

		b.ResetTimer()
		b.SetBytes(int64(DefaultBlockSize))

		for i := 0; i < b.N; i++ {
			_, _ = bm.ReadBlock(uint64(i % numBlocks))
		}

		stats := bm.CacheStats()
		b.ReportMetric(stats.HitRate(), "hit_rate_%")
	})

	b.Run("cold", func(b *testing.B) {
		bm.cache.Clear()
		bm.ResetCacheStats()

		b.ResetTimer()
		b.SetBytes(int64(DefaultBlockSize))

		for i := 0; i < b.N; i++ {
			bm.cache.Clear() // Force disk read
			_, _ = bm.ReadBlock(uint64(i % numBlocks))
		}
	})

	b.Run("sequential_readahead", func(b *testing.B) {
		bm.cache.Clear()
		bm.ResetCacheStats()
		ra := NewBlockReadAhead(bm, 4)

		b.ResetTimer()
		b.SetBytes(int64(DefaultBlockSize))

		for i := 0; i < b.N; i++ {
			_, _ = ra.ReadBlock(uint64(i % numBlocks))
		}

		stats := bm.CacheStats()
		b.ReportMetric(stats.HitRate(), "hit_rate_%")
	})
}

// BenchmarkValidityMask measures validity mask operations.
func BenchmarkValidityMask(b *testing.B) {
	sizes := []uint64{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size%d", size), func(b *testing.B) {
			// Create mask with ~50% valid
			mask := NewValidityMask(size)
			rng := rand.New(rand.NewSource(42))
			for i := uint64(0); i < size; i++ {
				if rng.Intn(2) == 0 {
					mask.SetInvalid(i)
				}
			}

			b.Run("IsValid", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					for j := uint64(0); j < size; j++ {
						_ = mask.IsValid(j)
					}
				}
			})

			b.Run("CountValid", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_ = mask.CountValid(0, size)
				}
			})

			b.Run("BatchIsValid", func(b *testing.B) {
				batch := NewValidityMaskBatch(mask)
				for i := 0; i < b.N; i++ {
					for j := uint64(0); j < size; j++ {
						_ = batch.IsValidAt(j)
					}
				}
			})
		})
	}
}
