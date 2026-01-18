// Package compatibility provides a test framework for verifying dukdb-go
// compatibility with the duckdb-go reference implementation.
//
// # Window Function Benchmarks
//
// This file contains performance benchmarks for window function operations.
// The benchmarks measure:
//
// 1. Large Single Partition Performance (Tasks 6.1-6.4)
//   - 100K rows in a single partition
//   - ROW_NUMBER, SUM window functions
//   - Target: < 1 second for 100K rows
//
// 2. Many Partitions Performance (Tasks 6.5-6.8)
//   - 100K rows across 10K partitions (10 rows per partition)
//   - Measures partitioning overhead
//   - Target: < 2 seconds for 100K rows / 10K partitions
//
// 3. Frame Overhead (Tasks 6.9-6.11)
//   - Various frame sizes to measure sliding window overhead
//   - Documents O(n) vs O(n^2) behavior for different cases
//
// Performance Characteristics:
//
//   - O(n): Row-by-row ranking functions (ROW_NUMBER, RANK, DENSE_RANK)
//   - O(n): Full partition aggregates (no sliding window)
//   - O(n^2) worst case: Sliding windows without optimization
//     (each row computes aggregate over potentially large frame)
//   - Partition overhead: O(k) where k is number of partitions
//
// Run benchmarks with:
//
//	go test -bench=BenchmarkWindow -benchmem ./compatibility/...
package compatibility

import (
	"database/sql"
	"fmt"
	"testing"

	// Import dukdb driver and engine (engine init() registers the backend)
	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

// setupBenchDB creates a new in-memory database for benchmarking
func setupBenchDB(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open db: %v", err)
	}
	return db
}

// insertBenchData inserts rowCount rows with optional partitioning
// If partitionCount > 0, creates partitionCount partitions with rowCount/partitionCount rows each
func insertBenchData(
	b *testing.B,
	db *sql.DB,
	rowCount int,
	partitionCount int,
) {
	b.Helper()

	// Create table with partition column
	_, err := db.Exec(`
		CREATE TABLE bench_data (
			id BIGINT,
			partition_id BIGINT,
			val DOUBLE
		)
	`)
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	// Insert data in batches for better performance
	// Use a prepared statement for efficiency
	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare("INSERT INTO bench_data VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("failed to prepare statement: %v", err)
	}
	defer func() { _ = stmt.Close() }()

	for i := 0; i < rowCount; i++ {
		var partitionID int64
		if partitionCount > 0 {
			// Distribute rows across partitions
			partitionID = int64(i % partitionCount)
		} else {
			// Single partition (partition_id = 0)
			partitionID = 0
		}

		_, err = stmt.Exec(int64(i), partitionID, float64(i)*1.5)
		if err != nil {
			b.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		b.Fatalf("failed to commit transaction: %v", err)
	}
}

// BenchmarkWindowLargeSinglePartition benchmarks window functions on 100K rows
// in a single partition. This tests the core window function evaluation without
// partitioning overhead.
//
// Tasks 6.1-6.4: Large single partition benchmark
// Target: < 1 second for 100K rows
func BenchmarkWindowLargeSinglePartition(b *testing.B) {
	const rowCount = 100_000

	db := setupBenchDB(b)
	defer func() { _ = db.Close() }()

	// Insert 100K rows with single partition
	insertBenchData(b, db, rowCount, 0)

	// Sub-benchmark: ROW_NUMBER (O(n) - simple sequential numbering)
	b.Run("ROW_NUMBER", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()

			if count != rowCount {
				b.Fatalf("expected %d rows, got %d", rowCount, count)
			}
		}
	})

	// Sub-benchmark: SUM running total (O(n) with default RANGE frame)
	// Default frame with ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	// This is O(n) because we maintain a running sum
	b.Run("SUM_RunningTotal", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, SUM(val) OVER (ORDER BY id) as running_sum
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()

			if count != rowCount {
				b.Fatalf("expected %d rows, got %d", rowCount, count)
			}
		}
	})

	// Sub-benchmark: RANK (O(n) - needs peer group detection)
	b.Run("RANK", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, RANK() OVER (ORDER BY partition_id) as rnk
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()

			if count != rowCount {
				b.Fatalf("expected %d rows, got %d", rowCount, count)
			}
		}
	})

	// Sub-benchmark: COUNT(*) full partition (O(n) - single pass)
	b.Run("COUNT_FullPartition", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, COUNT(*) OVER () as total
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()

			if count != rowCount {
				b.Fatalf("expected %d rows, got %d", rowCount, count)
			}
		}
	})

	// Sub-benchmark: LAG (O(n) - simple offset lookup)
	b.Run("LAG", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, LAG(val, 1) OVER (ORDER BY id) as prev_val
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()

			if count != rowCount {
				b.Fatalf("expected %d rows, got %d", rowCount, count)
			}
		}
	})
}

// BenchmarkWindowManyPartitions benchmarks window functions on 100K rows
// distributed across many partitions. This tests the partitioning overhead.
//
// Tasks 6.5-6.8: Many partitions benchmark
// Target: < 2 seconds for 100K rows / 10K partitions
func BenchmarkWindowManyPartitions(b *testing.B) {
	const rowCount = 100_000
	const partitionCount = 10_000 // 10 rows per partition

	db := setupBenchDB(b)
	defer func() { _ = db.Close() }()

	// Insert 100K rows distributed across 10K partitions
	insertBenchData(b, db, rowCount, partitionCount)

	// Sub-benchmark: ROW_NUMBER with partition
	b.Run("ROW_NUMBER_Partitioned", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, ROW_NUMBER() OVER (PARTITION BY partition_id ORDER BY id) as rn
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()

			if count != rowCount {
				b.Fatalf("expected %d rows, got %d", rowCount, count)
			}
		}
	})

	// Sub-benchmark: SUM with partition
	b.Run("SUM_Partitioned", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, SUM(val) OVER (PARTITION BY partition_id) as partition_sum
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()

			if count != rowCount {
				b.Fatalf("expected %d rows, got %d", rowCount, count)
			}
		}
	})

	// Sub-benchmark: Multiple window functions sharing partition
	b.Run("Multiple_SamePartition", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT
					id,
					ROW_NUMBER() OVER (PARTITION BY partition_id ORDER BY id) as rn,
					SUM(val) OVER (PARTITION BY partition_id) as partition_sum,
					COUNT(*) OVER (PARTITION BY partition_id) as partition_count
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()

			if count != rowCount {
				b.Fatalf("expected %d rows, got %d", rowCount, count)
			}
		}
	})

	// Sub-benchmark: RANK within partition
	b.Run("RANK_Partitioned", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, RANK() OVER (PARTITION BY partition_id ORDER BY val) as rnk
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()

			if count != rowCount {
				b.Fatalf("expected %d rows, got %d", rowCount, count)
			}
		}
	})
}

// BenchmarkWindowFrameOverhead benchmarks the overhead of different frame sizes.
// This demonstrates the O(n) vs O(n^2) behavior for different frame configurations.
//
// Tasks 6.9-6.11: Frame overhead benchmark
//
// Performance characteristics by frame type:
//
//	ROWS UNBOUNDED PRECEDING TO CURRENT ROW - O(n) with running accumulator
//	ROWS BETWEEN k PRECEDING AND k FOLLOWING - O(n*k) sliding window
//	ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING - O(n) single pass
//
// For very small frame sizes (k << n), performance is effectively O(n).
// For large frame sizes (k ~ n), performance degrades toward O(n^2).
func BenchmarkWindowFrameOverhead(b *testing.B) {
	// Use smaller dataset for frame overhead tests to keep benchmark time reasonable
	const rowCount = 10_000

	db := setupBenchDB(b)
	defer func() { _ = db.Close() }()

	insertBenchData(b, db, rowCount, 0)

	// Sub-benchmark: No frame (full partition) - O(n)
	b.Run("Frame_FullPartition", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, SUM(val) OVER () as total
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()
		}
	})

	// Sub-benchmark: Running total frame - O(n) with accumulator
	b.Run("Frame_RunningTotal", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()
		}
	})

	// Frame size benchmarks - varying window sizes
	frameSizes := []int{1, 5, 10, 50, 100}

	for _, size := range frameSizes {
		b.Run(fmt.Sprintf("Frame_Sliding_%d", size), func(b *testing.B) {
			query := fmt.Sprintf(`
				SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN %d PRECEDING AND %d FOLLOWING) as sliding_sum
				FROM bench_data
			`, size, size)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatalf("query failed: %v", err)
				}

				count := 0
				for rows.Next() {
					count++
				}
				if err := rows.Err(); err != nil {
					b.Fatalf("row iteration error: %v", err)
				}
				_ = rows.Close()
			}
		})
	}

	// Sub-benchmark: AVG sliding window (different aggregation)
	b.Run("Frame_AVG_Sliding_10", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id, AVG(val) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) as moving_avg
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()
		}
	})

	// Sub-benchmark: MIN/MAX sliding window
	b.Run("Frame_MINMAX_Sliding_10", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT id,
					MIN(val) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) as min_val,
					MAX(val) OVER (ORDER BY id ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING) as max_val
				FROM bench_data
			`)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				b.Fatalf("row iteration error: %v", err)
			}
			_ = rows.Close()
		}
	})
}

// BenchmarkWindowScaling tests how performance scales with dataset size.
// This helps document the overall complexity characteristics.
func BenchmarkWindowScaling(b *testing.B) {
	sizes := []int{1_000, 10_000, 50_000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("ROW_NUMBER_%dk", size/1000), func(b *testing.B) {
			db := setupBenchDB(b)
			defer func() { _ = db.Close() }()

			insertBenchData(b, db, size, 0)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				rows, err := db.Query(`
					SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn
					FROM bench_data
				`)
				if err != nil {
					b.Fatalf("query failed: %v", err)
				}

				count := 0
				for rows.Next() {
					count++
				}
				if err := rows.Err(); err != nil {
					b.Fatalf("row iteration error: %v", err)
				}
				_ = rows.Close()

				if count != size {
					b.Fatalf("expected %d rows, got %d", size, count)
				}
			}
		})
	}
}
