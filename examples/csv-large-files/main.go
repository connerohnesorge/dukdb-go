package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/dukdb/dukdb-go"
	// Import engine to register backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
	// Create a new database connection
	db, err := sql.Open("dukdb", "")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	fmt.Println("=== Large CSV Files Handling Example ===")

	// Configuration
	const (
		numRows     = 100000 // 100k rows for demonstration
		batchSize   = 10000  // Process in batches
		memoryLimit = "1GB"  // Memory limit for DuckDB
	)

	// Example 1: Generate large CSV file
	fmt.Printf("\n1. Generating large CSV file with %d rows...\n", numRows)

	largeCSVFile := "large_dataset.csv"
	startTime := time.Now()

	err = generateLargeCSV(largeCSVFile, numRows)
	if err != nil {
		log.Fatal("Failed to generate large CSV:", err)
	}
	defer os.Remove(largeCSVFile)

	generationTime := time.Since(startTime)
	fileSize := getFileSize(largeCSVFile)
	fmt.Printf(
		"Generated %s in %v (%.2f MB)\n",
		largeCSVFile,
		generationTime,
		float64(fileSize)/(1024*1024),
	)

	// Example 2: Efficient reading with sampling
	fmt.Println("\n2. Efficient reading with sampling:")

	// Set memory limit
	_, err = db.Exec(fmt.Sprintf("SET memory_limit='%s'", memoryLimit))
	if err != nil {
		log.Printf("Warning: Failed to set memory limit: %v", err)
	}

	// Read with sampling
	startTime = time.Now()
	rows, err := db.Query(
		fmt.Sprintf("SELECT * FROM read_csv_auto('%s', sample_size=%d)", largeCSVFile, 1000),
	)
	if err != nil {
		log.Fatal("Failed to read CSV with sampling:", err)
	}
	defer rows.Close()

	var sampleCount int
	for rows.Next() {
		sampleCount++
	}
	readTime := time.Since(startTime)
	fmt.Printf("Sampled %d rows in %v\n", sampleCount, readTime)

	// Example 3: Columnar reading with projection
	fmt.Println("\n3. Columnar reading with projection:")

	startTime = time.Now()
	var totalSales float64
	err = db.QueryRow(fmt.Sprintf("SELECT SUM(sale_amount) FROM read_csv_auto('%s')", largeCSVFile)).
		Scan(&totalSales)
	if err != nil {
		log.Fatal("Failed to calculate total sales:", err)
	}
	queryTime := time.Since(startTime)
	fmt.Printf("Calculated total sales ($%.2f) in %v\n", totalSales, queryTime)

	// Example 4: Batch processing
	fmt.Println("\n4. Batch processing with LIMIT and OFFSET:")

	startTime = time.Now()
	processedRows := 0
	offset := 0

	for offset < numRows {
		query := fmt.Sprintf(`
			SELECT
				customer_id,
				SUM(sale_amount) as total_spent,
				COUNT(*) as order_count
			FROM read_csv_auto('%s')
			LIMIT %d OFFSET %d
			GROUP BY customer_id
		`, largeCSVFile, batchSize, offset)

		rows, err := db.Query(query)
		if err != nil {
			log.Fatal("Failed to process batch:", err)
		}

		batchRows := 0
		for rows.Next() {
			var customerID int
			var totalSpent float64
			var orderCount int

			err := rows.Scan(&customerID, &totalSpent, &orderCount)
			if err != nil {
				log.Fatal("Failed to scan row:", err)
			}
			batchRows++
			processedRows++
		}
		rows.Close()

		if batchRows == 0 {
			break
		}

		offset += batchSize
		if offset%50000 == 0 {
			fmt.Printf("Processed %d rows...\n", offset)
		}
	}

	batchTime := time.Since(startTime)
	fmt.Printf("Processed %d customer records in %v\n", processedRows, batchTime)

	// Example 5: Parallel processing simulation
	fmt.Println("\n5. Parallel processing with partitioning:")

	startTime = time.Now()

	// Partition by date range
	partitions := []struct {
		name  string
		start string
		end   string
	}{
		{"Q1", "2023-01-01", "2023-03-31"},
		{"Q2", "2023-04-01", "2023-06-30"},
		{"Q3", "2023-07-01", "2023-09-30"},
		{"Q4", "2023-10-01", "2023-12-31"},
	}

	var wg sync.WaitGroup
	results := make(chan partitionResult, len(partitions))

	for _, partition := range partitions {
		wg.Add(1)
		go func(p struct{ name, start, end string }) {
			defer wg.Done()

			var count int
			var total float64
			query := fmt.Sprintf(`
				SELECT COUNT(*), SUM(sale_amount)
				FROM read_csv_auto('%s')
				WHERE sale_date >= '%s' AND sale_date < '%s'
			`, largeCSVFile, p.start, p.end)

			err := db.QueryRow(query).Scan(&count, &total)
			if err != nil {
				log.Printf("Error processing partition %s: %v", p.name, err)
				return
			}

			results <- partitionResult{
				partition: p.name,
				count:     count,
				total:     total,
			}
		}(partition)
	}

	// Wait for all partitions to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	for result := range results {
		fmt.Printf(
			"Partition %s: %d orders, $%.2f total\n",
			result.partition,
			result.count,
			result.total,
		)
	}

	partitionTime := time.Since(startTime)
	fmt.Printf("Parallel processing completed in %v\n", partitionTime)

	// Example 6: Streaming with compression
	fmt.Println("\n6. Streaming export with compression:")

	startTime = time.Now()
	compressedFile := "large_dataset_compressed.csv.gz"

	// Export with compression
	_, err = db.Exec(fmt.Sprintf(`
		COPY (
			SELECT
				customer_id,
				product_id,
				sale_amount,
				sale_date
			FROM read_csv_auto('%s')
			WHERE sale_amount > 100
		) TO '%s' WITH (COMPRESSION 'gzip')
	`, largeCSVFile, compressedFile))
	if err != nil {
		log.Printf("Note: Compression might not be supported in this version: %v", err)
		// Fallback to regular export
		_, err = db.Exec(fmt.Sprintf(`
			COPY (
				SELECT
					customer_id,
					product_id,
					sale_amount,
					sale_date
				FROM read_csv_auto('%s')
				WHERE sale_amount > 100
			) TO '%s'
		`, largeCSVFile, strings.TrimSuffix(compressedFile, ".gz")))
		if err != nil {
			log.Fatal("Failed to export filtered data:", err)
		}
		compressedFile = strings.TrimSuffix(compressedFile, ".gz")
	}
	defer os.Remove(compressedFile)

	compressionTime := time.Since(startTime)
	compressedSize := getFileSize(compressedFile)
	compressionRatio := float64(fileSize-compressedSize) / float64(fileSize) * 100

	fmt.Printf("Exported filtered data to %s in %v\n", compressedFile, compressionTime)
	fmt.Printf("Compression ratio: %.1f%%\n", compressionRatio)

	// Example 7: Memory-efficient aggregation
	fmt.Println("\n7. Memory-efficient aggregation using temporary tables:")

	startTime = time.Now()

	// Create temporary table for intermediate results
	_, err = db.Exec(`
		CREATE TEMPORARY TABLE monthly_sales AS
		SELECT
			DATE_TRUNC('month', sale_date::date) as month,
			COUNT(*) as order_count,
			SUM(sale_amount) as total_sales,
			AVG(sale_amount) as avg_order_value
		FROM read_csv_auto('large_dataset.csv')
		GROUP BY DATE_TRUNC('month', sale_date::date)
		ORDER BY month
	`)
	if err != nil {
		log.Fatal("Failed to create temporary table:", err)
	}

	// Query the aggregated data
	rows, err = db.Query("SELECT * FROM monthly_sales LIMIT 12")
	if err != nil {
		log.Fatal("Failed to query monthly sales:", err)
	}
	defer rows.Close()

	fmt.Println("Monthly sales summary:")
	for rows.Next() {
		var month string
		var orderCount int
		var totalSales float64
		var avgOrderValue float64

		err := rows.Scan(&month, &orderCount, &totalSales, &avgOrderValue)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		fmt.Printf(
			"%s: %d orders, $%.2f total, $%.2f avg\n",
			month[:7],
			orderCount,
			totalSales,
			avgOrderValue,
		)
	}

	aggTime := time.Since(startTime)
	fmt.Printf("Aggregation completed in %v\n", aggTime)

	// Example 8: Indexing for faster queries
	fmt.Println("\n8. Creating index for faster repeated queries:")

	startTime = time.Now()

	// Create a persistent table with index
	_, err = db.Exec(`
		CREATE TABLE sales_data AS
		SELECT * FROM read_csv_auto('large_dataset.csv')
	`)
	if err != nil {
		log.Fatal("Failed to create sales_data table:", err)
	}

	// Create index on frequently queried column
	_, err = db.Exec("CREATE INDEX idx_customer_id ON sales_data(customer_id)")
	if err != nil {
		log.Printf("Note: Index creation might not be fully supported: %v", err)
	}

	// Query with index
	var customerTotal float64
	err = db.QueryRow("SELECT SUM(sale_amount) FROM sales_data WHERE customer_id = 12345").
		Scan(&customerTotal)
	if err != nil {
		log.Fatal("Failed to query with index:", err)
	}

	indexTime := time.Since(startTime)
	fmt.Printf("Customer 12345 total: $%.2f (query time: %v)\n", customerTotal, indexTime)

	// Example 9: Progressive loading
	fmt.Println("\n9. Progressive loading with incremental processing:")

	// Simulate progressive loading
	progressiveFile := "progressive_dataset.csv"
	chunkSize := 10000
	totalChunks := 10

	startTime = time.Now()

	for chunk := 0; chunk < totalChunks; chunk++ {
		// Generate chunk
		chunkData := generateCSVChunk(chunk*chunkSize, chunkSize)

		// Write chunk to file
		mode := os.O_CREATE | os.O_WRONLY
		if chunk > 0 {
			mode |= os.O_APPEND
		}

		f, err := os.OpenFile(progressiveFile, mode, 0644)
		if err != nil {
			log.Fatal("Failed to open progressive file:", err)
		}

		if chunk == 0 {
			// Write header for first chunk
			f.WriteString("id,customer_id,product_id,sale_amount,sale_date\n")
		}

		f.WriteString(chunkData)
		f.Close()

		// Process the chunk immediately
		var chunkSum float64
		var chunkCount int
		query := fmt.Sprintf(`
			SELECT COUNT(*), SUM(sale_amount)
			FROM read_csv_auto('%s')
			WHERE id >= %d AND id < %d
		`, progressiveFile, chunk*chunkSize, (chunk+1)*chunkSize)

		err = db.QueryRow(query).Scan(&chunkCount, &chunkSum)
		if err != nil {
			log.Fatal("Failed to process chunk:", err)
		}

		fmt.Printf("Processed chunk %d/%d: %d records, $%.2f total\n",
			chunk+1, totalChunks, chunkCount, chunkSum)
	}

	progressiveTime := time.Since(startTime)
	fmt.Printf("Progressive processing completed in %v\n", progressiveTime)
	defer os.Remove(progressiveFile)

	// Example 10: Performance summary
	fmt.Println("\n10. Performance Summary:")
	fmt.Println("=" + strings.Repeat("=", 50))
	fmt.Printf("Dataset size: %d rows (%.2f MB)\n", numRows, float64(fileSize)/(1024*1024))
	fmt.Printf("Sampling: %v\n", generationTime)
	fmt.Printf("Aggregation: %v\n", queryTime)
	fmt.Printf("Batch processing: %v\n", batchTime)
	fmt.Printf("Parallel processing: %v\n", partitionTime)
	fmt.Printf("Compression: %v\n", compressionTime)
	fmt.Printf("Memory aggregation: %v\n", aggTime)
	fmt.Printf("Indexed query: %v\n", indexTime)
	fmt.Printf("Progressive loading: %v\n", progressiveTime)

	// Clean up
	_, err = db.Exec("DROP TABLE IF EXISTS sales_data")
	if err != nil {
		log.Printf("Warning: Failed to drop sales_data table: %v", err)
	}

	fmt.Println("\n✓ Large CSV files handling example completed successfully!")
}

// Helper function to generate large CSV data
func generateLargeCSV(filename string, numRows int) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write header
	_, err = f.WriteString("id,customer_id,product_id,sale_amount,sale_date\n")
	if err != nil {
		return err
	}

	// Generate data in batches
	batchSize := 1000
	for i := 0; i < numRows; i += batchSize {
		var batch strings.Builder
		end := i + batchSize
		if end > numRows {
			end = numRows
		}

		for j := i; j < end; j++ {
			saleAmount := float64(rand.Intn(1000)) + rand.Float64()
			saleDate := generateRandomDate()
			line := fmt.Sprintf("%d,%d,%d,%.2f,%s\n",
				j+1,
				rand.Intn(10000)+1000,
				rand.Intn(1000)+1,
				saleAmount,
				saleDate,
			)
			batch.WriteString(line)
		}

		_, err = f.WriteString(batch.String())
		if err != nil {
			return err
		}
	}

	return nil
}

// Helper function to generate random date in 2023
func generateRandomDate() string {
	start := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)

	duration := end.Sub(start)
	randomDuration := time.Duration(rand.Int63n(int64(duration)))

	randomDate := start.Add(randomDuration)
	return randomDate.Format("2006-01-02")
}

// Helper function to generate CSV chunk
func generateCSVChunk(startID, count int) string {
	var chunk strings.Builder

	for i := 0; i < count; i++ {
		id := startID + i + 1
		saleAmount := float64(rand.Intn(500)) + rand.Float64()
		saleDate := generateRandomDate()

		line := fmt.Sprintf("%d,%d,%d,%.2f,%s\n",
			id,
			rand.Intn(1000)+1000,
			rand.Intn(100)+1,
			saleAmount,
			saleDate,
		)
		chunk.WriteString(line)
	}

	return chunk.String()
}

// Helper function to get file size
func getFileSize(filename string) int64 {
	info, err := os.Stat(filename)
	if err != nil {
		return 0
	}
	return info.Size()
}

// Partition result structure
type partitionResult struct {
	partition string
	count     int
	total     float64
}
