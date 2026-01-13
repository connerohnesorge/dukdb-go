// Package main demonstrates reading Apache Iceberg tables with dukdb-go.
//
// This example shows:
// - Opening an Iceberg table
// - Reading the current snapshot
// - Time travel queries (by snapshot ID and timestamp)
// - Getting table metadata and snapshot history
// - Column projection
// - Error handling
//
// To run this example, you need an Iceberg table. You can use the test
// fixtures in internal/io/iceberg/testdata/ or point to your own table.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	_ "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/io/iceberg"
)

func main() {
	// Get table path from command line or use default
	tablePath := getTablePath()

	// Demonstrate SQL-based access
	fmt.Println("=== SQL-Based Access ===")
	sqlExample(tablePath)

	// Demonstrate Go API access
	fmt.Println("\n=== Go API Access ===")
	goAPIExample(tablePath)

	// Demonstrate time travel
	fmt.Println("\n=== Time Travel ===")
	timeTravelExample(tablePath)

	// Demonstrate error handling
	fmt.Println("\n=== Error Handling ===")
	errorHandlingExample()
}

// getTablePath returns the Iceberg table path from args or uses a default.
func getTablePath() string {
	if len(os.Args) > 1 {
		return os.Args[1]
	}

	// Default to the test fixture if no path provided
	// You can change this to point to your own Iceberg table
	return "./internal/io/iceberg/testdata/simple_table"
}

// sqlExample demonstrates reading an Iceberg table using SQL.
func sqlExample(tablePath string) {
	// Open a dukdb connection
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		log.Printf("Failed to open database: %v", err)
		return
	}
	defer func() { _ = db.Close() }()

	// Read from the Iceberg table using iceberg_scan
	query := fmt.Sprintf(`SELECT id, name, value
		FROM iceberg_scan('%s')
		LIMIT 5`, tablePath)

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Failed to query Iceberg table: %v", err)
		return
	}
	defer func() { _ = rows.Close() }()

	fmt.Println("First 5 rows:")
	for rows.Next() {
		var id int64
		var name sql.NullString
		var value sql.NullFloat64

		if err := rows.Scan(&id, &name, &value); err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		nameStr := "<null>"
		if name.Valid {
			nameStr = name.String
		}

		valueStr := "<null>"
		if value.Valid {
			valueStr = fmt.Sprintf("%.2f", value.Float64)
		}

		fmt.Printf("  id=%d, name=%s, value=%s\n", id, nameStr, valueStr)
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v", err)
	}

	// Get table metadata
	metadataQuery := fmt.Sprintf(`SELECT * FROM iceberg_metadata('%s')`, tablePath)
	var formatVersion int
	var rowCount int64

	err = db.QueryRow(metadataQuery).Scan(&formatVersion, &rowCount)
	if err != nil {
		// iceberg_metadata might have different columns, just log the error
		log.Printf("Note: Could not get metadata (this is expected if iceberg_metadata is not registered): %v", err)
	} else {
		fmt.Printf("\nTable metadata: format_version=%d, row_count=%d\n", formatVersion, rowCount)
	}
}

// goAPIExample demonstrates reading an Iceberg table using the Go API directly.
func goAPIExample(tablePath string) {
	ctx := context.Background()

	// Open the Iceberg table
	table, err := iceberg.OpenTable(ctx, tablePath, nil)
	if err != nil {
		log.Printf("Failed to open table: %v", err)
		return
	}
	defer func() { _ = table.Close() }()

	// Print table information
	metadata := table.Metadata()
	fmt.Printf("Table location: %s\n", table.Location())
	fmt.Printf("Format version: %d\n", metadata.Version)
	fmt.Printf("Table UUID: %s\n", metadata.TableUUID)

	// Print schema information
	fmt.Println("\nSchema columns:")
	columns, err := table.SchemaColumns()
	if err != nil {
		log.Printf("Failed to get schema columns: %v", err)
	} else {
		for _, col := range columns {
			required := "optional"
			if col.Required {
				required = "required"
			}
			fmt.Printf("  %s: %s (%s)\n", col.Name, col.Type, required)
		}
	}

	// Print snapshot information
	snapshot := table.CurrentSnapshot()
	if snapshot != nil {
		fmt.Printf("\nCurrent snapshot: %d (created at %s)\n",
			snapshot.SnapshotID,
			snapshot.Timestamp().Format(time.RFC3339))
	}

	// Get row count
	rowCount, err := table.RowCount(ctx)
	if err != nil {
		log.Printf("Failed to get row count: %v", err)
	} else {
		fmt.Printf("Total rows: %d\n", rowCount)
	}

	// Read data using the Reader
	reader, err := iceberg.NewReader(ctx, tablePath, &iceberg.ReaderOptions{
		SelectedColumns: []string{"id", "name"}, // Column projection
		Limit:           3,                      // Row limit
	})
	if err != nil {
		log.Printf("Failed to create reader: %v", err)
		return
	}
	defer func() { _ = reader.Close() }()

	fmt.Println("\nReading with column projection (id, name) and limit 3:")

	// Get schema
	schema, err := reader.Schema()
	if err != nil {
		log.Printf("Failed to get schema: %v", err)
		return
	}
	fmt.Printf("Projected columns: %v\n", schema)

	// Read chunks
	totalRows := 0
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Failed to read chunk: %v", err)
			break
		}
		totalRows += chunk.Count()
		fmt.Printf("Read chunk with %d rows\n", chunk.Count())
	}
	fmt.Printf("Total rows read: %d\n", totalRows)
}

// timeTravelExample demonstrates time travel queries.
func timeTravelExample(tablePath string) {
	ctx := context.Background()

	// Open the table
	table, err := iceberg.OpenTable(ctx, tablePath, nil)
	if err != nil {
		log.Printf("Failed to open table: %v", err)
		return
	}
	defer func() { _ = table.Close() }()

	// List all snapshots
	snapshots := table.Snapshots()
	fmt.Printf("Table has %d snapshot(s):\n", len(snapshots))
	for i, snap := range snapshots {
		operation := snap.Summary["operation"]
		if operation == "" {
			operation = "unknown"
		}
		fmt.Printf("  %d. ID=%d, time=%s, operation=%s\n",
			i+1,
			snap.SnapshotID,
			snap.Timestamp().Format(time.RFC3339),
			operation)
	}

	if len(snapshots) == 0 {
		fmt.Println("No snapshots available for time travel demo")
		return
	}

	// Time travel to the first snapshot
	firstSnapshotID := snapshots[0].SnapshotID
	fmt.Printf("\nReading first snapshot (ID=%d):\n", firstSnapshotID)

	reader, err := iceberg.NewReader(ctx, tablePath, &iceberg.ReaderOptions{
		SnapshotID: &firstSnapshotID,
		Limit:      5,
	})
	if err != nil {
		log.Printf("Failed to create reader for snapshot: %v", err)
		return
	}
	defer func() { _ = reader.Close() }()

	// Trigger initialization
	_, err = reader.Schema()
	if err != nil {
		log.Printf("Failed to get schema: %v", err)
		return
	}

	// Count rows
	totalRows := int64(0)
	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Failed to read chunk: %v", err)
			break
		}
		totalRows += int64(chunk.Count())
	}
	fmt.Printf("Snapshot %d contains %d rows\n", firstSnapshotID, totalRows)

	// Time travel by timestamp (if we have multiple snapshots)
	if len(snapshots) > 1 {
		// Pick a timestamp between first and second snapshot
		ts1 := snapshots[0].TimestampMs
		ts2 := snapshots[1].TimestampMs
		midTs := ts1 + (ts2-ts1)/2

		fmt.Printf("\nTime travel to timestamp %d (between snapshots):\n", midTs)

		reader2, err := iceberg.NewReader(ctx, tablePath, &iceberg.ReaderOptions{
			Timestamp: &midTs,
			Limit:     5,
		})
		if err != nil {
			log.Printf("Failed to create reader for timestamp: %v", err)
			return
		}
		defer func() { _ = reader2.Close() }()

		// Trigger initialization
		_, err = reader2.Schema()
		if err != nil {
			log.Printf("Failed to get schema: %v", err)
			return
		}

		plan := reader2.ScanPlan()
		if plan != nil && plan.Snapshot != nil {
			fmt.Printf("Selected snapshot: %d (at %s)\n",
				plan.Snapshot.SnapshotID,
				plan.Snapshot.Timestamp().Format(time.RFC3339))
		}
	}
}

// errorHandlingExample demonstrates proper error handling.
func errorHandlingExample() {
	ctx := context.Background()

	// Example 1: Non-existent table
	fmt.Println("Trying to open non-existent table...")
	_, err := iceberg.OpenTable(ctx, "/nonexistent/path/to/table", nil)
	if err != nil {
		fmt.Printf("Expected error: %v\n", err)
	}

	// Example 2: Non-existent snapshot
	// This requires a valid table, so we skip if table doesn't exist
	tablePath := getTablePath()
	table, err := iceberg.OpenTable(ctx, tablePath, nil)
	if err != nil {
		fmt.Printf("Skipping snapshot error demo (table not available)\n")
		return
	}
	defer func() { _ = table.Close() }()

	fmt.Println("\nTrying to read non-existent snapshot...")
	nonExistentID := int64(9999999999)
	_, err = iceberg.NewReader(ctx, tablePath, &iceberg.ReaderOptions{
		SnapshotID: &nonExistentID,
	})
	if err != nil {
		fmt.Printf("Expected error: %v\n", err)
	} else {
		fmt.Println("Note: Error may occur during schema initialization")
	}
}
