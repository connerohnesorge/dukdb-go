// Package iceberg provides Apache Iceberg table format support for dukdb-go.
//
// This package implements Iceberg metadata parsing, snapshot management, and type mapping
// to enable reading Iceberg tables from local and cloud storage. It leverages the official
// Apache iceberg-go library for spec compliance while integrating with dukdb-go's type
// system and existing Parquet infrastructure.
//
// # Architecture
//
// The package is organized into several components:
//
//	                    +-------------------+
//	                    |   SQL Query       |
//	                    | iceberg_scan(...) |
//	                    +--------+----------+
//	                             |
//	                             v
//	+-------------------+   +----+----+   +-------------------+
//	| MetadataReader    |<--| Table   |-->| SnapshotSelector  |
//	| (metadata.go)     |   |(table.go|   | (snapshot.go)     |
//	+-------------------+   +----+----+   +-------------------+
//	                             |
//	                             v
//	                    +--------+--------+
//	                    | ScanPlanner     |
//	                    | (scan.go)       |
//	                    +--------+--------+
//	                             |
//	                             v
//	+-------------------+   +----+----+   +-------------------+
//	| PartitionSpec     |   | Reader  |   | SchemaMapper      |
//	| (partition.go)    |   |(reader. |   | (schema.go)       |
//	+-------------------+   | go)     |   +-------------------+
//	                        +----+----+
//	                             |
//	                             v
//	                    +--------+--------+
//	                    | Parquet Reader  |
//	                    | (io/parquet)    |
//	                    +-----------------+
//
// # Key Components
//
//   - [Table]: Main entry point for accessing Iceberg tables
//   - [Reader]: Implements [io.FileReader] for reading table data
//   - [MetadataReader]: Parses metadata.json files
//   - [ManifestReader]: Parses manifest list and manifest files (AVRO)
//   - [ScanPlanner]: Creates scan plans with partition pruning
//   - [SchemaMapper]: Maps Iceberg types to DuckDB types
//   - [SnapshotSelector]: Handles time travel snapshot selection
//   - [PartitionSpec]: Evaluates partition transforms
//   - [RESTCatalog]: REST catalog client with OAuth2 support
//
// # Usage Patterns
//
// ## Opening a Table
//
// Use [OpenTable] to open an Iceberg table:
//
//	table, err := iceberg.OpenTable(ctx, "/path/to/iceberg/table", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer table.Close()
//
//	// Access table metadata
//	metadata := table.Metadata()
//	fmt.Printf("Format version: %d\n", metadata.Version)
//	fmt.Printf("Table UUID: %s\n", metadata.TableUUID)
//
// ## Reading Data
//
// Use [NewReader] for reading table data:
//
//	reader, err := iceberg.NewReader(ctx, "/path/to/table", &ReaderOptions{
//	    SelectedColumns: []string{"id", "name"},  // Column projection
//	    Limit:           1000,                    // Row limit
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer reader.Close()
//
//	for {
//	    chunk, err := reader.ReadChunk()
//	    if err == io.EOF {
//	        break
//	    }
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    // Process chunk
//	}
//
// ## Time Travel
//
// Query historical snapshots using [ReaderOptions]:
//
//	// By snapshot ID
//	snapshotID := int64(1234567890)
//	reader, err := iceberg.NewReader(ctx, path, &ReaderOptions{
//	    SnapshotID: &snapshotID,
//	})
//
//	// By timestamp (milliseconds since epoch)
//	timestamp := time.Now().Add(-24 * time.Hour).UnixMilli()
//	reader, err := iceberg.NewReader(ctx, path, &ReaderOptions{
//	    Timestamp: &timestamp,
//	})
//
// Alternatively, use the Table API for snapshot exploration:
//
//	// List all snapshots
//	for _, snap := range table.Snapshots() {
//	    fmt.Printf("Snapshot %d at %s\n", snap.SnapshotID, snap.Timestamp())
//	}
//
//	// Get snapshot by ID
//	snap, err := table.SnapshotByID(1234567890)
//
//	// Get snapshot by timestamp
//	snap, err := table.SnapshotAsOfTimestamp(time.Now().Add(-24 * time.Hour))
//
// ## Partition Pruning
//
// Partition filters are applied automatically during scan planning:
//
//	reader, err := iceberg.NewReader(ctx, path, &ReaderOptions{
//	    PartitionFilters: []PartitionFilterExpr{
//	        {FieldName: "date", Operator: "=", Value: "2024-01-15"},
//	        {FieldName: "region", Operator: "IN", Value: []any{"US", "EU"}},
//	    },
//	})
//
// After initialization, check the scan plan to verify pruning:
//
//	plan := reader.ScanPlan()
//	fmt.Printf("Total rows: %d\n", plan.TotalRowCount)
//	fmt.Printf("Rows after pruning: %d\n", plan.EstimatedRowCount)
//	fmt.Printf("Data files to read: %d\n", len(plan.DataFiles))
//
// ## REST Catalog
//
// Use [RESTCatalog] to access tables via the Iceberg REST catalog API:
//
//	// With OAuth2 client credentials
//	catalog, err := iceberg.NewRESTCatalog(ctx, &iceberg.RESTCatalogOptions{
//	    URI:        "https://catalog.example.com",
//	    Credential: "client_id:client_secret",
//	    Scope:      "catalog",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer catalog.Close()
//
//	// List namespaces
//	namespaces, err := catalog.ListNamespaces(ctx, nil)
//
//	// List tables in a namespace
//	tables, err := catalog.ListTables(ctx, []string{"production"})
//
//	// Load table metadata
//	table, err := catalog.LoadTable(ctx, []string{"production"}, "events")
//
// With a pre-existing bearer token:
//
//	catalog, err := iceberg.NewRESTCatalog(ctx, &iceberg.RESTCatalogOptions{
//	    URI:   "https://catalog.example.com",
//	    Token: "your-bearer-token",
//	})
//
// You can also parse catalog URIs in the format "iceberg://host/namespace/table":
//
//	catalogURI, namespace, table, err := iceberg.ParseCatalogURI("iceberg://catalog.example.com/prod/events")
//
// # Type Mapping
//
// Iceberg types are mapped to DuckDB types as follows:
//
//	Iceberg Type    -> DuckDB Type
//	-----------------------------
//	boolean         -> BOOLEAN
//	int             -> INTEGER
//	long            -> BIGINT
//	float           -> FLOAT
//	double          -> DOUBLE
//	string          -> VARCHAR
//	binary          -> BLOB
//	date            -> DATE
//	timestamp       -> TIMESTAMP
//	timestamptz     -> TIMESTAMPTZ
//	time            -> TIME
//	uuid            -> UUID
//	fixed[N]        -> BLOB
//	decimal(P,S)    -> DECIMAL(P,S)
//	struct          -> STRUCT
//	list            -> LIST
//	map             -> MAP
//
// Use [SchemaMapper] for custom type mapping needs.
//
// # Partition Transforms
//
// Supported partition transforms for pruning:
//
//   - identity: Partition by exact value
//   - bucket[N]: Hash into N buckets using murmur3
//   - truncate[W]: Truncate strings to W characters or integers to W-width
//   - year: Extract year from date/timestamp (years since 1970)
//   - month: Extract month from date/timestamp (months since 1970-01)
//   - day: Extract day from date/timestamp (days since epoch)
//   - hour: Extract hour from timestamp (hours since epoch)
//   - void: Always returns null (no partitioning)
//
// # Error Handling
//
// The package defines several error types for specific error conditions:
//
//   - [ErrTableNotFound]: No Iceberg table at the specified location
//   - [ErrSnapshotNotFound]: Requested snapshot ID does not exist
//   - [ErrNoSnapshotAtTimestamp]: No snapshot exists at or before timestamp
//   - [ErrInvalidMetadata]: Corrupt or invalid metadata.json
//   - [ErrUnsupportedVersion]: Iceberg format version not supported (v3+)
//   - [ErrUnsupportedType]: Iceberg type cannot be mapped to DuckDB
//   - [ErrNoCurrentSnapshot]: Table has no snapshots (empty)
//
// # Supported Features
//
//   - Iceberg format v1 and v2
//   - Parquet data files
//   - AVRO manifest files
//   - Time travel (snapshot ID and timestamp)
//   - Partition pruning (identity, bucket, truncate, temporal)
//   - Column projection
//   - Schema evolution (read compatibility)
//   - Local and cloud storage (S3, GCS, Azure via filesystem abstraction)
//   - REST catalog with OAuth2 authentication
//
// # Limitations
//
// Current limitations (may be addressed in future versions):
//
//   - ORC and AVRO data files not supported (Parquet only)
//   - Write operations not supported (read-only)
//   - Iceberg format v3 not supported
//
// # Performance Considerations
//
// For optimal performance:
//
//   - Use column projection to read only needed columns
//   - Place filters on partition columns for automatic pruning
//   - Use row limits when exploring data
//   - Reuse [Table] objects for repeated queries on the same table
//   - Consider parallel reading for large tables (future work)
//
// # Thread Safety
//
// [Table] objects are safe for concurrent read access after creation.
// [Reader] objects should be used from a single goroutine.
//
// # References
//
//   - Apache Iceberg Specification: https://iceberg.apache.org/spec/
//   - Apache iceberg-go library: https://github.com/apache/iceberg-go
//   - dukdb-go Parquet reader: github.com/dukdb/dukdb-go/internal/io/parquet
package iceberg
