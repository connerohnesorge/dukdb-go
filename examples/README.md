# dukdb-go Examples

Complete collection of pure Go examples demonstrating dukdb-go driver capabilities.

## Structure

Every example is a self-contained Go application with:
- `main.go` - Complete working example
- `go.mod` - Go module definition
- `README.md` - Documentation and usage instructions
- Sample data files (where applicable)

## All Examples (29 Total)

### Core Database Operations
- **alter-tables** - Modify table schemas
- **create-tables** - Create tables with various types
- **crud-operations** - Complete CRUD operations
- **delete-operations** - DELETE statements
- **insert-statements** - INSERT operations
- **select-queries** - SELECT with WHERE clauses
- **update-operations** - UPDATE statements

### CSV Operations
- **csv** - Overview of all CSV capabilities
- **csv-analysis** - Data analysis on CSV files
- **csv-data-cleaning** - Data cleaning operations
- **csv-large-files** - Handle large datasets
- **csv-multiple-files** - Multi-file processing
- **csv-read-auto** - Auto-detect CSV format
- **csv-read-basic** - Basic CSV reading
- **csv-read-options** - Reading with custom options
- **cvsv-transformation** - Transform CSV data
- **csv-write-basic** - Basic CSV writing
- **csv-write-options** - Writing with custom options

### Advanced Features
- **etl** - Complete ETL pipeline (Extract, Transform, Load)
- **indexes** - Create and use indexes for performance
- **json** - JSON storage and querying
- **limit-orderby** - LIMIT and ORDER BY clauses
- **null-handling** - Handle NULL values
- **parquet** - Apache Parquet format support
- **schemas** - Database schema organization
- **table-joins** - JOIN operations
- **transactions** - ACID transactions with savepoints
- **iceberg** - Apache Iceberg table format
- **wasm** - WebAssembly compilation (special build)

## Running Examples

```bash
cd examples/any-example
go run main.go
```

## Requirements

- Go 1.24.9+
- dukdb-go driver (dependency managed by go.mod)
- No CGO required (pure Go implementation)
- All examples build and run successfully

## Key Features Demonstrated

- **Zero CGO**: Pure Go implementation
- **API Compatibility**: Drop-in replacement for duckdb-go
- **Multiple Formats**: CSV, JSON, Parquet, Iceberg
- **Transactions**: ACID compliance with savepoints
- **Performance**: Indexes, aggregations, optimizations
- **ETL**: Complete data pipeline workflows
- **Schemas**: Namespace organization
