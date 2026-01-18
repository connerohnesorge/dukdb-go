# dukdb-go

Pure Go implementation of a DuckDB-compatible database driver.

## Features

- **Pure Go** - Zero CGO dependencies, compiles anywhere Go compiles
- **database/sql Compatible** - Standard Go database interface
- **Cloud Storage** - Native support for S3, GCS, Azure, and HTTP/HTTPS
- **Glob Patterns** - Read multiple files with `*.csv`, `**/*.parquet`, `[0-9]` patterns
- **Extended Type Support** - JSON, GEOMETRY, BIGNUM, VARIANT, LAMBDA types
- **Math Functions** - 45+ mathematical functions for numerical analysis
- **String Functions** - 30+ string functions for text processing and fuzzy matching
- **Spatial Functions** - 30+ ST_* functions for GIS operations
- **JSON Operators** - Path extraction with `->` and `->>` operators
- **Cross-Platform** - Works with TinyGo, WebAssembly, and standard Go

## Installation

```bash
go get github.com/dukdb/dukdb-go
```

## Quick Start

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/dukdb/dukdb-go"
    _ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
    // Open an in-memory database
    db, err := sql.Open("dukdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create a table
    _, err = db.Exec(`CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        data JSON
    )`)
    if err != nil {
        log.Fatal(err)
    }

    // Insert data
    _, err = db.Exec(`INSERT INTO users VALUES (1, 'Alice', '{"age": 30}')`)
    if err != nil {
        log.Fatal(err)
    }

    // Query with JSON extraction
    var name string
    var age string
    err = db.QueryRow(`SELECT name, data->>'age' FROM users WHERE id = 1`).Scan(&name, &age)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("%s's age: %s\n", name, age)
}
```

## Extended Types

dukdb-go supports several extended types beyond standard SQL. See [docs/types.md](docs/types.md) for comprehensive documentation.

### JSON Type

Store and query JSON documents with extraction operators:

```sql
-- Create table with JSON column
CREATE TABLE events (id INTEGER, data JSON);
INSERT INTO events VALUES (1, '{"type": "click", "x": 100, "y": 200}');

-- Extract values
SELECT data->'type' FROM events;           -- '"click"' (JSON)
SELECT data->>'type' FROM events;          -- '"click"' (text)
SELECT data->'x' FROM events;              -- '100'

-- Chained extraction
SELECT data->'nested'->'deep' FROM events;

-- JSON functions
SELECT json_valid('{"valid": true}');      -- true
SELECT json_extract(data, 'type') FROM events;
```

### Geometry Type

Full spatial support with 30+ functions:

```sql
-- Create points
SELECT ST_Point(0, 0);
SELECT ST_GeomFromText('POINT(1.5 2.5)');

-- Distance calculation (Pythagorean theorem)
SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4));  -- 5.0

-- Spatial predicates
SELECT ST_Contains(
    ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
    ST_Point(5, 5)
);  -- true

-- Geometry operations
SELECT ST_Buffer(ST_Point(0, 0), 10);  -- Circle polygon
SELECT ST_Union(geom1, geom2);
SELECT ST_Intersection(geom1, geom2);

-- Convert formats
SELECT ST_AsText(ST_Point(3, 4));  -- 'POINT(3 4)'
SELECT ST_GeometryType(geom);      -- 'POLYGON', 'POINT', etc.
```

### BIGNUM Type

Arbitrary precision integers:

```sql
CREATE TABLE big_numbers (val BIGNUM);
INSERT INTO big_numbers VALUES ('123456789012345678901234567890');

-- Values beyond int64 range are preserved
SELECT val FROM big_numbers;
```

### VARIANT Type

Dynamic/flexible data storage:

```sql
CREATE TABLE flexible (data VARIANT);
INSERT INTO flexible VALUES ('{"dynamic": true}');
INSERT INTO flexible VALUES ('[1, 2, 3]');
INSERT INTO flexible VALUES ('"string"');
INSERT INTO flexible VALUES ('42');
```

### LAMBDA Type

Store lambda expressions:

```sql
CREATE TABLE transforms (expr LAMBDA);
INSERT INTO transforms VALUES ('x -> x + 1');
INSERT INTO transforms VALUES ('(x, y) -> x * y');
```

## Math Functions

dukdb-go provides 45+ mathematical functions for numerical analysis and scientific computing. See [docs/math-functions.md](docs/math-functions.md) for comprehensive documentation.

### Rounding Functions

```sql
-- Round to decimal places
SELECT ROUND(3.14159, 2);        -- 3.14
SELECT ROUND(2.5);               -- 3.0

-- Banker's rounding (round half to even)
SELECT ROUND_EVEN(2.5, 0);       -- 2.0
SELECT ROUND_EVEN(3.5, 0);       -- 4.0

-- Ceiling and floor
SELECT CEIL(2.1);                -- 3.0
SELECT FLOOR(2.9);               -- 2.0

-- Truncate towards zero
SELECT TRUNC(-2.9);              -- -2.0
```

### Scientific Functions

```sql
-- Square and cube roots
SELECT SQRT(16);                 -- 4.0
SELECT CBRT(27);                 -- 3.0

-- Exponentiation and logarithms
SELECT POW(2, 10);               -- 1024.0
SELECT EXP(1);                   -- 2.718281828...
SELECT LN(2.718281828);          -- ~1.0
SELECT LOG10(100);               -- 2.0
SELECT LOG2(8);                  -- 3.0

-- Special functions
SELECT FACTORIAL(5);             -- 120
SELECT GAMMA(5);                 -- 24.0 (factorial of 4)
```

### Trigonometric Functions

```sql
-- Basic trig (input in radians)
SELECT SIN(0);                   -- 0.0
SELECT COS(0);                   -- 1.0
SELECT TAN(PI() / 4);            -- ~1.0

-- Inverse trig
SELECT ASIN(1);                  -- ~1.5707... (PI/2)
SELECT ATAN2(1, 1);              -- ~0.7853... (PI/4)

-- Angle conversion
SELECT DEGREES(PI());            -- 180.0
SELECT RADIANS(180);             -- ~3.14159...
```

### Hyperbolic Functions

```sql
SELECT SINH(0);                  -- 0.0
SELECT COSH(0);                  -- 1.0
SELECT TANH(0);                  -- 0.0
SELECT ASINH(0);                 -- 0.0
```

### Utility Functions

```sql
-- Constants
SELECT PI();                     -- 3.141592653589793

-- Random numbers
SELECT RANDOM();                 -- Random value between 0 and 1

-- Number theory
SELECT GCD(12, 18);              -- 6
SELECT LCM(4, 6);                -- 12

-- Floating-point validation
SELECT ISNAN(0.0/0.0);           -- true
SELECT ISINF(1.0/0.0);           -- true
SELECT ISFINITE(42.0);           -- true
```

### Bitwise Operations

```sql
-- Bitwise operators (integer types only)
SELECT 5 & 3;                    -- 1 (AND)
SELECT 5 | 3;                    -- 7 (OR)
SELECT 5 ^ 3;                    -- 6 (XOR)
SELECT ~5;                       -- -6 (NOT)

-- Bit shifting
SELECT 1 << 4;                   -- 16
SELECT 16 >> 2;                  -- 4

-- Count set bits
SELECT BIT_COUNT(255);           -- 8
```

### Practical Examples

```sql
-- Financial: Round to currency
SELECT ROUND(price * 1.0825, 2) AS total_with_tax FROM products;

-- Scientific: Calculate distance
SELECT SQRT(POW(x2 - x1, 2) + POW(y2 - y1, 2)) AS distance FROM points;

-- Trigonometry: Convert coordinates
SELECT
    distance * COS(RADIANS(angle)) AS x,
    distance * SIN(RADIANS(angle)) AS y
FROM polar_coords;

-- Bitwise: Check permission flags
SELECT * FROM users WHERE permissions & 4 != 0; -- Has write permission
```

## String Functions

dukdb-go provides 30+ string functions for text processing, data validation, pattern matching, and fuzzy matching. See [docs/string-functions.md](docs/string-functions.md) for comprehensive documentation.

### Regular Expressions (RE2)

```sql
-- Test if string matches pattern
SELECT REGEXP_MATCHES('hello123', '[0-9]+');     -- true

-- Replace matches (first only by default, 'g' for global)
SELECT REGEXP_REPLACE('foo bar foo', 'foo', 'baz');       -- 'baz bar foo'
SELECT REGEXP_REPLACE('foo bar foo', 'foo', 'baz', 'g');  -- 'baz bar baz'

-- Extract matches
SELECT REGEXP_EXTRACT('user@example.com', '([^@]+)@(.+)', 1);  -- 'user'
SELECT REGEXP_EXTRACT('user@example.com', '([^@]+)@(.+)', 2);  -- 'example.com'

-- Extract all matches
SELECT REGEXP_EXTRACT_ALL('a1b2c3', '[0-9]+', 0);  -- ['1', '2', '3']

-- Split by regex pattern
SELECT REGEXP_SPLIT_TO_ARRAY('one,two;three', '[,;]');  -- ['one', 'two', 'three']
```

### String Manipulation

```sql
-- Concatenation with separator (skips NULLs)
SELECT CONCAT_WS(', ', 'Alice', NULL, 'Bob');    -- 'Alice, Bob'

-- Split string into array
SELECT STRING_SPLIT('a,b,c', ',');               -- ['a', 'b', 'c']

-- Padding
SELECT LPAD('42', 5, '0');                       -- '00042'
SELECT RPAD('hello', 10, '.');                   -- 'hello.....'

-- Manipulation
SELECT REVERSE('hello');                         -- 'olleh'
SELECT REPEAT('ab', 3);                          -- 'ababab'
SELECT LEFT('hello', 2);                         -- 'he'
SELECT RIGHT('hello', 2);                        -- 'lo'

-- Search
SELECT POSITION('world' IN 'hello world');       -- 7 (1-based)
SELECT CONTAINS('hello world', 'world');         -- true
SELECT STARTS_WITH('hello', 'he');               -- true
SELECT ENDS_WITH('hello', 'lo');                 -- true
```

### Cryptographic Hashes

```sql
-- MD5 and SHA256 (lowercase hex output)
SELECT MD5('hello');     -- '5d41402abc4b2a76b9719d911017c592'
SELECT SHA256('hello');  -- '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'

-- General hash (FNV-1a, returns BIGINT)
SELECT HASH('hello');    -- Returns a 64-bit integer
```

### String Distance (Fuzzy Matching)

```sql
-- Edit distance (lower = more similar)
SELECT LEVENSHTEIN('kitten', 'sitting');           -- 3
SELECT DAMERAU_LEVENSHTEIN('ab', 'ba');            -- 1 (transposition)
SELECT HAMMING('karolin', 'kathrin');              -- 3 (equal-length only)

-- Similarity ratios (0.0 to 1.0, higher = more similar)
SELECT JACCARD('hello', 'hallo');                  -- ~0.6
SELECT JARO_SIMILARITY('MARTHA', 'MARHTA');        -- ~0.944
SELECT JARO_WINKLER_SIMILARITY('MARTHA', 'MARHTA'); -- ~0.961
```

### Encoding Functions

```sql
-- ASCII/Unicode code points
SELECT ASCII('A');                                 -- 65
SELECT CHR(65);                                    -- 'A'
SELECT UNICODE('$');                               -- 36
```

### Practical Examples

```sql
-- Email validation
SELECT * FROM users WHERE REGEXP_MATCHES(email, '^[^@]+@[^@]+\.[^@]+$');

-- Data cleaning: normalize whitespace and case
SELECT TRIM(LOWER(REGEXP_REPLACE(name, '\s+', ' ', 'g'))) AS clean_name
FROM raw_data;

-- Fuzzy name matching
SELECT a.name, b.name, JARO_WINKLER_SIMILARITY(a.name, b.name) AS score
FROM customers a, prospects b
WHERE JARO_WINKLER_SIMILARITY(a.name, b.name) > 0.85;

-- Log parsing
SELECT
    REGEXP_EXTRACT(line, '\[([^\]]+)\]', 1) AS timestamp,
    REGEXP_EXTRACT(line, '(ERROR|WARN|INFO)', 1) AS level,
    REGEXP_EXTRACT(line, '] (.+)$', 1) AS message
FROM logs;

-- Data integrity check
SELECT *, MD5(CONCAT(id, name, email)) AS checksum FROM users;
```

## Supported SQL Features

### DDL

- `CREATE TABLE`, `DROP TABLE`
- `CREATE VIEW`, `DROP VIEW`
- `CREATE INDEX`, `DROP INDEX`
- `CREATE SEQUENCE`, `DROP SEQUENCE`
- `CREATE SCHEMA`, `DROP SCHEMA`
- `ALTER TABLE` (RENAME, ADD COLUMN, DROP COLUMN)

### DML

- `INSERT`, `UPDATE`, `DELETE`
- `SELECT` with JOINs, subqueries, aggregations
- `COPY TO/FROM` for file import/export

### File Formats

- CSV: `read_csv()`, `read_csv_auto()`
- JSON: `read_json()`, `read_json_auto()`, `read_ndjson()`
- Parquet: `read_parquet()` with column projection
- XLSX: `read_xlsx()`, `read_xlsx_auto()`
- Arrow IPC: `read_arrow()`, `read_arrow_auto()`
- Iceberg: `iceberg_scan()` with time travel and partition pruning

### Glob Patterns and Multi-File Reading

Read multiple files with glob patterns:

```sql
-- Read all CSV files in a directory
SELECT * FROM read_csv('data/*.csv');

-- Recursive glob (all subdirectories)
SELECT * FROM read_parquet('warehouse/**/*.parquet');

-- Character classes and ranges
SELECT * FROM read_json('logs/2024-0[1-6]-*.json');

-- Array of specific files
SELECT * FROM read_csv(['file1.csv', 'file2.csv', 'file3.csv']);

-- Add source filename to results
SELECT *, filename FROM read_csv('data/*.csv', filename=true);

-- Hive partitioning (year=2024/month=01/)
SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=true);
```

See [docs/cloud-storage.md](docs/cloud-storage.md) for full glob pattern documentation.

## Apache Iceberg Support

dukdb-go provides native support for reading Apache Iceberg tables. See [docs/iceberg.md](docs/iceberg.md) for comprehensive documentation.

### Key Features

- Time travel queries (by snapshot ID or timestamp)
- Partition pruning for efficient reads
- Schema evolution support
- Column projection
- Cloud storage access (S3, GCS, Azure)

### Quick Example

```sql
-- Read current snapshot
SELECT * FROM iceberg_scan('/warehouse/sales');

-- Time travel to a specific snapshot
SELECT * FROM iceberg_scan('/warehouse/sales', snapshot_id := 1234567890);

-- Time travel by timestamp
SELECT * FROM iceberg_scan('/warehouse/sales',
    timestamp := TIMESTAMP '2024-01-15 10:00:00');

-- Get table metadata
SELECT * FROM iceberg_metadata('/warehouse/sales');

-- List snapshot history
SELECT snapshot_id, timestamp_ms, operation
FROM iceberg_snapshots('/warehouse/sales');
```

### Go API

```go
import "github.com/dukdb/dukdb-go/internal/io/iceberg"

// Open an Iceberg table
table, err := iceberg.OpenTable(ctx, "/warehouse/sales", nil)
if err != nil {
    log.Fatal(err)
}
defer table.Close()

// Get metadata
fmt.Printf("Snapshots: %d\n", len(table.Snapshots()))

// Read with options
reader, err := iceberg.NewReader(ctx, "/warehouse/sales", &iceberg.ReaderOptions{
    SelectedColumns: []string{"id", "name"},
    Limit:           1000,
})
```

## Cloud Storage

dukdb-go supports reading and writing data from cloud storage providers. See [docs/cloud-storage.md](docs/cloud-storage.md) for comprehensive documentation.

### Supported Providers

| Provider | URL Schemes |
|----------|-------------|
| Amazon S3 | `s3://`, `s3a://`, `s3n://` |
| Google Cloud Storage | `gs://`, `gcs://` |
| Azure Blob Storage | `azure://`, `az://` |
| HTTP/HTTPS | `http://`, `https://` |

### Quick Example

```sql
-- Create credentials
CREATE SECRET my_s3 (
    TYPE S3,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);

-- Read from S3
SELECT * FROM read_parquet('s3://my-bucket/data.parquet');

-- Write to S3
COPY my_table TO 's3://my-bucket/exports/data.csv' (FORMAT CSV);

-- Read from public URL
SELECT * FROM read_csv('https://example.com/data.csv');
```

### Secrets Management

Manage cloud credentials with the secrets system:

```sql
-- Create a secret
CREATE SECRET gcs_creds (
    TYPE GCS,
    SERVICE_ACCOUNT_JSON '/path/to/service-account.json'
);

-- List all secrets
SELECT * FROM duckdb_secrets();

-- Find matching secret for a URL
SELECT * FROM which_secret('s3://bucket/file.csv', 'S3');

-- Drop a secret
DROP SECRET my_s3;
```

See [docs/secrets.md](docs/secrets.md) for detailed secrets management documentation.

## Database Configuration

dukdb-go provides PRAGMA commands for configuring database behavior and accessing system information. Key configuration options include:

- **checkpoint_threshold**: Control when automatic checkpoints are triggered based on WAL file size
- **database_size**: View database and WAL file sizes
- **table_info**: Query table schema and column information
- **settings**: List and modify database settings

### Example: Configuring Checkpoint Behavior

```sql
-- Set checkpoint threshold for high-performance bulk loading
PRAGMA checkpoint_threshold = '2GB';

-- Perform bulk operations
-- INSERT statements...

-- Restore default threshold
PRAGMA checkpoint_threshold = '256MB';
```

See [docs/pragmas.md](docs/pragmas.md) for comprehensive PRAGMA documentation and configuration recommendations.

## Running Tests

```bash
# Using Nix (recommended)
nix develop -c tests

# Or directly
go test ./...

# Run specific test
go test -run TestJSONColumnScanning ./tests/...
```

## Development

```bash
# Enter development shell
nix develop

# Run linting
nix develop -c lint

# Format code
nix fmt
```

## Architecture

dukdb-go uses a pluggable backend architecture:

```
database/sql API
      |
      v
   Driver (dukdb)
      |
      v
   Backend Interface
      |
      v
   Engine (internal/engine)
      |
      +-- Catalog (schemas, tables, views)
      +-- Parser (SQL -> AST)
      +-- Planner (AST -> execution plan)
      +-- Executor (plan execution)
      +-- Storage (columnar data storage)
```

## WebAssembly Support

dukdb-go compiles to WebAssembly for use in browsers:

```bash
GOOS=js GOARCH=wasm go build -o dukdb.wasm ./cmd/wasm/
```

In WASM, the HTTP filesystem is fully supported. For S3/GCS/Azure access, use pre-signed URLs via the HTTP filesystem. See [docs/wasm.md](docs/wasm.md) for comprehensive WASM documentation.

## Comparison with go-duckdb

| Feature | dukdb-go | go-duckdb |
|---------|----------|-----------|
| CGO Required | No | Yes |
| WebAssembly | Yes | No |
| TinyGo | Yes | No |
| Cross-compilation | Easy | Complex |
| API Compatibility | Compatible | Native |
| Iceberg Tables | Yes | Via extension |
| Parquet | Yes | Yes |
| S3/GCS/Azure | Yes | Yes |

## Contributing

Contributions are welcome! Please read the development guidelines and ensure tests pass before submitting PRs.

## License

See LICENSE file for details.
