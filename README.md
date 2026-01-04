# dukdb-go

Pure Go implementation of a DuckDB-compatible database driver.

## Features

- **Pure Go** - Zero CGO dependencies, compiles anywhere Go compiles
- **database/sql Compatible** - Standard Go database interface
- **Extended Type Support** - JSON, GEOMETRY, BIGNUM, VARIANT, LAMBDA types
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

## Comparison with go-duckdb

| Feature | dukdb-go | go-duckdb |
|---------|----------|-----------|
| CGO Required | No | Yes |
| WebAssembly | Yes | No |
| TinyGo | Yes | No |
| Cross-compilation | Easy | Complex |
| API Compatibility | Compatible | Native |

## Contributing

Contributions are welcome! Please read the development guidelines and ensure tests pass before submitting PRs.

## License

See LICENSE file for details.
