# Extended Type System

dukdb-go supports several extended types beyond standard SQL types, providing rich functionality for JSON documents, spatial data, arbitrary precision numbers, and more.

## JSON Type

### Description

The JSON type stores and validates JSON documents, supporting extraction operators and path-based queries.

### Storage

JSON values are stored as validated JSON strings internally. Invalid JSON is rejected at insertion time.

### Operators

- `->` - Extract JSON value by key: `data->'name'` returns JSON
- `->>` - Extract text value by key: `data->>'name'` returns VARCHAR (with JSON string quotes)
- `->N` - Extract array element by index: `data->0` returns first element

### Functions

- `json_extract(json, path)` - Extract value at path
- `json_extract_string(json, path)` - Extract as text
- `json_valid(string)` - Check if string is valid JSON (returns BOOLEAN)

### Example

```sql
-- Create table with JSON column
CREATE TABLE users (id INTEGER, data JSON);

-- Insert JSON data
INSERT INTO users VALUES (1, '{"name": "Alice", "age": 30, "tags": ["admin", "active"]}');

-- Extract values
SELECT data->'name' FROM users;           -- Returns '"Alice"' (JSON string)
SELECT data->>'name' FROM users;          -- Returns '"Alice"' (text)
SELECT data->'age' FROM users;            -- Returns '30'
SELECT data->'tags'->0 FROM users;        -- Returns '"admin"'

-- Nested extraction
INSERT INTO users VALUES (2, '{"user": {"profile": {"name": "Bob"}}}');
SELECT data->'user'->'profile'->'name' FROM users WHERE id = 2;

-- Validate JSON
SELECT json_valid('{"valid": true}');     -- Returns true
SELECT json_valid('not json');            -- Returns false

-- Use in WHERE clause
SELECT * FROM users WHERE data->'name' = '"Alice"';
```

### Go Usage

```go
// Insert JSON
db.Exec(`INSERT INTO users VALUES ($1, $2)`, 1, `{"name":"Alice"}`)

// Scan to string and unmarshal
var jsonStr string
db.QueryRow(`SELECT data FROM users WHERE id = 1`).Scan(&jsonStr)

var parsed map[string]any
json.Unmarshal([]byte(jsonStr), &parsed)
```

## GEOMETRY Type

### Description

The GEOMETRY type stores spatial data in WKB (Well-Known Binary) format, with extensive support for spatial operations.

### Supported Geometry Types

- `POINT` - Single coordinate pair
- `LINESTRING` - Sequence of connected points
- `POLYGON` - Closed ring with optional holes
- `MULTIPOINT` - Collection of points
- `MULTILINESTRING` - Collection of linestrings
- `MULTIPOLYGON` - Collection of polygons
- `GEOMETRYCOLLECTION` - Heterogeneous collection

### Constructor Functions

| Function | Description |
|----------|-------------|
| `ST_GeomFromText(wkt)` | Create geometry from WKT string |
| `ST_Point(x, y)` | Create a point geometry |
| `ST_MakeLine(g1, g2)` | Create linestring from two points |
| `ST_MakePolygon(ring)` | Create polygon from closed linestring |

### Accessor Functions

| Function | Description |
|----------|-------------|
| `ST_AsText(geom)` | Convert to WKT string |
| `ST_AsBinary(geom)` | Convert to WKB binary |
| `ST_GeometryType(geom)` | Get type name (POINT, POLYGON, etc.) |
| `ST_X(geom)` | Get X coordinate of point |
| `ST_Y(geom)` | Get Y coordinate of point |
| `ST_Z(geom)` | Get Z coordinate of point (if 3D) |
| `ST_SRID(geom)` | Get spatial reference ID |
| `ST_SetSRID(geom, srid)` | Set spatial reference ID |

### Measurement Functions

| Function | Description |
|----------|-------------|
| `ST_Distance(g1, g2)` | Euclidean distance between geometries |
| `ST_Distance_Sphere(g1, g2)` | Great-circle distance in meters |
| `ST_Area(geom)` | Area of polygon |
| `ST_Length(geom)` | Length of linestring or polygon perimeter |
| `ST_Centroid(geom)` | Geometric center point |

### Spatial Predicates

| Function | Description |
|----------|-------------|
| `ST_Contains(g1, g2)` | g1 completely contains g2 |
| `ST_Within(g1, g2)` | g1 is completely within g2 |
| `ST_Intersects(g1, g2)` | Geometries share any space |
| `ST_Disjoint(g1, g2)` | Geometries share no space |
| `ST_Touches(g1, g2)` | Geometries touch at boundary only |
| `ST_Crosses(g1, g2)` | Geometries cross each other |
| `ST_Overlaps(g1, g2)` | Geometries overlap partially |
| `ST_Equals(g1, g2)` | Geometries are spatially equal |

### Set Operations

| Function | Description |
|----------|-------------|
| `ST_Union(g1, g2)` | Combine geometries |
| `ST_Intersection(g1, g2)` | Common area between geometries |
| `ST_Difference(g1, g2)` | g1 minus g2 |
| `ST_Buffer(geom, distance)` | Expand geometry by distance |
| `ST_Envelope(geom)` | Bounding box as polygon |

### Examples

```sql
-- Create points
SELECT ST_Point(0, 0);
SELECT ST_GeomFromText('POINT(1.5 2.5)');

-- Calculate distance (Pythagorean theorem: 3-4-5 triangle)
SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4));  -- Returns 5.0

-- Check containment
SELECT ST_Contains(
    ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
    ST_Point(5, 5)
);  -- Returns true

-- Get geometry type
SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'));
-- Returns 'LINESTRING'

-- Convert to WKT
SELECT ST_AsText(ST_Point(3, 4));  -- Returns 'POINT(3 4)'

-- Set SRID for geographic coordinates
SELECT ST_SetSRID(ST_Point(-122.4194, 37.7749), 4326);

-- Create a buffer (circle approximation around a point)
SELECT ST_Buffer(ST_Point(0, 0), 10);

-- Spatial join example
CREATE TABLE locations (id INTEGER, geom GEOMETRY);
CREATE TABLE regions (id INTEGER, boundary GEOMETRY);

SELECT l.id, r.id
FROM locations l, regions r
WHERE ST_Contains(r.boundary, l.geom);
```

### Go Usage

```go
// Query geometry as WKT
var wkt string
db.QueryRow(`SELECT ST_AsText(ST_Point(3, 4))`).Scan(&wkt)
// wkt = "POINT(3 4)"

// Store geometry in BLOB column
pointWKB := createPointWKB(1.0, 2.0) // WKB binary
db.Exec(`INSERT INTO geom_table VALUES ($1, $2)`, 1, pointWKB)

// Query coordinates
var x, y float64
db.QueryRow(`SELECT ST_X(ST_Point(5, 6)), ST_Y(ST_Point(5, 6))`).Scan(&x, &y)
```

## BIGNUM Type

### Description

Arbitrary precision integers for numbers exceeding the INT64 range (beyond 9,223,372,036,854,775,807).

### Storage

Stored as string representation internally, preserving full precision. Compatible with Go's `math/big.Int` for arbitrary precision arithmetic.

### Example

```sql
-- Create table with BIGNUM column
CREATE TABLE big_numbers (id INTEGER, val BIGNUM);

-- Insert values larger than int64
INSERT INTO big_numbers VALUES (1, '123456789012345678901234567890');
INSERT INTO big_numbers VALUES (2, '-98765432109876543210');

-- Query values
SELECT val FROM big_numbers WHERE id = 1;

-- NULL handling
INSERT INTO big_numbers VALUES (3, NULL);
SELECT val FROM big_numbers WHERE val IS NULL;
```

### Go Usage

```go
import "math/big"

// Insert large number
largeNum := "123456789012345678901234567890"
db.Exec(`INSERT INTO big_numbers VALUES ($1, $2)`, 1, largeNum)

// Retrieve and convert to big.Int
var valueStr string
db.QueryRow(`SELECT val FROM big_numbers WHERE id = 1`).Scan(&valueStr)

bigInt, ok := new(big.Int).SetString(valueStr, 10)
if ok {
    // Use bigInt for arbitrary precision arithmetic
    result := new(big.Int).Mul(bigInt, big.NewInt(2))
}
```

## VARIANT Type

### Description

Dynamic type that can hold any JSON-compatible value. Useful for schema-flexible data storage.

### Storage

Stored as JSON string internally, can represent any JSON type (object, array, string, number, boolean, null).

### Example

```sql
-- Create table with VARIANT column
CREATE TABLE flexible_data (id INTEGER, data VARIANT);

-- Store different types
INSERT INTO flexible_data VALUES (1, '{"type": "object", "value": 42}');
INSERT INTO flexible_data VALUES (2, '[1, 2, 3, "mixed", true]');
INSERT INTO flexible_data VALUES (3, '"just a string"');
INSERT INTO flexible_data VALUES (4, '42');
INSERT INTO flexible_data VALUES (5, 'true');
INSERT INTO flexible_data VALUES (6, 'null');

-- Query and process in application
SELECT data FROM flexible_data;
```

### Go Usage

```go
// Insert any JSON value
db.Exec(`INSERT INTO flexible_data VALUES ($1, $2)`, 1, `{"key":"value"}`)

// Scan to string and unmarshal to any
var dataStr string
db.QueryRow(`SELECT data FROM flexible_data WHERE id = 1`).Scan(&dataStr)

var data any
json.Unmarshal([]byte(dataStr), &data)

// Type switch to handle different types
switch v := data.(type) {
case map[string]any:
    // Handle object
case []any:
    // Handle array
case string:
    // Handle string
case float64:
    // Handle number
case bool:
    // Handle boolean
case nil:
    // Handle null
}
```

## LAMBDA Type

### Description

Stores lambda expressions as strings for use with higher-order functions. The expressions are stored but not executed directly by the database.

### Storage

Lambda expressions are stored as VARCHAR strings, preserving the exact expression text.

### Example

```sql
-- Create table with LAMBDA column
CREATE TABLE transforms (id INTEGER, name VARCHAR, expr LAMBDA);

-- Store lambda expressions
INSERT INTO transforms VALUES (1, 'increment', 'x -> x + 1');
INSERT INTO transforms VALUES (2, 'multiply', '(x, y) -> x * y');
INSERT INTO transforms VALUES (3, 'transform', '(a, b, c) -> a * b + c');
INSERT INTO transforms VALUES (4, 'uppercase', 'x -> upper(trim(x))');

-- Query expressions
SELECT expr FROM transforms WHERE name = 'increment';
```

### Go Usage

```go
// Insert lambda expression
db.Exec(`INSERT INTO transforms VALUES ($1, $2)`, 1, "x -> x + 1")

// Retrieve expression
var expr string
db.QueryRow(`SELECT expr FROM transforms WHERE id = 1`).Scan(&expr)
// expr = "x -> x + 1"

// Use in application logic to build dynamic queries or process data
```

## Type Comparison

| Type | Internal Storage | Go Type | Primary Use Case |
|------|-----------------|---------|------------------|
| JSON | Validated JSON string | `string` | Document storage with query support |
| GEOMETRY | WKB binary / WKT string | `[]byte` or `string` | Spatial data and GIS operations |
| BIGNUM | Decimal string | `string` / `*big.Int` | Arbitrary precision integers |
| VARIANT | JSON string | `string` / `any` | Schema-flexible dynamic data |
| LAMBDA | Expression string | `string` | Higher-order function expressions |

## NULL Handling

All extended types support SQL NULL values:

```go
// Using sql.NullString for NULL-safe scanning
var data sql.NullString
db.QueryRow(`SELECT json_col FROM table`).Scan(&data)
if data.Valid {
    // Process data.String
} else {
    // Handle NULL
}
```

Note: JSON `null` (the literal value) is different from SQL `NULL`:
- JSON `null`: Valid JSON value, `data.Valid = true`, `data.String = "null"`
- SQL `NULL`: Absent value, `data.Valid = false`
