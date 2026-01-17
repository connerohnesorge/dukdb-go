## Context

DuckDB's PostgreSQL compatibility mode allows applications designed for PostgreSQL to connect to DuckDB using the PostgreSQL wire protocol. This is different from SQL syntax compatibility - it means:
1. Applications use PostgreSQL client libraries/drivers
2. Communication happens over PostgreSQL wire protocol
3. DuckDB translates PostgreSQL messages to internal operations

## Goals / Non-Goals

### Goals
- Enable PostgreSQL wire protocol connections to DuckDB
- Support common PostgreSQL data types with automatic mapping
- Provide PostgreSQL function aliases for compatibility
- Support basic PostgreSQL syntax variations
- Enable ORM compatibility for testing purposes

### Non-Goals
- Full PostgreSQL dialect compatibility (complex, out of scope)
- PostgreSQL-specific features (JSONB operators, arrays, etc.)
- PostgreSQL procedural language (PL/pgSQL) support
- Full pg_catalog implementation
- Wire protocol features not needed for basic compatibility

## PostgreSQL Types to DuckDB Mapping

| PostgreSQL Type | DuckDB Type | Notes |
|-----------------|-------------|-------|
| serial | INTEGER | Auto-increment emulation |
| bigserial | BIGINT | Auto-increment emulation |
| smallserial | SMALLINT | Auto-increment emulation |
| text | VARCHAR | |
| varchar(n) | VARCHAR(n) | |
| char(n) | CHAR(n) | Padded storage |
| integer | INTEGER | |
| bigint | BIGINT | |
| smallint | SMALLINT | |
| real | FLOAT | |
| double precision | DOUBLE | |
| numeric(p,s) | DECIMAL(p,s) | |
| boolean | BOOLEAN | |
| date | DATE | |
| timestamp | TIMESTAMP | |
| timestamptz | TIMESTAMPTZ | Store as UTC |
| time | TIME | |
| timetz | TIMETZ | |
| json | JSON | |
| jsonb | JSON | Store as string |
| uuid | UUID | |
| bytea | BLOB | |
| xml | VARCHAR | Store as string |
| point | GEOMETRY(POINT) | |
| line | GEOMETRY(LINESTRING) | |
| polygon | GEOMETRY(POLYGON) | |

## PostgreSQL Functions to DuckDB Aliases

| PostgreSQL | DuckDB |
|------------|--------|
| now() | current_timestamp |
| current_timestamp | current_timestamp |
| current_date | current_date |
| current_time | current_time |
| current_user | current_user |
| session_user | session_user |
| version() | version() |
| pg_catalog.pg_current_time() | current_time |
| concat() | concat() |
| concat_ws() | concat_ws() |
| coalesce() | coalesce() |
| nullif() | nullif() |
| greatest() | greatest() |
| least() | least() |
| generate_series() | range() |

## PostgreSQL Syntax Variations

DuckDB already supports many PostgreSQL syntax variations:

### Supported
```sql
-- DISTINCT ON
SELECT DISTINCT ON (col1) col1, col2 FROM table;

-- LIMIT ALL
SELECT * FROM table LIMIT ALL;

-- ILIKE
SELECT * FROM table WHERE name ILIKE '%test%';

--:: type casting
SELECT '123'::integer;

-- COMMENT syntax
COMMENT ON TABLE table IS 'comment';
```

### May Need Implementation
```sql
-- GROUP BY ordinal
SELECT col1, col2 FROM table GROUP BY 1, 2;

-- Using in WITH
WITH RECURSIVE cte AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM cte WHERE n < 10
) SELECT * FROM cte;
```

## Wire Protocol Requirements

The PostgreSQL wire protocol uses a message-based communication:
1. Startup message (SSL request, startup message)
2. Authentication (SASL, md5, etc.)
3. Simple query protocol
4. Extended query protocol (prepared statements)
5. Row description and data rows
6. Command complete and ready for query

## Implementation Approach

### Option 1: PostgreSQL Wire Protocol Library

**Pros**:
- Battle-tested implementation
- Faster time to market
- Handles edge cases

**Cons**:
- May not be pure Go
- May have licensing constraints
- May not integrate well with DuckDB internals

**Libraries**:
- `github.com/jackc/pgx` (has protocol implementation)
- `github.com/lib/pq` (client only)
- Custom implementation

### Option 2: Custom Implementation

**Pros**:
- Pure Go
- Full control
- Integration with DuckDB internals

**Cons**:
- Significant effort
- Protocol complexity
- Testing burden

### Decision: Option 1 with Custom Extensions

Start with a PostgreSQL wire protocol library and customize for DuckDB.

## Open Questions

1. Which PostgreSQL wire protocol library to use?
2. What's the minimum PostgreSQL version compatibility?
3. Should we support prepared statement caching?
4. How to handle PostgreSQL transaction isolation levels?

## References

- PostgreSQL Wire Protocol: https://www.postgresql.org/docs/current/protocol.html
- DuckDB PostgreSQL Mode: https://duckdb.org/docs/connect/postgres
- pgx PostgreSQL client: https://github.com/jackc/pgx
