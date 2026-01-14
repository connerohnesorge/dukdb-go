# PostgreSQL Compatibility Mode for dukdb-go

This package provides PostgreSQL wire protocol compatibility for dukdb-go, enabling connections from PostgreSQL clients (psql, pgx, JDBC, etc.), ORMs (TypeORM, SQLAlchemy, Prisma, etc.), and database tools (DBeaver, DataGrip, pgAdmin, Metabase, Tableau).

## Overview

The PostgreSQL compatibility mode implements:

- **Wire Protocol Server**: Full PostgreSQL v3 wire protocol support using the psql-wire library
- **Type System Mapping**: Bidirectional mapping between PostgreSQL and DuckDB types
- **Function Aliases**: 180+ PostgreSQL function names mapped to DuckDB equivalents
- **System Catalog Views**: `information_schema` and `pg_catalog` compatibility views
- **Authentication**: Configurable authentication with clear text password support

## Quick Start

### Basic Server (No Authentication)

```go
package main

import (
    "log"

    "github.com/dukdb/dukdb-go/internal/postgres/server"
)

func main() {
    // Create server with default configuration
    config := server.NewConfig()
    config.Host = "127.0.0.1"
    config.Port = 5432
    config.Database = "dukdb"

    srv, err := server.NewServer(config)
    if err != nil {
        log.Fatal(err)
    }

    // Start serving connections
    log.Println("Starting PostgreSQL server on", config.Address())
    if err := srv.ListenAndServe(); err != nil {
        log.Fatal(err)
    }
}
```

### Connecting from psql

```bash
psql -h 127.0.0.1 -p 5432 -d dukdb
```

### Connecting from Go (pgx)

```go
import "github.com/jackc/pgx/v5"

conn, err := pgx.Connect(ctx, "postgres://127.0.0.1:5432/dukdb")
if err != nil {
    log.Fatal(err)
}
defer conn.Close(ctx)

rows, err := conn.Query(ctx, "SELECT * FROM my_table")
```

## Configuration

### Server Configuration Options

```go
type Config struct {
    // Network settings
    Host            string        // Default: "127.0.0.1"
    Port            int           // Default: 5432
    Database        string        // Default: "dukdb"

    // Connection limits
    MaxConnections  int           // Default: 100
    ShutdownTimeout time.Duration // Default: 30s

    // PostgreSQL version to report
    ServerVersion   string        // Default: "16.0.0"

    // Authentication
    RequireAuth     bool          // Default: false
    Username        string        // Simple auth username
    Password        string        // Simple auth password
    AuthMethod      auth.Method   // Default: MethodPassword
    UserProvider    auth.UserProvider  // For multi-user auth
    Authenticator   auth.Authenticator // Custom authenticator

    // Logging
    LogStartupParams bool         // Log client startup parameters

    // TLS
    TLSConfig       *tls.Config   // nil = TLS disabled
}
```

### Connection String Format

Standard PostgreSQL connection string format:

```
postgres://[user[:password]@]host[:port]/database[?parameters]
```

Examples:
```
postgres://localhost:5432/dukdb
postgres://user:pass@localhost:5432/dukdb
postgres://localhost:5432/dukdb?sslmode=disable
```

## Authentication

### No Authentication (Default)

```go
config := server.NewConfig()
config.RequireAuth = false
```

### Simple Username/Password

```go
config := server.NewConfig()
config.RequireAuth = true
config.Username = "admin"
config.Password = "secret"
```

### Multi-User Authentication

```go
import "github.com/dukdb/dukdb-go/internal/postgres/server/auth"

// Create in-memory user provider
provider := auth.NewMemoryProvider()
provider.AddUser(&auth.User{
    Username:  "admin",
    Superuser: true,
    Databases: []string{}, // Empty = access to all
}, "admin_password")

provider.AddUser(&auth.User{
    Username:  "reader",
    Superuser: false,
    Databases: []string{"analytics", "reporting"},
}, "reader_password")

// Configure server
config := server.NewConfig()
config.RequireAuth = true
config.UserProvider = provider
```

### Custom Authenticator

```go
type MyAuthenticator struct{}

func (a *MyAuthenticator) Authenticate(ctx context.Context, username, password, database string) (bool, error) {
    // Implement custom authentication logic
    // e.g., LDAP, OAuth, external service
    return validateWithExternalService(username, password), nil
}

func (a *MyAuthenticator) Method() auth.Method {
    return auth.MethodPassword
}

config.Authenticator = &MyAuthenticator{}
```

### Supported Authentication Methods

| Method | Status | Description |
|--------|--------|-------------|
| `none` | Supported | No authentication required |
| `password` | Supported | Clear text password authentication |
| `md5` | Planned | MD5 hashed password |
| `scram-sha-256` | Planned | SCRAM-SHA-256 (PostgreSQL 10+) |

## Supported Data Types

### Type Mapping Table

| PostgreSQL Type | DuckDB Type | OID | Notes |
|-----------------|-------------|-----|-------|
| `boolean`, `bool` | `BOOLEAN` | 16 | |
| `smallint`, `int2` | `SMALLINT` | 21 | |
| `integer`, `int`, `int4` | `INTEGER` | 23 | |
| `bigint`, `int8` | `BIGINT` | 20 | |
| `real`, `float4` | `FLOAT` | 700 | |
| `double precision`, `float8` | `DOUBLE` | 701 | |
| `numeric`, `decimal` | `DECIMAL` | 1700 | |
| `text` | `VARCHAR` | 25 | |
| `varchar`, `character varying` | `VARCHAR` | 1043 | |
| `char`, `character`, `bpchar` | `VARCHAR` | 1042 | |
| `bytea` | `BLOB` | 17 | |
| `date` | `DATE` | 1082 | |
| `time`, `time without time zone` | `TIME` | 1083 | |
| `time with time zone`, `timetz` | `TIMETZ` | 1266 | |
| `timestamp`, `timestamp without time zone` | `TIMESTAMP` | 1114 | |
| `timestamp with time zone`, `timestamptz` | `TIMESTAMPTZ` | 1184 | |
| `interval` | `INTERVAL` | 1186 | |
| `json` | `JSON` | 114 | |
| `jsonb` | `JSON` | 3802 | Stored as JSON |
| `uuid` | `UUID` | 2950 | |
| `serial`, `serial4` | `INTEGER` | 23 | Auto-increment |
| `bigserial`, `serial8` | `BIGINT` | 20 | Auto-increment |
| `smallserial`, `serial2` | `SMALLINT` | 21 | Auto-increment |

### Compatibility Types (Stored as VARCHAR)

These PostgreSQL types are accepted but stored as VARCHAR for compatibility:

- Network types: `inet`, `cidr`, `macaddr`, `macaddr8`
- Geometric types: `point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle`
- Text search: `tsvector`, `tsquery`
- XML: `xml`
- Money: `money` (stored as DECIMAL)

### Array Types

| PostgreSQL Array | DuckDB Type | OID |
|------------------|-------------|-----|
| `integer[]` | `INTEGER[]` | 1007 |
| `bigint[]` | `BIGINT[]` | 1016 |
| `smallint[]` | `SMALLINT[]` | 1005 |
| `text[]` | `VARCHAR[]` | 1009 |
| `boolean[]` | `BOOLEAN[]` | 1000 |
| `real[]` | `FLOAT[]` | 1021 |
| `double precision[]` | `DOUBLE[]` | 1022 |

## Supported Functions

### Date/Time Functions

| PostgreSQL Function | DuckDB Equivalent | Description |
|--------------------|-------------------|-------------|
| `now()` | `current_timestamp` | Current date and time |
| `current_timestamp` | `current_timestamp` | Current date and time |
| `current_date` | `current_date` | Current date |
| `current_time` | `current_time` | Current time |
| `localtime` | `current_time` | Current local time |
| `localtimestamp` | `current_timestamp` | Current local timestamp |
| `timeofday()` | `current_timestamp` | Current time as text |
| `transaction_timestamp()` | `current_timestamp` | Transaction start time |
| `statement_timestamp()` | `current_timestamp` | Statement start time |
| `clock_timestamp()` | `current_timestamp` | Current clock time |
| `date_part(field, source)` | `date_part` | Extract date/time part |
| `date_trunc(field, source)` | `date_trunc` | Truncate to precision |
| `extract(field FROM source)` | `extract` | Extract date/time field |
| `age(timestamp)` | `age` | Calculate age |
| `to_timestamp(value)` | `to_timestamp` | Convert to timestamp |

### String Functions

| PostgreSQL Function | DuckDB Equivalent | Description |
|--------------------|-------------------|-------------|
| `concat(str, ...)` | `concat` | Concatenate strings |
| `concat_ws(sep, str, ...)` | `concat_ws` | Concatenate with separator |
| `length(str)` | `length` | String length |
| `char_length(str)` | `length` | Character length |
| `character_length(str)` | `length` | Character length |
| `octet_length(str)` | `octet_length` | Byte length |
| `bit_length(str)` | `bit_length` | Bit length |
| `lower(str)` | `lower` | Convert to lowercase |
| `upper(str)` | `upper` | Convert to uppercase |
| `initcap(str)` | `initcap` | Capitalize words |
| `trim(str)` | `trim` | Remove whitespace |
| `ltrim(str)` | `ltrim` | Remove leading whitespace |
| `rtrim(str)` | `rtrim` | Remove trailing whitespace |
| `btrim(str)` | `trim` | Remove both sides |
| `lpad(str, len, fill)` | `lpad` | Left-pad string |
| `rpad(str, len, fill)` | `rpad` | Right-pad string |
| `substring(str, start, len)` | `substring` | Extract substring |
| `substr(str, start, len)` | `substr` | Extract substring |
| `replace(str, from, to)` | `replace` | Replace substring |
| `translate(str, from, to)` | `translate` | Replace characters |
| `reverse(str)` | `reverse` | Reverse string |
| `repeat(str, n)` | `repeat` | Repeat string |
| `position(sub IN str)` | `position` | Find position |
| `strpos(str, sub)` | `strpos` | Find position |
| `left(str, n)` | `left` | Leftmost characters |
| `right(str, n)` | `right` | Rightmost characters |
| `split_part(str, delim, n)` | `split_part` | Split and get part |
| `string_to_array(str, delim)` | `string_split` | Split to array |
| `regexp_replace(str, pattern, replacement)` | `regexp_replace` | Regex replace |
| `regexp_matches(str, pattern)` | `regexp_matches` | Regex match |
| `overlay(str PLACING new FROM start)` | `overlay` | Replace substring |
| `md5(str)` | `md5` | MD5 hash |
| `ascii(str)` | `ascii` | ASCII code |
| `chr(n)` | `chr` | Character from code |
| `format(fmt, ...)` | `format` | Printf-style format |

### Math Functions

| PostgreSQL Function | DuckDB Equivalent | Description |
|--------------------|-------------------|-------------|
| `abs(n)` | `abs` | Absolute value |
| `ceil(n)`, `ceiling(n)` | `ceil` | Ceiling |
| `floor(n)` | `floor` | Floor |
| `round(n, s)` | `round` | Round |
| `trunc(n, s)`, `truncate(n, s)` | `trunc` | Truncate |
| `mod(n, m)` | `mod` | Modulo |
| `power(n, e)`, `pow(n, e)` | `pow` | Power |
| `sqrt(n)` | `sqrt` | Square root |
| `cbrt(n)` | `cbrt` | Cube root |
| `exp(n)` | `exp` | Exponential |
| `ln(n)` | `ln` | Natural log |
| `log(n)`, `log(b, n)` | `log` | Logarithm |
| `log10(n)` | `log10` | Base-10 log |
| `sign(n)` | `sign` | Sign of number |
| `random()` | `random` | Random 0-1 |
| `setseed(n)` | `setseed` | Set random seed |
| `pi()` | `pi` | Pi constant |
| `degrees(n)` | `degrees` | Radians to degrees |
| `radians(n)` | `radians` | Degrees to radians |
| `sin(n)` | `sin` | Sine |
| `cos(n)` | `cos` | Cosine |
| `tan(n)` | `tan` | Tangent |
| `cot(n)` | `cot` | Cotangent |
| `asin(n)` | `asin` | Arc sine |
| `acos(n)` | `acos` | Arc cosine |
| `atan(n)` | `atan` | Arc tangent |
| `atan2(y, x)` | `atan2` | Arc tangent of y/x |
| `gcd(a, b)` | `gcd` | Greatest common divisor |
| `lcm(a, b)` | `lcm` | Least common multiple |

### Aggregate Functions

| PostgreSQL Function | DuckDB Equivalent | Description |
|--------------------|-------------------|-------------|
| `array_agg(value)` | `list` | Aggregate to array |
| `string_agg(str, delim)` | `string_agg` | Concatenate strings |
| `count(*)`, `count(col)` | `count` | Count rows |
| `sum(col)` | `sum` | Sum values |
| `avg(col)` | `avg` | Average |
| `min(col)` | `min` | Minimum |
| `max(col)` | `max` | Maximum |
| `bool_and(col)` | `bool_and` | Logical AND |
| `bool_or(col)` | `bool_or` | Logical OR |
| `every(col)` | `bool_and` | Logical AND (alias) |
| `bit_and(col)` | `bit_and` | Bitwise AND |
| `bit_or(col)` | `bit_or` | Bitwise OR |
| `variance(col)` | `var_samp` | Sample variance |
| `var_samp(col)` | `var_samp` | Sample variance |
| `var_pop(col)` | `var_pop` | Population variance |
| `stddev(col)` | `stddev_samp` | Sample std dev |
| `stddev_samp(col)` | `stddev_samp` | Sample std dev |
| `stddev_pop(col)` | `stddev_pop` | Population std dev |
| `corr(y, x)` | `corr` | Correlation |
| `covar_pop(y, x)` | `covar_pop` | Population covariance |
| `covar_samp(y, x)` | `covar_samp` | Sample covariance |
| `mode()` | `mode` | Most frequent value |
| `percentile_cont(p)` | `quantile_cont` | Continuous percentile |
| `percentile_disc(p)` | `quantile_disc` | Discrete percentile |

### Window Functions

| PostgreSQL Function | DuckDB Equivalent | Description |
|--------------------|-------------------|-------------|
| `row_number()` | `row_number` | Sequential row number |
| `rank()` | `rank` | Rank with gaps |
| `dense_rank()` | `dense_rank` | Rank without gaps |
| `percent_rank()` | `percent_rank` | Relative rank |
| `cume_dist()` | `cume_dist` | Cumulative distribution |
| `ntile(n)` | `ntile` | Divide into n buckets |
| `lag(col, offset, default)` | `lag` | Previous row value |
| `lead(col, offset, default)` | `lead` | Next row value |
| `first_value(col)` | `first_value` | First in window |
| `last_value(col)` | `last_value` | Last in window |
| `nth_value(col, n)` | `nth_value` | Nth in window |

### JSON Functions

| PostgreSQL Function | DuckDB Equivalent | Description |
|--------------------|-------------------|-------------|
| `json_array_length(json)` | `json_array_length` | Array length |
| `jsonb_array_length(jsonb)` | `json_array_length` | Array length |
| `json_extract_path(json, path...)` | `json_extract` | Extract at path |
| `jsonb_extract_path(jsonb, path...)` | `json_extract` | Extract at path |
| `json_extract_path_text(json, path...)` | `json_extract_string` | Extract as text |
| `jsonb_extract_path_text(jsonb, path...)` | `json_extract_string` | Extract as text |
| `json_typeof(json)` | `json_type` | Value type |
| `jsonb_typeof(jsonb)` | `json_type` | Value type |
| `to_json(value)` | `to_json` | Convert to JSON |
| `to_jsonb(value)` | `to_json` | Convert to JSONB |
| `json_agg(value)` | `json_group_array` | Aggregate to array |
| `jsonb_agg(value)` | `json_group_array` | Aggregate to array |
| `json_object_agg(key, value)` | `json_group_object` | Aggregate to object |
| `jsonb_object_agg(key, value)` | `json_group_object` | Aggregate to object |

### Conditional Functions

| PostgreSQL Function | DuckDB Equivalent | Description |
|--------------------|-------------------|-------------|
| `coalesce(val, ...)` | `coalesce` | First non-null |
| `nullif(val1, val2)` | `nullif` | Null if equal |
| `greatest(val, ...)` | `greatest` | Largest value |
| `least(val, ...)` | `least` | Smallest value |
| `ifnull(val1, val2)` | `ifnull` | If null then |

### Miscellaneous Functions

| PostgreSQL Function | DuckDB Equivalent | Description |
|--------------------|-------------------|-------------|
| `gen_random_uuid()` | `uuid` | Generate UUID |
| `uuid_generate_v4()` | `uuid` | Generate UUID v4 |
| `nextval(sequence)` | `nextval` | Next sequence value |
| `currval(sequence)` | `currval` | Current sequence value |
| `setval(sequence, value)` | `setval` | Set sequence value |
| `version()` | `version` | Database version |
| `current_user` | `current_user` | Current username |
| `session_user` | `session_user` | Session username |
| `current_database()` | `current_database` | Current database |
| `current_catalog` | `current_database` | Current catalog |
| `cast(value AS type)` | `cast` | Type cast |

### PostgreSQL System Functions

These functions provide PostgreSQL compatibility for tools and ORMs:

| Function | Description | Behavior |
|----------|-------------|----------|
| `pg_typeof(value)` | Returns type name | Maps to `typeof()` |
| `pg_column_size(value)` | Estimated storage size | Returns estimated bytes |
| `pg_database_size(name)` | Database size | Returns size in bytes |
| `pg_table_size(name)` | Table size (no indexes) | Returns size in bytes |
| `pg_relation_size(name)` | Relation size | Returns size in bytes |
| `pg_total_relation_size(name)` | Total size with indexes | Returns size in bytes |
| `pg_indexes_size(name)` | Index size | Returns size in bytes |
| `current_schema()` | Current schema | Returns 'main' |
| `current_schemas(include_implicit)` | Search path | Returns array |
| `pg_backend_pid()` | Backend process ID | Returns session ID |
| `pg_postmaster_start_time()` | Server start time | Returns timestamp |
| `current_setting(name)` | Get setting value | Returns setting |
| `set_config(name, value, is_local)` | Set configuration | Sets and returns |
| `pg_is_in_recovery()` | Recovery mode check | Always false |
| `pg_is_wal_replay_paused()` | WAL replay check | Always false |
| `pg_client_encoding()` | Client encoding | Returns 'UTF8' |
| `txid_current()` | Transaction ID | Returns current txid |

### Privilege Check Functions

These functions always return `true` since dukdb-go does not implement PostgreSQL's privilege system:

| Function | Description |
|----------|-------------|
| `has_table_privilege(table, privilege)` | Check table privilege |
| `has_column_privilege(table, column, privilege)` | Check column privilege |
| `has_schema_privilege(schema, privilege)` | Check schema privilege |
| `has_database_privilege(database, privilege)` | Check database privilege |
| `has_function_privilege(function, privilege)` | Check function privilege |
| `has_sequence_privilege(sequence, privilege)` | Check sequence privilege |
| `pg_has_role(role, privilege)` | Check role membership |

### Catalog Functions

| Function | Description | Behavior |
|----------|-------------|----------|
| `pg_get_constraintdef(oid)` | Constraint definition | Returns SQL |
| `pg_get_indexdef(oid)` | Index definition | Returns SQL |
| `pg_get_viewdef(oid)` | View definition | Returns SQL |
| `pg_get_function_arguments(oid)` | Function arguments | Returns signature |
| `pg_get_function_result(oid)` | Function return type | Returns type |
| `pg_get_expr(expr, relid)` | Deparse expression | Returns SQL |
| `obj_description(oid, catalog)` | Object comment | Returns empty |
| `col_description(oid, column)` | Column comment | Returns empty |
| `shobj_description(oid, catalog)` | Shared object comment | Returns empty |

## System Catalog Views

### information_schema Views

| View | Description |
|------|-------------|
| `information_schema.tables` | List of tables and views |
| `information_schema.columns` | Column metadata |
| `information_schema.schemata` | Available schemas |
| `information_schema.views` | View definitions |
| `information_schema.table_constraints` | Primary keys, unique constraints |
| `information_schema.key_column_usage` | Columns in constraints |
| `information_schema.sequences` | Sequence definitions |

### pg_catalog Views

| View | Description |
|------|-------------|
| `pg_catalog.pg_namespace` | Schemas/namespaces |
| `pg_catalog.pg_class` | Tables, views, indexes, sequences |
| `pg_catalog.pg_attribute` | Table columns |
| `pg_catalog.pg_type` | Data types |
| `pg_catalog.pg_index` | Index information |
| `pg_catalog.pg_database` | Databases |
| `pg_catalog.pg_settings` | Server configuration |
| `pg_catalog.pg_tables` | Simplified table listing |
| `pg_catalog.pg_views` | Simplified view listing |
| `pg_catalog.pg_proc` | Functions/procedures |
| `pg_catalog.pg_constraint` | Constraints |

## Limitations and Known Issues

### Not Supported

1. **Advanced Authentication**
   - MD5 password authentication (planned)
   - SCRAM-SHA-256 (planned)
   - Certificate authentication
   - GSSAPI/Kerberos
   - LDAP integration

2. **PostgreSQL-Specific Features**
   - LISTEN/NOTIFY (async notifications)
   - Large Objects (LOB)
   - Cursors (WITH HOLD)
   - Advisory locks
   - Row-level security
   - PostgreSQL extensions
   - Full-text search (tsvector, tsquery)

3. **Data Types**
   - Composite types (custom types)
   - Range types (int4range, tsrange, etc.)
   - Domain types
   - Enum types (use VARCHAR)
   - hstore
   - PostGIS geometry types

4. **Protocol Features**
   - COPY protocol (binary format)
   - Logical replication protocol
   - Streaming replication

### Known Differences from PostgreSQL

1. **Type Storage**
   - `jsonb` is stored as `json` (no binary JSON)
   - Network types stored as VARCHAR
   - Geometric types stored as VARCHAR

2. **Transaction Isolation**
   - Default isolation is SERIALIZABLE (like DuckDB)
   - PostgreSQL defaults to READ COMMITTED

3. **Privilege System**
   - All privilege check functions return `true`
   - No actual role/permission enforcement

4. **System Functions**
   - `pg_is_in_recovery()` always returns false
   - `pg_is_wal_replay_paused()` always returns false
   - Comment functions return empty strings (no COMMENT support)

5. **Catalog OIDs**
   - Synthetic OIDs generated via hash function
   - May not match PostgreSQL OIDs exactly

### Extended Query Protocol Limitations

1. Parameter types must be explicitly provided or inferred
2. Binary format parameters use text encoding internally
3. Portal suspension is not supported

### Performance Considerations

1. Each query creates a new prepared statement internally
2. Connection pooling recommended for high-concurrency workloads
3. Large result sets are streamed to avoid memory issues

## Example Configurations

### Basic Development Server

```go
config := server.NewConfig()
config.Host = "127.0.0.1"
config.Port = 5432
config.Database = "devdb"
config.RequireAuth = false

srv, _ := server.NewServer(config)
srv.ListenAndServe()
```

### Server with Simple Authentication

```go
config := server.NewConfig()
config.Host = "0.0.0.0"  // Listen on all interfaces
config.Port = 5432
config.Database = "myapp"
config.RequireAuth = true
config.Username = "admin"
config.Password = "secret123"

srv, _ := server.NewServer(config)
srv.ListenAndServe()
```

### Server with Multiple Users

```go
import "github.com/dukdb/dukdb-go/internal/postgres/server/auth"

// Create user provider with multiple users
provider := auth.NewMemoryProvider()

// Admin user - access to all databases
provider.AddUser(&auth.User{
    Username:  "admin",
    Superuser: true,
}, "admin_secret")

// Application user - limited database access
provider.AddUser(&auth.User{
    Username:  "app_user",
    Databases: []string{"production", "staging"},
}, "app_password")

// Read-only user
provider.AddUser(&auth.User{
    Username:  "readonly",
    Databases: []string{"analytics"},
}, "readonly_pass")

// Configure server
config := server.NewConfig()
config.Host = "0.0.0.0"
config.Port = 5432
config.Database = "production"
config.RequireAuth = true
config.UserProvider = provider

srv, _ := server.NewServer(config)
srv.ListenAndServe()
```

### Server with TLS

```go
import "crypto/tls"

// Load TLS certificate
cert, err := tls.LoadX509KeyPair("server.crt", "server.key")
if err != nil {
    log.Fatal(err)
}

tlsConfig := &tls.Config{
    Certificates: []tls.Certificate{cert},
    MinVersion:   tls.VersionTLS12,
}

config := server.NewConfig()
config.Host = "0.0.0.0"
config.Port = 5432
config.Database = "secure_db"
config.RequireAuth = true
config.Username = "admin"
config.Password = "secret"
config.TLSConfig = tlsConfig

srv, _ := server.NewServer(config)
srv.ListenAndServe()
```

### Production Configuration

```go
import (
    "log/slog"
    "os"
)

// Create structured logger
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))

// Create user provider (could be database-backed in production)
provider := auth.NewMemoryProvider()
provider.AddUser(&auth.User{
    Username:  os.Getenv("DB_ADMIN_USER"),
    Superuser: true,
}, os.Getenv("DB_ADMIN_PASSWORD"))

config := server.NewConfig()
config.Host = "0.0.0.0"
config.Port = 5432
config.Database = os.Getenv("DB_NAME")
config.MaxConnections = 200
config.ShutdownTimeout = 60 * time.Second
config.ServerVersion = "16.0.0"
config.RequireAuth = true
config.UserProvider = provider
config.LogStartupParams = false  // Don't log sensitive startup params

srv, _ := server.NewServer(config)
srv.SetLogger(logger)

// Graceful shutdown handling
go func() {
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    srv.Shutdown(ctx)
}()

srv.ListenAndServe()
```

## Troubleshooting

### Connection Refused

1. Verify the server is running and listening on the correct port
2. Check firewall rules
3. Ensure the host setting allows connections from your client

### Authentication Failed

1. Verify username and password are correct
2. Check if RequireAuth is enabled
3. For multi-user auth, verify the user exists in UserProvider

### Query Errors

1. Check if the PostgreSQL function has a DuckDB equivalent
2. Verify data types are supported
3. For system catalog queries, ensure the view is implemented

### Slow Performance

1. Enable connection pooling in your client
2. Use prepared statements for repeated queries
3. Check query plans with EXPLAIN

### Client Compatibility Issues

1. Set ServerVersion to match client expectations
2. Some clients require specific pg_catalog queries - check logs
3. Try disabling SSL if using sslmode=prefer

## Package Structure

```
internal/postgres/
    catalog/           # information_schema and pg_catalog views
        catalog.go     # InformationSchema implementation
        pg_catalog.go  # PgCatalog implementation
        tables.go      # information_schema.tables
        columns.go     # information_schema.columns
        schemata.go    # information_schema.schemata
        pg_*.go        # pg_catalog views

    functions/         # Function alias registry
        registry.go    # FunctionAliasRegistry
        aliases_*.go   # Category-specific aliases
        pg_*.go        # System function implementations

    server/            # Wire protocol server
        server.go      # Server implementation
        config.go      # Configuration
        handler.go     # Query handler
        session.go     # Session management
        prepared.go    # Prepared statements
        auth/          # Authentication
            auth.go    # Authenticator interface
            memory.go  # In-memory provider

    types/             # Type mapping
        oids.go        # PostgreSQL OID constants
        aliases.go     # Type aliases
        mapper.go      # Type mapper
        convert.go     # Type conversion
```
