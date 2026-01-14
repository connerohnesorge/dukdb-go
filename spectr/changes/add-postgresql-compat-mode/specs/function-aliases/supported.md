# Supported PostgreSQL Function Aliases

This document lists all PostgreSQL functions supported by dukdb-go's PostgreSQL compatibility mode.

## Overview

dukdb-go provides compatibility aliases for 140+ PostgreSQL functions, allowing applications designed for PostgreSQL to work with minimal changes. The alias system handles three categories of functions:

1. **Direct Aliases**: Functions that map directly to DuckDB equivalents with identical semantics
2. **Transformed Functions**: Functions that require argument transformation to match PostgreSQL behavior
3. **System Functions**: PostgreSQL-specific pg_* functions with custom implementations

## Function Categories

### Date/Time Functions

| PostgreSQL Function | DuckDB Function | Arguments | Description |
|---------------------|-----------------|-----------|-------------|
| `now()` | `current_timestamp` | 0 | Returns current date and time |
| `current_timestamp` | `current_timestamp` | 0 | Returns current date and time |
| `current_date` | `current_date` | 0 | Returns current date |
| `current_time` | `current_time` | 0 | Returns current time |
| `localtime` | `current_time` | 0 | Returns current local time |
| `localtimestamp` | `current_timestamp` | 0 | Returns current local timestamp |
| `clock_timestamp()` | `current_timestamp` | 0 | Returns current clock time |
| `transaction_timestamp()` | `current_timestamp` | 0 | Returns timestamp of current transaction |
| `statement_timestamp()` | `current_timestamp` | 0 | Returns timestamp of current statement |
| `timeofday()` | `current_timestamp` | 0 | Returns current time as text |
| `age(timestamp)` | `age` | 1-2 | Calculates age between timestamps |
| `date_trunc(text, timestamp)` | `date_trunc` | 2 | Truncates timestamp to specified precision |
| `date_part(text, timestamp)` | `date_part` | 2 | Extracts date/time part |
| `extract(field FROM timestamp)` | `extract` | 1 | Extracts date/time field |
| `to_timestamp(value)` | `to_timestamp` | 1-2 | Converts to timestamp |

### String Functions

| PostgreSQL Function | DuckDB Function | Arguments | Description |
|---------------------|-----------------|-----------|-------------|
| `concat(...)` | `concat` | 1+ | Concatenates strings |
| `concat_ws(sep, ...)` | `concat_ws` | 2+ | Concatenates strings with separator |
| `length(text)` | `length` | 1 | Returns string length |
| `char_length(text)` | `length` | 1 | Returns character length |
| `character_length(text)` | `length` | 1 | Returns character length |
| `octet_length(text)` | `octet_length` | 1 | Returns byte length |
| `bit_length(text)` | `bit_length` | 1 | Returns bit length |
| `lower(text)` | `lower` | 1 | Converts to lowercase |
| `upper(text)` | `upper` | 1 | Converts to uppercase |
| `initcap(text)` | `initcap` | 1 | Capitalizes first letter of each word |
| `trim(text)` | `trim` | 1-2 | Removes whitespace from string |
| `ltrim(text)` | `ltrim` | 1-2 | Removes leading whitespace |
| `rtrim(text)` | `rtrim` | 1-2 | Removes trailing whitespace |
| `btrim(text)` | `trim` | 1-2 | Removes leading and trailing characters |
| `lpad(text, len)` | `lpad` | 2-3 | Left-pads string |
| `rpad(text, len)` | `rpad` | 2-3 | Right-pads string |
| `substring(text, start)` | `substring` | 2-3 | Extracts substring |
| `substr(text, start)` | `substr` | 2-3 | Extracts substring |
| `left(text, n)` | `left` | 2 | Returns leftmost n characters |
| `right(text, n)` | `right` | 2 | Returns rightmost n characters |
| `replace(text, from, to)` | `replace` | 3 | Replaces substring |
| `translate(text, from, to)` | `translate` | 3 | Replaces characters |
| `reverse(text)` | `reverse` | 1 | Reverses string |
| `repeat(text, n)` | `repeat` | 2 | Repeats string n times |
| `position(sub IN text)` | `position` | 1 | Finds position of substring |
| `strpos(text, sub)` | `strpos` | 2 | Finds position of substring |
| `split_part(text, delim, n)` | `split_part` | 3 | Splits string and returns part |
| `string_to_array(text, delim)` | `string_split` | 2-3 | Splits string into array |
| `regexp_replace(text, pattern, replacement)` | `regexp_replace` | 3-4 | Regular expression replace |
| `regexp_matches(text, pattern)` | `regexp_matches` | 2-3 | Regular expression match |
| `overlay(text, replacement, start)` | `overlay` | 3-4 | Replaces substring |
| `md5(text)` | `md5` | 1 | Computes MD5 hash |
| `ascii(text)` | `ascii` | 1 | Returns ASCII code of first character |
| `chr(code)` | `chr` | 1 | Returns character from ASCII code |
| `format(format_string, ...)` | `format` | 1+ | Formats string with printf-style |

### Math Functions

| PostgreSQL Function | DuckDB Function | Arguments | Description |
|---------------------|-----------------|-----------|-------------|
| `abs(n)` | `abs` | 1 | Absolute value |
| `ceil(n)` | `ceil` | 1 | Ceiling |
| `ceiling(n)` | `ceil` | 1 | Ceiling (alias) |
| `floor(n)` | `floor` | 1 | Floor |
| `round(n)` | `round` | 1-2 | Round to nearest integer or decimal places |
| `trunc(n)` | `trunc` | 1-2 | Truncate toward zero |
| `truncate(n)` | `trunc` | 1-2 | Truncate (alias) |
| `mod(n, m)` | `mod` | 2 | Modulo |
| `power(n, m)` | `pow` | 2 | Power |
| `pow(n, m)` | `pow` | 2 | Power |
| `sqrt(n)` | `sqrt` | 1 | Square root |
| `cbrt(n)` | `cbrt` | 1 | Cube root |
| `exp(n)` | `exp` | 1 | Exponential |
| `ln(n)` | `ln` | 1 | Natural logarithm |
| `log(n)` | `log` | 1-2 | Logarithm |
| `log10(n)` | `log10` | 1 | Base 10 logarithm |
| `sign(n)` | `sign` | 1 | Sign of number |
| `random()` | `random` | 0 | Random value between 0 and 1 |
| `setseed(n)` | `setseed` | 1 | Sets random seed |
| `degrees(radians)` | `degrees` | 1 | Radians to degrees |
| `radians(degrees)` | `radians` | 1 | Degrees to radians |
| `pi()` | `pi` | 0 | Returns pi constant |
| `sin(n)` | `sin` | 1 | Sine |
| `cos(n)` | `cos` | 1 | Cosine |
| `tan(n)` | `tan` | 1 | Tangent |
| `cot(n)` | `cot` | 1 | Cotangent |
| `asin(n)` | `asin` | 1 | Arc sine |
| `acos(n)` | `acos` | 1 | Arc cosine |
| `atan(n)` | `atan` | 1 | Arc tangent |
| `atan2(y, x)` | `atan2` | 2 | Arc tangent of two arguments |
| `gcd(a, b)` | `gcd` | 2 | Greatest common divisor |
| `lcm(a, b)` | `lcm` | 2 | Least common multiple |

### Aggregate Functions

| PostgreSQL Function | DuckDB Function | Arguments | Description |
|---------------------|-----------------|-----------|-------------|
| `count(*)` | `count` | 0-1 | Count rows |
| `sum(n)` | `sum` | 1 | Sum of values |
| `avg(n)` | `avg` | 1 | Average of values |
| `min(n)` | `min` | 1 | Minimum value |
| `max(n)` | `max` | 1 | Maximum value |
| `array_agg(value)` | `list` | 1 | Aggregates values into array |
| `string_agg(text, delim)` | `string_agg` | 2 | Concatenates strings with delimiter |
| `bool_and(bool)` | `bool_and` | 1 | Logical AND aggregate |
| `bool_or(bool)` | `bool_or` | 1 | Logical OR aggregate |
| `every(bool)` | `bool_and` | 1 | Logical AND aggregate (alias) |
| `bit_and(n)` | `bit_and` | 1 | Bitwise AND aggregate |
| `bit_or(n)` | `bit_or` | 1 | Bitwise OR aggregate |
| `variance(n)` | `var_samp` | 1 | Sample variance |
| `var_samp(n)` | `var_samp` | 1 | Sample variance |
| `var_pop(n)` | `var_pop` | 1 | Population variance |
| `stddev(n)` | `stddev_samp` | 1 | Sample standard deviation |
| `stddev_samp(n)` | `stddev_samp` | 1 | Sample standard deviation |
| `stddev_pop(n)` | `stddev_pop` | 1 | Population standard deviation |
| `corr(y, x)` | `corr` | 2 | Correlation coefficient |
| `covar_pop(y, x)` | `covar_pop` | 2 | Population covariance |
| `covar_samp(y, x)` | `covar_samp` | 2 | Sample covariance |
| `mode(value)` | `mode` | 1 | Mode (most frequent value) |
| `percentile_cont(fraction)` | `quantile_cont` | 1 | Continuous percentile |
| `percentile_disc(fraction)` | `quantile_disc` | 1 | Discrete percentile |

### Window Functions

| PostgreSQL Function | DuckDB Function | Arguments | Description |
|---------------------|-----------------|-----------|-------------|
| `row_number()` | `row_number` | 0 | Sequential row number within partition |
| `rank()` | `rank` | 0 | Rank with gaps |
| `dense_rank()` | `dense_rank` | 0 | Rank without gaps |
| `percent_rank()` | `percent_rank` | 0 | Relative rank |
| `cume_dist()` | `cume_dist` | 0 | Cumulative distribution |
| `ntile(n)` | `ntile` | 1 | Divides into N buckets |
| `lag(value)` | `lag` | 1-3 | Access previous row value |
| `lead(value)` | `lead` | 1-3 | Access next row value |
| `first_value(value)` | `first_value` | 1 | First value in window |
| `last_value(value)` | `last_value` | 1 | Last value in window |
| `nth_value(value, n)` | `nth_value` | 2 | Nth value in window |

### Conditional Functions

| PostgreSQL Function | DuckDB Function | Arguments | Description |
|---------------------|-----------------|-----------|-------------|
| `coalesce(...)` | `coalesce` | 1+ | Returns first non-null value |
| `nullif(a, b)` | `nullif` | 2 | Returns null if arguments are equal |
| `greatest(...)` | `greatest` | 1+ | Returns largest value |
| `least(...)` | `least` | 1+ | Returns smallest value |
| `ifnull(value, default)` | `ifnull` | 2 | Returns second arg if first is null |

### JSON Functions

| PostgreSQL Function | DuckDB Function | Arguments | Description |
|---------------------|-----------------|-----------|-------------|
| `json_array_length(json)` | `json_array_length` | 1 | Returns JSON array length |
| `jsonb_array_length(json)` | `json_array_length` | 1 | Returns JSONB array length |
| `json_extract_path(json, ...)` | `json_extract` | 2+ | Extracts JSON value at path |
| `jsonb_extract_path(json, ...)` | `json_extract` | 2+ | Extracts JSONB value at path |
| `json_extract_path_text(json, ...)` | `json_extract_string` | 2+ | Extracts JSON value as text |
| `jsonb_extract_path_text(json, ...)` | `json_extract_string` | 2+ | Extracts JSONB value as text |
| `json_typeof(json)` | `json_type` | 1 | Returns JSON value type |
| `jsonb_typeof(json)` | `json_type` | 1 | Returns JSONB value type |
| `to_json(value)` | `to_json` | 1 | Converts value to JSON |
| `to_jsonb(value)` | `to_json` | 1 | Converts value to JSONB |
| `json_agg(value)` | `json_group_array` | 1 | Aggregates values into JSON array |
| `jsonb_agg(value)` | `json_group_array` | 1 | Aggregates values into JSONB array |
| `json_object_agg(key, value)` | `json_group_object` | 2 | Aggregates key-value pairs into JSON object |
| `jsonb_object_agg(key, value)` | `json_group_object` | 2 | Aggregates key-value pairs into JSONB object |

### Miscellaneous Functions

| PostgreSQL Function | DuckDB Function | Arguments | Description |
|---------------------|-----------------|-----------|-------------|
| `gen_random_uuid()` | `uuid` | 0 | Generates random UUID |
| `uuid_generate_v4()` | `uuid` | 0 | Generates random UUID (v4) |
| `nextval(sequence)` | `nextval` | 1 | Gets next value from sequence |
| `currval(sequence)` | `currval` | 1 | Gets current sequence value |
| `setval(sequence, value)` | `setval` | 2-3 | Sets sequence value |
| `version()` | `version` | 0 | Returns database version |
| `current_user` | `current_user` | 0 | Returns current user name |
| `session_user` | `session_user` | 0 | Returns session user name |
| `current_database()` | `current_database` | 0 | Returns current database name |
| `current_catalog` | `current_database` | 0 | Returns current catalog name |
| `cast(value AS type)` | `cast` | 2 | Type cast |

---

## Transformed Functions

These functions require argument transformation to match PostgreSQL semantics.

| PostgreSQL Function | DuckDB Function | Transformation | Description |
|---------------------|-----------------|----------------|-------------|
| `generate_series(start, stop)` | `range(start, stop+1)` | Stop value incremented by 1 | PostgreSQL is inclusive, DuckDB exclusive |
| `generate_series(start, stop, step)` | `range(start, stop+1, step)` | Stop value incremented by 1 | Preserves step parameter |

### generate_series Details

PostgreSQL's `generate_series(start, stop)` returns values from `start` to `stop` inclusive, while DuckDB's `range(start, stop)` excludes the stop value. The transformer automatically adjusts:

```sql
-- PostgreSQL: generate_series(1, 5) returns 1, 2, 3, 4, 5
-- DuckDB:     range(1, 6)           returns 1, 2, 3, 4, 5
```

For non-literal stop values, the transformer wraps in an expression:
```sql
-- generate_series(1, n) becomes range(1, (n + 1))
```

---

## System Functions

These are PostgreSQL-specific functions with custom implementations in dukdb-go.

### Introspection Functions

| PostgreSQL Function | Arguments | Description |
|---------------------|-----------|-------------|
| `pg_typeof(value)` | 1 | Returns type name of argument |
| `pg_column_size(value)` | 1 | Estimated storage size of value |
| `pg_database_size(name)` | 1 | Database size in bytes |
| `pg_table_size(name)` | 1 | Table size excluding indexes |
| `pg_relation_size(name)` | 1-2 | Relation size in bytes |
| `pg_total_relation_size(name)` | 1 | Total size including indexes |
| `pg_indexes_size(name)` | 1 | Total size of indexes |

### Catalog Functions

| PostgreSQL Function | Arguments | Description |
|---------------------|-----------|-------------|
| `pg_get_constraintdef(oid)` | 1-2 | Constraint definition |
| `pg_get_indexdef(oid)` | 1-3 | Index definition |
| `pg_get_viewdef(oid)` | 1-2 | View definition |
| `pg_get_function_arguments(oid)` | 1 | Function arguments |
| `pg_get_function_result(oid)` | 1 | Function return type |
| `pg_get_expr(expr, relid)` | 2-3 | Deparse expression |
| `obj_description(oid, catalog)` | 1-2 | Object comment (returns empty) |
| `col_description(table_oid, column_num)` | 2 | Column comment (returns empty) |
| `shobj_description(oid, catalog)` | 2 | Shared object comment (returns empty) |

### Privilege Functions

All privilege functions return `true` since dukdb-go does not implement a privilege system.

| PostgreSQL Function | Arguments | Description |
|---------------------|-----------|-------------|
| `has_table_privilege(user, table, privilege)` | 2-3 | Table privilege check (always true) |
| `has_column_privilege(user, table, col, privilege)` | 3-4 | Column privilege check (always true) |
| `has_schema_privilege(user, schema, privilege)` | 2-3 | Schema privilege check (always true) |
| `has_database_privilege(user, db, privilege)` | 2-3 | Database privilege check (always true) |
| `has_function_privilege(user, func, privilege)` | 2-3 | Function privilege check (always true) |
| `has_sequence_privilege(user, seq, privilege)` | 2-3 | Sequence privilege check (always true) |
| `pg_has_role(user, role, privilege)` | 2-3 | Role membership check (always true) |

### Session/Backend Functions

| PostgreSQL Function | Arguments | Description |
|---------------------|-----------|-------------|
| `current_schema()` | 0 | Current schema name (returns 'main') |
| `current_schemas(include_implicit)` | 1 | Search path schemas |
| `pg_backend_pid()` | 0 | Backend process ID |
| `pg_postmaster_start_time()` | 0 | Server start time |
| `current_setting(name)` | 1-2 | Get setting value |
| `set_config(name, value, is_local)` | 3 | Set configuration |
| `pg_is_in_recovery()` | 0 | Recovery mode check (always false) |
| `pg_is_wal_replay_paused()` | 0 | WAL replay paused (always false) |
| `pg_client_encoding()` | 0 | Client encoding (returns UTF8) |
| `txid_current()` | 0 | Current transaction ID |

---

## Usage Notes

### Case Insensitivity

All function names are case-insensitive:
```sql
SELECT NOW();        -- works
SELECT now();        -- works
SELECT Now();        -- works
```

### Qualified Names

Functions can be called with the `pg_catalog.` prefix:
```sql
SELECT pg_catalog.now();          -- same as now()
SELECT pg_catalog.current_user;   -- same as current_user
```

### Unknown Functions

Functions not in this list are passed through to DuckDB unchanged. If DuckDB supports the function natively, it will work. If not, an error will be returned.

---

## Compatibility Notes

### Key Differences from PostgreSQL

1. **generate_series**: Automatically transforms to DuckDB's exclusive `range()` function
2. **array_agg**: Maps to DuckDB's `list()` function
3. **power**: Maps to DuckDB's `pow()` function
4. **Privilege functions**: Always return true (no access control in dukdb-go)
5. **Size functions**: Return 0 when storage size tracking is not available
6. **Comment functions**: Return empty strings (comments not supported)
7. **JSONB functions**: Map to JSON equivalents (DuckDB uses JSON internally)

### ORM Compatibility

The function alias system has been designed to support common ORMs:

| ORM | Compatibility | Notes |
|-----|---------------|-------|
| GORM | Supported | Schema introspection and CRUD operations |
| SQLAlchemy | Supported | Reflection and ORM operations |
| TypeORM | Supported | Entity scanning and migrations |
| Prisma | Supported | Basic query operations |

### psql Compatibility

The alias system handles psql startup queries including:
- `current_schema()` and `current_schemas()`
- `pg_catalog.` qualified function calls
- Session information functions

---

## Implementation Details

### File Structure

The function alias system is implemented in:

```
internal/postgres/functions/
    registry.go           # FunctionAliasRegistry implementation
    aliases_direct.go     # Initializes direct alias registrations
    aliases_datetime.go   # Date/time function aliases
    aliases_string.go     # String function aliases
    aliases_math.go       # Math function aliases
    aliases_aggregate.go  # Aggregate function aliases
    aliases_window.go     # Window function aliases
    aliases_conditional.go # Conditional function aliases
    aliases_json.go       # JSON function aliases
    aliases_misc.go       # Miscellaneous function aliases
    aliases_transform.go  # Transformed function aliases
    aliases_system.go     # System function registrations
    pg_typeof.go          # pg_typeof implementation
    pg_current.go         # current_schema, current_schemas
    pg_privilege.go       # Privilege functions
    pg_backend.go         # Backend/session functions
    pg_settings.go        # Settings functions
    pg_size.go            # Size functions
```

### Alias Categories

The alias system uses three categories:

1. **DirectAlias**: Function name differs but behavior is identical
2. **TransformedAlias**: Function requires argument transformation
3. **SystemFunction**: PostgreSQL system function requiring custom implementation

### Adding New Aliases

To add a new function alias:

```go
r.Register(&FunctionAlias{
    PostgreSQLName: "pg_function_name",
    DuckDBName:     "duckdb_function_name",
    Category:       DirectAlias,
    MinArgs:        1,
    MaxArgs:        2,
    Description:    "Description of the function",
})
```

For functions requiring transformation, implement an `ArgumentTransformer`:

```go
func transformMyFunction(funcName string, args []string) (string, []string, error) {
    // Transform arguments as needed
    return "target_function", transformedArgs, nil
}
```

---

## References

- [PostgreSQL Functions and Operators](https://www.postgresql.org/docs/current/functions.html)
- [PostgreSQL System Information Functions](https://www.postgresql.org/docs/current/functions-info.html)
- [PostgreSQL System Administration Functions](https://www.postgresql.org/docs/current/functions-admin.html)
- [DuckDB Functions](https://duckdb.org/docs/sql/functions/overview)
