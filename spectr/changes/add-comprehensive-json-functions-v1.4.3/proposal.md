# Change: Add comprehensive JSON scalar, aggregate, and table functions for DuckDB v1.4.3 compatibility

## Why

DuckDB v1.4.3 provides a rich set of JSON functions beyond basic extraction. While dukdb-go already implements JSON_EXTRACT, JSON_EXTRACT_STRING, JSON_VALID, JSON_TYPE, JSON_KEYS, JSON_ARRAY_LENGTH, JSON_MERGE_PATCH, JSON_OBJECT, JSON_ARRAY, and TO_JSON, it is missing JSON_CONTAINS, JSON_QUOTE, the JSON_GROUP_ARRAY and JSON_GROUP_OBJECT aggregate functions, and the JSON_EACH table function. Adding these closes the gap for applications that depend on DuckDB's full JSON function set.

## What Changes

- ADDED: `JSON_CONTAINS(json, value)` scalar function -- check if a JSON document contains a specified value
- ADDED: `JSON_QUOTE(value)` scalar function -- quote a Go value as a JSON string literal
- ADDED: `JSON_GROUP_ARRAY(expr)` aggregate function -- collect grouped values into a JSON array string
- ADDED: `JSON_GROUP_OBJECT(key, value)` aggregate function -- collect grouped key/value pairs into a JSON object string
- ADDED: `JSON_EACH(json)` table function -- expand a JSON object or array into one row per entry with `key` and `value` columns

## Impact

- Affected specs: `json-functions` (new capability)
- Affected code:
  - `internal/executor/expr.go` -- add scalar function dispatch for JSON_CONTAINS and JSON_QUOTE
  - `internal/executor/physical_aggregate.go` -- add aggregate cases for JSON_GROUP_ARRAY and JSON_GROUP_OBJECT
  - `internal/executor/table_function_csv.go` -- add table function dispatch for JSON_EACH
  - `internal/executor/table_function_json_each.go` -- new file implementing JSON_EACH
  - `internal/executor/expr.go` -- aggregate recognition in the function-name switch
