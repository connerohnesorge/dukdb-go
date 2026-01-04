# Change: add-json-operators-parser-support

## Why
DuckDB JSON ops (-> ->> #> #>> ? ?| ?& json_extract etc.) essential for JSON handling (common in Excel/Office data). Missing expr parser gaps compatibility.

## What Changes
- Parse JSONPathExpr: col->'key', ->>'key', #>'path', json_extract etc.

## Impact
- parser expr parsing, ast.go JSONPathExpr

## Breaking
None.