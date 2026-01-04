# Change: add-excel-table-function-parser-support

## Why
DuckDB supports `read_excel('file.xlsx')` and `read_excel_auto()` table functions for Excel import with high visual fidelity to MS Excel rendering (sheets, ranges, headers). Pure Go driver lacks parser support, blocking I/O compatibility. Enables testing fidelity via LibreOffice xlsx->pdf conversion + visual inspection.

## What Changes
- Parse `read_excel('path.xlsx', sheet='Sheet1', range='A1:Z100', header=true)` and variants in FROM clause TableRef.TableFunction.
- Supports all DuckDB Excel options for pixel-perfect data extraction matching OpenXML spec.
- No execution impl (parser only).

## Impact
- Affected specs: parser (table functions), file-io (later impl)
- Affected code: parser.go (extend parseTableFunction), ast.go (add Excel-specific if needed)
- Tests: new parser_test.go cases matching DuckDB Excel parsing

## Breaking Changes
None (additive parsing).