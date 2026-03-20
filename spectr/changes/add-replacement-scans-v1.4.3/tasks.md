# Tasks: Add Replacement Scans for DuckDB v1.4.3

- [ ] 1. Add ReplacementScan AST node — Add `ReplacementScan` struct with `Path string` field to `internal/parser/ast.go`. Add `ReplacementScan *ReplacementScan` field to the `TableRef` struct. Validate: AST compiles with no errors.

- [ ] 2. Parse string literals in FROM position — In `parseTableRef()` at `internal/parser/parser.go:761`, before the else error clause at line 826, add a case for `tokenString`. When a string literal is found, create a `ReplacementScan{Path: value}` and set it on the TableRef. Set `ref.TableName = path` for alias resolution. Validate: `SELECT * FROM 'test.csv'` parses without error and produces a TableRef with ReplacementScan.Path = "test.csv".

- [ ] 3. Add file extension detection utility — Add `detectTableFunction(path string) string` to `internal/binder/bind_stmt.go` (or a shared utility file). Map extensions: .csv/.tsv → "read_csv_auto", .parquet → "read_parquet", .json → "read_json_auto", .ndjson/.jsonl → "read_ndjson", .xlsx/.xls → "read_xlsx", .arrow/.ipc → "arrow_scan". Handle URL query parameters by stripping them before extension detection. Return "" for unknown extensions. Validate: Unit test for all supported extensions and edge cases.

- [ ] 4. Bind replacement scans to table function calls — In the binder's table reference resolution (bind_stmt.go), detect `ref.ReplacementScan != nil` and call a new `bindReplacementScan()` method. This method: (a) calls `detectTableFunction()` to get the function name, (b) creates a synthetic `parser.TableFunction` AST node with the path as a string argument, (c) delegates to the existing table function binding logic. Error on unrecognized extensions. Validate: `SELECT * FROM 'test.csv'` binds to the same plan as `SELECT * FROM read_csv_auto('test.csv')`.

- [ ] 5. Integration tests — Write tests covering: CSV, Parquet, JSON, NDJSON file paths as table references; alias support (`FROM 'file.csv' AS t`); JOIN with two file paths; unrecognized extension error; cloud URL format detection (s3://, https://). Verify no regressions in existing FROM clause parsing (table names, subqueries, table functions, VALUES).
