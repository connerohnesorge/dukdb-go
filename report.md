# Useless Test Report

Scope: Static review of the repository without running tests.

## Criteria
- Unconditionally skipped tests (never executed).
- Tests gated on missing features or external tooling that commonly skip.
- Tests/examples not executed in a default `go test` run.

## Findings

### Unconditionally skipped (never run)
- `prepared_introspection_test.go:TestExecBoundWithoutParams` — `t.Skip` at start; points to internal integration test.
- `prepared_introspection_test.go:TestExecBoundWithParams` — `t.Skip` at start; points to internal integration test.
- `prepared_introspection_test.go:TestQueryBoundWithParams` — `t.Skip` at start; points to internal integration test.
- `prepared_introspection_test.go:TestRebindParameters` — `t.Skip` at start; points to internal integration test.
- `internal/executor/phase_c_e2e_test.go:TestPhaseC_UpdateWithWhere` — placeholder; `UPDATE` operator not implemented.
- `internal/executor/phase_c_e2e_test.go:TestPhaseC_DeleteWithWhere` — placeholder; `DELETE` operator not implemented.

### Feature-gated (likely skipped until implementation exists)
- `internal/executor/phase_d_errors_test.go:TestPhaseD_Error_Constraint` — skips if PRIMARY KEY constraints are unsupported or if violations are not enforced; current code comments indicate constraints are not fully implemented.

### External tooling gated (skips when DuckDB CLI is missing)
- `internal/format/duckdb_compat_test.go:TestDuckDBCLITypeCompatibility`
- `internal/format/duckdb_compat_test.go:TestDuckDBCLIQueryTypes`
- `internal/format/duckdb_compat_test.go:TestCatalogDuckDBCompatibility`
- `internal/format/duckdb_compat_test.go:TestStandardSQLDatabaseDriver`
- `internal/format/duckdb_compat_test.go:TestDuckDBVersion`

### Build-tag gated (not in default `go test ./...`)
- `duckdb-go/arrow_test.go` — requires `-tags duckdb_arrow`.
- `duckdb-go/duckdb_extensions_test.go` — excluded when `duckdb_use_lib` or `duckdb_use_static_lib` build tags are set.

### Examples not executed as tests
- `duckdb-go/duckdb_test.go:ExampleNewConnector` — no `// Output:` comment, so it is compiled but not executed by `go test`.
- `duckdb-go/duckdb_test.go:Example_simpleConnection` — no `// Output:` comment, so it is compiled but not executed by `go test`.

### Nested module test isolation
- All tests under `duckdb-go/` live in a nested Go module (`duckdb-go/go.mod`), so they are not run by `go test ./...` from the repository root unless CI explicitly runs tests in that submodule.
