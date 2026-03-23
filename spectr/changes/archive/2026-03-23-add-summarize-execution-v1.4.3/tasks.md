## 0. Remove Existing Partial Implementation

- [x] 0.1 Remove `handleSummarize()` from `internal/engine/conn.go` (lines ~811-949) and its dispatch call site. This partial implementation bypasses the planner/executor architecture and has incorrect column names (`unique_count`, `null_count`), missing columns (`q25`, `q50`, `q75`), wrong `count` semantics (total rows instead of non-NULL count), biased std dev (N instead of N-1), and string-based min/max comparison. The `toFloat64()` helper can be kept or moved to a shared utility.

## 1. Binder

- [ ] 1.1 Add a binder case for `*parser.SummarizeStmt` in `internal/binder/` that resolves the table reference (schema + table name) or binds the inner SELECT query.
- [ ] 1.2 Validate that the referenced table exists during binding (not during execution). Return a bound summarize node.

## 2. Planner

- [ ] 2.1 Add `PhysicalPlanSummarize` type constant to `PhysicalPlanType` enum in `internal/planner/physical.go`.
- [ ] 2.2 Add `PhysicalSummarize` struct with Schema, TableName, and Query fields.
- [ ] 2.3 Add `planStatement()` case for `*parser.SummarizeStmt` — resolve table or plan inner query.

## 3. Executor

- [ ] 3.1 Create `internal/executor/physical_summarize.go` with `executeSummarize()` function.
- [ ] 3.2 Implement per-column statistics accumulation: min, max, count (non-NULL count per column, NOT total row count), null_count, distinct_count. NOTE: the existing implementation incorrectly uses `len(dataRows)` as `count`; the correct semantics is non-NULL values per column.
- [ ] 3.3 Implement type-aware min/max comparison. Compare numeric types as float64, strings lexicographically, dates/timestamps by underlying integer. Do NOT use `fmt.Sprintf("%v", val)` and string comparison (the existing implementation does this, which makes "100" < "20").
- [ ] 3.4 Implement running mean and sample standard deviation for avg/std on numeric columns. Use Bessel's correction (divide by N-1, not N) to match DuckDB's sample std dev behavior. For a single value, std should be NULL.
- [ ] 3.5 Implement percentile computation (q25, q50, q75): collect non-NULL values during scan, sort using type-aware comparison after scan, compute using linear interpolation formula (see design.md). Use reservoir sampling for tables with >10,000 rows.
- [ ] 3.6 Format output as a result set with 12 columns: column_name, column_type, min, max, approx_unique, avg, std, q25, q50, q75, count, null_percentage. Note: column names must match DuckDB exactly (approx_unique NOT unique_count, null_percentage NOT null_count).
- [ ] 3.7 Handle non-numeric columns: avg and std return NULL for VARCHAR, BOOLEAN, DATE, etc.
- [ ] 3.8 Handle empty tables: return rows with NULL statistics and count=0.

## 4. SUMMARIZE SELECT ... Support

- [ ] 4.1 When `PhysicalSummarize.Query` is set, execute the inner query plan to collect all result rows. NOTE: the existing implementation returns an error for SUMMARIZE SELECT; this must be fully implemented.
- [ ] 4.2 Use the inner query's column metadata for column_name and column_type.
- [ ] 4.3 Compute the same statistics over the query result set (reuse the same columnStats accumulation logic).
- [ ] 4.4 Add integration test: `SUMMARIZE SELECT price FROM products WHERE category = 'A'`.

## 5. Parser Fix

- [ ] 5.1 Fix `SummarizeStmt.Type()` at `internal/parser/ast.go:1753` — currently returns `STATEMENT_TYPE_SELECT` which makes it indistinguishable from regular SELECT. Consider adding a dedicated `STATEMENT_TYPE_SUMMARIZE` constant or reusing an appropriate existing type.

## 6. Operator Registration

- [ ] 6.1 Register `PhysicalPlanSummarize` in `internal/executor/operator.go`.
- [ ] 6.2 Add execution dispatch in the main executor switch statement.

## 7. Integration Tests

- [ ] 7.1 Test `SUMMARIZE table_name` with numeric, string, date, and boolean columns.
- [ ] 7.2 Test `SUMMARIZE schema.table_name` with schema-qualified table.
- [ ] 7.3 Test `SUMMARIZE SELECT ...` with a query (verifying it works, not just errors).
- [ ] 7.4 Test SUMMARIZE on an empty table returns zero counts.
- [ ] 7.5 Test SUMMARIZE with NULL values shows correct null_percentage.
- [ ] 7.6 Test SUMMARIZE output column types match DuckDB format (VARCHAR for min/max/q*, BIGINT for count/approx_unique, DOUBLE for avg/std/null_percentage).
- [ ] 7.7 Verify SUMMARIZE on a table with 100+ rows produces reasonable statistics (min <= q25 <= q50 <= q75 <= max).
- [ ] 7.8 Test that `count` returns non-NULL count per column (not total row count). Insert rows with NULLs in some columns and verify count differs per column.
- [ ] 7.9 Test that numeric min/max comparison is correct (e.g., a table with values 2, 10, 100 should have min=2 and max=100, not min="10" and max="2").
- [ ] 7.10 Test that std uses sample standard deviation (N-1). For values [2, 4], std should be sqrt(2) ~ 1.414, not 1.0.
