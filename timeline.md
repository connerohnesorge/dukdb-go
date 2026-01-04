# DuckDB-Go Parser Completion Timeline

Chronological dependency order for remaining proposals. Assumes parallel where possible (parser independent). Est weekly, serial for deps.

## Phase 1: Parser Gaps (Weeks 1-2)
1. **add-excel-table-function-parser-support** (Week 1): Excel read TF for visual fidelity tests. Enables LibreOffice pdf validation.
2. **add-create-scalar-udf-parser-support** (Week 1 parallel): UDF create for extensibility/Excel helpers.

## Phase 2: Advanced Exprs (Week 2)
3. **add-json-operators-parser-support** (Week 2): JSON ->/->> #> @> for DuckDB JSON support.
4. **add-array-struct-support-parser** (Week 2 parallel): ARRAY literals/subscript, STRUCT access.

## Phase 3: DDL/Meta (Week 3)
5. **add-show-commands-parser** (Week 3): SHOW tables/columns for meta queries.
6. **add-full-pragmas-parser** (Week 3): Complex PRAGMA opts.

## Phase 4: Verify & Timeline (Week 4)
- Run full go test ./internal/parser
- Create examples/**/xlsx for visual tests (LibreOffice -> pdf -> inspect)
- Archive all proposals
- Full DuckDB SQL grammar parity (98% covered)

**Deps:** Parser only (no binder/executor). Total: 4 weeks serial/par.

**Milestones:**
- Week 1: Excel + UDF parser/tests
- Week 2: JSON/Array (test complex queries)
- Week 3: Meta (compatibility tests pass)
- Week 4: Visual fidelity suite + full validate