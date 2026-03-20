# Change: Add ICU Collation Support

## Why

DuckDB v1.4.3 provides ICU extension support for locale-aware string comparison, sorting, and case conversion. Currently, dukdb-go has minimal collation infrastructure: `TypeModifier` stores a `Collation` string field, `PRAGMA collations` returns a hardcoded list (NOCASE, NOACCENT, NFC), and the parser in the PostgreSQL compatibility layer recognizes the COLLATE keyword. However, no actual collation-aware comparison, sorting, or case conversion is implemented. Strings are compared using Go's default byte-level ordering, which produces incorrect results for non-ASCII text and does not support locale-specific rules.

Without collation support, queries like `ORDER BY name COLLATE de_DE` silently ignore the collation, producing incorrect sort orders for German, French, Chinese, and other locale-sensitive text. Additionally, case-insensitive operations (`COLLATE NOCASE`) and accent-insensitive operations (`COLLATE NOACCENT`) are not functional.

## What Changes

- Add a `CollationRegistry` that maps collation names to Go collators backed by `golang.org/x/text/collate`
- Add support for `COLLATE` clause in `ORDER BY` expressions in the parser and AST
- Add support for `COLLATE` clause in `CREATE TABLE` column definitions
- Integrate collation-aware comparison into the sort operator (`PhysicalSortOperator`)
- Implement `NOCASE` and `NOACCENT` modifier collations using Unicode folding
- Implement locale-aware `UPPER()` and `LOWER()` functions that respect column collation
- Extend `PRAGMA collations` to return all registered collations (built-in + locale-based)
- Add collation field to `BoundOrderBy` so the planner and executor can access it
- Support chained collation modifiers (e.g., `COLLATE de_DE.NOCASE.NOACCENT`)

## Impact

- Affected specs: `collation` (new capability)
- Affected code:
  - `internal/parser/ast.go` - Add `Collation` field to `OrderByExpr`
  - `internal/parser/parser.go` - Parse `COLLATE` in ORDER BY and column definitions
  - `internal/binder/statements.go` - Add `Collation` field to `BoundOrderBy`
  - `internal/binder/bind_stmt.go` - Propagate collation from AST to bound tree
  - `internal/executor/physical_sort.go` - Use collation-aware comparison
  - `internal/executor/physical_maintenance.go` - Extend `PRAGMA collations`
  - `internal/executor/expr.go` - Locale-aware UPPER/LOWER
  - New file: `internal/collation/registry.go` - Collation registry and collator creation
  - New file: `internal/collation/collator.go` - Collator interface and implementations
- New dependency: `golang.org/x/text` (pure Go, no CGO)
