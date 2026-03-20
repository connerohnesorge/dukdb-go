## 1. Collation Package

- [ ] 1.1 Create `internal/collation/collator.go` with `Collator` interface (`Compare`, `Name`)
- [ ] 1.2 Create `internal/collation/registry.go` with `Registry` struct (register, get, list)
- [ ] 1.3 Implement `BinaryCollator` (default byte-level comparison)
- [ ] 1.4 Implement `NocaseCollator` using Unicode case folding (`golang.org/x/text/cases`)
- [ ] 1.5 Implement `NoaccentCollator` using NFKD decomposition and combining mark removal
- [ ] 1.6 Implement `NFCCollator` using NFC normalization before comparison
- [ ] 1.7 Implement `LocaleCollator` wrapping `golang.org/x/text/collate.Collator`
- [ ] 1.8 Implement chained collation resolution (e.g., `de_DE.NOCASE.NOACCENT`)
- [ ] 1.9 Implement `NocaseWrapper` and `NoaccentWrapper` for modifier chaining
- [ ] 1.10 Add `golang.org/x/text` dependency to `go.mod`
- [ ] 1.11 Write unit tests for all collators and registry operations

## 2. Parser Extensions

- [ ] 2.1 Add `Collation` field to `OrderByExpr` in `internal/parser/ast.go`
- [ ] 2.2 Parse `COLLATE <name>` in ORDER BY expressions in `internal/parser/parser.go`
- [ ] 2.3 Add `Collation` field to `ColumnDef` in `internal/parser/ast.go` (if not already present)
- [ ] 2.4 Parse `COLLATE <name>` in CREATE TABLE column definitions
- [ ] 2.5 Write parser tests for COLLATE syntax in ORDER BY and CREATE TABLE

## 3. Binder Extensions

- [ ] 3.1 Add `Collation` field to `BoundOrderBy` in `internal/binder/statements.go`
- [ ] 3.2 Propagate `Collation` from `OrderByExpr` to `BoundOrderBy` in `internal/binder/bind_stmt.go`
- [ ] 3.3 Resolve column-level default collation when no explicit COLLATE in ORDER BY
- [ ] 3.4 Validate collation names exist in registry during binding
- [ ] 3.5 Write binder tests for collation propagation

## 4. Sort Operator Integration

- [ ] 4.1 Modify `PhysicalSortOperator.compareRowData` to use collation-aware comparison
- [ ] 4.2 Add `compareWithCollation` helper to sort operator
- [ ] 4.3 Propagate collation from `BoundOrderBy` through planner to physical sort
- [ ] 4.4 Write sort operator tests with COLLATE (locale ordering, NOCASE, NOACCENT)
- [ ] 4.5 Write sort operator tests with chained collations (e.g., `de_DE.NOCASE`)

## 5. Locale-Aware String Functions

- [ ] 5.1 Add locale-aware `UPPER()` implementation using `golang.org/x/text/cases`
- [ ] 5.2 Add locale-aware `LOWER()` implementation using `golang.org/x/text/cases`
- [ ] 5.3 Detect column collation to determine locale for UPPER/LOWER
- [ ] 5.4 Write tests for locale-aware UPPER/LOWER (Turkish i, German sharp-s)

## 6. PRAGMA and Metadata

- [ ] 6.1 Extend `PRAGMA collations` to return all registered collations from registry
- [ ] 6.2 Include locale-based collations in PRAGMA output
- [ ] 6.3 Write tests for PRAGMA collations with dynamic collation registration

## 7. Integration Testing

- [ ] 7.1 Test `ORDER BY name COLLATE de_DE` produces correct German sort order
- [ ] 7.2 Test `ORDER BY name COLLATE NOCASE` performs case-insensitive sort
- [ ] 7.3 Test `CREATE TABLE t (name VARCHAR COLLATE en_US)` stores collation in catalog
- [ ] 7.4 Test chained collation `ORDER BY name COLLATE de_DE.NOCASE.NOACCENT`
- [ ] 7.5 Test unknown collation returns clear error message
- [ ] 7.6 Test collation does not affect non-string columns (numeric ORDER BY unchanged)
- [ ] 7.7 End-to-end test via `database/sql` interface with collation queries
