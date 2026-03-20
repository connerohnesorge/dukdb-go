# ICU Collation Support - Design Details

## Implementation Details

### CollationRegistry

The registry maps collation names to collator factories. It is initialized at engine startup with built-in collations and supports runtime registration of locale-based collations.

```go
// internal/collation/registry.go

package collation

import (
    "strings"
    "sync"

    "golang.org/x/text/collate"
    "golang.org/x/text/language"
    "golang.org/x/text/transform"
    "golang.org/x/text/unicode/norm"
)

// Collator defines the interface for string comparison with collation.
type Collator interface {
    // Compare returns -1 if a < b, 0 if a == b, 1 if a > b
    // under this collation's rules.
    Compare(a, b string) int
    // Name returns the canonical name of this collation.
    Name() string
}

// Registry holds all available collations.
type Registry struct {
    mu        sync.RWMutex
    collators map[string]Collator
}

// DefaultRegistry is the global collation registry.
var DefaultRegistry = NewRegistry()

func NewRegistry() *Registry {
    r := &Registry{
        collators: make(map[string]Collator),
    }
    r.registerBuiltins()
    return r
}

func (r *Registry) registerBuiltins() {
    r.Register(&NocaseCollator{})
    r.Register(&NoaccentCollator{})
    r.Register(&NFCCollator{})
    r.Register(&BinaryCollator{})
}

func (r *Registry) Register(c Collator) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.collators[strings.ToUpper(c.Name())] = c
}

// Get retrieves a collator by name. Supports chained modifiers
// like "de_DE.NOCASE.NOACCENT" and auto-creates locale collators.
func (r *Registry) Get(name string) (Collator, error) {
    normalized := strings.ToUpper(strings.TrimSpace(name))

    r.mu.RLock()
    if c, ok := r.collators[normalized]; ok {
        r.mu.RUnlock()
        return c, nil
    }
    r.mu.RUnlock()

    // Parse chained collation: "de_DE.NOCASE.NOACCENT"
    return r.resolveChained(normalized)
}

// ListCollations returns the names of all registered collations.
func (r *Registry) ListCollations() []string {
    r.mu.RLock()
    defer r.mu.RUnlock()
    names := make([]string, 0, len(r.collators))
    for name := range r.collators {
        names = append(names, name)
    }
    return names
}
```

### Locale Collator (golang.org/x/text/collate)

Locale-based collators wrap `collate.Collator` from the Go text package:

```go
// internal/collation/locale.go

// LocaleCollator wraps golang.org/x/text/collate for locale-aware comparison.
type LocaleCollator struct {
    name     string
    collator *collate.Collator
}

func NewLocaleCollator(locale string) (*LocaleCollator, error) {
    tag, err := language.Parse(locale)
    if err != nil {
        return nil, fmt.Errorf("unsupported locale %q: %w", locale, err)
    }
    c := collate.New(tag)
    return &LocaleCollator{
        name:     strings.ToUpper(locale),
        collator: c,
    }, nil
}

func (lc *LocaleCollator) Compare(a, b string) int {
    return lc.collator.CompareString(a, b)
}

func (lc *LocaleCollator) Name() string {
    return lc.name
}
```

### Chained Collation Resolution

DuckDB supports chained collation modifiers like `de_DE.NOCASE.NOACCENT`. The resolution algorithm:

1. Split the collation string on `.` (e.g., `["de_DE", "NOCASE", "NOACCENT"]`)
2. First segment is the base locale (or a built-in like `NOCASE`)
3. Remaining segments are modifier wrappers applied in order
4. If only one segment and it matches a built-in, return it directly
5. If the base is a locale tag, create a `LocaleCollator`
6. Wrap with modifier collators in order

```go
func (r *Registry) resolveChained(name string) (Collator, error) {
    parts := strings.Split(name, ".")
    if len(parts) == 0 {
        return nil, fmt.Errorf("empty collation name")
    }

    // Resolve base collator
    var base Collator
    var err error

    r.mu.RLock()
    if c, ok := r.collators[parts[0]]; ok {
        base = c
    }
    r.mu.RUnlock()

    if base == nil {
        // Try as locale
        base, err = NewLocaleCollator(parts[0])
        if err != nil {
            return nil, fmt.Errorf("unknown collation %q: %w", parts[0], err)
        }
        r.Register(base)
    }

    // Apply modifiers
    for _, mod := range parts[1:] {
        switch mod {
        case "NOCASE":
            base = &NocaseWrapper{inner: base, name: name}
        case "NOACCENT":
            base = &NoaccentWrapper{inner: base, name: name}
        default:
            return nil, fmt.Errorf("unknown collation modifier %q", mod)
        }
    }

    // Cache the chained collator
    r.Register(base)
    return base, nil
}
```

### Built-in Collators

```go
// BinaryCollator uses byte-level comparison (default Go behavior).
type BinaryCollator struct{}
func (c *BinaryCollator) Name() string         { return "BINARY" }
func (c *BinaryCollator) Compare(a, b string) int {
    if a < b { return -1 }
    if a > b { return 1 }
    return 0
}

// NocaseCollator performs case-insensitive comparison using Unicode case folding.
type NocaseCollator struct{}
func (c *NocaseCollator) Name() string         { return "NOCASE" }
func (c *NocaseCollator) Compare(a, b string) int {
    fa := strings.ToUpper(cases.Fold().String(a))
    fb := strings.ToUpper(cases.Fold().String(b))
    if fa < fb { return -1 }
    if fa > fb { return 1 }
    return 0
}

// NoaccentCollator removes accents before comparison using Unicode NFKD decomposition
// followed by stripping combining marks.
type NoaccentCollator struct{}
func (c *NoaccentCollator) Name() string         { return "NOACCENT" }
func (c *NoaccentCollator) Compare(a, b string) int {
    sa := removeAccents(a)
    sb := removeAccents(b)
    if sa < sb { return -1 }
    if sa > sb { return 1 }
    return 0
}

func removeAccents(s string) string {
    // NFKD decomposition splits accented chars into base + combining mark
    // Then strip combining marks (Unicode category Mn)
    t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
    result, _, _ := transform.String(t, s)
    return result
}

// NocaseWrapper wraps another collator with case folding applied first.
type NocaseWrapper struct {
    inner Collator
    name  string
}
func (c *NocaseWrapper) Name() string { return c.name }
func (c *NocaseWrapper) Compare(a, b string) int {
    return c.inner.Compare(
        cases.Fold().String(a),
        cases.Fold().String(b),
    )
}

// NoaccentWrapper wraps another collator with accent removal applied first.
type NoaccentWrapper struct {
    inner Collator
    name  string
}
func (c *NoaccentWrapper) Name() string { return c.name }
func (c *NoaccentWrapper) Compare(a, b string) int {
    return c.inner.Compare(removeAccents(a), removeAccents(b))
}
```

### ORDER BY COLLATE Integration with Sort Operator

The `COLLATE` clause is parsed in `ORDER BY` and stored in the AST:

```go
// internal/parser/ast.go - Modified OrderByExpr
type OrderByExpr struct {
    Expr       Expr
    Desc       bool
    NullsFirst *bool
    Collation  string // e.g., "de_DE", "NOCASE", "de_DE.NOCASE.NOACCENT"
}
```

The binder propagates collation to `BoundOrderBy`:

```go
// internal/binder/statements.go - Modified BoundOrderBy
type BoundOrderBy struct {
    Expr      BoundExpr
    Desc      bool
    Collation string // Resolved collation name, empty = default binary
}
```

The sort operator resolves the collator and uses it for string comparison:

```go
// internal/executor/physical_sort.go - Modified compareRowData
func (op *PhysicalSortOperator) compareRowData(a, b []any) (int, error) {
    rowA := op.buildRowMap(a)
    rowB := op.buildRowMap(b)

    for _, order := range op.orderBy {
        valA, err := op.executor.evaluateExpr(op.ctx, order.Expr, rowA)
        if err != nil { return 0, err }
        valB, err := op.executor.evaluateExpr(op.ctx, order.Expr, rowB)
        if err != nil { return 0, err }

        var cmp int
        if order.Collation != "" {
            // Use collation-aware comparison for strings
            cmp = op.compareWithCollation(valA, valB, order.Collation)
        } else {
            cmp = compareValues(valA, valB)
        }

        if cmp != 0 {
            if order.Desc { return -cmp, nil }
            return cmp, nil
        }
    }
    return 0, nil
}

func (op *PhysicalSortOperator) compareWithCollation(a, b any, collName string) int {
    sa, aOk := a.(string)
    sb, bOk := b.(string)
    if !aOk || !bOk {
        // Non-string values fall back to default comparison
        return compareValues(a, b)
    }

    coll, err := collation.DefaultRegistry.Get(collName)
    if err != nil {
        // Unknown collation falls back to default
        return compareValues(a, b)
    }
    return coll.Compare(sa, sb)
}
```

### COLLATE in CREATE TABLE Column Definitions

Column-level collation is stored in the parser AST and propagated to the catalog:

```go
// Parser: CREATE TABLE t (name VARCHAR COLLATE en_US)
// The Collation field on ColumnDef is set to "en_US"

// When ORDER BY does not specify COLLATE, the column's default collation
// from the table definition is used if present.
```

The binder checks if the ORDER BY expression references a column with a default collation and uses it when no explicit COLLATE is provided.

### Locale-Aware UPPER/LOWER Functions

The existing `UPPER()` and `LOWER()` functions use Go's `strings.ToUpper` and `strings.ToLower`, which are not locale-aware. The implementation adds locale-aware variants:

```go
import "golang.org/x/text/cases"

// Locale-aware upper: handles Turkish dotless-i, German sharp-s, etc.
func localeUpper(s string, locale string) string {
    tag, err := language.Parse(locale)
    if err != nil {
        return strings.ToUpper(s) // fallback
    }
    return cases.Upper(tag).String(s)
}

func localeLower(s string, locale string) string {
    tag, err := language.Parse(locale)
    if err != nil {
        return strings.ToLower(s) // fallback
    }
    return cases.Lower(tag).String(s)
}
```

When a column has a collation defined, `UPPER()` and `LOWER()` on that column use the locale from the collation. Without a collation, they use the default Go behavior.

### NOCASE and NOACCENT Modifier Implementation

**NOCASE** uses Unicode case folding (`cases.Fold()`) rather than simple `ToLower()`. Case folding is a Unicode-standard operation that maps characters to a common form for case-insensitive comparison. For example, the German sharp-s maps to "ss".

**NOACCENT** uses Unicode NFKD normalization to decompose accented characters into base character + combining marks, then strips the combining marks (Unicode category Mn). For example, "cafe" == "cafe" under NOACCENT.

These modifiers can be chained with locale collations: `COLLATE de_DE.NOCASE.NOACCENT` applies the German locale ordering after case folding and accent removal.

## Context

**Architecture Alignment**:
- Builds on existing `TypeModifier.Collation` field in `internal/storage/duckdb/`
- Extends existing `OrderByExpr` in `internal/parser/ast.go`
- Extends existing `BoundOrderBy` in `internal/binder/statements.go`
- Modifies existing `PhysicalSortOperator` in `internal/executor/physical_sort.go`
- Extends existing `PRAGMA collations` in `internal/executor/physical_maintenance.go`
- New `internal/collation/` package follows the project's modular package structure

**Pure Go Constraint**:
- Uses `golang.org/x/text/collate` which is a pure Go implementation of the Unicode Collation Algorithm (UCA)
- No ICU C library dependency
- Supports CLDR locale data for all major languages
- Already used by many pure Go projects for internationalization

**golang.org/x/text/collate Coverage**:
- Full Unicode Collation Algorithm Level 1-3 support
- Locale-specific tailoring for 200+ locales via CLDR
- Comparable to ICU for most common use cases
- Missing: ICU-specific extensions, some rare locale tailorings

## Goals / Non-Goals

**Goals**:
- Locale-aware string comparison in ORDER BY with COLLATE clause
- Case-insensitive collation (NOCASE) using Unicode case folding
- Accent-insensitive collation (NOACCENT) using Unicode normalization
- Chained collation modifiers (e.g., `de_DE.NOCASE.NOACCENT`)
- Column-level default collation in CREATE TABLE
- Locale-aware UPPER/LOWER functions
- Dynamic PRAGMA collations listing
- Pure Go implementation (no CGO)
- DuckDB-compatible collation syntax

**Non-Goals**:
- Full ICU extension compatibility (e.g., ICU break iterators, date formatting)
- Custom user-defined collation rules (CLDR tailoring files)
- Collation-aware indexing (hash/B-tree with collation key)
- Collation in WHERE clause comparisons (future work)
- Collation in GROUP BY (future work)
- LIKE/ILIKE with collation awareness (future work)

## Decisions

**Decision 1: golang.org/x/text/collate vs Custom Implementation**
- **Choice**: Use `golang.org/x/text/collate`
- **Rationale**: Production-quality UCA implementation, maintained by Go team, pure Go, covers 200+ locales
- **Alternative**: Custom sort key generation - too complex, error-prone for Unicode edge cases

**Decision 2: Collation Resolution Strategy**
- **Choice**: Lazy resolution with caching in registry
- **Rationale**: Most queries use a small set of collations; lazy creation avoids loading all CLDR data upfront
- **Alternative**: Eager loading of all collations at startup - wastes memory for unused locales

**Decision 3: NOCASE Implementation**
- **Choice**: Unicode case folding via `cases.Fold()`
- **Rationale**: Standard Unicode approach, handles edge cases (sharp-s, Turkish i)
- **Alternative**: Simple `strings.ToLower()` - incorrect for many languages

**Decision 4: Collation in Comparison vs Sort Key**
- **Choice**: Direct comparison via `Collator.Compare()` for sorting
- **Rationale**: Simpler implementation, avoids sort key materialization overhead
- **Alternative**: Pre-compute sort keys for each value - faster for repeated comparisons but uses more memory

## Risks / Trade-offs

**Risk 1: Performance of collation-aware sorting**
- **Mitigation**: Collators are cached in registry; `collate.Collator` is reused across comparisons
- **Trade-off**: ~2-5x slower than byte comparison for locale sorting; acceptable for correctness

**Risk 2: Incomplete locale coverage**
- **Mitigation**: `golang.org/x/text` covers all CLDR locales; return clear error for unrecognized locales
- **Trade-off**: Some ICU-specific locale tailorings may differ from DuckDB's ICU output

**Risk 3: Memory usage for cached collators**
- **Mitigation**: Each `collate.Collator` is ~10KB; registry only holds used collations
- **Trade-off**: Long-running servers may accumulate many collators; acceptable for typical use

**Risk 4: golang.org/x/text dependency size**
- **Mitigation**: Module is well-maintained, commonly used; CLDR tables are embedded but compress well
- **Trade-off**: Adds ~5MB to binary for CLDR data; acceptable for correctness

## Migration Plan

No migration needed. This is a new capability with no breaking changes.

1. **Phase 1**: Implement `internal/collation/` package (registry, built-in collators, locale collator)
2. **Phase 2**: Extend parser and AST for COLLATE syntax
3. **Phase 3**: Extend binder to propagate collation
4. **Phase 4**: Integrate collation into sort operator
5. **Phase 5**: Implement locale-aware UPPER/LOWER
6. **Phase 6**: Extend PRAGMA collations
7. **Phase 7**: Add column-level default collation in CREATE TABLE

All phases are backward compatible. Queries without COLLATE behave identically.

## Open Questions

1. **Collation in WHERE comparisons**: Should `WHERE name = 'Muller' COLLATE NOACCENT` match 'Mueller'? Deferred to future work.
2. **Collation in GROUP BY**: Should `GROUP BY name COLLATE NOCASE` merge 'Alice' and 'alice'? Deferred to future work.
3. **Default database collation**: Should there be a `SET default_collation = 'en_US'`? Possible future enhancement.
4. **Sort key caching**: For very large sorts, should sort keys be pre-computed and cached? Monitor performance first.
