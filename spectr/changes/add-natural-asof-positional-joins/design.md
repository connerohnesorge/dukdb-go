## Implementation Details

### AST Changes

Extend `JoinType` enum and `JoinClause` struct in `internal/parser/ast.go`:

```go
const (
    JoinTypeInner      JoinType = iota
    JoinTypeLeft
    JoinTypeRight
    JoinTypeFull
    JoinTypeCross
    JoinTypeNatural           // NATURAL JOIN (auto-match by column name)
    JoinTypeNaturalLeft       // NATURAL LEFT JOIN
    JoinTypeNaturalRight      // NATURAL RIGHT JOIN
    JoinTypeNaturalFull       // NATURAL FULL JOIN
    JoinTypeAsOf              // ASOF JOIN (nearest match for time-series)
    JoinTypeAsOfLeft          // ASOF LEFT JOIN
    JoinTypePositional        // POSITIONAL JOIN (match by row position)
)

type JoinClause struct {
    Type      JoinType
    Table     TableRef
    Condition Expr       // ON condition
    Using     []string   // USING (col1, col2, ...) - shared column names
}
```

### NATURAL JOIN Implementation

NATURAL JOIN is a syntactic sugar that automatically generates equi-join conditions for all columns with the same name in both tables.

**Binder transformation** (`internal/binder/bind_stmt.go`):

```go
// bindNaturalJoin transforms a NATURAL JOIN into an explicit equi-join
// by finding columns common to both tables.
func (b *Binder) bindNaturalJoin(
    leftCols []string,
    rightCols []string,
    joinType JoinType,
) (JoinType, Expr, error) {
    // 1. Find common column names (case-insensitive)
    common := intersectColumnNames(leftCols, rightCols)
    if len(common) == 0 {
        // NATURAL JOIN with no common columns = CROSS JOIN
        return JoinTypeCross, nil, nil
    }

    // 2. Build AND chain of equality conditions
    var condition Expr
    for _, col := range common {
        eq := &BinaryExpr{Op: "=",
            Left:  &ColumnRef{Column: col, TableIdx: leftTableIdx},
            Right: &ColumnRef{Column: col, TableIdx: rightTableIdx},
        }
        if condition == nil {
            condition = eq
        } else {
            condition = &BinaryExpr{Op: "AND", Left: condition, Right: eq}
        }
    }

    // 3. Map NATURAL variant to base join type
    baseType := map[JoinType]JoinType{
        JoinTypeNatural:      JoinTypeInner,
        JoinTypeNaturalLeft:  JoinTypeLeft,
        JoinTypeNaturalRight: JoinTypeRight,
        JoinTypeNaturalFull:  JoinTypeFull,
    }[joinType]

    return baseType, condition, nil
}
```

**Result columns**: NATURAL JOIN de-duplicates common columns. The result set contains each common column once (from the appropriate side based on join type), followed by remaining left columns, then remaining right columns.

### ASOF JOIN Implementation

ASOF JOIN finds the nearest match rather than exact match, critical for time-series data. DuckDB syntax:

```sql
SELECT * FROM trades t ASOF JOIN quotes q
    ON t.ticker = q.ticker AND t.timestamp >= q.timestamp
```

**Semantics**: For each row in the left table, find the row in the right table where:
1. All equality conditions match exactly
2. The inequality condition finds the "closest" match (greatest value <= left value for `>=`, smallest value >= left value for `<=`)

**ON clause decomposition algorithm**: The binder must decompose the ASOF JOIN's ON clause into equality conditions and exactly one inequality condition. The algorithm is:

1. Flatten the ON expression into a list of conjuncts (split on top-level AND). Top-level OR is **not** allowed and must produce a binder error.
2. Classify each conjunct:
   - If the operator is `=`, it is an **equality condition**. Add it to `equalityCols`.
   - If the operator is `>=`, `>`, `<=`, or `<`, it is an **inequality condition** candidate.
   - Any other expression (function calls, OR sub-expressions, etc.) produces a binder error.
3. Validate exactly **one** inequality condition exists. Zero or more than one is an error.
4. The inequality condition's left operand must reference the left table and the right operand must reference the right table (or vice versa, normalized so left table is on the left).

**Ordering requirements**: The ASOF JOIN executor requires both inputs to be sorted by the equality key columns followed by the inequality key column (ASC for `>=`/`>`, DESC for `<=`/`<`). The **planner** is responsible for inserting `PhysicalSort` operators below the `PhysicalAsOfJoin` node when the inputs are not already ordered. The planner must check whether the child operators already provide the required ordering (e.g., from an ORDER BY or index scan) and only add Sort nodes when needed.

**Executor** (`internal/executor/asof_join.go`):

```go
// PhysicalAsOfJoinExecutor implements ASOF JOIN using a sort-merge approach.
// 1. Sort both sides by equality key + inequality key
// 2. For each left row, binary search in the right side for the closest match
type PhysicalAsOfJoinExecutor struct {
    left         PhysicalOperator
    right        PhysicalOperator
    equalityCols []ColumnPair    // exact match columns
    inequalityCond InequalityCondition // the >= or <= condition
    isLeftJoin   bool            // true for ASOF LEFT JOIN
}

type InequalityCondition struct {
    LeftCol  int       // column index in left table
    RightCol int       // column index in right table
    Op       string    // ">=" or "<="
}
```

**Algorithm**:
1. Materialize right side, sorted by equality keys + inequality key
2. Group right rows by equality key into a sorted index
3. For each left row:
   a. Look up equality key group in right index
   b. Binary search within group for closest match to inequality value
   c. If found, emit combined row; if not found and LEFT JOIN, emit left + NULLs

### POSITIONAL JOIN Implementation

POSITIONAL JOIN matches rows by their ordinal position (row 1 with row 1, row 2 with row 2, etc.). If one side is shorter, the missing rows are padded with NULLs (like a FULL OUTER JOIN by position).

**Chunked execution semantics**: Position matching uses **global ordinal row numbers** across the entire table, not per-chunk offsets. The executor maintains a persistent position counter that increments across chunk boundaries. For example, if the left side produces chunks of 2048 rows each, left row 0 of chunk 2 has global position 4096 and matches right row at global position 4096. Both sides are consumed in lockstep: the executor reads from both left and right children simultaneously, advancing each independently, and pairs rows by their cumulative position.

**Executor** (`internal/executor/positional_join.go`):

```go
// PhysicalPositionalJoinExecutor implements POSITIONAL JOIN.
// It reads from both sides simultaneously, matching rows by position.
type PhysicalPositionalJoinExecutor struct {
    left  PhysicalOperator
    right PhysicalOperator
}

// Next returns the next combined row.
// If left is exhausted, left columns are NULL.
// If right is exhausted, right columns are NULL.
// If both exhausted, returns io.EOF.
func (e *PhysicalPositionalJoinExecutor) Next() (Row, error) {
    leftRow, leftErr := e.left.Next()
    rightRow, rightErr := e.right.Next()

    if leftErr == io.EOF && rightErr == io.EOF {
        return nil, io.EOF
    }

    return combineRows(leftRow, rightRow), nil
}
```

### USING Clause

The `USING` clause is a shorthand for equi-joins on named columns:

```sql
SELECT * FROM a JOIN b USING (id, name)
-- equivalent to: JOIN b ON a.id = b.id AND a.name = b.name
```

The binder transforms USING into explicit ON conditions, similar to NATURAL JOIN but with explicitly listed columns.

### Parser Changes

```go
// parseJoinClause parses JOIN syntax including new types
func (p *Parser) parseJoinClause() (*JoinClause, error) {
    // Check for NATURAL prefix
    if p.matchKeyword("NATURAL") {
        // NATURAL [LEFT|RIGHT|FULL] JOIN
        joinType := p.parseJoinDirection() // returns Inner/Left/Right/Full
        // Map to Natural variant
    }

    // Check for ASOF prefix
    if p.matchKeyword("ASOF") {
        // ASOF [LEFT] JOIN
    }

    // Check for POSITIONAL
    if p.matchKeyword("POSITIONAL") {
        // POSITIONAL JOIN (no ON clause)
    }

    // Parse table reference
    table := p.parseTableRef()

    // Parse ON or USING (mutually exclusive)
    if p.matchKeyword("ON") {
        condition := p.parseExpr()
    } else if p.matchKeyword("USING") {
        columns := p.parseColumnList() // (col1, col2, ...)
    }
}
```

**ON/USING mutual exclusion**: The parser must enforce that a join clause contains at most one of ON or USING. If both are present, the parser returns an error. After parsing, the binder must also validate that `JoinClause.Condition` and `JoinClause.Using` are not both set (defensive check). POSITIONAL JOIN must have neither ON nor USING; the parser returns an error if either is present. NATURAL JOIN must have neither ON nor USING; specifying either is an error since conditions are auto-generated.

### Planner JoinType Mapping

The parser defines `JoinType` values including NATURAL variants (`JoinTypeNatural`, `JoinTypeNaturalLeft`, etc.). These are **parser-level** types only. The planner uses its own `JoinType` enum that does **not** include NATURAL variants, because the binder rewrites NATURAL joins into standard join types (Inner, Left, Right, Full) with explicit conditions before the planner sees them.

The mapping flow is:
1. **Parser** produces AST with `JoinTypeNatural`, `JoinTypeNaturalLeft`, etc.
2. **Binder** rewrites NATURAL to base type + generated conditions. The binder output uses only `JoinTypeInner`, `JoinTypeLeft`, `JoinTypeRight`, `JoinTypeFull`, `JoinTypeCross`, `JoinTypeAsOf`, `JoinTypeAsOfLeft`, `JoinTypePositional`.
3. **Planner** maps binder join types 1:1 to planner/physical join types. ASOF and POSITIONAL get dedicated physical operators; standard types use existing hash join or nested loop join operators.

### Column Deduplication for NATURAL and USING Joins

NATURAL JOIN and USING clause joins require column deduplication: common columns appear exactly once in the output. This is implemented as a **projection** inserted by the binder/planner after the join operator.

Implementation details:
1. The binder records the list of common (deduplicated) column names during NATURAL/USING binding.
2. The planner wraps the join's physical operator with a `PhysicalProjection` that:
   - Emits each common column once, sourced from the appropriate side (left for INNER/LEFT, COALESCE(left, right) for FULL).
   - Emits remaining left columns (excluding common columns already emitted).
   - Emits remaining right columns (excluding common columns already emitted).
3. For `SELECT *`, the projection defines the column order: common columns first, then remaining left, then remaining right. For explicit column references (e.g., `SELECT id`), the unqualified reference to a common column resolves to the deduplicated column.

This approach keeps the join operators themselves unchanged; deduplication is purely a projection concern.

```go
// Example: deduplication projection for NATURAL JOIN
// Tables: a(id, name, x), b(id, name, y)
// Common columns: [id, name]
// Projection output: [a.id, a.name, a.x, b.y]
```

## Context

- Current `JoinType` enum has 5 values: Inner, Left, Right, Full, Cross
- `JoinClause` has `Type`, `Table`, and `Condition` fields — needs `Using` field added
- The executor already has hash join and nested loop join implementations
- ASOF JOIN is unique to DuckDB (not standard SQL) — critical for time-series analytics

## Goals / Non-Goals

**Goals:**
- Full NATURAL JOIN support (NATURAL, NATURAL LEFT, NATURAL RIGHT, NATURAL FULL)
- ASOF JOIN and ASOF LEFT JOIN with equality + inequality conditions
- POSITIONAL JOIN
- USING clause for all standard join types
- Column deduplication for NATURAL and USING joins

**Non-Goals:**
- ASOF RIGHT JOIN (DuckDB doesn't support this)
- LATERAL ASOF JOIN (complex, deferred)
- SEMI/ANTI JOIN syntax (already handled differently via IN/EXISTS)

## Decisions

- **NATURAL JOIN as binder rewrite**: Transform to explicit equi-join in the binder rather than adding new executor operators. This reuses existing hash join infrastructure.
- **ASOF JOIN as dedicated executor**: Requires a specialized sort-merge algorithm that doesn't fit existing join operators.
- **POSITIONAL JOIN as dedicated executor**: Simple zip-merge, separate operator for clarity.
- **USING stored on JoinClause**: Rather than creating a separate AST node, add `Using []string` field to existing `JoinClause`.

## Risks / Trade-offs

- **ASOF JOIN performance**: Binary search approach is O(n log m) per equality group. For large datasets, consider hash partitioning by equality key.
  → Mitigation: Start with sort-merge, optimize later based on benchmarks.
- **NATURAL JOIN ambiguity**: Column name matching is case-insensitive, which could cause unexpected matches.
  → Mitigation: Document behavior, match DuckDB's case-insensitive semantics.

## Open Questions

- None — all previously open questions have been resolved and documented above (ASOF ON clause decomposition, ordering requirements, POSITIONAL chunked semantics, ON/USING mutual exclusion, column deduplication integration, planner type mapping, NATURAL type mismatch handling).
