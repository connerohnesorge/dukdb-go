## Implementation Details

### AST Changes (`internal/parser/ast.go`)

Add two new `SetOpType` values:

```go
const (
    SetOpNone SetOpType = iota
    SetOpUnion
    SetOpUnionAll
    SetOpIntersect
    SetOpIntersectAll
    SetOpExcept
    SetOpExceptAll
    SetOpUnionByName      // NEW
    SetOpUnionAllByName   // NEW
)
```

No new AST node needed — `SelectStmt.SetOp` already carries the set operation
type and `SelectStmt.Right` carries the right side.

### Parser Changes (`internal/parser/parser.go`)

After parsing `UNION` or `UNION ALL`, check for the `BY NAME` keyword pair:

```go
if p.isKeyword("BY") {
    p.advance() // consume BY
    p.expectKeyword("NAME")
    if isAll {
        setOp = SetOpUnionAllByName
    } else {
        setOp = SetOpUnionByName
    }
}
```

### Column Matching Algorithm (Binder)

Standard UNION matches columns by position. UNION BY NAME matches by name:

1. Collect column names from both sides (left and right SELECT)
2. Build the output column list as the **union** of all column names,
   preserving order: left columns first (in their order), then any
   right-only columns (in their order)
3. For each side, create a mapping: output column index → source column index
   (or -1 if the column doesn't exist on that side)
4. At execution time, pad missing columns with NULL

Example:
```sql
SELECT a, b FROM t1
UNION BY NAME
SELECT b, c FROM t2
```

Output columns: `[a, b, c]`
- Left mapping:  `a→0, b→1, c→-1` (c missing from left, padded with NULL)
- Right mapping: `a→-1, b→0, c→1` (a missing from right, padded with NULL)

### Type Resolution

When the same column name appears on both sides:
1. Types must be compatible (implicitly castable)
2. The output type is the common supertype (e.g., INTEGER + BIGINT → BIGINT)
3. If types are incompatible, return a binding error

For columns that appear on only one side:
1. Output type is the column's type from the side where it appears
2. NULL values for the missing side are typed as the output column type

### Planner/Executor Changes

The existing set operation execution (`PhysicalSetOperation` or equivalent)
is extended:

```go
type PhysicalSetOperation struct {
    Op       SetOpType
    Left     PhysicalPlan
    Right    PhysicalPlan
    // NEW for BY NAME:
    LeftMapping  []int // For each output column, index in left result (-1 = NULL)
    RightMapping []int // For each output column, index in right result (-1 = NULL)
    OutputTypes  []dukdb.Type
}
```

At execution time, for BY NAME operations:
1. Execute left and right plans
2. For each row from left: reorder columns using LeftMapping, pad NULLs
3. For each row from right: reorder columns using RightMapping, pad NULLs
4. Apply UNION (distinct) or UNION ALL (keep duplicates) semantics

### Edge Cases

| Case | Behavior |
|------|----------|
| No overlapping columns | All columns from both sides, all padded with NULLs on opposite side |
| Identical columns | Same as regular UNION (no reordering needed) |
| Case-insensitive matching | Column names matched case-insensitively |
| Duplicate column names in one side | Error: ambiguous column name |
| Chained UNION BY NAME | Left-to-right: (A UNION BY NAME B) UNION BY NAME C |

## Context

UNION BY NAME is a DuckDB extension to SQL that simplifies combining
heterogeneous datasets. It's particularly useful for combining CSV/Parquet
files with different column sets, or querying across schema-evolving tables.

## Goals / Non-Goals

**Goals:**
- `UNION BY NAME` with deduplication
- `UNION ALL BY NAME` without deduplication
- Column matching by name (case-insensitive)
- NULL padding for missing columns
- Correct type promotion for matching columns

**Non-Goals:**
- `INTERSECT BY NAME` / `EXCEPT BY NAME` — not supported by DuckDB
- Column renaming or aliasing in BY NAME context

## Decisions

- **Column order**: Left columns first, then right-only columns. This matches
  DuckDB behavior and gives predictable output ordering.

- **Case-insensitive matching**: Column names are matched case-insensitively,
  consistent with DuckDB's identifier handling.

- **Mapping arrays in plan**: Store the column mapping in the physical plan
  node rather than rewriting the child plans. This keeps the change localized
  to the set operation executor.
