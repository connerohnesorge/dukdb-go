## Implementation Details

### AST Changes (`internal/parser/ast.go`)

```go
// OnConflictAction specifies what to do when an INSERT conflicts.
type OnConflictAction int

const (
    OnConflictDoNothing OnConflictAction = iota
    OnConflictDoUpdate
)

// OnConflictClause represents the ON CONFLICT clause of an INSERT statement.
type OnConflictClause struct {
    // ConflictColumns lists the columns that define the conflict target.
    // Empty means "ON CONFLICT ON CONSTRAINT <pk>" (infer from PK).
    ConflictColumns []string

    // ConflictWhere is an optional WHERE filter that must be satisfied for a
    // row to be considered as conflicting. Used for matching against partial
    // unique indexes (future work). For example:
    //   ON CONFLICT (id) WHERE status = 'active'
    // only conflicts with rows where status='active'.
    ConflictWhere Expr

    // Action is DO NOTHING or DO UPDATE.
    Action OnConflictAction

    // UpdateSet is the list of SET assignments for DO UPDATE.
    // Each entry maps column name -> expression (may reference EXCLUDED.col).
    UpdateSet []SetClause

    // UpdateWhere is an optional WHERE filter on the DO UPDATE action.
    // Only rows matching this predicate are updated; others are skipped.
    UpdateWhere Expr
}
```

Extend `InsertStmt`:

```go
type InsertStmt struct {
    Schema     string
    Table      string
    Columns    []string
    Values     [][]Expr
    Select     *SelectStmt
    Returning  []SelectColumn
    OnConflict *OnConflictClause  // NEW — nil when not present
}
```

### Parser Grammar (`internal/parser/parser.go`)

After parsing the INSERT body (VALUES or SELECT) and before RETURNING, add:

```
insert_stmt
    : INSERT INTO table_ref ['(' column_list ')']
      (VALUES value_list | select_stmt)
      [on_conflict_clause]
      [RETURNING returning_columns]
    ;

on_conflict_clause
    : ON CONFLICT ['(' column_list ')'] [WHERE expr]
      conflict_action
    ;

conflict_action
    : DO NOTHING
    | DO UPDATE SET set_clause_list [WHERE expr]
    ;
```

The parser must:
1. Consume `ON CONFLICT` keywords
2. Optionally parse `(col1, col2, ...)` conflict target columns
3. Optionally parse `WHERE expr` for the conflict target (partial index filter)
4. Parse `DO NOTHING` or `DO UPDATE SET ...`
5. For `DO UPDATE`, parse the set clause list (reuse existing SET parsing)
6. Optionally parse `WHERE expr` for the update action filter

### Binder Changes (`internal/binder/`)

#### BoundOnConflictClause

```go
type BoundOnConflictClause struct {
    // ConflictColumnIndices are resolved column indices in the target table.
    ConflictColumnIndices []int

    // ConflictWhere is the bound conflict target filter (optional).
    ConflictWhere BoundExpr

    // Action is DO NOTHING or DO UPDATE.
    Action parser.OnConflictAction

    // UpdateSet are the bound SET assignments for DO UPDATE.
    UpdateSet []*BoundSetClause

    // UpdateWhere is the bound update action filter (optional).
    UpdateWhere BoundExpr
}
```

Extend `BoundInsertStmt`:

```go
type BoundInsertStmt struct {
    Schema     string
    Table      string
    TableDef   *catalog.TableDef
    Columns    []int
    Values     [][]BoundExpr
    Select     *BoundSelectStmt
    Returning  []*BoundSelectColumn
    OnConflict *BoundOnConflictClause  // NEW
}
```

#### EXCLUDED Pseudo-Table

During binding of the `DO UPDATE SET` expressions, introduce a virtual scope
named `EXCLUDED` that maps column references to the values from the
attempted-insert row. Implementation:

1. Create a `TableBinding` with name `"excluded"` pointing to the target
   table's schema but flagged as a pseudo-table.
2. When resolving `EXCLUDED.col` in SET expressions, bind it to a
   `BoundExcludedColumnRef` expression that captures the column index.
3. At execution time, `BoundExcludedColumnRef` evaluates to the
   corresponding value from the conflicting INSERT row.

```go
// BoundExcludedColumnRef references a column from the EXCLUDED pseudo-table.
// Implements BoundExpr interface (boundExprNode() and ResultType() methods).
// Must be added as a case in evaluateExpr() switch in executor/expr.go.
type BoundExcludedColumnRef struct {
    ColumnIndex int        // Index into the INSERT row values
    ColumnName  string     // For error messages
    DataType    dukdb.Type // Return type matching target table column type
}

func (*BoundExcludedColumnRef) boundExprNode() {}
func (e *BoundExcludedColumnRef) ResultType() dukdb.Type { return e.DataType }
```

#### Validation Rules

The binder SHALL enforce:
1. Conflict columns must reference columns covered by a UNIQUE index or the
   table's PRIMARY KEY.
2. If no conflict columns are specified, the table MUST have a PRIMARY KEY
   (infer conflict target from PK).
3. DO UPDATE SET columns cannot include the conflict target columns themselves.
   Both DuckDB and PostgreSQL forbid this (DuckDB issue #16698 documents a bug
   when this is attempted). The binder SHALL reject such assignments with a
   clear error message.
4. The EXCLUDED reference is only valid inside ON CONFLICT DO UPDATE SET
   expressions and the associated WHERE clause.
5. For composite keys, conflict target columns MUST include all columns of the
   target index/PK. Partial column matching (e.g., `ON CONFLICT (a)` when PK
   is `(a, b)`) is not allowed.
6. NULL values in UNIQUE index conflict columns are treated as non-matching
   per the SQL standard (NULL != NULL). Multiple rows with NULL in a UNIQUE
   column are allowed. PRIMARY KEY columns cannot be NULL (enforced separately).

#### DEFAULT Value Handling in DO UPDATE

For DO UPDATE SET, columns NOT mentioned in the SET clause:
- Keep their existing values (they are NOT updated)
- DEFAULT values are NOT applied during the update (unlike a fresh INSERT)
- This matches MERGE statement semantics and PostgreSQL behavior

#### RowsAffected Calculation

The RowsAffected count for an upsert operation:
- Each successfully inserted row: +1
- Each successfully updated row (DO UPDATE): +1
- Each skipped row (DO NOTHING): +0
- Each row skipped due to UpdateWhere filter failing: +0

### Planner Changes (`internal/planner/physical.go`)

Extend `PhysicalInsert` rather than creating a new node (simpler approach):

```go
type PhysicalInsert struct {
    Schema     string
    Table      string
    TableDef   *catalog.TableDef
    Columns    []int
    Values     [][]binder.BoundExpr
    Source     PhysicalPlan
    Returning  []*binder.BoundSelectColumn
    OnConflict *binder.BoundOnConflictClause  // NEW — nil for plain INSERT
}
```

When `OnConflict` is non-nil, the executor switches to the upsert execution
path.

### Executor Changes (`internal/executor/operator.go`)

#### Execution Algorithm

The upsert executor follows a **check-before-insert** strategy rather than
catch-exception, avoiding the overhead of Go error handling on every row:

```
for each row in INSERT values:
    1. Extract conflict key values from row
    2. Look up conflict key in primary key map (pkKeys) or unique index
    3. If NO conflict:
        a. Insert row normally (same as current executeInsert)
        b. Track row for RETURNING
    4. If CONFLICT found:
        a. If DO NOTHING:
            - Skip row entirely (no RETURNING output for skipped rows)
        b. If DO UPDATE:
            - Check UpdateWhere predicate against existing row (if present)
            - If UpdateWhere fails: skip row (treat as DO NOTHING)
            - Build EXCLUDED bindings from insert row values
            - Evaluate SET expressions with EXCLUDED scope
            - Apply UPDATE to the existing row (by RowID)
            - Track updated row for RETURNING
```

#### Batch Optimization

For bulk inserts, the conflict check uses the existing `pkKeys` map (already
built in `executeInsert`). For unique indexes beyond PK:

1. Build an in-memory hash set of unique index key values from the target
   table at the start of the upsert operation.
2. For each batch (DataChunk), check all rows against the hash set.
3. Partition rows into "insert" and "conflict" sets.
4. Bulk-insert the non-conflicting rows.
5. Process conflicting rows individually (UPDATE or skip).
6. Update the hash set with newly inserted keys.

This avoids per-row table lookups for the common case of mostly non-conflicting
inserts.

#### EXCLUDED Evaluation

```go
// excludedScope holds the values of the row that triggered the conflict.
type excludedScope struct {
    values []any  // Column values from the attempted INSERT row
}

// evaluateExcludedRef resolves a BoundExcludedColumnRef against the scope.
func (s *excludedScope) evaluateExcludedRef(ref *binder.BoundExcludedColumnRef) any {
    return s.values[ref.ColumnIndex]
}
```

The executor's expression evaluator must be extended to handle
`BoundExcludedColumnRef` by delegating to the `excludedScope`.

### WAL Integration

Upsert operations log as follows:
- Non-conflicting rows: standard INSERT WAL entries (existing behavior)
- DO NOTHING skipped rows: no WAL entry
- DO UPDATE rows: standard UPDATE WAL entries (column values + RowID)

This ensures crash recovery correctly replays the upsert outcome.

### Unique Index Lookup Helpers (`internal/storage/`)

Add helper functions to the storage layer:

```go
// LookupUniqueIndex checks if a key exists in a unique index and returns
// the RowID if found.
func (t *Table) LookupUniqueIndex(indexName string, keyValues []any) (rowID int64, found bool)

// GetUniqueIndexForColumns returns the unique index (or PK index) that
// covers exactly the given columns, or nil if none exists.
func (t *Table) GetUniqueIndexForColumns(columns []string) *HashIndex
```

#### Catalog-Storage Index Bridge

Currently indexes are stored in two places:
- `catalog.Schema.indexes` — metadata (IndexDef with name, columns, isUnique)
- `storage.HashIndex` — runtime data structure with actual key→RowID mapping

The executor needs both: catalog metadata to validate conflict columns, and
storage HashIndex to perform key lookups. The bridge works as follows:

1. During binder validation, use `catalog.GetIndexesForTable(tableName)` to
   find IndexDef entries matching the conflict columns.
2. Store the matched index name in `BoundOnConflictClause`.
3. At execution time, the executor retrieves the corresponding `HashIndex`
   from storage using the index name.
4. If no HashIndex exists yet (table has PK but no explicit index), fall back
   to the existing `pkKeys` hash map approach already used in `executeInsert`.

This avoids coupling catalog and storage while reusing existing infrastructure.

## Context

DuckDB's ON CONFLICT implementation follows PostgreSQL's INSERT ... ON
CONFLICT syntax (SQL:2016 MERGE is a separate feature, already implemented).
The key difference from MERGE is that ON CONFLICT is INSERT-centric: it
starts with an INSERT and handles conflicts, while MERGE starts with a
target table and matches rows from a source.

The duckdb-go reference driver tests at `references/duckdb-go/appender_test.go:1546`
show the expected upsert behavior:
```sql
INSERT INTO test SELECT * FROM my_append_tbl ON CONFLICT DO UPDATE SET u = EXCLUDED.u;
```

## Goals / Non-Goals

**Goals:**
- Full `INSERT ... ON CONFLICT DO NOTHING` support
- Full `INSERT ... ON CONFLICT DO UPDATE SET ... WHERE ...` support
- EXCLUDED pseudo-table for referencing incoming values
- Integration with PRIMARY KEY and UNIQUE INDEX constraints
- RETURNING clause works with upserted rows
- Batch-optimized conflict detection for bulk inserts

**Non-Goals:**
- ON CONFLICT ON CONSTRAINT <name> (named constraint reference) — future work
- Partial unique indexes as conflict targets — future work
- INSERT ... ON DUPLICATE KEY UPDATE (MySQL syntax) — not DuckDB compatible
- MERGE statement changes (already implemented separately)

## Decisions

- **Extend PhysicalInsert vs new PhysicalUpsert**: Extend PhysicalInsert with
  optional OnConflict field. Rationale: avoids duplicating the entire insert
  pipeline; the upsert is a behavioral modifier on INSERT, not a separate
  operation. The executor checks `OnConflict != nil` to switch paths.

- **Check-before-insert vs catch-error**: Use check-before-insert (proactive
  key lookup). Rationale: Go doesn't have exceptions; using error returns for
  control flow is expensive and not idiomatic. Proactive lookup also enables
  batch optimization.

- **EXCLUDED as BoundExcludedColumnRef**: Create a distinct expression type
  rather than reusing BoundColumnRef with a flag. Rationale: clearer
  semantics, no risk of accidentally treating EXCLUDED columns as regular
  table columns in other contexts.

- **Conflict column validation**: Initially require conflict columns to match
  an existing UNIQUE index or PRIMARY KEY exactly. Relaxation (partial matches,
  prefix matches) can be added later.

## Risks / Trade-offs

- **Performance on large upserts**: Building a hash set of all existing keys
  at the start could be memory-intensive for very large tables. Mitigation:
  for tables with >1M rows, consider streaming lookup against the index
  instead of pre-building a hash set.

- **Concurrent upserts**: Under SERIALIZABLE isolation, two concurrent upserts
  to the same key will conflict at commit time (existing conflict detection
  handles this). Under READ COMMITTED, the second upsert may see the first's
  committed values. This matches PostgreSQL behavior.

- **RETURNING semantics ambiguity**: For DO NOTHING, PostgreSQL returns
  nothing for skipped rows (no RETURNING output). DuckDB follows this same
  behavior. We match DuckDB/PostgreSQL: RETURNING only includes rows that
  were actually inserted or updated.

## Open Questions

- Should we support `ON CONFLICT ON CONSTRAINT <constraint_name>` in the
  initial implementation? Decision: No, defer to future work. Named
  constraints require a constraint catalog that doesn't exist yet.
- Should DO UPDATE SET allow modifying conflict target columns? Decision:
  Initially no (match PostgreSQL safety), can relax if DuckDB reference
  tests require it.
