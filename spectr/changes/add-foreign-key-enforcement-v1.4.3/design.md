## Implementation Details

### 1. Parser Changes

#### AST Additions (`internal/parser/ast.go`)

```go
// ForeignKeyAction represents the referential action for ON DELETE/UPDATE.
type ForeignKeyAction int

const (
    FKActionNoAction  ForeignKeyAction = iota // Default
    FKActionRestrict
    FKActionCascade   // Parsed but rejected
    FKActionSetNull   // Parsed but rejected
    FKActionSetDefault // Parsed but rejected
)

// ForeignKeyRef represents a column-level REFERENCES clause.
type ForeignKeyRef struct {
    RefTable   string
    RefColumns []string
    OnDelete   ForeignKeyAction
    OnUpdate   ForeignKeyAction
}
```

Add to `ColumnDefClause`:

```go
type ColumnDefClause struct {
    // ... existing fields ...
    ForeignKey *ForeignKeyRef // Column-level REFERENCES clause
}
```

Extend `TableConstraint.Type` to accept `"FOREIGN_KEY"` and add fields:

```go
type TableConstraint struct {
    Name       string
    Type       string   // "UNIQUE", "CHECK", "FOREIGN_KEY"
    Columns    []string // Child columns (for UNIQUE or FK)
    Expression Expr     // For CHECK
    RefTable   string   // For FOREIGN_KEY: referenced table
    RefColumns []string // For FOREIGN_KEY: referenced columns
    OnDelete   ForeignKeyAction
    OnUpdate   ForeignKeyAction
}
```

#### Parser Logic (`internal/parser/parser.go`)

**Column-level REFERENCES**: In `parseColumnDef`, after the existing constraint loop's `case` branches (NOT, NULL, PRIMARY KEY, DEFAULT, UNIQUE, CHECK, COLLATE — note that NOT NULL and bare NULL are separate cases), add:

```go
case p.isKeyword("REFERENCES"):
    p.advance()
    ref, err := p.parseForeignKeyRef()
    if err != nil {
        return col, err
    }
    col.ForeignKey = ref
```

**Table-level FOREIGN KEY**: In `parseTableConstraint`, before the final error return, add:

```go
if p.isKeyword("FOREIGN") {
    p.advance()
    if err := p.expectKeyword("KEY"); err != nil {
        return tc, err
    }
    tc.Type = "FOREIGN_KEY"
    // Parse (child_col1, child_col2, ...)
    // Parse REFERENCES parent_table(parent_col1, parent_col2, ...)
    // Parse optional ON DELETE/UPDATE actions
    return tc, nil
}
```

**Shared helper** `parseForeignKeyRef`:

```go
func (p *parser) parseForeignKeyRef() (*ForeignKeyRef, error) {
    // Parse table name
    // Parse (column list)
    // Parse optional ON DELETE action
    // Parse optional ON UPDATE action
    // Reject CASCADE, SET NULL, SET DEFAULT with error:
    //   "FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT"
    // Default to FKActionNoAction for both
}
```

**In `parseCreateTable`**: Add `p.isKeyword("FOREIGN")` to the condition at line 2053 that dispatches to `parseTableConstraint`. Column-level FKs are converted to `TableConstraint` entries with type `"FOREIGN_KEY"`.

### 2. Catalog Changes

#### New Struct (`internal/catalog/constraint.go`)

```go
// ForeignKeyConstraintDef represents a FOREIGN KEY constraint.
type ForeignKeyConstraintDef struct {
    Name       string           // Optional constraint name
    Columns    []string         // Child table column names
    RefTable   string           // Referenced parent table name
    RefColumns []string         // Referenced parent column names
    OnDelete   ForeignKeyAction // Referential action on parent DELETE
    OnUpdate   ForeignKeyAction // Referential action on parent UPDATE
}

// ForeignKeyAction mirrors parser.ForeignKeyAction for catalog storage.
type ForeignKeyAction int

const (
    FKActionNoAction ForeignKeyAction = iota
    FKActionRestrict
)

func (f *ForeignKeyConstraintDef) Clone() *ForeignKeyConstraintDef {
    // Deep copy all slice fields
}
```

Update `TableDef.Clone()` to handle `*ForeignKeyConstraintDef` in the constraint switch.

### 3. Executor Changes

#### CREATE TABLE Validation (`internal/executor/operator.go:executeCreateTable`, ~line 2573)

In the CREATE TABLE execution path (note: this is in `operator.go`, not `ddl.go`), after building the TableDef and storing constraints (around line 2604):

1. For each `ForeignKeyConstraintDef` in the constraints, resolve the referenced table from the catalog
2. Verify the referenced table exists (error: "referenced table does not exist")
3. Verify the referenced columns exist in the parent table
4. Verify the referenced columns form a PRIMARY KEY or have a UNIQUE constraint on the parent table
5. Verify column count and types are compatible between child FK columns and parent key columns

#### INSERT Enforcement (`internal/executor/operator.go:executeInsert`)

After the existing `checkPrimaryKey` function and before actually appending rows, add FK validation:

```go
checkForeignKeys := func(values []any) error {
    if plan.TableDef == nil {
        return nil
    }
    for _, c := range plan.TableDef.Constraints {
        fk, ok := c.(*catalog.ForeignKeyConstraintDef)
        if !ok {
            continue
        }
        // For each FK constraint:
        // 1. Extract child column values from the row
        // 2. If all FK column values are NULL, skip (NULLs are allowed)
        // 3. Look up the parent table in storage
        // 4. Scan the parent table for a matching row on the referenced columns
        // 5. If no match found, return FK violation error
    }
    return nil
}
```

Call `checkForeignKeys(values)` for each row being inserted, after PK check passes.

#### UPDATE Enforcement (`internal/executor/physical_update.go:executeUpdate`)

Two-sided check:

**Child table update** (FK columns being modified):
- After computing new values for the row, for each FK constraint on this table, check that the new FK column values exist in the parent table

**Parent table update** (PK/unique columns being modified):
- Before updating, check if any child table has FK constraints referencing this table
- For each such FK, scan the child table for rows referencing the old key values
- If any child rows exist, reject the update (NO ACTION/RESTRICT behavior)

To find child tables referencing a parent: iterate all tables in the catalog, check their `ForeignKeyConstraintDef` constraints for `RefTable` matching the current table name.

#### DELETE Enforcement (`internal/executor/physical_delete.go:executeDelete`)

Before marking rows as deleted:

1. For each row being deleted, extract the key column values (PK or unique columns)
2. Find all child tables with FK constraints referencing this table
3. For each child table, scan for rows whose FK column values match the deleted key values
4. If any child rows exist, return FK violation error (NO ACTION/RESTRICT)

#### FK Lookup Helper

Create a shared helper in `internal/executor/fk_check.go`:

```go
// checkParentKeyExists verifies that the given values exist as a row
// in the parent table's referenced columns. Returns nil if found or
// if all values are NULL.
func (e *Executor) checkParentKeyExists(
    parentTableName string,
    refColumns []string,
    values []any,
) error

// checkNoChildReferences verifies that no child table rows reference
// the given parent key values. Returns nil if no references found.
func (e *Executor) checkNoChildReferences(
    parentTableName string,
    parentKeyColumns []string,
    keyValues []any,
) error

// findChildForeignKeys returns all FK constraints across all tables
// that reference the given table name.
func (e *Executor) findChildForeignKeys(
    parentTableName string,
) []*childFKInfo
```

### 4. Error Messages

FK violation errors use:

```go
&dukdb.Error{
    Type: dukdb.ErrorTypeConstraint,
    Msg:  fmt.Sprintf("foreign key violation: key (%s)=(%s) is not present in table \"%s\"", columns, values, parentTable),
}
```

For parent deletion/update:

```go
&dukdb.Error{
    Type: dukdb.ErrorTypeConstraint,
    Msg:  fmt.Sprintf("foreign key violation: key (%s)=(%s) is still referenced from table \"%s\"", columns, values, childTable),
}
```

### 5. Performance Considerations

For the initial implementation, FK lookups use sequential scans of the parent/child tables. This is correct but not optimal for large tables. Future work can add:
- Index-based FK lookups (use existing hash indexes on PK columns)
- FK constraint caching per transaction
- Deferred constraint checking

## Context

DuckDB v1.4.3 supports FOREIGN KEY constraints with full enforcement. The dukdb-go engine currently has spec requirements for FK enforcement (`spectr/specs/execution-engine/spec.md:2616`, `spectr/specs/parser/spec.md:890`, `spectr/specs/catalog/spec.md:276`) but zero implementation. This change bridges that gap for NO ACTION and RESTRICT actions.

## Goals / Non-Goals

- **Goals**: Parse FK syntax, store FK metadata in catalog, enforce FK on INSERT/UPDATE/DELETE for NO ACTION and RESTRICT
- **Non-Goals**: CASCADE, SET NULL, SET DEFAULT actions (rejected at parse time); deferred constraint checking; cross-schema FK; index-based FK lookup optimization

## Decisions

- **Action scope**: Only NO ACTION and RESTRICT are supported. CASCADE/SET NULL/SET DEFAULT are rejected at parse time with a clear error. This matches the incremental approach and avoids complex cascading logic.
- **Lookup strategy**: Sequential scan for FK validation. Correct-first, optimize-later.
- **FK action type location**: Define `ForeignKeyAction` in both parser (AST) and catalog packages to avoid circular imports. Note: a `ForeignKeyAction` type already exists in `internal/storage/duckdb/catalog.go` (used for DuckDB file format deserialization); the new types in parser/catalog are intentionally separate to avoid coupling to the storage format layer. Could be extracted to a shared types package later.
- **NULL handling**: NULL FK column values always pass validation (standard SQL behavior: NULL does not match any parent key).

## Risks / Trade-offs

- **Performance**: Sequential scan for FK validation is O(n) per FK check per row. For bulk inserts into tables with FK constraints, this could be slow. Mitigation: document as known limitation, plan index-based lookups as follow-up.
- **Catalog iteration**: Finding child FK constraints requires iterating all tables. Mitigation: acceptable for initial implementation; can add reverse-lookup map later.

## Open Questions

- Should FK constraints be validated when loading data via the Appender (bulk load) interface? For now, skip Appender enforcement (DuckDB itself defers some constraint checks during bulk load).
