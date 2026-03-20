## Implementation Details

### Constraint Types

```go
// ConstraintType identifies the kind of table constraint.
type ConstraintType int

const (
    ConstraintUnique     ConstraintType = iota
    ConstraintCheck
    ConstraintForeignKey
)

// ConstraintDef is the base for all constraint definitions.
type ConstraintDef struct {
    Name string         // Optional constraint name (CONSTRAINT name ...)
    Type ConstraintType
}

// UniqueConstraintDef represents a UNIQUE constraint on one or more columns.
type UniqueConstraintDef struct {
    ConstraintDef
    Columns []string // Column names in the unique constraint
}

// CheckConstraintDef represents a CHECK constraint with a boolean expression.
type CheckConstraintDef struct {
    ConstraintDef
    Expression string // Original SQL text of the CHECK expression
    // BoundExpr is populated during binding for runtime evaluation.
}

// ForeignKeyAction specifies the action on DELETE or UPDATE of referenced row.
// DuckDB v1.4.3 only supports NO ACTION and RESTRICT.
// CASCADE, SET NULL, and SET DEFAULT are explicitly rejected by DuckDB's parser.
type ForeignKeyAction int

const (
    FKActionNoAction  ForeignKeyAction = iota // Default
    FKActionRestrict
)

// ForeignKeyConstraintDef represents a FOREIGN KEY referential constraint.
type ForeignKeyConstraintDef struct {
    ConstraintDef
    Columns          []string         // Local column names
    RefTable         string           // Referenced table name
    RefSchema        string           // Referenced table schema (default "main")
    RefColumns       []string         // Referenced column names
    OnDelete         ForeignKeyAction // Action on DELETE of referenced row
    OnUpdate         ForeignKeyAction // Action on UPDATE of referenced row
}
```

### Catalog Storage (`internal/catalog/table.go`)

Add constraints to TableDef:

```go
type TableDef struct {
    Name        string
    Schema      string
    Columns     []*ColumnDef
    PrimaryKey  []int
    Constraints []any // UniqueConstraintDef, CheckConstraintDef, ForeignKeyConstraintDef
    columnIndex map[string]int
    Statistics  *optimizer.TableStatistics
}
```

Using `[]any` with type assertions keeps the change minimal. Each constraint
is stored as its concrete type and retrieved via type switch.

### Parser Changes (`internal/parser/ast.go`)

Extend `CreateTableStmt` to include constraints:

```go
type CreateTableStmt struct {
    Schema      string
    Table       string
    IfNotExists bool
    Columns     []ColumnDef
    PrimaryKey  []string
    Constraints []TableConstraint // NEW
    AsSelect    *SelectStmt
}

// TableConstraint represents a table-level constraint in CREATE TABLE.
type TableConstraint struct {
    Name       string // Optional CONSTRAINT name
    Type       string // "UNIQUE", "CHECK", "FOREIGN KEY"
    Columns    []string
    Expression Expr   // For CHECK constraints
    RefTable   string // For FK: referenced table
    RefSchema  string // For FK: referenced schema
    RefColumns []string // For FK: referenced columns
    OnDelete   string // For FK: "CASCADE", "SET NULL", etc.
    OnUpdate   string // For FK: "CASCADE", "SET NULL", etc.
}
```

Column-level constraints are also supported:
```sql
CREATE TABLE t (
    id INTEGER PRIMARY KEY,
    email VARCHAR UNIQUE,               -- column-level UNIQUE
    age INTEGER CHECK (age >= 0),        -- column-level CHECK
    dept_id INTEGER REFERENCES dept(id)  -- column-level FK
);
```

Column-level constraints are normalized to table-level constraints during
parsing for uniform handling.

The existing `ColumnDefClause` (ast.go) must be extended to store parsed
column-level constraints before normalization:

```go
type ColumnDefClause struct {
    Name       string
    DataType   dukdb.Type
    TypeInfo   dukdb.TypeInfo
    NotNull    bool
    Default    Expr
    PrimaryKey bool
    Unique     bool              // NEW: column-level UNIQUE
    Check      Expr              // NEW: column-level CHECK expression
    References *ColumnReference  // NEW: column-level REFERENCES
}

// ColumnReference stores parsed column-level FK reference.
type ColumnReference struct {
    Table    string
    Schema   string
    Column   string
    OnDelete string
    OnUpdate string
}
```

During CREATE TABLE binding, column-level constraints are converted to
`TableConstraint` entries for uniform storage in the catalog.

### Parser Grammar

```
table_constraint
    : [CONSTRAINT name] UNIQUE '(' column_list ')'
    | [CONSTRAINT name] CHECK '(' expr ')'
    | [CONSTRAINT name] FOREIGN KEY '(' column_list ')'
      REFERENCES table_name ['(' column_list ')']
      [ON DELETE action] [ON UPDATE action]
    ;

column_constraint
    : UNIQUE
    | CHECK '(' expr ')'
    | REFERENCES table_name ['(' column_name ')']
      [ON DELETE action] [ON UPDATE action]
    ;

action
    : RESTRICT | NO ACTION
    ;
    -- CASCADE, SET NULL, SET DEFAULT are rejected at parse time
    -- (matching DuckDB v1.4.3 behavior)
```

### Enforcement Points

#### UNIQUE Enforcement

On INSERT and UPDATE, before committing the row:
1. For each UNIQUE constraint, extract the constrained column values
2. Build a hash key from the values (reuse `primaryKeyKey()` infrastructure)
3. Check against an in-memory hash set (built at operation start, like `pkKeys`)
4. If duplicate found: return constraint violation error
5. NULL values in UNIQUE columns are NOT considered duplicates (SQL standard)

Implementation reuses the existing PK enforcement in `executeInsert`
(operator.go:1718-1748). The `checkPrimaryKey` function is generalized to
`checkUniqueConstraint` that works for both PK and UNIQUE constraints.

#### CHECK Enforcement

On INSERT and UPDATE, for each row:
1. Parse the CHECK expression during table creation and store as SQL text
2. At enforcement time, re-parse the CHECK SQL text into an AST expression
3. Bind the expression using a synthetic table context: create a temporary
   scope with the table's column names and types so column references (e.g.,
   `age` in `CHECK (age >= 0)`) resolve correctly
4. Evaluate the bound expression with the current row values
5. If expression evaluates to FALSE: return constraint violation error
6. If expression evaluates to NULL: the CHECK passes (SQL standard)

CHECK binding validation at CREATE TABLE time:
- Expression must only reference columns in the same table
- Expression must type-check as boolean
- No subqueries or aggregate functions allowed
- Column references that don't exist in the table produce an error

```go
func (e *Executor) checkConstraints(
    tableDef *catalog.TableDef,
    row []any,
    columnNames []string,
) error {
    for _, c := range tableDef.Constraints {
        switch constraint := c.(type) {
        case *catalog.CheckConstraintDef:
            result := e.evaluateCheckExpr(constraint, row, columnNames)
            if result == false { // NOT NULL false
                return constraintErrorf("CHECK constraint %q violated", constraint.Name)
            }
        case *catalog.UniqueConstraintDef:
            // handled separately via hash set
        }
    }
    return nil
}
```

#### FOREIGN KEY Enforcement

**On INSERT/UPDATE of child table** (table with FK):
1. Extract FK column values from the new row
2. Look up the referenced table
3. Check that a matching row exists in the referenced table/columns
4. If no match: return FK violation error
5. NULL FK values skip the check (NULL reference is allowed)

**On DELETE of parent table** (referenced table):
1. For each FK that references this table, check if any child rows reference
   the deleted row
2. NO ACTION/RESTRICT: error if child rows exist (only supported actions)

**On UPDATE of parent table** (referenced table, PK columns changed):
1. Same logic as DELETE but for updated PK values
2. NO ACTION/RESTRICT: error if child rows reference the old value

Note: DuckDB v1.4.3 explicitly rejects CASCADE, SET NULL, and SET DEFAULT
actions with: "FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET
DEFAULT". The parser SHALL reject these actions at parse time.

FK lookup uses the referenced table's PK or UNIQUE index for efficient
checking.

### Constraint Validation During CREATE TABLE

The binder validates constraints at table creation time:
1. UNIQUE: referenced columns must exist in the table
2. CHECK: expression must be a valid boolean expression referencing only
   table columns (no subqueries, no aggregates)
3. FOREIGN KEY: referenced table must exist, referenced columns must form
   a PRIMARY KEY or UNIQUE constraint on the referenced table, column types
   must be compatible

### Interaction with Existing PK Enforcement

The existing PK enforcement in `executeInsert` (the `checkPrimaryKey` closure)
is refactored into the general constraint checking system:
- PK → treated as `UniqueConstraintDef` with `IsPrimary=true` flag
- PK also implies NOT NULL (already enforced)
- The `pkKeys` hash map is generalized to support multiple unique constraints

## Context

DuckDB treats constraints as metadata-only in some modes (e.g., FOREIGN KEY
is parsed but not enforced by default for performance). However, dukdb-go
should enforce constraints by default to ensure data integrity, matching
PostgreSQL behavior. A future `SET check_constraints = false` pragma could
disable enforcement for bulk loading.

## Goals / Non-Goals

**Goals:**
- UNIQUE constraint parsing and enforcement on INSERT/UPDATE
- CHECK constraint parsing and enforcement on INSERT/UPDATE
- FOREIGN KEY constraint parsing with ON DELETE/UPDATE actions
- FK enforcement on INSERT (child), DELETE/UPDATE (parent)
- Named constraints (`CONSTRAINT name ...`)
- Both column-level and table-level constraint syntax

**Non-Goals:**
- ALTER TABLE ADD/DROP CONSTRAINT — future work
- Deferred constraint checking (INITIALLY DEFERRED) — future work
- EXCLUDE constraints — not supported by DuckDB
- Partial UNIQUE constraints — future work
- Constraint-based query optimization — future work

## Decisions

- **Enforce by default**: Unlike DuckDB which may skip FK enforcement for
  performance, we enforce all constraints. This matches user expectations
  and prevents data integrity issues.

- **Generalize PK enforcement**: Rather than keeping PK enforcement separate,
  integrate it into the constraint system. PK becomes a UNIQUE NOT NULL
  constraint with the `IsPrimary` flag.

- **Store CHECK as SQL text**: Store the CHECK expression as SQL text in the
  catalog (for serialization and DDL generation). Re-parse and bind at
  enforcement time. This avoids storing bound AST in the catalog.

## Risks / Trade-offs

- **Performance on INSERT/UPDATE**: Each constraint adds overhead. UNIQUE
  requires hash lookup, CHECK requires expression evaluation, FK requires
  cross-table lookup. Mitigation: these are typically O(1) operations per row.

- **FK on self-referencing tables**: A table can reference itself (e.g.,
  employee.manager_id REFERENCES employee(id)). Since only NO ACTION/RESTRICT
  are supported, there is no cascading complexity. The enforcement simply
  checks if the referenced row exists before INSERT, and checks for child
  references before DELETE.
