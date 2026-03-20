# SIMILAR TO Operator and CREATE TYPE AS ENUM DDL - Design Details

## Implementation Details

### SIMILAR TO AST Node

SIMILAR TO reuses the existing BinaryExpr node with new operator constants. This follows the same pattern as LIKE/ILIKE.

```go
// In internal/parser/ast.go, add to BinaryOp constants:
const (
    // ... existing operators ...

    // String operators (additions)
    OpSimilarTo    // SIMILAR TO (SQL standard regex)
    OpNotSimilarTo // NOT SIMILAR TO
)
```

The parser will produce:

```go
// SELECT name FROM t WHERE name SIMILAR TO '%(John|Jane)%'
// Parses to:
&BinaryExpr{
    Left:  &ColumnRef{Table: "", Column: "name"},
    Op:    OpSimilarTo,
    Right: &Literal{Value: "%(John|Jane)%"},
}
```

**NOT SIMILAR TO parsing strategy**: NOT SIMILAR TO requires multi-token lookahead. The parser's `peek()` only looks one token ahead, but NOT SIMILAR TO needs two tokens (SIMILAR then TO) after NOT. The correct approach: when `current=NOT` and `peek()=SIMILAR`, advance past NOT, advance past SIMILAR, then expect TO. This follows the same pattern as NOT LIKE/NOT ILIKE (which check `peek()` for a single keyword), but extended by one more advance and keyword check. Specifically:

```go
// NOT SIMILAR TO
if p.isKeyword("NOT") &&
    p.peek().typ == tokenIdent &&
    strings.EqualFold(p.peek().value, "SIMILAR") {
    p.advance() // consume NOT
    p.advance() // consume SIMILAR
    if !p.isKeyword("TO") {
        return nil, p.expectedError("TO")
    }
    p.advance() // consume TO
    // ... parse pattern expression ...
}
```

For ESCAPE clause support, a new SimilarToExpr node is needed since BinaryExpr cannot hold three operands:

```go
// SimilarToExpr represents a SIMILAR TO expression with optional ESCAPE clause.
// When Escape is empty, the default escape character '\' is used.
type SimilarToExpr struct {
    Expr   Expr   // Left-hand expression
    Pattern Expr  // The SQL regex pattern
    Escape string // Escape character (empty means default '\')
    Not    bool   // true for NOT SIMILAR TO
}

func (e *SimilarToExpr) exprNode() {}
```

### SQL Regex to Go Regexp Conversion

SQL SIMILAR TO uses a regex dialect that differs from Go's regexp syntax. The conversion function translates SQL regex to Go regexp:

```go
// sqlRegexToGoRegex converts a SQL SIMILAR TO pattern to a Go regexp pattern.
// SQL regex rules:
//   _ matches any single character       -> .
//   % matches zero or more characters    -> .*
//   | alternation                        -> | (same)
//   [...] character class                -> [...] (same)
//   ( ) grouping                         -> ( ) (same)
//   escape char (default \) escapes the next character
//
// All other regex metacharacters in the input are escaped for Go regexp.
func sqlRegexToGoRegex(pattern string, escape rune) (string, error) {
    var buf strings.Builder
    buf.WriteString("^") // SIMILAR TO matches the entire string

    runes := []rune(pattern)
    for i := 0; i < len(runes); i++ {
        ch := runes[i]

        // Handle escape character
        if ch == escape {
            i++
            if i >= len(runes) {
                return "", fmt.Errorf("SIMILAR TO pattern ends with escape character")
            }
            // Escape the next character for Go regexp
            buf.WriteString(regexp.QuoteMeta(string(runes[i])))
            continue
        }

        switch ch {
        case '%':
            buf.WriteString(".*")
        case '_':
            buf.WriteString(".")
        case '|':
            buf.WriteRune('|')
        case '(':
            buf.WriteRune('(')
        case ')':
            buf.WriteRune(')')
        case '[':
            // Character class: pass through until closing ]
            // SQL uses [!...] for negation; Go regexp uses [^...]
            buf.WriteRune('[')
            i++
            if i < len(runes) && runes[i] == '!' {
                // Convert SQL negation syntax [!abc] to Go regexp [^abc]
                buf.WriteRune('^')
                i++
            }
            for i < len(runes) && runes[i] != ']' {
                buf.WriteRune(runes[i])
                i++
            }
            if i < len(runes) {
                buf.WriteRune(']')
            }
        default:
            // Escape Go regexp metacharacters
            buf.WriteString(regexp.QuoteMeta(string(ch)))
        }
    }

    buf.WriteString("$") // SIMILAR TO matches the entire string
    return buf.String(), nil
}
```

Key differences from LIKE:
- SIMILAR TO matches the **entire** string (anchored with ^ and $)
- Supports alternation with `|`
- Supports grouping with `(` and `)`
- Supports character classes with `[...]`
- `%` and `_` have the same meaning as LIKE

### CREATE TYPE AS ENUM AST Node

```go
// CreateTypeStmt represents a CREATE TYPE statement.
// Currently only ENUM types are supported.
type CreateTypeStmt struct {
    Name        string   // Type name
    Schema      string   // Schema name (empty for default)
    TypeKind    string   // "ENUM" (extensible for future composite types)
    EnumValues  []string // For ENUM types: the list of allowed values
    IfNotExists bool     // CREATE TYPE IF NOT EXISTS
}

func (s *CreateTypeStmt) stmtNode() {}
```

```go
// DropTypeStmt represents a DROP TYPE statement.
type DropTypeStmt struct {
    Name     string // Type name
    Schema   string // Schema name (empty for default)
    IfExists bool   // DROP TYPE IF EXISTS
}

func (s *DropTypeStmt) stmtNode() {}
```

### Parser Changes for CREATE TYPE

The parser needs to handle CREATE TYPE in the existing CREATE dispatch:

```go
// In parseCreateStatement, after checking for TABLE, VIEW, INDEX, SEQUENCE, SCHEMA:
case "TYPE":
    p.advance() // consume TYPE
    return p.parseCreateType()
```

```go
func (p *Parser) parseCreateType() (Stmt, error) {
    // Parse optional IF NOT EXISTS
    ifNotExists := false
    if p.isKeyword("IF") {
        p.advance()
        if !p.isKeyword("NOT") {
            return nil, p.expectedError("NOT")
        }
        p.advance()
        if !p.isKeyword("EXISTS") {
            return nil, p.expectedError("EXISTS")
        }
        p.advance()
        ifNotExists = true
    }

    // Parse type name (optionally schema-qualified)
    name, schema, err := p.parseQualifiedName()
    if err != nil {
        return nil, err
    }

    // Expect AS
    if !p.isKeyword("AS") {
        return nil, p.expectedError("AS")
    }
    p.advance()

    // Currently only ENUM is supported
    if !p.isKeyword("ENUM") {
        return nil, fmt.Errorf("unsupported type kind: %s (only ENUM is supported)", p.current().value)
    }
    p.advance()

    // Parse enum values: ('value1', 'value2', ...)
    if err := p.expect(tokenLParen); err != nil {
        return nil, err
    }

    var values []string
    for {
        tok := p.current()
        if tok.typ != tokenString {
            return nil, fmt.Errorf("expected string literal for enum value, got %s", tok.value)
        }
        values = append(values, tok.value)
        p.advance()

        if p.current().typ == tokenComma {
            p.advance()
            continue
        }
        break
    }

    if err := p.expect(tokenRParen); err != nil {
        return nil, err
    }

    return &CreateTypeStmt{
        Name:        name,
        Schema:      schema,
        TypeKind:    "ENUM",
        EnumValues:  values,
        IfNotExists: ifNotExists,
    }, nil
}
```

### Catalog Storage for User-Defined Types

A new TypeEntry is added to the catalog to store named types:

```go
// TypeEntry represents a user-defined type in the catalog.
type TypeEntry struct {
    Name       string          // Type name
    Schema     string          // Schema (empty for default)
    TypeKind   string          // "ENUM"
    EnumValues []string        // For ENUM: ordered list of allowed values
    TypeInfo   dukdb.TypeInfo  // Resolved TypeInfo for use in column definitions
}
```

Types are stored in the Schema struct (not the Catalog struct), alongside tables, views, indexes, and sequences. This is consistent with how all other named objects are stored -- the Schema struct is the namespace container, and the Catalog simply holds schemas.

```go
// In Schema struct, add:
types map[string]*TypeEntry // key: type name (lowercased)
```

The NewSchema constructor must initialize the types map:

```go
func NewSchema(name string) *Schema {
    return &Schema{
        name:      name,
        tables:    make(map[string]*TableDef),
        views:     make(map[string]*ViewDef),
        indexes:   make(map[string]*IndexDef),
        sequences: make(map[string]*SequenceDef),
        types:     make(map[string]*TypeEntry),
    }
}
```

Methods on Schema:
- `CreateType(entry *TypeEntry) error` - Register a new type, error if exists (unless IF NOT EXISTS)
- `DropType(name string, ifExists bool) error` - Remove a type
- `GetType(name string) (*TypeEntry, bool)` - Look up a type by name
- `ListTypes() []*TypeEntry` - List all types in this schema

Catalog-level convenience methods delegate to the appropriate schema:
- `Catalog.CreateType(entry *TypeEntry) error` - Resolves schema then calls schema.CreateType
- `Catalog.DropType(name, schema string, ifExists bool) error` - Resolves schema then calls schema.DropType
- `Catalog.GetType(name, schema string) (*TypeEntry, bool)` - Resolves schema then calls schema.GetType

### Type Resolution: Parser vs Binder Responsibilities

The parser's `ParseTypeExpression` does not have access to the catalog, so it cannot resolve user-defined type names. When the parser encounters an unknown type name (one that is not a built-in type), it must store the raw type name string rather than failing. The binder is then responsible for resolving that raw type name against the catalog.

Concretely, the parser should produce an AST column definition with the unresolved type name (e.g., `TypeName: "mood"`). The binder then looks up this name in the catalog during CREATE TABLE binding, converting it to a resolved TypeInfo.

### Binder Resolution for Enum Type References

When the binder encounters a column type that is not a built-in type, it checks the catalog for user-defined types:

```go
// In the binder, when resolving column types in CREATE TABLE:
func (b *Binder) resolveColumnType(typeName string) (dukdb.TypeInfo, error) {
    // First try built-in types
    info, err := types.ParseType(typeName)
    if err == nil {
        return info, nil
    }

    // Then try user-defined types from catalog
    typeEntry, found := b.catalog.GetType(typeName, b.currentSchema)
    if found {
        return typeEntry.TypeInfo, nil
    }

    return nil, fmt.Errorf("unknown type: %s", typeName)
}
```

For enum columns, INSERT validation ensures the inserted value is one of the allowed enum values. The existing EnumTypeInfo from `dukdb.NewEnumInfo(values)` handles value validation.

### Binder Bind() Case Statements

The binder's main `Bind()` switch in `internal/binder/binder.go` must be extended with cases for CreateTypeStmt and DropTypeStmt. Without these, the binder will fall through to the default "unsupported statement" error.

```go
// In binder.go Bind() switch, add alongside other DDL cases:
case *parser.CreateTypeStmt:
    return b.bindCreateType(s)
case *parser.DropTypeStmt:
    return b.bindDropType(s)
```

The `bindCreateType` method validates that the type does not already exist (respecting IF NOT EXISTS) and produces a `BoundCreateTypeStmt`. The `bindDropType` method validates that the type exists (respecting IF EXISTS) and checks for dependent tables before producing a `BoundDropTypeStmt`.

### DROP TYPE in Parser

```go
// In parseDropStatement, after checking for TABLE, VIEW, INDEX, SEQUENCE, SCHEMA:
case "TYPE":
    p.advance() // consume TYPE
    return p.parseDropType()
```

```go
func (p *Parser) parseDropType() (Stmt, error) {
    ifExists := false
    if p.isKeyword("IF") {
        p.advance()
        if !p.isKeyword("EXISTS") {
            return nil, p.expectedError("EXISTS")
        }
        p.advance()
        ifExists = true
    }

    name, schema, err := p.parseQualifiedName()
    if err != nil {
        return nil, err
    }

    return &DropTypeStmt{
        Name:     name,
        Schema:   schema,
        IfExists: ifExists,
    }, nil
}
```

## Context

- The parser already has LIKE, NOT LIKE, ILIKE, NOT ILIKE as BinaryExpr with OpLike, OpNotLike, OpILike, OpNotILike operators
- SIMILAR TO follows the same parsing pattern as LIKE but with `SIMILAR` followed by `TO`
- The type system already supports ENUM via `parseEnumType` in `internal/types/type_system.go`
- `dukdb.NewEnumInfo(values)` creates an EnumTypeInfo with the given string values
- The catalog already stores tables, views, indexes, sequences, and schemas
- WAL already has `EntryCreateType` (entry type 14) defined

## Goals / Non-Goals

- **Goals**:
  - Parse and evaluate SIMILAR TO and NOT SIMILAR TO expressions
  - Parse CREATE TYPE name AS ENUM (...) and DROP TYPE statements
  - Store user-defined enum types in the catalog
  - Resolve user-defined type references in CREATE TABLE column definitions
  - Support ESCAPE clause for SIMILAR TO

- **Non-Goals**:
  - Composite types (CREATE TYPE name AS (...))
  - Range types
  - Domain types
  - ALTER TYPE (adding/removing enum values)
  - Enum ordering or comparison operators beyond equality
  - SIMILAR TO with collation

## Decisions

- **Reuse BinaryExpr for simple SIMILAR TO**: For the common case without ESCAPE, OpSimilarTo/OpNotSimilarTo in BinaryExpr is sufficient. The SimilarToExpr node is used when ESCAPE is specified.
- **SQL regex compiled per evaluation**: For the initial implementation, compile the regex each time. Caching can be added later as an optimization if profiling shows it matters.
- **Schema types map**: User-defined types are stored in the Schema struct (not the Catalog struct) in a `types map[string]*TypeEntry`, consistent with how tables, views, indexes, and sequences are stored at the schema level. Catalog provides convenience methods that delegate to the appropriate schema.
- **Parser stores raw type names**: When the parser encounters an unknown type name in a column definition, it stores the raw string. The binder is responsible for resolving it against the catalog (the parser has no catalog access).
- **Binder cases for DDL**: The binder's Bind() switch must include cases for `*parser.CreateTypeStmt` and `*parser.DropTypeStmt`, following the same pattern as other DDL statements.
- **SQL character class negation**: SQL uses `[!abc]` for negated character classes; the `sqlRegexToGoRegex` function converts this to Go regexp `[^abc]` syntax.
- **WAL integration**: Use the existing `EntryCreateType` WAL entry type (14) for crash recovery.

## Risks / Trade-offs

- SQL regex edge cases (nested brackets, escape within character classes) may need careful handling. Mitigation: comprehensive test cases covering edge cases from the SQL standard.
- Go's regexp does not support backreferences, but SQL SIMILAR TO does not require them, so this is not a concern.
- Adding types to the catalog introduces a new dependency check for DROP TYPE (cannot drop a type in use by a table column). Mitigation: implement dependency checking in the DROP TYPE executor.

## Open Questions

- Should we support `CREATE OR REPLACE TYPE` syntax? DuckDB does not, so likely not needed.
- Should enum values be case-sensitive? DuckDB treats them as case-sensitive strings, so we follow that behavior.
