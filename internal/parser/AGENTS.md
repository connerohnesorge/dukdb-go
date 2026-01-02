# PARSER KNOWLEDGE BASE

## OVERVIEW
The `parser` package handles the conversion of raw SQL query strings into a structured Abstract Syntax Tree (AST). It defines the AST nodes and the parsing logic.

## STRUCTURE
- `parser.go`: Main entry point (`ParseQuery`).
- `ast.go`: Definitions of AST nodes (Statements, Expressions).
- `visitor.go`: AST Traversal interface.
- `table_ref.go`: Table reference handling.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| **Parsing Logic** | `parser.go` | Main parsing function |
| **AST Definitions** | `ast.go` | Structs for SQL constructs |
| **Traversal** | `visitor.go` | Walking the AST |

## CONVENTIONS
- **DuckDB Syntax**: Aims to support DuckDB's SQL dialect.
- **Error Handling**: Returns syntax errors with location info if possible.
