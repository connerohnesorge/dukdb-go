# BINDER KNOWLEDGE BASE

## OVERVIEW
The `binder` package performs semantic analysis on the parsed AST. It resolves table names, column references, and types, converting the raw AST into a "Bound AST" that is fully resolved and type-checked.

## STRUCTURE
- `binder.go`: Main `Binder` struct.
- `bind_stmt.go`: Statement binding (SELECT, INSERT, etc.).
- `bind_expr.go`: Expression binding (function calls, operators).
- `expressions.go`: Bound expression definitions.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| **Name Resolution** | `bind_expr.go` | Resolving column names to indices |
| **Type Checking** | `bind_expr.go` | Ensuring types match in expressions |
| **Statement Logic** | `bind_stmt.go` | Validation of SQL statements |

## CONVENTIONS
- **Fail Early**: The binder should catch all semantic errors (undefined tables, type mismatches).
- **Bound Types**: Output is a tree of `Bound*` structs.
