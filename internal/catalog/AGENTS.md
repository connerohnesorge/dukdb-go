# CATALOG KNOWLEDGE BASE

## OVERVIEW
The `catalog` package manages the database metadata: schemas, tables, columns, and views. It provides the source of truth for database structure during binding and planning.

## STRUCTURE
- `catalog.go`: Main `Catalog` struct.
- `schema.go`: Schema definitions.
- `table.go`: Table definitions (metadata only, not data).
- `serialize.go`: Catalog serialization.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| **Table Lookup** | `catalog.go` | `GetTable` |
| **Schema Mgmt** | `schema.go` | Create/Drop schema |
| **Serialization** | `serialize.go` | For persistence |

## CONVENTIONS
- **In-Memory**: The catalog is primarily in-memory, backed by persistence.
- **Concurrency**: Operations should be thread-safe (check mutex usage).
