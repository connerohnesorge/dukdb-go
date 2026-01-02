# ENGINE KNOWLEDGE BASE

## OVERVIEW
The `engine` package is the heart of the `dukdb-go` driver. It implements the `Backend` interface used by the top-level driver to execute queries. It orchestrates the flow from SQL text to result sets by coordinating the parser, binder, planner, and executor.

## STRUCTURE
- `engine.go`: Main `Engine` struct and execution logic (`Query`, `Exec`).
- `conn.go`: Session/Connection state management.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| **Query Execution Flow** | `engine.go` | See `Query` method |
| **Transaction Mgmt** | `engine.go` | `Begin`, `Commit`, `Rollback` |
| **Connection State** | `conn.go` | Settings, current schema |

## CONVENTIONS
- **Statelessness**: The engine itself is mostly stateless; state is held in `Conn`.
- **Error Handling**: Wraps internal errors to user-facing standard errors.

## INTERACTIONS
- **Calls**: `parser` -> `binder` -> `planner` -> `executor`
- **Called By**: Top-level `conn.go` (via `backend.go` interface)
