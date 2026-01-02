# EXECUTOR KNOWLEDGE BASE

## OVERVIEW
The `executor` package implements the physical execution of query plans. It processes a tree of physical operators, pulling `DataChunks` up the tree (vectorized execution model).

## STRUCTURE
- `physical_*.go`: Implementations of physical operators (Scan, Filter, Project, Aggregate, Join).
- `executor.go`: Main entry point for execution.
- `result_set.go`: Wraps execution results for the driver.
- `expr.go`: Expression evaluation.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| **Operator Logic** | `physical_*.go` | e.g., `physical_filter.go` |
| **Aggregations** | `physical_aggregate.go` | SUM, COUNT, etc. |
| **Joins** | `physical_hashjoin.go` | Hash join implementation |
| **Expression Eval** | `expr.go` | Vectorized expression evaluation |

## CONVENTIONS
- **Vectorized**: All operators work on `DataChunk`s, not individual rows.
- **Pull Model**: Parent operators pull chunks from children (`GetChunk`).
- **Memory**: Be mindful of memory usage in blocking operators (Agg, Sort, Join).
