# PLANNER KNOWLEDGE BASE

## OVERVIEW
The `planner` package defines the structure of Logical and Physical query plans. It provides the node definitions used to represent the query execution flow.

## STRUCTURE
- `logical.go`: Defines `LogicalPlan` interface and implementations (`LogicalScan`, `LogicalFilter`, `LogicalJoin`, etc.).
- `physical.go`: Defines `PhysicalPlan` nodes AND the main `Planner` logic (`NewPlanner`, `Plan`).

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| **Logical Nodes** | `logical.go` | Def of Filter, Project, Join, etc. |
| **Output Columns** | `logical.go` | `OutputColumns()` method on nodes |
| **Planner Logic** | `physical.go` | `Plan()` and `createLogicalPlan` methods |

## CONVENTIONS
- **Tree Structure**: Plans are trees of nodes.
- **Column Bindings**: Uses `ColumnBinding` to track data flow between nodes.
