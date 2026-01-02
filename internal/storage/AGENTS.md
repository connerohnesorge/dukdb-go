# STORAGE KNOWLEDGE BASE

## OVERVIEW
The `storage` package handles the in-memory and on-disk representation of data. It defines the core columnar structures (`DataChunk`, `Vector`) used throughout the execution engine.

## STRUCTURE
- `chunk.go`: `DataChunk` definition (batch of vectors).
- `column.go`: `Vector` definition (typed data + validity mask).
- `table.go`: Table storage and schema.
- `serialize.go`: Serialization logic for persistence.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| **Data Batching** | `chunk.go` | `DataChunk` manipulation |
| **Column Data** | `column.go` | `Vector` access and modification |
| **Table Schema** | `table.go` | Table definition and storage |

## CONVENTIONS
- **Columnar**: Data is always processed in columns (Vectors).
- **Validity Mask**: NULLs are handled via validity bitmasks, not special values.
- **Zero-Copy**: Prefer slicing/viewing vectors over copying data.
