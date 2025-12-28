## Context

Replacement scans intercept table name resolution, allowing custom handling of table references. When a query references a table like `my_file.parquet`, the replacement scan can redirect it to `read_parquet('my_file.parquet')`.

**Stakeholders**: Users with custom file formats, virtual table providers

**Constraints**:
- Single callback per connection (matches duckdb-go)
- Must integrate early in binding phase
- Cannot replace system tables

## Goals / Non-Goals

### Goals
- Intercept table references during binding
- Replace with function calls dynamically
- Pass parameters extracted from table name

### Non-Goals
- Multiple callback chains
- Async replacement resolution
- Replacing system catalog tables

## Decisions

### Decision 1: Single Callback Model

**What**: One replacement scan per connection

**Why**: Matches duckdb-go behavior, simpler implementation

**Implementation**:
```go
type Conn struct {
    // ... existing fields
    replacementScan ReplacementScanCallback
}

func RegisterReplacementScan(c *sql.Conn, callback ReplacementScanCallback) error {
    return c.Raw(func(driverConn any) error {
        conn := driverConn.(*Conn)
        conn.replacementScan = callback
        return nil
    })
}
```

### Decision 2: Binding Integration

**What**: Call replacement scan during table name resolution

**Why**: Early interception allows proper type checking

**Implementation**:
```go
func (b *Binder) resolveTable(name string) (BoundExpr, error) {
    // Try replacement scan first
    if b.conn.replacementScan != nil {
        funcName, params, err := b.conn.replacementScan(name)
        if err != nil {
            return nil, err
        }
        if funcName != "" {
            return b.resolveFunction(funcName, params...)
        }
    }
    // Normal table resolution
    return b.resolveCatalogTable(name)
}
```

## Risks / Trade-offs

### Risk 1: Performance Overhead
**Risk**: Callback on every table reference
**Mitigation**: Fast early-out for non-matching patterns

## Migration Plan

New capability with no migration required.
