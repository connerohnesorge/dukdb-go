## Context

Query profiling provides visibility into execution plans and performance metrics. This is essential for optimizing queries and understanding execution behavior.

**Stakeholders**: Database administrators, application developers

**Constraints**:
- Must integrate with execution engine
- Profiling should have minimal overhead when disabled
- Must capture metrics during execution

## Goals / Non-Goals

### Goals
- Capture query plan tree with metrics
- Per-operator timing and row counts
- Memory usage tracking
- API-compatible with duckdb-go GetProfilingInfo

### Non-Goals
- Real-time streaming profiling
- Historical query log
- Automatic performance recommendations

## Decisions

### Decision 1: Profile Storage

**What**: Store profiling data in connection-scoped storage

**Why**: Profile data is per-query, per-connection

**Implementation**:
```go
type Conn struct {
    // ... existing fields
    lastProfile *ProfilingInfo
}
```

### Decision 2: Metric Collection

**What**: Collect metrics during operator execution

**Why**: Must capture timing and row counts as they happen

**Implementation**:
```go
type operatorMetrics struct {
    startTime  time.Time
    endTime    time.Time
    rowsIn     int64
    rowsOut    int64
}

func (op *operator) execute(ctx context.Context) error {
    if profiling {
        op.metrics.startTime = time.Now()
        defer func() { op.metrics.endTime = time.Now() }()
    }
    // ... execution
}
```

## Risks / Trade-offs

### Risk 1: Profiling Overhead
**Risk**: Profiling slows down queries
**Mitigation**: Off by default; fast early-out check

## Migration Plan

New capability with no migration required.
