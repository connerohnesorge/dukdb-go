## Context

The dukdb-go project implements a pure Go DuckDB driver without CGO dependencies. The checkpoint mechanism is crucial for:
1. **Data durability**: Ensuring committed transactions are persisted to disk
2. **WAL management**: Preventing unbounded WAL growth
3. **Recovery optimization**: Reducing replay time after crashes

Currently, the CheckpointManager uses a hardcoded threshold, while DuckDB allows users to configure this via `PRAGMA checkpoint_threshold`.

## Goals / Non-Goals

### Goals
- Enable runtime configuration of checkpoint threshold via `PRAGMA checkpoint_threshold`
- Persist the threshold setting across database restarts
- Support standard DuckDB size suffixes (b, kb, mb, gb)
- Maintain backward compatibility with existing databases
- Follow existing code patterns and error handling

### Non-Goals
- Changing the checkpoint algorithm itself (that's a separate optimization)
- Implementing checkpoint throttling or rate limiting
- Adding automatic threshold tuning based on workload
- Supporting checkpoint scheduling (that's DuckDB's background checkpoint feature)

## Decisions

### Decision 1: Store threshold in duckdb.settings table

**Status**: Approved

We will store the `checkpoint_threshold` value in the `duckdb.settings` table alongside other PRAGMA settings. This ensures:
- Persistence across database restarts
- Consistent with how other DuckDB settings are stored
- Easy to query and modify

**Alternative considered**: Store in a separate checkpoint config file
**Rationale**: Rejected - would add complexity and deviate from DuckDB's approach

### Decision 2: Parse threshold at storage initialization

**Status**: Approved

The threshold value will be:
1. Read from `duckdb.settings` table during database open
2. Passed to CheckpointManager constructor
3. Used as the trigger point for automatic checkpoints

**Alternative considered**: Read threshold on every checkpoint
**Rationale**: Rejected - would add unnecessary database lookups

### Decision 3: Validate threshold format using existing utilities

**Status**: Pending

We should use or create a shared utility for parsing size strings with suffixes (b, kb, mb, gb) to ensure consistency with other PRAGMA settings.

**Options**:
1. Create new utility in `internal/config/`
2. Reuse existing parsing logic if available
3. Reference DuckDB's implementation for correctness

## Implementation Approach

### Phase 1: Configuration Storage

```go
// internal/config/config.go

type CheckpointConfig struct {
    Threshold string `json:"threshold"` // Stored as string like "256MB"
}
```

### Phase 2: PRAGMA Handler

```go
// internal/executor/physical_maintenance.go

func (p *PhysicalCheckpointThreshold) Execute(ctx context.Context, op *PhysicalOperator) error {
    // Parse and validate the threshold value
    // Store in duckdb.settings table
    // Notify CheckpointManager of new threshold
}
```

### Phase 3: CheckpointManager Integration

```go
// internal/wal/checkpoint.go

type CheckpointManager struct {
    thresholdBytes int64  // Calculated from PRAGMA value
    // ... existing fields
}

func NewCheckpointManager(db *Database, thresholdBytes int64) *CheckpointManager {
    return &CheckpointManager{
        thresholdBytes: thresholdBytes,
        // ... initialize other fields
    }
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Migration of existing databases | Existing dbs have no threshold stored | Default to hardcoded value if not found |
| Invalid threshold values | Database may not start | Validate on set, use safe default on parse failure |
| Performance impact of threshold check | Check on every write | Cache threshold, check only on WAL growth |

## Open Questions

1. Should we support percentage-based thresholds (e.g., "10% of available disk space")?
2. Should we expose the current threshold value via a read-only PRAGMA?
3. Should we add a minimum threshold to prevent thrashing?

## References

- DuckDB PRAGMA documentation: https://duckdb.org/docs/sql/configuration
- DuckDB Checkpoint: https://duckdb.org/docs/write-ahead-log/checkpoint
- Existing CheckpointManager: `internal/wal/checkpoint.go`
