# Change: Integrate PRAGMA checkpoint_threshold with CheckpointManager

## Why

The `PRAGMA checkpoint_threshold` setting is currently defined but not connected to the CheckpointManager implementation. This means users cannot control when DuckDB triggers automatic checkpoints based on WAL size, which is a critical tuning parameter for production workloads. Without this integration, the CheckpointManager operates with a hardcoded threshold, reducing flexibility and making it impossible to optimize for different storage and performance requirements.

## What Changes

- **ADDED**: Integration between `PRAGMA checkpoint_threshold` value and CheckpointManager's threshold configuration
- **ADDED**: Validation and parsing of checkpoint_threshold values (supports b, kb, mb, gb suffixes)
- **ADDED**: Unit tests for threshold configuration persistence
- **ADDED**: Documentation for checkpoint configuration PRAGMAs
- **MODIFIED**: CheckpointManager constructor to accept configurable threshold from PRAGMA setting
- **MODIFIED**: Storage initialization to read and apply checkpoint_threshold on database open

## Impact

- Affected specs: `specs/persistence/spec.md`
- Affected code:
  - `internal/wal/checkpoint.go` - CheckpointManager implementation
  - `internal/storage/duckdb/storage.go` - Storage initialization
  - `internal/executor/physical_maintenance.go` - PRAGMA execution
  - `internal/config/config.go` - Configuration storage
- Breaking changes: None
- Dependencies: None (this is a foundational improvement)

## Priority

**HIGH** - This is a critical configuration gap that affects production reliability and performance tuning.
