## 1. Configuration Infrastructure

- [x] 1.1 Add Threshold field to Config struct in `internal/config/config.go`
- [x] 1.2 Create threshold parsing utility in `internal/config/parse.go`
- [x] 1.3 Add validation for threshold values (minimum 1MB recommended)
- [x] 1.4 Write unit tests for threshold parsing utility

## 2. PRAGMA Handler Implementation

- [x] 2.1 Add checkpoint_threshold column to duck
- [x] 2.2 db.settings table schema Implement PhysicalCheckpointThreshold executor in `internal/executor/physical_maintenance.go`
- [x] 2.3 Add threshold reading logic for SET operations
- [x] 2.4 Write integration tests for PRAGMA checkpoint_threshold

## 3. CheckpointManager Integration

- [x] 3.1 Modify CheckpointManager to accept configurable threshold
- [x] 3.2 Update checkpoint trigger logic to use configurable threshold
- [x] 3.3 Add threshold validation at CheckpointManager initialization
- [x] 3.4 Write unit tests for threshold-based checkpoint triggering

## 4. Storage Layer Integration

- [x] 4.1 Read checkpoint_threshold from settings during database open
- [x] 4.2 Pass threshold to CheckpointManager constructor
- [x] 4.3 Handle missing threshold (use default value)
- [x] 4.4 Write integration test for threshold persistence

## 5. Documentation

- [x] 5.1 Add PRAGMA checkpoint_threshold to documentation
- [x] 5.2 Document supported suffixes and minimum values
- [x] 5.3 Add example configurations for different workloads

## 6. Verification

- [x] 6.1 Run `spectr validate add-pragma-checkpoint-threshold`
- [x] 6.2 Verify with go-duckdb API compatibility tests
- [x] 6.3 Ensure all existing tests pass
- [x] 6.4 Manual testing with different threshold values
