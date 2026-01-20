# Test Stability Fixes

## Overview
This document details the fixes applied to prevent recurring test failures in dukdb-go.

## Issues Fixed

### Issue #1: Performance Test Timeout (TestTPCHQuery3JoinAggregation)

**Problem:**
- TPC-H query performance tests would timeout in CI
- Tests execute real queries with variable performance depending on machine load
- No timeout handling in CI configuration

**Root Cause:**
- Performance tests are designed to be run locally or in controlled benchmarking environments
- They require significant computational resources and are sensitive to machine load
- Tests take 10-15 seconds on typical hardware but can exceed CI timeout (30s)

**Solution Applied:**

1. **Code-level fix** (Already present in `internal/executor/tpch_performance_test.go`):
   ```go
   if testing.Short() {
       t.Skip("Skipping performance test in short mode")
   }
   ```
   All TPC-H tests check for `-short` flag and skip execution in CI mode.

2. **CI Configuration Updated** (`.github/workflows/write-compat.yml`):
   - Added `-short` flag to all test commands
   - Ensures performance tests are automatically skipped in CI

3. **New CI Workflow** (`.github/workflows/test-stability.yml`):
   - `clean-resources` job: Removes leftover Docker containers before tests
   - `test-short-mode` job: Runs full test suite with `-short` flag (30s timeout)
   - `test-main-suite` job: Runs standard tests with 30s timeout for safety

**How to Skip Performance Tests Locally:**
```bash
# Run with -short flag to skip performance tests
go test -short ./...

# Or use the provided command
nix develop -c test-short
```

**How to Run Performance Tests Locally:**
```bash
# Run without -short to execute performance tests
# Ensure you have sufficient resources
go test ./internal/executor -run TestTPCH

# Or benchmark
go test -bench=BenchmarkTPCH ./internal/executor
```

---

### Issue #2: Docker Container Conflicts

**Problem:**
- Previous test runs leave Docker containers in conflicted state
- Tests like TestCloudStorage_MinIO, TestCloudStorage_GCS fail to start
- Docker networks/volumes also accumulate

**Root Cause:**
- Integration tests using testcontainers don't always clean up properly
- Docker containers from previous CI runs remain on disk
- No cleanup step in CI pipeline

**Solution Applied:**

1. **Pre-test Cleanup** (`.github/workflows/test-stability.yml`):
   ```bash
   docker ps -a | grep -E "iceberg|testcontainers|duckdb" | awk '{print $1}' | xargs -r docker rm -f
   docker network prune -f
   docker volume prune -f
   ```

2. **CI Workflow Order:**
   - `clean-resources` job runs first (before any tests)
   - Both `test-short-mode` and `test-main-suite` depend on `clean-resources`
   - Ensures fresh Docker state for each test run

**How to Clean Up Locally:**
```bash
# Clean up Docker containers
docker ps -a | grep -E "iceberg|testcontainers" | awk '{print $1}' | xargs -r docker rm -f

# Clean up networks and volumes
docker network prune -f && docker volume prune -f
```

---

## Test Configuration Summary

### `-short` Flag Behavior
The `-short` flag is a Go testing convention:
- Performance tests should check `if testing.Short() { t.Skip(...) }`
- Integration tests may also skip in short mode
- Used in CI to run fast, critical tests only
- Used locally when you want quick feedback

### Current CI Workflow

```
GitHub Push/PR
    ↓
clean-resources (Docker cleanup)
    ↓
test-short-mode (fast, critical tests) ←— MAIN CI PATH
    ├─ 30s timeout
    └─ Performance tests skipped
    ↓
test-main-suite (all tests)
    ├─ 30s timeout
    └─ Includes all tests
```

---

## Files Modified

1. `.github/workflows/write-compat.yml`
   - Added `-short` flag to all write compatibility test commands

2. `.github/workflows/test-stability.yml` (NEW)
   - New workflow for test stability and cleanup
   - Runs on every push/PR

---

## Verification

### Test Timeouts
All tests now have explicit 30s timeout to prevent GitHub Actions from hanging:
```bash
go test -timeout 30s ./...
```

### Performance Tests Skip in CI
```bash
# CI runs with -short flag
go test -short -v ./...

# Performance tests automatically skip:
# === RUN   TestTPCHQuery3JoinAggregation
# --- SKIP: TestTPCHQuery3JoinAggregation (0.00s)
```

### Docker Cleanup Works
```bash
# Before tests run, cleanup job executes:
# docker ps -a | grep -E "iceberg|testcontainers|duckdb" | awk '{print $1}' | xargs -r docker rm -f
# ✅ Docker cleanup complete
```

---

## Future Improvements

1. **Performance Test Monitoring**
   - Consider running TPC-H tests in a separate nightly job
   - Could use relaxed timeout (60-120s) for full performance benchmarking
   - Would require dedicated test environment

2. **Test Duration Metrics**
   - Track test execution times over time
   - Alert if tests regress in performance
   - Use data to inform timeout thresholds

3. **Integration Test Isolation**
   - Each test that uses Docker should clean up after itself
   - Add `t.Cleanup()` hooks to integration tests
   - Document Docker requirements for integration tests

---

## References

- Go Testing Flags: https://golang.org/cmd/go/#hdr-Testing_flags
- Testcontainers Best Practices: https://testcontainers.com/
- GitHub Actions Timeouts: https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions#jobsjob_idtimeout-minutes
