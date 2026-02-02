# Tasks: Window Functions Implementation (v1.4.3 Compatibility)

This document outlines the tasks required to implement full window function support in `dukdb-go`, matching DuckDB v1.4.3 capabilities.

## Phase 1: Storage & State Management
Refactor and enhance state management to support large datasets and vectorized execution.

- [ ] **Task 1.1**: Create `internal/storage/window_state.go` and migrate `WindowState`, `WindowPartition`, and `WindowRow` from `internal/executor/physical_window.go`.
- [ ] **Task 1.2**: Implement `WindowSpillManager` in `internal/storage/window_state.go` to support spilling partitions to disk when memory limits are exceeded.
- [ ] **Task 1.3**: Add memory tracking and row counting to `WindowState`.
- [ ] **Task 1.4**: Implement `WindowMemoryPool` for efficient allocation of reusable slices and maps during window evaluation.
- [ ] **Task 1.5**: Implement serialization/deserialization for `WindowRow` to support out-of-core processing.

## Phase 2: Frame Computation Engine
Implement a high-performance, vectorized frame computation engine in `internal/executor/window_frames.go`.

- [ ] **Task 2.1**: Implement `RowsFrameComputer` with support for all physical boundary types.
- [ ] **Task 2.2**: Implement `RangeFrameComputer` with binary search optimization for value-based boundaries.
- [ ] **Task 2.3**: Implement `GroupsFrameComputer` for peer-group-based boundaries.
- [ ] **Task 2.4**: Implement `SlidingFrameComputer` to support incremental frame updates for sliding windows.
- [ ] **Task 2.5**: Implement vectorized `computeExcludedRows` supporting all `EXCLUDE` modes:
    - `EXCLUDE CURRENT ROW`
    - `EXCLUDE GROUP`
    - `EXCLUDE TIES`
    - `EXCLUDE NO OTHERS`
- [ ] **Task 2.6**: Add support for `INTERVAL` offsets in `RangeFrameComputer` for temporal types.

## Phase 3: Vectorized Window Function Library
Implement the core window function library in `internal/executor/window_functions.go`.

- [ ] **Task 3.1**: Implement `RankingFunctionEvaluator` with vectorized versions of:
    - `ROW_NUMBER()`
    - `RANK()`
    - `DENSE_RANK()`
    - `NTILE(n)`
- [ ] **Task 3.2**: Implement `ValueFunctionEvaluator` with vectorized versions of:
    - `LAG(expr, offset, default)`
    - `LEAD(expr, offset, default)`
    - `FIRST_VALUE(expr)`
    - `LAST_VALUE(expr)`
    - `NTH_VALUE(expr, n)`
- [ ] **Task 3.3**: Implement `IGNORE NULLS` support for all value functions.
- [ ] **Task 3.4**: Implement distribution functions:
    - `PERCENT_RANK()`
    - `CUME_DIST()`

## Phase 4: Optimized Aggregate Window Functions
Implement optimized aggregate window functions in `internal/executor/window_aggregates.go`.

- [ ] **Task 4.1**: Implement `AggregateWindowEvaluator` for standard aggregates (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`).
- [ ] **Task 4.2**: Implement `SlidingWindowState` using ring buffers or segments for efficient incremental aggregation.
- [ ] **Task 4.3**: Add support for `DISTINCT` within window aggregates.
- [ ] **Task 4.4**: Add support for `FILTER (WHERE ...)` clause in window aggregate evaluation.
- [ ] **Task 4.5**: Implement specialized aggregators for high-precision numeric types.

## Phase 5: Planner & Optimizer Enhancements
Support named windows and optimize window query plans.

- [ ] **Task 5.1**: Update `internal/parser/` to support the `WINDOW` clause and named window specifications.
- [ ] **Task 5.2**: Update `internal/binder/` to handle named window resolution and inheritance rules.
- [ ] **Task 5.3**: Implement `WindowRegistry` to manage and validate named windows during binding.
- [ ] **Task 5.4**: Create `internal/planner/window_optimizer.go` and implement `WindowPushdownRule` to push window functions through joins where possible.
- [ ] **Task 5.5**: Implement `PartitionPruningRule` to eliminate unnecessary partition processing based on query predicates.

## Phase 6: Executor Integration & Parallelism
Integrate the new components into the physical execution engine.

- [ ] **Task 6.1**: Refactor `PhysicalWindowExecutor` in `internal/executor/physical_window.go` to use the new vectorized evaluators and frame computers.
- [ ] **Task 6.2**: Implement `ParallelWindowProcessor` to process independent partitions concurrently using a worker pool.
- [ ] **Task 6.3**: Integrate `WindowSpillManager` into the `materialize` phase of `PhysicalWindowExecutor`.
- [ ] **Task 6.4**: Implement optimized peer group computation using vectorized comparisons.

## Phase 7: Verification & Testing
Ensure compatibility and performance.

- [ ] **Task 7.1**: Create comprehensive unit tests for each frame computer in `internal/executor/window_frames_test.go`.
- [ ] **Task 7.2**: Implement the DuckDB v1.4.3 window function test suite in `internal/executor/testdata/window/`.
- [ ] **Task 7.3**: Create integration tests for named windows and inheritance.
- [ ] **Task 7.4**: Develop performance benchmarks for:
    - Large partition processing
    - Sliding window aggregation
    - Concurrent partition evaluation
- [ ] **Task 7.5**: Verify correct `NULL` handling across all functions and frame types.
