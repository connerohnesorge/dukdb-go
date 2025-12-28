## 1. Core Types

- [ ] 1.1 Create `profiling.go` with ProfilingInfo struct
- [ ] 1.2 Define Metrics map structure
- [ ] 1.3 Add profiling fields to Conn

## 2. Metric Collection

- [ ] 2.1 Add metric collection to operator interface
- [ ] 2.2 Add quartz.Clock field to operatorMetrics struct
- [ ] 2.3 Implement timing measurement using clock.Now() and clock.Since()
- [ ] 2.4 Track row counts per operator
- [ ] 2.5 Build plan tree from operators

## 3. API Implementation

- [ ] 3.1 Implement GetProfilingInfo function
- [ ] 3.2 Handle PRAGMA enable_profiling
- [ ] 3.3 Clear profile after retrieval
- [ ] 3.4 Write tests for profiling

## 4. Deterministic Testing Integration

- [ ] 4.1 Add quartz.Clock field to ProfilingContext struct
- [ ] 4.2 Implement WithClock() method for clock injection
- [ ] 4.3 Use clock.Now() and clock.Since() for all timing
- [ ] 4.4 Write deterministic tests for timing metrics using quartz.Mock
- [ ] 4.5 Verify exact durations in test assertions
- [ ] 4.6 Verify zero time.Now() calls in production code
- [ ] 4.7 Verify zero time.Sleep calls in test files

## 5. Validation

- [ ] 5.1 Run `go test -race`
- [ ] 5.2 Run `golangci-lint`
- [ ] 5.3 Verify API matches duckdb-go
- [ ] 5.4 Verify compliance with deterministic-testing spec
