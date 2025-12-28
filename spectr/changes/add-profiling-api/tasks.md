## 1. Core Types

- [ ] 1.1 Create `profiling.go` with ProfilingInfo struct
- [ ] 1.2 Define Metrics map structure
- [ ] 1.3 Add profiling fields to Conn

## 2. Metric Collection

- [ ] 2.1 Add metric collection to operator interface
- [ ] 2.2 Implement timing measurement
- [ ] 2.3 Track row counts per operator
- [ ] 2.4 Build plan tree from operators

## 3. API Implementation

- [ ] 3.1 Implement GetProfilingInfo function
- [ ] 3.2 Handle PRAGMA enable_profiling
- [ ] 3.3 Clear profile after retrieval
- [ ] 3.4 Write tests for profiling

## 4. Validation

- [ ] 4.1 Run `go test -race`
- [ ] 4.2 Run `golangci-lint`
- [ ] 4.3 Verify API matches duckdb-go
