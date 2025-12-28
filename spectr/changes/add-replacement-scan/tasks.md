## 1. Core Types

- [ ] 1.1 Create `replacement_scan.go` with ReplacementScanCallback type
- [ ] 1.2 Add replacementScan field to Conn struct

## 2. Registration

- [ ] 2.1 Implement RegisterReplacementScan function
- [ ] 2.2 Handle re-registration (replace existing)
- [ ] 2.3 Write tests for registration

## 3. Binder Integration

- [ ] 3.1 Call replacement scan during table resolution
- [ ] 3.2 Handle callback errors
- [ ] 3.3 Convert returned params to function arguments
- [ ] 3.4 Write tests for table replacement

## 4. Error Handling

- [ ] 4.1 Propagate callback errors to query
- [ ] 4.2 Handle nil/empty function name return
- [ ] 4.3 Write tests for error scenarios

## 5. Deterministic Testing Integration

- [ ] 5.1 Add quartz.Clock field to ReplacementScanContext struct
- [ ] 5.2 Implement WithClock() method for clock injection
- [ ] 5.3 Use clock.Until() for deadline checking
- [ ] 5.4 Write deterministic tests for callback timeout using quartz.Mock
- [ ] 5.5 Verify zero time.Sleep calls in test files

## 6. Validation

- [ ] 6.1 Run `go test -race`
- [ ] 6.2 Run `golangci-lint`
- [ ] 6.3 Verify API matches duckdb-go
- [ ] 6.4 Verify compliance with deterministic-testing spec
