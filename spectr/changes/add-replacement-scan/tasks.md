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

## 5. Validation

- [ ] 5.1 Run `go test -race`
- [ ] 5.2 Run `golangci-lint`
- [ ] 5.3 Verify API matches duckdb-go
