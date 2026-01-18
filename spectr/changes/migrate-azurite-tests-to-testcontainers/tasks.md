## 1. Implementation

- [x] 1.1 Add `github.com/testcontainers/testcontainers-go/modules/azurite` dependency
- [x] 1.2 Implement `setupAzuriteContainer` helper function in `azure_azurite_test.go`
- [x] 1.3 Refactor Azurite tests to use the automated container setup
- [x] 1.4 Update test documentation and comments

## 2. Verification

- [x] 2.1 Run Azurite integration tests: `go test -tags integration -v ./internal/io/filesystem/ -run Azurite`
- [x] 2.2 Verify container is properly cleaned up after tests
- [ ] 2.3 Run `spectr validate migrate-azurite-tests-to-testcontainers`
