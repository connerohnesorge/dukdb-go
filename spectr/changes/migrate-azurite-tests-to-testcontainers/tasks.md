## 1. Implementation

- [ ] 1.1 Add `github.com/testcontainers/testcontainers-go/modules/azurite` dependency
- [ ] 1.2 Implement `setupAzuriteContainer` helper function in `azure_azurite_test.go`
- [ ] 1.3 Refactor Azurite tests to use the automated container setup
- [ ] 1.4 Update test documentation and comments

## 2. Verification

- [ ] 2.1 Run Azurite integration tests: `go test -tags integration -v ./internal/io/filesystem/ -run Azurite`
- [ ] 2.2 Verify container is properly cleaned up after tests
- [ ] 2.3 Run `spectr validate migrate-azurite-tests-to-testcontainers`
