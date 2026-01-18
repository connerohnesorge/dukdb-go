## 1. Implementation

- [ ] 1.1 Add `github.com/testcontainers/testcontainers-go/modules/localstack` dependency
- [ ] 1.2 Implement `setupLocalStackContainer` helper function in `s3_localstack_test.go`
- [ ] 1.3 Refactor LocalStack tests to use the automated container setup
- [ ] 1.4 Update test documentation and comments

## 2. Verification

- [ ] 2.1 Run LocalStack integration tests: `go test -tags integration -v ./internal/io/filesystem/ -run LocalStack`
- [ ] 2.2 Verify container is properly cleaned up after tests
- [ ] 2.3 Run `spectr validate migrate-localstack-tests-to-testcontainers`
