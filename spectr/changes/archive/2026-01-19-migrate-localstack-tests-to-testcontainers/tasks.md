## 1. Implementation

- [x] 1.1 Add `github.com/testcontainers/testcontainers-go/modules/localstack` dependency
- [x] 1.2 Implement `setupLocalStackContainer` helper function in `s3_localstack_test.go`
- [x] 1.3 Refactor LocalStack tests to use the automated container setup
- [x] 1.4 Update test documentation and comments

## 2. Verification

- [x] 2.1 Run LocalStack integration tests: `go test -tags integration -v ./internal/io/filesystem/ -run LocalStack`
- [x] 2.2 Verify container is properly cleaned up after tests
- [x] 2.3 Run `spectr validate migrate-localstack-tests-to-testcontainers`
