# Change: Migrate LocalStack Integration Tests to Testcontainers

## Why

Currently, S3 integration tests in `internal/io/filesystem/s3_localstack_test.go` require a manually started LocalStack instance. This makes it harder to run tests in CI/CD and for new contributors. Migrating to `testcontainers-go` will automate the lifecycle of the LocalStack container, ensuring a consistent and isolated environment for every test run.

## What Changes

- **MODIFIED**: `internal/io/filesystem/s3_localstack_test.go` to use `testcontainers-go` for LocalStack orchestration.
- **ADDED**: Automatic LocalStack container startup and teardown in `TestMain` or individual test setup.
- **REMOVED**: Requirement for manual `docker run` commands before running tests.
- **UPDATED**: Instructions in comments to reflect the automated setup.

## Impact

- Affected specs: `specs/filesystem/spec.md` (integration testing section)
- Affected code: `internal/io/filesystem/s3_localstack_test.go`
- Dependencies: `github.com/testcontainers/testcontainers-go/modules/localstack`
