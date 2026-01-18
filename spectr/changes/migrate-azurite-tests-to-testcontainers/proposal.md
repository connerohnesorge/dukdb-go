# Change: Migrate Azurite Integration Tests to Testcontainers

## Why

Currently, Azure integration tests in `internal/io/filesystem/azure_azurite_test.go` require a manually started Azurite instance. This makes it harder to run tests in CI/CD and for new contributors. Migrating to `testcontainers-go` will automate the lifecycle of the Azurite container, ensuring a consistent and isolated environment for every test run.

## What Changes

- **MODIFIED**: `internal/io/filesystem/azure_azurite_test.go` to use `testcontainers-go` for Azurite orchestration.
- **ADDED**: Automatic Azurite container startup and teardown in `TestMain` or individual test setup.
- **REMOVED**: Requirement for manual `docker run` or `docker-compose` commands before running tests.
- **UPDATED**: Instructions in comments to reflect the automated setup.

## Impact

- Affected specs: `specs/filesystem/spec.md` (integration testing section)
- Affected code: `internal/io/filesystem/azure_azurite_test.go`
- Dependencies: `github.com/testcontainers/testcontainers-go/modules/azurite`
