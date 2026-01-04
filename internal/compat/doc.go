// Package compat provides compatibility testing between dukdb-go and the
// official DuckDB go-duckdb driver. These tests verify API compatibility
// and behavioral parity with the CGO-based reference implementation.
//
// The tests in this package fall into two categories:
//
//  1. CGO-based comparison tests (duckdb_compat_test.go): These tests only
//     run when CGO is enabled AND the duckdb_compat build tag is set. They
//     compare behavior directly between dukdb-go and the official go-duckdb
//     driver. To run these tests:
//
//     CGO_ENABLED=1 go test -tags "cgo duckdb_compat" ./internal/compat/...
//
//  2. Spec compliance tests (spec_compliance_test.go): These tests document
//     expected DuckDB behavior and verify that dukdb-go matches the spec.
//     These tests run regardless of CGO availability and are included in the
//     normal test suite.
//
// Test Coverage:
//
// Spec Compliance Tests:
//   - CREATE SECRET syntax and options for all secret types (S3, GCS, Azure, HTTP, HuggingFace)
//   - DROP SECRET syntax with IF EXISTS support
//   - Secret type to URL scheme mapping
//   - Longest-prefix-match scope resolution
//   - Credential provider types (CONFIG, ENV, CREDENTIAL_CHAIN, IAM)
//   - Type-specific options (S3 credentials, Azure auth, GCS service accounts, HTTP headers)
//   - Secret lifecycle operations (create, alter, drop, list)
//   - Error conditions and validation
//   - Type and provider name normalization (case-insensitive)
//
// CGO Comparison Tests (when duckdb_compat tag is set):
//   - SQL syntax compatibility
//   - CREATE/DROP SECRET behavior comparison
//   - URL parsing consistency
//   - Error handling parity
//   - Transaction behavior
//   - Prepared statement compatibility
//   - Data type handling
//
// Known Differences:
//
// Some behaviors may differ between dukdb-go and the official driver due to
// implementation choices or missing features. Known differences are documented
// in the individual test cases.
package compat
