# dukdb-go Context

## Purpose

A pure Go implementation of the DuckDB database driver, eliminating cgo dependencies while maintaining full API parity with the original duckdb-go driver. This enables cross-platform compilation without C toolchains, reduces build complexity, and allows use in pure Go environments (TinyGo, WebAssembly, etc.).

**Goals:**
- Complete drop-in replacement for duckdb-go
- Zero cgo dependencies
- Support cross-compilation to any Go-supported platform
- Enable use in constrained environments (WASM, embedded systems)
- Simplified dependency management and build process

## Tech Stack

- **Language**: Pure Go (1.21+), no cgo allowed
- **Testing**:
  - Go standard library `testing` package
  - Testify for assertions and test helpers
  - Integration tests against real DuckDB operations
  - Performance benchmarks comparing to original duckdb-go
- **Code Quality**:
  - `gofmt` for standard Go formatting
  - `golangci-lint` for comprehensive linting
  - `go vet` for static analysis
- **Build Tools**: Standard Go toolchain, no C compiler required

## Project Conventions

### Code Style

- **Formatting**: Use `gofmt` exclusively (no goimports or other formatters)
- **Linting**: Pass `golangci-lint run` with no errors before committing
- **Naming**: Follow standard Go conventions (MixedCaps for exports, camelCase for internal)
- **Comments**: Godoc-style comments for all exported types, functions, and packages
- **Error Handling**: Always return errors, avoid panic except for programmer errors
- **API Compatibility**: Match duckdb-go's public API exactly for drop-in replacement

### Architecture Patterns

- **Package Layout**: Flat structure - all code in root package until complexity demands otherwise
- **Pure Go**: No cgo, no C dependencies, no syscalls to C libraries
- **Domain Organization**: Code organized by domain concepts:
  - Connection management
  - Query execution
  - Type system and conversions
  - Result set handling
  - Transaction management
- **Interfaces**: Define interfaces for testability and future extensibility
- **Immutability**: Prefer immutable data structures where practical

### Testing Strategy

- **Unit Tests**: Standard library `testing` package with Testify assertions
- **Integration Tests**: Full end-to-end tests against real DuckDB operations
- **Benchmarks**: Performance comparisons to original duckdb-go driver
- **Coverage**: Aim for >80% code coverage on critical paths
- **Test Data**: Use `./duckdb/` reference implementation for test case validation
- **Verification**: All tests must actually run and pass - never skip verification

### Git Workflow

- **Commit Messages**: Conventional Commits format
  - `feat:` for new features
  - `fix:` for bug fixes
  - `docs:` for documentation
  - `test:` for test additions/changes
  - `refactor:` for code refactoring
  - `perf:` for performance improvements
  - Format: `<type>(<scope>): <description>`
- **Branching**: Main branch for stable code, feature branches for development
- **Pull Requests**: Required for all changes, must pass CI checks

## Domain Context

**DuckDB Background:**
- DuckDB is an in-process SQL OLAP database management system
- Optimized for analytical queries (fast aggregations, complex joins)
- Columnar storage format, vectorized query execution
- ACID compliant with full SQL support

**Key Domain Concepts:**
- **Appender**: Bulk data loading interface for high-performance inserts
- **Vector**: Columnar data representation (1024 values per vector)
- **Logical Types**: Rich type system including nested types (STRUCT, LIST, MAP)
- **Prepared Statements**: Parameterized queries for performance and security
- **Extensions**: Plugin system for additional functionality

**Reference Materials:**
- `./duckdb/`: DuckDB C++ source code (reference for behavior)
- Original duckdb-go driver: API compatibility target

## Important Constraints

1. **NO CGO**: Absolute requirement - zero C dependencies or cgo usage
2. **API Compatibility**: Must maintain full API parity with duckdb-go
3. **Pure Go Ecosystem**: Must work in TinyGo, WASM, and other pure Go environments
4. **Performance**: Should be competitive with cgo version (within 2x acceptable)
5. **Cross-Platform**: Must compile and run on all Go-supported platforms
6. **Build Simplicity**: No C compiler or external dependencies required

## External Dependencies

**Reference Projects** (not runtime dependencies):
- `./duckdb/`: DuckDB C++ source - reference for understanding behavior
- `github.com/marcboeker/go-duckdb`: Original cgo driver - API compatibility reference

**Runtime Dependencies** (minimize these):
- Go standard library (preferred for all functionality)
- `github.com/stretchr/testify`: Testing assertions and helpers only

**Development Tools**:
- `golangci-lint`: Code quality enforcement
- `spectr`: Spec-driven development and change management
