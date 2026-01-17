# PostgreSQL Wire Protocol Library Research

## Executive Summary

For dukdb-go's PostgreSQL compatibility mode, we need a pure-Go library that can implement a PostgreSQL wire protocol **server** (not client). After evaluating the available options, **jeroenrinzema/psql-wire** is the recommended choice.

## Summary Comparison

| Library | Pure Go | License | Last Updated | Stars | Server Mode | Protocol Support | Recommendation |
|---------|---------|---------|--------------|-------|-------------|------------------|----------------|
| jeroenrinzema/psql-wire | Yes | Apache 2.0 | Jan 2026 | 214 | **Yes** (primary purpose) | Simple + Extended + COPY | **RECOMMENDED** |
| jackc/pgx (pgproto3) | Yes | MIT | Jan 2026 | 13.1k | Yes (low-level) | Complete protocol primitives | Alternative |
| jackc/pgproto3 (v4) | Yes | MIT | Jan 2025 | 170 | Yes (low-level) | Complete protocol primitives | EOL Jul 2025 |
| lib/pq | Yes | MIT | Jan 2026 | 9.7k | **No** (client only) | N/A | Not applicable |
| jackc/pgio | Yes | MIT | 2019 | 14 | Partial (primitives only) | Low-level byte ops | Too low-level |

## Detailed Analysis

### 1. jeroenrinzema/psql-wire

**Repository**: https://github.com/jeroenrinzema/psql-wire

**Purpose**: A pure-Go PostgreSQL wire protocol **server** implementation. This is specifically designed for building PostgreSQL-compatible servers.

**Evaluation**:

| Criterion | Assessment |
|-----------|------------|
| Pure Go | Yes, 100% pure Go (no CGO) |
| License | Apache 2.0 (permissive, compatible with MIT) |
| Maintenance | Very active - last commit Jan 6, 2026 |
| Protocol Support | Simple query, Extended query (prepared statements), COPY |
| Server Mode | Primary purpose - designed specifically for building servers |
| Integration | High-level API with good abstractions |

**Strengths**:
- Purpose-built for creating PostgreSQL servers
- High-level abstractions reduce implementation complexity
- Active development with recent features (parallel pipelining, SCRAM-SHA-256)
- Used in production by Shopify and CloudProud
- Excellent documentation and examples
- Built on top of jackc/pgx v5's pgproto3 for low-level protocol handling
- Supports session management, authentication strategies, TLS

**Weaknesses**:
- Does NOT include SQL parser (we have our own, so this is fine)
- Smaller community than pgx (214 stars vs 13k)
- Some PostgreSQL features may require additional implementation

**Protocol Features**:
- Simple Query Protocol (Q message)
- Extended Query Protocol (Parse, Bind, Describe, Execute, Sync)
- COPY FROM STDIN (bulk data import)
- Cancel Request handling
- Parameter Status messages
- Row Description and DataRow messages
- Error handling with PostgreSQL error codes
- SSL/TLS support
- Authentication: Clear text, MD5, SCRAM-SHA-256
- Parallel pipeline execution (new feature)

**Example Usage**:
```go
package main

import (
    "context"
    wire "github.com/jeroenrinzema/psql-wire"
)

func main() {
    wire.ListenAndServe("127.0.0.1:5432", handler)
}

func handler(ctx context.Context, query string) (wire.PreparedStatements, error) {
    // Parse query and execute against DuckDB engine
    // Return results via wire.DataWriter
}
```

---

### 2. jackc/pgx (pgproto3)

**Repository**: https://github.com/jackc/pgx (pgproto3 is a subpackage)

**Purpose**: The most popular PostgreSQL driver for Go. Contains `pgproto3` which provides low-level wire protocol encoding/decoding.

**Evaluation**:

| Criterion | Assessment |
|-----------|------------|
| Pure Go | Yes, 100% pure Go (no CGO) |
| License | MIT |
| Maintenance | Very active - last commit Jan 10, 2026 |
| Protocol Support | Complete protocol message types |
| Server Mode | Yes, via Backend type |
| Integration | Low-level - requires significant wrapper code |

**Strengths**:
- Battle-tested in production (most popular PostgreSQL Go driver)
- Complete protocol support including edge cases
- Well-documented protocol message types
- MIT license (very permissive)
- Actively maintained by the same author
- `Backend` type specifically designed for server implementations

**Weaknesses**:
- Low-level library - requires building server abstractions from scratch
- No high-level server framework (authentication, connection management, etc.)
- Designed primarily as a client library; server support is secondary
- Would require implementing connection handling, authentication, etc.

**Protocol Support via pgproto3**:
- All message types defined (Bind, Execute, Parse, Query, etc.)
- Backend type for server-side message handling
- Frontend type for client-side message handling
- Complete message encoding/decoding

**Integration Approach**:
psql-wire actually uses pgproto3 internally, providing a higher-level abstraction on top of it.

---

### 3. jackc/pgproto3 (Standalone v4)

**Repository**: https://github.com/jackc/pgproto3

**Purpose**: Standalone version of pgproto3 extracted from pgx v4.

**Evaluation**:

| Criterion | Assessment |
|-----------|------------|
| Pure Go | Yes |
| License | MIT |
| Maintenance | EOL July 1, 2025 (security fixes only) |
| Protocol Support | Complete protocol primitives |
| Server Mode | Yes |
| Integration | Low-level |

**Note**: This is the v4 standalone version. In pgx v5, pgproto3 was moved back into the pgx repository. This version is end-of-life and should NOT be used for new projects.

---

### 4. lib/pq

**Repository**: https://github.com/lib/pq

**Purpose**: PostgreSQL driver for Go's database/sql interface.

**Evaluation**:

| Criterion | Assessment |
|-----------|------------|
| Pure Go | Yes |
| License | MIT |
| Maintenance | Active |
| Protocol Support | Client-side only |
| Server Mode | **No** |
| Integration | Not applicable |

**Conclusion**: lib/pq is a **client-only** library. It cannot be used to build a PostgreSQL server. Not suitable for our use case.

---

### 5. jackc/pgio

**Repository**: https://github.com/jackc/pgio

**Purpose**: Low-level toolkit for building PostgreSQL wire protocol messages.

**Evaluation**:

| Criterion | Assessment |
|-----------|------------|
| Pure Go | Yes |
| License | MIT |
| Maintenance | Last updated 2019 |
| Protocol Support | Byte-level primitives only |
| Server Mode | No (too low-level) |
| Integration | Would require implementing everything |

**Conclusion**: pgio is far too low-level. It only provides functions for byte order conversion when building messages. Not suitable for our use case.

---

### 6. Other Notable Projects

#### yjhatfdu/duck_server
A standalone DuckDB server supporting PostgreSQL wire protocol. Interesting as a reference implementation, but:
- Uses CGO (via go-duckdb)
- Implements wire protocol from scratch (no library)
- Not actively maintained (last update June 2024)

#### ybrs/pgduckdb
PostgreSQL wire protocol proxy for DuckDB. Uses jackc/pgx but:
- Uses CGO (via go-duckdb)
- More of a proxy than a standalone implementation

---

## Recommendation

### Primary Recommendation: jeroenrinzema/psql-wire

**Rationale**:

1. **Purpose-Built**: psql-wire is specifically designed for building PostgreSQL-compatible servers, which is exactly our use case.

2. **Pure Go**: 100% pure Go with no CGO dependencies, meeting dukdb-go's core requirement.

3. **Active Maintenance**: Regular commits, responsive maintainer, used in production by notable companies (Shopify).

4. **High-Level Abstractions**: Provides server framework with:
   - Connection management
   - Authentication strategies (clear text, MD5, SCRAM-SHA-256)
   - Session management
   - TLS support
   - Error handling with PostgreSQL error codes
   - COPY protocol support

5. **Protocol Coverage**: Supports both Simple Query and Extended Query protocols, which are required for ORM and driver compatibility.

6. **License**: Apache 2.0 is compatible with our project.

7. **Foundation**: Built on top of jackc/pgx's pgproto3, ensuring correct low-level protocol handling.

### Alternative: Direct pgproto3 Usage

If psql-wire proves too opinionated or limited, we could build our own server framework using pgproto3 from jackc/pgx directly. This would provide:
- Maximum flexibility
- Direct control over all protocol details
- Ability to handle edge cases ourselves

However, this would require significantly more implementation effort.

---

## Integration Strategy with psql-wire

### Architecture Overview

```
PostgreSQL Client (psql, pgx, JDBC, etc.)
            |
            v
    psql-wire Server
            |
            v
    Query Handler (our code)
            |
            v
    dukdb-go Parser -> Planner -> Executor
            |
            v
    Results formatted via wire.DataWriter
```

### Key Integration Points

1. **Query Handler**: Implement `ParseFn` to receive SQL queries and return prepared statements.

2. **Type Mapping**: Map DuckDB types to PostgreSQL OIDs for result descriptions.

3. **Result Writer**: Use `wire.DataWriter` to stream results back to clients.

4. **Authentication**: Configure authentication strategy based on user requirements.

5. **Session Context**: Use session attributes to maintain connection state.

### Concerns and Limitations

1. **No SQL Parser**: psql-wire does not parse SQL. We must use our own parser (which we have).

2. **PostgreSQL Catalog Queries**: Tools like psql and ORMs query pg_catalog tables. We need to implement compatibility views.

3. **Type System**: Need to carefully map DuckDB types to PostgreSQL types (already planned in type-mapping spec).

4. **Function Aliases**: PostgreSQL-specific functions need aliases (already planned in function-aliases spec).

5. **Transaction Handling**: Need to map transaction semantics between psql-wire's expectations and our engine.

---

## Next Steps

1. Add `github.com/jeroenrinzema/psql-wire` as a dependency
2. Create wire protocol server integration in `internal/postgres/` or `internal/wire/`
3. Implement query handler that bridges psql-wire to our engine
4. Implement type mapping between DuckDB and PostgreSQL OIDs
5. Implement pg_catalog compatibility views
6. Test with psql, pgx, and common ORMs

---

## References

- [PostgreSQL Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [psql-wire Documentation](https://pkg.go.dev/github.com/jeroenrinzema/psql-wire)
- [pgproto3 Documentation](https://pkg.go.dev/github.com/jackc/pgx/v5/pgproto3)
- [PostgreSQL Type OIDs](https://www.postgresql.org/docs/current/datatype-oid.html)
