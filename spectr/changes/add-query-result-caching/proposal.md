# Change: Add Query Result Caching

## Why

Many analytical workloads execute the same or similar queries repeatedly. Query result caching can dramatically reduce execution time for:
- Repeated exact queries (e.g., dashboard refreshes)
- Queries with different parameters but same plan structure
- Common aggregations and transformations
- BI tool queries with standard filters

DuckDB v1.4.3 implements query result caching with intelligent invalidation, reducing latency for cached queries from milliseconds to microseconds. Currently, dukdb-go has no caching mechanism, missing this significant performance optimization.

A query caching layer would:
- Store results of recent queries in memory
- Invalidate caches when underlying tables change
- Support parameterized cache hits (same query, different literals)
- Provide visibility into cache statistics and hit rates
- Allow tuning cache behavior via configuration

This enables dukdb-go to serve analytical workloads with minimal latency for repeated queries, improving both throughput and user experience.

## What Changes

- Add query result cache with LRU eviction
- Implement cache key generation (plan signature + parameter fingerprint)
- Build cache invalidation tracking on table modifications
- Add PRAGMA-based cache configuration
- Extend EXPLAIN to show cache hits
- Integrate with executor to check cache before execution
- Add PRAGMA CLEAR CACHE for manual invalidation

BREAKING: No breaking changes. Internal optimization only.

## Impact

- **Affected specs**:
  - `execution-engine` - Check cache before execution
  - New spec: `query-result-caching` - Define caching semantics

- **Affected code**:
  - `internal/engine/` - Add cache layer
  - `internal/executor/` - Check cache, populate on execution
  - `internal/catalog/` - Track invalidations

- **New components**:
  - Query result cache
  - Cache key generator
  - Cache statistics tracker

- **Dependencies**:
  - None on external packages
  - Builds on existing query execution infrastructure
