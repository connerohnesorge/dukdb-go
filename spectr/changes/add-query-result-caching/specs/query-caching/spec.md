# Query Result Caching - Delta Spec

## ADDED Requirements

### Requirement: Query Result Cache

The system MUST maintain an in-memory cache of query results for performance.

#### Scenario: Cache exact query results
- GIVEN identical query executed multiple times
- WHEN query executes for the first time
- THEN result is cached with deterministic key
- WHEN query executes second time
- THEN cached result is returned immediately
- AND execution is skipped

#### Scenario: Cache with bounded memory
- GIVEN cache memory limit of 1GB
- WHEN queries accumulate more than 1GB of results
- THEN LRU entries are evicted
- AND total cache memory stays below limit

#### Scenario: Cache supports parametric queries
- GIVEN query `SELECT * FROM t WHERE id = ?` with parameters 1, 2, 3
- WHEN query executes with id=1, then id=2, then id=1 again
- THEN separate cache entries for each parameter value
- AND repeated id=1 hits cache

#### Scenario: Cache invalidates on data modification
- GIVEN cached result for `SELECT * FROM t`
- WHEN row is inserted into table t
- THEN cache entry is invalidated
- AND next query executes fresh

#### Scenario: Cache provides hit/miss statistics
- GIVEN cache running with queries
- WHEN PRAGMA CACHE_STATUS executed
- THEN shows total queries, cache hits, misses, hit rate %
- AND shows cache size and entry count

---

### Requirement: Cache Key Generation

The system MUST generate stable, collision-free cache keys.

#### Scenario: Deterministic key from plan
- GIVEN any query
- WHEN cache key is generated
- THEN key is deterministic (same query → same key)
- AND independent of parameter values (if normalized)

#### Scenario: Different plans have different keys
- GIVEN `SELECT * FROM a WHERE x = 1` and `SELECT * FROM b WHERE x = 1`
- WHEN cache keys generated
- THEN keys are different
- AND queries don't share cache

#### Scenario: Same query, different parameters
- GIVEN `SELECT * FROM t WHERE id = 5` then `SELECT * FROM t WHERE id = 10`
- WHEN cache keys generated
- THEN different keys if parameters are part of key
- AND same key if parameters normalized (optional mode)

#### Scenario: Case sensitivity in keys
- GIVEN `SELECT * FROM T WHERE X = 1` and `SELECT * FROM t WHERE x = 1`
- WHEN cache keys generated
- THEN keys may be same (SQL normalized)
- AND consistent behavior

#### Scenario: Whitespace and formatting ignored
- GIVEN queries with different formatting/whitespace
- WHEN cache keys generated
- THEN identical if semantics are same
- AND cosmetic differences don't affect caching

---

### Requirement: Cache Invalidation

The system MUST invalidate cached results when underlying data changes.

#### Scenario: Invalidate on INSERT
- GIVEN cached result for `SELECT COUNT(*) FROM t`
- WHEN INSERT adds rows to table t
- THEN cached result is invalidated
- AND next query recalculates

#### Scenario: Invalidate on UPDATE
- GIVEN cached result for `SELECT * FROM t WHERE active = true`
- WHEN UPDATE changes row values in table t
- THEN cached result is invalidated
- AND next query recalculates

#### Scenario: Invalidate on DELETE
- GIVEN cached result for `SELECT * FROM t`
- WHEN DELETE removes rows from table t
- THEN cached result is invalidated
- AND next query recalculates

#### Scenario: Invalidate on ALTER TABLE
- GIVEN cached result for table
- WHEN ALTER TABLE changes table structure
- THEN all cached results for this table invalidated
- AND next query recalculates

#### Scenario: Selective invalidation
- GIVEN cached results for multiple tables
- WHEN table A is modified
- THEN only caches involving table A are invalidated
- AND other table caches remain valid

#### Scenario: Transaction-scoped invalidation
- GIVEN transaction modifying table within transaction
- WHEN transaction is committed
- THEN cache invalidation happens after commit
- AND other transactions don't see premature invalidation

---

### Requirement: Cache Hit Performance

The system MUST return cached results very quickly.

#### Scenario: Sub-millisecond cache hit
- GIVEN query in cache
- WHEN query is executed
- THEN total time to return results < 1ms
- AND includes cache lookup and result assembly

#### Scenario: No parsing on cache hit
- GIVEN query is cached
- WHEN query executes
- THEN SQL parsing is skipped
- AND planning is skipped
- AND only result retrieval happens

#### Scenario: Maintain result order
- GIVEN cached result with ORDER BY
- WHEN returned from cache
- THEN result order is identical to original
- AND results are not re-sorted

---

### Requirement: Configuration and Control

The system MUST provide options to control cache behavior.

#### Scenario: Enable/disable cache
- GIVEN PRAGMA query_result_cache = false
- WHEN queries execute
- THEN caching is disabled
- AND all queries execute fresh

#### Scenario: Configure cache size
- GIVEN PRAGMA query_cache_memory_limit = 2GB
- WHEN cache accumulates entries
- THEN maximum memory is 2GB
- AND LRU eviction enforces limit

#### Scenario: Configure entry TTL
- GIVEN PRAGMA query_cache_ttl = 3600 (seconds)
- WHEN cached entry is older than TTL
- THEN entry is evicted even if not LRU
- AND time-based expiration enforced

#### Scenario: Configure parameter sensitivity
- GIVEN PRAGMA cache_parameter_sensitive = true
- WHEN query parameters vary
- THEN different parameters create different cache entries
- AND `SELECT * FROM t WHERE id = ?` creates entry per id value

#### Scenario: Clear cache manually
- GIVEN PRAGMA CLEAR CACHE executed
- WHEN executed
- THEN all cache entries are removed
- AND cache is reset

---

### Requirement: Cache Diagnostics

The system MUST provide visibility into cache behavior.

#### Scenario: Show cache status
- GIVEN PRAGMA CACHE_STATUS executed
- WHEN executed
- THEN shows:
  - Total cached queries
  - Cache hits and misses
  - Hit rate percentage
  - Total cache memory used
  - Largest cached queries

#### Scenario: Show cached entries
- GIVEN PRAGMA LIST_CACHED_QUERIES executed
- WHEN executed
- THEN shows list of cached queries
- AND shows cache size per query
- AND shows time cached
- AND shows hit count

#### Scenario: Show cache key
- GIVEN PRAGMA EXPLAIN_CACHE executed in debug mode
- WHEN executed
- THEN shows cache key for query
- AND useful for debugging cache hits/misses

#### Scenario: Explain shows cache info
- GIVEN EXPLAIN on query that's in cache
- WHEN executed
- THEN output shows "Cache Hit: Yes" or "Cache Hit: No"
- AND shows estimated speedup from cache

---

### Requirement: Non-Cacheable Queries

The system MUST identify and skip caching for non-deterministic queries.

#### Scenario: Don't cache NOW() function
- GIVEN query `SELECT NOW()`
- WHEN planning
- THEN query marked non-cacheable
- AND not cached even if same query repeats

#### Scenario: Don't cache RANDOM()
- GIVEN query `SELECT RANDOM()`
- WHEN planning
- THEN query marked non-cacheable
- AND fresh result generated each time

#### Scenario: Don't cache INSERT/UPDATE
- GIVEN INSERT or UPDATE statement
- WHEN planning
- THEN query marked non-cacheable
- AND result not cached

#### Scenario: Don't cache with UDFs
- GIVEN query with user-defined function
- WHEN planning
- THEN query marked non-cacheable (unless UDF registered as pure)
- AND assumed to have side effects

#### Scenario: Don't cache file access
- GIVEN query with FILE() or external table access
- WHEN planning
- THEN query marked non-cacheable
- AND result may change outside database

---

### Requirement: Memory Management

The system MUST efficiently manage cache memory.

#### Scenario: LRU eviction on memory limit
- GIVEN cache with multiple entries
- WHEN memory limit exceeded
- THEN least recently used entry evicted first
- AND memory freed for new entries

#### Scenario: Result serialization
- GIVEN large query result
- WHEN cached
- THEN result may be serialized/compressed
- AND memory efficiency optimized

#### Scenario: Cache memory accounting
- GIVEN PRAGMA CACHE_STATUS showing memory
- WHEN shown
- THEN reported size is accurate
- AND includes all overhead

#### Scenario: Garbage collection
- GIVEN evicted cache entries
- WHEN entries removed
- THEN memory properly released
- AND no memory leaks

---

### Requirement: Correctness Preservation

The system MUST ensure cached results are correct.

#### Scenario: Cached result matches fresh execution
- GIVEN any query
- WHEN executed twice (first fresh, second cached)
- THEN results are identical
- AND row count, column values, ordering all match

#### Scenario: Cache respects transaction isolation
- GIVEN transaction T1 with modifications
- WHEN T1 is open and other transactions execute
- THEN other transactions don't see T1's uncommitted modifications in cache
- AND cache reflects committed state

#### Scenario: Cache handles NULL correctly
- GIVEN query with NULL values
- WHEN cached and retrieved
- THEN NULLs are preserved exactly
- AND NULL comparisons work correctly

#### Scenario: Cache preserves data types
- GIVEN query returning typed data
- WHEN cached and retrieved
- THEN data types are preserved
- AND conversions don't occur

---

### Requirement: Parameterized Cache

The system MUST support caching with query parameters.

#### Scenario: Separate cache for different parameters
- GIVEN prepared statement `SELECT * FROM t WHERE id = ?`
- WHEN executed with id=1, id=2, id=1
- THEN cache maintains separate entries for id=1 and id=2
- AND third execution hits cache for id=1

#### Scenario: Parameter normalization
- GIVEN PRAGMA cache_parameter_mode = 'normalized'
- WHEN `SELECT * FROM t WHERE id IN (1, 2)` vs `SELECT * FROM t WHERE id IN (2, 1)`
- THEN treated as different queries (order matters)
- AND parameter matching is exact

#### Scenario: Large parameter set
- GIVEN query with many parameter combinations
- WHEN cache accumulates entries for different parameters
- THEN cache evicts oldest or least used
- AND doesn't bloat indefinitely

---

### Requirement: Performance Improvement

The system MUST deliver significant performance benefits for cached queries.

#### Scenario: 1000x speedup for cached query
- GIVEN query normally taking 100ms to execute
- WHEN cached and re-executed
- THEN execution time < 1ms (100x speedup or more)
- AND overhead is minimal

#### Scenario: Improve throughput with cache
- GIVEN workload with repeated queries
- WHEN caching enabled
- THEN system throughput increases 2-10x
- AND cache hit rate determines improvement

#### Scenario: Reduce latency for dashboards
- GIVEN BI dashboard with 10 standard queries
- WHEN all 10 are cached
- THEN dashboard load time < 100ms
- AND independent of query complexity
