package cache

import (
	"container/list"
	"sync"
	"time"
)

const (
	DefaultMaxBytes = 64 << 20
	DefaultTTL      = 5 * time.Minute
)

// QueryResult stores cached query outputs.
type QueryResult struct {
	Rows         []map[string]any
	Columns      []string
	RowsAffected int64
}

// CacheStats captures cache metrics.
type CacheStats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
	Entries   int
	Bytes     int64
}

type cacheEntry struct {
	key        string
	result     QueryResult
	sizeBytes  int64
	expiresAt  time.Time
	tableState map[string]uint64
}

// QueryResultCache provides a memory-bounded, TTL-aware LRU cache.
type QueryResultCache struct {
	mu            sync.Mutex
	entries       map[string]*list.Element
	order         *list.List
	maxBytes      int64
	ttl           time.Duration
	bytes         int64
	hits          uint64
	misses        uint64
	evictions     uint64
	tableVersions map[string]uint64
}

// NewQueryResultCache initializes a new cache with defaults when values are unset.
func NewQueryResultCache(maxBytes int64, ttl time.Duration) *QueryResultCache {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &QueryResultCache{
		entries:       make(map[string]*list.Element),
		order:         list.New(),
		maxBytes:      maxBytes,
		ttl:           ttl,
		tableVersions: make(map[string]uint64),
	}
}

// Get returns a cached entry if present and valid.
func (c *QueryResultCache) Get(key string) (QueryResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.getEntryLocked(key)
	if entry == nil {
		c.misses++
		return QueryResult{}, false
	}

	if c.entryExpiredLocked(entry) || c.entryInvalidLocked(entry) {
		c.removeEntryLocked(entry.key)
		c.misses++
		return QueryResult{}, false
	}

	c.hits++
	return entry.result, true
}

// Put adds a query result to the cache.
func (c *QueryResultCache) Put(key string, result QueryResult, sizeBytes int64, ttl time.Duration, tables []string) {
	if key == "" {
		return
	}

	if sizeBytes <= 0 {
		sizeBytes = EstimateResultSize(result)
	}

	if sizeBytes > c.maxBytes {
		return
	}

	if ttl <= 0 {
		ttl = c.ttl
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		entry := elem.Value.(*cacheEntry)
		c.bytes -= entry.sizeBytes
		c.order.Remove(elem)
		delete(c.entries, key)
	}

	state := c.snapshotTableVersionsLocked(tables)
	entry := &cacheEntry{
		key:        key,
		result:     result,
		sizeBytes:  sizeBytes,
		expiresAt:  time.Now().Add(ttl),
		tableState: state,
	}

	if entry.sizeBytes > c.maxBytes {
		return
	}

	c.bytes += entry.sizeBytes
	if c.bytes > c.maxBytes {
		c.evictLocked(c.bytes - c.maxBytes)
	}

	elem := c.order.PushFront(entry)
	c.entries[key] = elem
}

// InvalidateTables marks cache entries that depend on tables as stale.
func (c *QueryResultCache) InvalidateTables(tables []string) {
	if len(tables) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, table := range tables {
		c.tableVersions[table]++
	}

	for _, elem := range c.entries {
		entry := elem.Value.(*cacheEntry)
		if entryDependsOn(entry, tables) {
			c.removeEntryLocked(entry.key)
		}
	}
}

// Clear removes all cached entries.
func (c *QueryResultCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*list.Element)
	c.order.Init()
	c.bytes = 0
}

// Stats returns a snapshot of cache statistics.
func (c *QueryResultCache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	return CacheStats{
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
		Entries:   len(c.entries),
		Bytes:     c.bytes,
	}
}

// SetMaxBytes updates the cache size limit and evicts as needed.
func (c *QueryResultCache) SetMaxBytes(maxBytes int64) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxBytes = maxBytes
	if c.bytes > c.maxBytes {
		c.evictLocked(c.bytes - c.maxBytes)
	}
}

// SetTTL updates the default TTL for new entries.
func (c *QueryResultCache) SetTTL(ttl time.Duration) {
	if ttl <= 0 {
		ttl = DefaultTTL
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.ttl = ttl
}

// SnapshotTableVersions returns table version numbers for the provided tables.
func (c *QueryResultCache) SnapshotTableVersions(tables []string) map[string]uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.snapshotTableVersionsLocked(tables)
}

func (c *QueryResultCache) snapshotTableVersionsLocked(tables []string) map[string]uint64 {
	state := make(map[string]uint64, len(tables))
	for _, table := range tables {
		state[table] = c.tableVersions[table]
	}
	return state
}

func (c *QueryResultCache) getEntryLocked(key string) *cacheEntry {
	if elem, ok := c.entries[key]; ok {
		c.order.MoveToFront(elem)
		return elem.Value.(*cacheEntry)
	}
	return nil
}

func (c *QueryResultCache) entryExpiredLocked(entry *cacheEntry) bool {
	if entry.expiresAt.IsZero() {
		return false
	}
	return time.Now().After(entry.expiresAt)
}

func (c *QueryResultCache) entryInvalidLocked(entry *cacheEntry) bool {
	for table, version := range entry.tableState {
		if c.tableVersions[table] != version {
			return true
		}
	}
	return false
}

func (c *QueryResultCache) removeEntryLocked(key string) {
	if elem, ok := c.entries[key]; ok {
		entry := elem.Value.(*cacheEntry)
		c.bytes -= entry.sizeBytes
		c.order.Remove(elem)
		delete(c.entries, key)
	}
}

func (c *QueryResultCache) evictLocked(bytesToFree int64) {
	for bytesToFree > 0 && c.order.Len() > 0 {
		elem := c.order.Back()
		if elem == nil {
			return
		}
		entry := elem.Value.(*cacheEntry)
		bytesToFree -= entry.sizeBytes
		c.bytes -= entry.sizeBytes
		c.order.Remove(elem)
		delete(c.entries, entry.key)
		c.evictions++
	}
}

func entryDependsOn(entry *cacheEntry, tables []string) bool {
	for _, table := range tables {
		if _, ok := entry.tableState[table]; ok {
			return true
		}
	}
	return false
}
