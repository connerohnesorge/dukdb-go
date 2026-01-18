// Package server provides PostgreSQL wire protocol server functionality.
// This file implements performance optimizations for the wire protocol.
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	wire "github.com/jeroenrinzema/psql-wire"
)

// Buffer pool sizes for different use cases.
const (
	// SmallBufferSize is used for small operations like encoding primitives.
	SmallBufferSize = 64

	// MediumBufferSize is used for typical row data.
	MediumBufferSize = 1024

	// LargeBufferSize is used for large result sets.
	LargeBufferSize = 16 * 1024

	// DefaultPrefetchSize is the default number of rows to prefetch for cursors.
	DefaultPrefetchSize = 100

	// DefaultCacheSize is the default size for the prepared statement cache.
	DefaultCacheSize = 100

	// DefaultPlanCacheSize is the default size for the query plan cache.
	DefaultPlanCacheSize = 200
)

// BufferPool provides reusable byte buffers to reduce allocations.
// It uses sync.Pool under the hood for efficient memory reuse.
type BufferPool struct {
	smallPool  sync.Pool
	mediumPool sync.Pool
	largePool  sync.Pool
}

// NewBufferPool creates a new BufferPool.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		smallPool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, SmallBufferSize)
				return &b
			},
		},
		mediumPool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, MediumBufferSize)
				return &b
			},
		},
		largePool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, LargeBufferSize)
				return &b
			},
		},
	}
}

// GetSmall gets a small buffer from the pool.
func (p *BufferPool) GetSmall() *[]byte {
	return p.smallPool.Get().(*[]byte)
}

// PutSmall returns a small buffer to the pool.
func (p *BufferPool) PutSmall(b *[]byte) {
	if b == nil {
		return
	}
	*b = (*b)[:0]
	p.smallPool.Put(b)
}

// GetMedium gets a medium buffer from the pool.
func (p *BufferPool) GetMedium() *[]byte {
	return p.mediumPool.Get().(*[]byte)
}

// PutMedium returns a medium buffer to the pool.
func (p *BufferPool) PutMedium(b *[]byte) {
	if b == nil {
		return
	}
	*b = (*b)[:0]
	p.mediumPool.Put(b)
}

// GetLarge gets a large buffer from the pool.
func (p *BufferPool) GetLarge() *[]byte {
	return p.largePool.Get().(*[]byte)
}

// PutLarge returns a large buffer to the pool.
func (p *BufferPool) PutLarge(b *[]byte) {
	if b == nil {
		return
	}
	*b = (*b)[:0]
	p.largePool.Put(b)
}

// GetSized gets a buffer of at least the specified size.
func (p *BufferPool) GetSized(size int) *[]byte {
	if size <= SmallBufferSize {
		return p.GetSmall()
	}
	if size <= MediumBufferSize {
		return p.GetMedium()
	}
	if size <= LargeBufferSize {
		return p.GetLarge()
	}
	// For very large buffers, allocate a new one
	b := make([]byte, 0, size)
	return &b
}

// PutSized returns a buffer to the appropriate pool based on its capacity.
func (p *BufferPool) PutSized(b *[]byte) {
	if b == nil {
		return
	}
	cap := cap(*b)
	*b = (*b)[:0]
	if cap <= SmallBufferSize {
		p.smallPool.Put(b)
	} else if cap <= MediumBufferSize {
		p.mediumPool.Put(b)
	} else if cap <= LargeBufferSize {
		p.largePool.Put(b)
	}
	// Very large buffers are not pooled - let GC handle them
}

// Global buffer pool instance.
var globalBufferPool = NewBufferPool()

// GetBufferPool returns the global buffer pool.
func GetBufferPool() *BufferPool {
	return globalBufferPool
}

// BytesBufferPool provides reusable bytes.Buffer instances.
type BytesBufferPool struct {
	pool sync.Pool
}

// NewBytesBufferPool creates a new BytesBufferPool.
func NewBytesBufferPool() *BytesBufferPool {
	return &BytesBufferPool{
		pool: sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}
}

// Get retrieves a buffer from the pool.
func (p *BytesBufferPool) Get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

// Put returns a buffer to the pool after resetting it.
func (p *BytesBufferPool) Put(b *bytes.Buffer) {
	if b == nil {
		return
	}
	b.Reset()
	p.pool.Put(b)
}

// Global bytes.Buffer pool.
var globalBytesBufferPool = NewBytesBufferPool()

// GetBytesBufferPool returns the global bytes.Buffer pool.
func GetBytesBufferPool() *BytesBufferPool {
	return globalBytesBufferPool
}

// RowPrefetcher handles prefetching rows for cursor operations.
// It maintains a buffer of rows that can be fetched in batches.
type RowPrefetcher struct {
	mu sync.Mutex

	// rows holds the prefetched rows
	rows []map[string]any

	// columns holds the column names
	columns []string

	// position is the current read position
	position int

	// prefetchSize is how many rows to prefetch at a time
	prefetchSize int

	// exhausted indicates no more rows available from source
	exhausted bool

	// stmt is the underlying statement for fetching more rows
	stmt dukdb.BackendStmt

	// ctx is the context for fetching
	ctx context.Context

	// args are the query arguments
	args []driver.NamedValue
}

// NewRowPrefetcher creates a new RowPrefetcher with the specified prefetch size.
func NewRowPrefetcher(prefetchSize int) *RowPrefetcher {
	if prefetchSize <= 0 {
		prefetchSize = DefaultPrefetchSize
	}
	return &RowPrefetcher{
		prefetchSize: prefetchSize,
		rows:         make([]map[string]any, 0, prefetchSize),
	}
}

// Initialize sets up the prefetcher with initial data.
func (p *RowPrefetcher) Initialize(rows []map[string]any, columns []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.rows = rows
	p.columns = columns
	p.position = 0
	p.exhausted = len(rows) < p.prefetchSize
}

// InitializeWithStmt sets up the prefetcher with a statement for lazy fetching.
func (p *RowPrefetcher) InitializeWithStmt(
	ctx context.Context,
	stmt dukdb.BackendStmt,
	args []driver.NamedValue,
) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ctx = ctx
	p.stmt = stmt
	p.args = args
	p.position = 0
	p.exhausted = false
	p.rows = nil
	p.columns = nil
}

// Fetch retrieves the next batch of rows up to the specified count.
// Returns the rows, whether there are more rows, and any error.
func (p *RowPrefetcher) Fetch(count int) ([]map[string]any, []string, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If we need more rows and have a statement, fetch them
	if p.rows == nil && p.stmt != nil && !p.exhausted {
		rows, cols, err := p.stmt.Query(p.ctx, p.args)
		if err != nil {
			return nil, nil, false, err
		}
		p.rows = rows
		p.columns = cols
		p.exhausted = true // For now, we fetch all at once
	}

	if p.rows == nil || p.position >= len(p.rows) {
		return nil, p.columns, false, nil
	}

	endPos := p.position + count
	if count == 0 || endPos > len(p.rows) {
		endPos = len(p.rows)
	}

	result := p.rows[p.position:endPos]
	p.position = endPos
	hasMore := p.position < len(p.rows)

	return result, p.columns, hasMore, nil
}

// Position returns the current cursor position.
func (p *RowPrefetcher) Position() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.position
}

// Remaining returns the number of remaining rows.
func (p *RowPrefetcher) Remaining() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rows == nil {
		return 0
	}
	remaining := len(p.rows) - p.position
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Reset resets the prefetcher position.
func (p *RowPrefetcher) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.position = 0
}

// CachedPreparedStatement represents a cached prepared statement with metadata.
type CachedPreparedStatement struct {
	// Query is the original SQL query
	Query string

	// ParamTypes are the parameter OIDs
	ParamTypes []uint32

	// Columns are the result columns
	Columns wire.Columns

	// Stmt is the underlying prepared statement
	Stmt dukdb.BackendStmt

	// CreatedAt is when the statement was cached
	CreatedAt time.Time

	// LastUsed is when the statement was last used
	LastUsed time.Time

	// UseCount tracks how many times the statement has been used
	UseCount int64
}

// PreparedStatementLRUCache is an LRU cache for prepared statements.
type PreparedStatementLRUCache struct {
	mu sync.RWMutex

	// cache maps query hash to cached statement
	cache map[string]*CachedPreparedStatement

	// order tracks access order for LRU eviction
	order []string

	// maxSize is the maximum number of cached statements
	maxSize int

	// stats tracks cache statistics
	stats CacheStats
}

// CacheStats holds cache performance statistics.
type CacheStats struct {
	Hits      atomic.Int64
	Misses    atomic.Int64
	Evictions atomic.Int64
	Size      atomic.Int64
}

// NewPreparedStatementLRUCache creates a new LRU cache for prepared statements.
func NewPreparedStatementLRUCache(maxSize int) *PreparedStatementLRUCache {
	if maxSize <= 0 {
		maxSize = DefaultCacheSize
	}
	return &PreparedStatementLRUCache{
		cache:   make(map[string]*CachedPreparedStatement),
		order:   make([]string, 0, maxSize),
		maxSize: maxSize,
	}
}

// hashQuery computes a hash for a query string.
func hashQuery(query string) string {
	h := sha256.Sum256([]byte(query))
	return hex.EncodeToString(h[:16]) // Use first 16 bytes
}

// Get retrieves a cached prepared statement.
func (c *PreparedStatementLRUCache) Get(query string) (*CachedPreparedStatement, bool) {
	key := hashQuery(query)

	c.mu.Lock()
	defer c.mu.Unlock()

	stmt, ok := c.cache[key]
	if !ok {
		c.stats.Misses.Add(1)
		return nil, false
	}

	// Update access order (move to end)
	c.moveToEnd(key)

	// Update stats
	stmt.LastUsed = time.Now()
	stmt.UseCount++
	c.stats.Hits.Add(1)

	return stmt, true
}

// Put adds a prepared statement to the cache.
func (c *PreparedStatementLRUCache) Put(query string, stmt *CachedPreparedStatement) {
	key := hashQuery(query)

	c.mu.Lock()
	defer c.mu.Unlock()

	// If already exists, just update
	if existing, ok := c.cache[key]; ok {
		// Close the old statement
		if existing.Stmt != nil {
			_ = existing.Stmt.Close()
		}
		c.cache[key] = stmt
		c.moveToEnd(key)
		return
	}

	// Evict if at capacity
	for len(c.cache) >= c.maxSize && len(c.order) > 0 {
		c.evictOldest()
	}

	// Add new entry
	c.cache[key] = stmt
	c.order = append(c.order, key)
	c.stats.Size.Add(1)
}

// Delete removes a prepared statement from the cache.
func (c *PreparedStatementLRUCache) Delete(query string) bool {
	key := hashQuery(query)

	c.mu.Lock()
	defer c.mu.Unlock()

	stmt, ok := c.cache[key]
	if !ok {
		return false
	}

	// Close the statement
	if stmt.Stmt != nil {
		_ = stmt.Stmt.Close()
	}

	delete(c.cache, key)
	c.removeFromOrder(key)
	c.stats.Size.Add(-1)

	return true
}

// Clear removes all cached statements.
func (c *PreparedStatementLRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, stmt := range c.cache {
		if stmt.Stmt != nil {
			_ = stmt.Stmt.Close()
		}
	}

	c.cache = make(map[string]*CachedPreparedStatement)
	c.order = make([]string, 0, c.maxSize)
	c.stats.Size.Store(0)
}

// GetStats returns the cache statistics.
func (c *PreparedStatementLRUCache) GetStats() CacheStats {
	return CacheStats{
		Hits:      atomic.Int64{},
		Misses:    atomic.Int64{},
		Evictions: atomic.Int64{},
		Size:      atomic.Int64{},
	}
}

// GetStatsSnapshot returns a snapshot of cache statistics.
func (c *PreparedStatementLRUCache) GetStatsSnapshot() (hits, misses, evictions, size int64) {
	return c.stats.Hits.Load(), c.stats.Misses.Load(), c.stats.Evictions.Load(), c.stats.Size.Load()
}

// moveToEnd moves a key to the end of the order slice.
func (c *PreparedStatementLRUCache) moveToEnd(key string) {
	c.removeFromOrder(key)
	c.order = append(c.order, key)
}

// removeFromOrder removes a key from the order slice.
func (c *PreparedStatementLRUCache) removeFromOrder(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

// evictOldest removes the oldest entry from the cache.
func (c *PreparedStatementLRUCache) evictOldest() {
	if len(c.order) == 0 {
		return
	}

	key := c.order[0]
	c.order = c.order[1:]

	if stmt, ok := c.cache[key]; ok {
		if stmt.Stmt != nil {
			_ = stmt.Stmt.Close()
		}
		delete(c.cache, key)
		c.stats.Size.Add(-1)
		c.stats.Evictions.Add(1)
	}
}

// QueryPlanEntry represents a cached query plan.
type QueryPlanEntry struct {
	// QueryHash is the hash of the query
	QueryHash string

	// Plan holds the cached query plan (opaque to this layer)
	Plan any

	// CreatedAt is when the plan was cached
	CreatedAt time.Time

	// LastUsed is when the plan was last used
	LastUsed time.Time

	// UseCount tracks how many times the plan has been used
	UseCount int64

	// SchemaVersion is the schema version when the plan was created
	// Used for invalidation when schema changes
	SchemaVersion int64
}

// QueryPlanCache caches query plans for prepared statements.
type QueryPlanCache struct {
	mu sync.RWMutex

	// cache maps query hash to cached plan
	cache map[string]*QueryPlanEntry

	// order tracks access order for LRU eviction
	order []string

	// maxSize is the maximum number of cached plans
	maxSize int

	// currentSchemaVersion is the current schema version
	currentSchemaVersion atomic.Int64

	// stats tracks cache statistics
	stats CacheStats
}

// NewQueryPlanCache creates a new query plan cache.
func NewQueryPlanCache(maxSize int) *QueryPlanCache {
	if maxSize <= 0 {
		maxSize = DefaultPlanCacheSize
	}
	return &QueryPlanCache{
		cache:   make(map[string]*QueryPlanEntry),
		order:   make([]string, 0, maxSize),
		maxSize: maxSize,
	}
}

// Get retrieves a cached query plan.
func (c *QueryPlanCache) Get(query string) (*QueryPlanEntry, bool) {
	key := hashQuery(query)

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.cache[key]
	if !ok {
		c.stats.Misses.Add(1)
		return nil, false
	}

	// Check if the plan is still valid (schema hasn't changed)
	currentVersion := c.currentSchemaVersion.Load()
	if entry.SchemaVersion != currentVersion {
		// Plan is stale, remove it
		c.removeEntryLocked(key)
		c.stats.Misses.Add(1)
		return nil, false
	}

	// Update access order
	c.moveToEndLocked(key)

	// Update stats
	entry.LastUsed = time.Now()
	entry.UseCount++
	c.stats.Hits.Add(1)

	return entry, true
}

// Put adds a query plan to the cache.
func (c *QueryPlanCache) Put(query string, plan any) {
	key := hashQuery(query)

	c.mu.Lock()
	defer c.mu.Unlock()

	// If already exists, update it
	if existing, ok := c.cache[key]; ok {
		existing.Plan = plan
		existing.LastUsed = time.Now()
		existing.SchemaVersion = c.currentSchemaVersion.Load()
		c.moveToEndLocked(key)
		return
	}

	// Evict if at capacity
	for len(c.cache) >= c.maxSize && len(c.order) > 0 {
		c.evictOldestLocked()
	}

	// Add new entry
	now := time.Now()
	entry := &QueryPlanEntry{
		QueryHash:     key,
		Plan:          plan,
		CreatedAt:     now,
		LastUsed:      now,
		UseCount:      1,
		SchemaVersion: c.currentSchemaVersion.Load(),
	}
	c.cache[key] = entry
	c.order = append(c.order, key)
	c.stats.Size.Add(1)
}

// InvalidateAll invalidates all cached plans by incrementing the schema version.
func (c *QueryPlanCache) InvalidateAll() {
	c.currentSchemaVersion.Add(1)
}

// Clear removes all cached plans.
func (c *QueryPlanCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*QueryPlanEntry)
	c.order = make([]string, 0, c.maxSize)
	c.stats.Size.Store(0)
}

// GetStatsSnapshot returns a snapshot of cache statistics.
func (c *QueryPlanCache) GetStatsSnapshot() (hits, misses, evictions, size int64) {
	return c.stats.Hits.Load(), c.stats.Misses.Load(), c.stats.Evictions.Load(), c.stats.Size.Load()
}

// moveToEndLocked moves a key to the end of the order slice.
// Must be called with lock held.
func (c *QueryPlanCache) moveToEndLocked(key string) {
	c.removeFromOrderLocked(key)
	c.order = append(c.order, key)
}

// removeFromOrderLocked removes a key from the order slice.
// Must be called with lock held.
func (c *QueryPlanCache) removeFromOrderLocked(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

// removeEntryLocked removes an entry from the cache.
// Must be called with lock held.
func (c *QueryPlanCache) removeEntryLocked(key string) {
	delete(c.cache, key)
	c.removeFromOrderLocked(key)
	c.stats.Size.Add(-1)
}

// evictOldestLocked removes the oldest entry from the cache.
// Must be called with lock held.
func (c *QueryPlanCache) evictOldestLocked() {
	if len(c.order) == 0 {
		return
	}

	key := c.order[0]
	c.order = c.order[1:]

	if _, ok := c.cache[key]; ok {
		delete(c.cache, key)
		c.stats.Size.Add(-1)
		c.stats.Evictions.Add(1)
	}
}

// StreamingResultWriter wraps a wire.DataWriter with streaming capabilities.
// It batches rows for more efficient network transmission.
type StreamingResultWriter struct {
	writer     wire.DataWriter
	batchSize  int
	rowCount   int
	currentRow int
	bufferPool *BufferPool
}

// NewStreamingResultWriter creates a new streaming result writer.
func NewStreamingResultWriter(writer wire.DataWriter, batchSize int) *StreamingResultWriter {
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}
	return &StreamingResultWriter{
		writer:     writer,
		batchSize:  batchSize,
		bufferPool: globalBufferPool,
	}
}

// WriteRow writes a single row, potentially batching it.
func (w *StreamingResultWriter) WriteRow(values []any) error {
	// Write the row immediately for now
	// Future optimization: batch rows for network efficiency
	err := w.writer.Row(values)
	if err != nil {
		return err
	}
	w.rowCount++
	w.currentRow++
	return nil
}

// Complete signals the end of results.
func (w *StreamingResultWriter) Complete(tag string) error {
	return w.writer.Complete(tag)
}

// Empty signals an empty result set.
func (w *StreamingResultWriter) Empty() error {
	return w.writer.Empty()
}

// RowCount returns the number of rows written.
func (w *StreamingResultWriter) RowCount() int {
	return w.rowCount
}

// PerformanceConfig holds performance-related configuration options.
type PerformanceConfig struct {
	// PreparedStatementCacheSize is the max number of cached prepared statements
	PreparedStatementCacheSize int

	// QueryPlanCacheSize is the max number of cached query plans
	QueryPlanCacheSize int

	// CursorPrefetchSize is the number of rows to prefetch for cursors
	CursorPrefetchSize int

	// StreamingBatchSize is the number of rows to batch when streaming
	StreamingBatchSize int

	// EnableBufferPooling enables buffer pool usage for encoding/decoding
	EnableBufferPooling bool

	// EnablePreparedStatementCaching enables prepared statement caching
	EnablePreparedStatementCaching bool

	// EnableQueryPlanCaching enables query plan caching
	EnableQueryPlanCaching bool
}

// NewPerformanceConfig creates a new PerformanceConfig with default values.
func NewPerformanceConfig() *PerformanceConfig {
	return &PerformanceConfig{
		PreparedStatementCacheSize:     DefaultCacheSize,
		QueryPlanCacheSize:             DefaultPlanCacheSize,
		CursorPrefetchSize:             DefaultPrefetchSize,
		StreamingBatchSize:             100,
		EnableBufferPooling:            true,
		EnablePreparedStatementCaching: true,
		EnableQueryPlanCaching:         true,
	}
}

// PerformanceMetrics holds performance metrics for monitoring.
type PerformanceMetrics struct {
	// PreparedStmtCacheHits is the number of prepared statement cache hits
	PreparedStmtCacheHits atomic.Int64

	// PreparedStmtCacheMisses is the number of prepared statement cache misses
	PreparedStmtCacheMisses atomic.Int64

	// QueryPlanCacheHits is the number of query plan cache hits
	QueryPlanCacheHits atomic.Int64

	// QueryPlanCacheMisses is the number of query plan cache misses
	QueryPlanCacheMisses atomic.Int64

	// BytesPooledSmall is the number of small buffers pooled
	BytesPooledSmall atomic.Int64

	// BytesPooledMedium is the number of medium buffers pooled
	BytesPooledMedium atomic.Int64

	// BytesPooledLarge is the number of large buffers pooled
	BytesPooledLarge atomic.Int64

	// RowsStreamed is the total number of rows streamed
	RowsStreamed atomic.Int64

	// RowsPrefetched is the total number of rows prefetched
	RowsPrefetched atomic.Int64
}

// GlobalPerformanceMetrics is the global performance metrics instance.
var GlobalPerformanceMetrics = &PerformanceMetrics{}

// GetPerformanceMetrics returns the global performance metrics.
func GetPerformanceMetrics() *PerformanceMetrics {
	return GlobalPerformanceMetrics
}

// Snapshot returns a snapshot of the current metrics.
func (m *PerformanceMetrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"prepared_stmt_cache_hits":   m.PreparedStmtCacheHits.Load(),
		"prepared_stmt_cache_misses": m.PreparedStmtCacheMisses.Load(),
		"query_plan_cache_hits":      m.QueryPlanCacheHits.Load(),
		"query_plan_cache_misses":    m.QueryPlanCacheMisses.Load(),
		"bytes_pooled_small":         m.BytesPooledSmall.Load(),
		"bytes_pooled_medium":        m.BytesPooledMedium.Load(),
		"bytes_pooled_large":         m.BytesPooledLarge.Load(),
		"rows_streamed":              m.RowsStreamed.Load(),
		"rows_prefetched":            m.RowsPrefetched.Load(),
	}
}

// Reset resets all metrics to zero.
func (m *PerformanceMetrics) Reset() {
	m.PreparedStmtCacheHits.Store(0)
	m.PreparedStmtCacheMisses.Store(0)
	m.QueryPlanCacheHits.Store(0)
	m.QueryPlanCacheMisses.Store(0)
	m.BytesPooledSmall.Store(0)
	m.BytesPooledMedium.Store(0)
	m.BytesPooledLarge.Store(0)
	m.RowsStreamed.Store(0)
	m.RowsPrefetched.Store(0)
}
