package stats

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Collector accumulates runtime statistics with minimal overhead.
// It is safe for concurrent use.
type Collector struct {
	mu               sync.RWMutex
	tables           map[string]*TableStats
	histogramBuckets int
	hllPrecision     uint8
	enabled          atomic.Bool
}

// CollectorOptions configures runtime collection behavior.
type CollectorOptions struct {
	HistogramBuckets int
	HLLPrecision     uint8
	Enabled          bool
}

// TableStats captures per-table runtime statistics.
type TableStats struct {
	mu        sync.RWMutex
	RowCount  uint64
	Columns   map[string]*ColumnStats
	UpdatedAt time.Time
}

// ColumnStats captures per-column runtime statistics.
type ColumnStats struct {
	mu        sync.Mutex
	Count     uint64
	Nulls     uint64
	Min       float64
	Max       float64
	HasMinMax bool
	Histogram *Histogram
	Sketch    *HyperLogLog
}

// TableSnapshot is a read-only snapshot for estimation.
type TableSnapshot struct {
	RowCount uint64
	Columns  map[string]ColumnSnapshot
}

// ColumnSnapshot is a read-only snapshot for estimation.
type ColumnSnapshot struct {
	Count        uint64
	NullFraction float64
	Distinct     uint64
	Min          float64
	Max          float64
	HasMinMax    bool
	Histogram    *Histogram
}

// NewCollector creates a new stats collector.
func NewCollector(opts CollectorOptions) *Collector {
	c := &Collector{
		tables:           make(map[string]*TableStats),
		histogramBuckets: opts.HistogramBuckets,
		hllPrecision:     opts.HLLPrecision,
	}
	c.enabled.Store(opts.Enabled)
	return c
}

// SetEnabled toggles statistics collection on or off.
func (c *Collector) SetEnabled(enabled bool) {
	if c == nil {
		return
	}
	c.enabled.Store(enabled)
}

// RecordRow records a full row of values for a table.
func (c *Collector) RecordRow(table string, values map[string]any) {
	if c == nil || !c.enabled.Load() {
		return
	}

	stats := c.getOrCreateTable(table)
	stats.mu.Lock()
	stats.RowCount++
	stats.UpdatedAt = time.Now()
	stats.mu.Unlock()

	for column, value := range values {
		c.RecordValue(table, column, value)
	}
}

// RecordValue records a single column value.
func (c *Collector) RecordValue(table, column string, value any) {
	if c == nil || !c.enabled.Load() {
		return
	}

	tableStats := c.getOrCreateTable(table)
	columnStats := tableStats.getOrCreateColumn(column, c.histogramBuckets, c.hllPrecision)

	columnStats.mu.Lock()
	defer columnStats.mu.Unlock()

	columnStats.Count++

	if value == nil {
		columnStats.Nulls++
		return
	}

	if numeric, ok := toFloat64(value); ok {
		if !columnStats.HasMinMax {
			columnStats.Min = numeric
			columnStats.Max = numeric
			columnStats.HasMinMax = true
		} else {
			if numeric < columnStats.Min {
				columnStats.Min = numeric
			}
			if numeric > columnStats.Max {
				columnStats.Max = numeric
			}
		}
		if columnStats.Histogram != nil {
			columnStats.Histogram.Observe(numeric)
		}
	}

	if columnStats.Sketch != nil {
		switch val := value.(type) {
		case string:
			columnStats.Sketch.AddString(val)
		case []byte:
			columnStats.Sketch.AddBytes(val)
		case uint64:
			columnStats.Sketch.AddUint64(val)
		case int:
			columnStats.Sketch.AddUint64(uint64(val))
		case int64:
			columnStats.Sketch.AddUint64(uint64(val))
		default:
			columnStats.Sketch.AddString(fmt.Sprint(val))
		}
	}
}

// Snapshot returns a read-only snapshot for a table.
func (c *Collector) Snapshot(table string) TableSnapshot {
	if c == nil {
		return TableSnapshot{}
	}

	c.mu.RLock()
	tableStats, ok := c.tables[table]
	c.mu.RUnlock()
	if !ok || tableStats == nil {
		return TableSnapshot{}
	}

	tableStats.mu.RLock()
	rowCount := tableStats.RowCount
	columns := make(map[string]ColumnSnapshot, len(tableStats.Columns))
	for name, col := range tableStats.Columns {
		columns[name] = col.snapshot()
	}
	tableStats.mu.RUnlock()

	return TableSnapshot{
		RowCount: rowCount,
		Columns:  columns,
	}
}

// SnapshotAll returns snapshots for all tables.
func (c *Collector) SnapshotAll() map[string]TableSnapshot {
	if c == nil {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	snapshots := make(map[string]TableSnapshot, len(c.tables))
	for name := range c.tables {
		snapshots[name] = c.Snapshot(name)
	}
	return snapshots
}

func (c *Collector) getOrCreateTable(table string) *TableStats {
	c.mu.RLock()
	stats, ok := c.tables[table]
	c.mu.RUnlock()
	if ok {
		return stats
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stats, ok = c.tables[table]; ok {
		return stats
	}
	stats = &TableStats{Columns: make(map[string]*ColumnStats)}
	c.tables[table] = stats
	return stats
}

func (t *TableStats) getOrCreateColumn(column string, buckets int, precision uint8) *ColumnStats {
	t.mu.RLock()
	stats, ok := t.Columns[column]
	t.mu.RUnlock()
	if ok {
		return stats
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if stats, ok = t.Columns[column]; ok {
		return stats
	}
	stats = &ColumnStats{
		Histogram: NewHistogram(buckets),
		Sketch:    NewHyperLogLog(precision),
	}
	t.Columns[column] = stats
	return stats
}

func (c *ColumnStats) snapshot() ColumnSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	nullFraction := 0.0
	if c.Count > 0 {
		nullFraction = float64(c.Nulls) / float64(c.Count)
	}

	distinct := uint64(0)
	if c.Sketch != nil {
		distinct = c.Sketch.Estimate()
	}

	return ColumnSnapshot{
		Count:        c.Count,
		NullFraction: nullFraction,
		Distinct:     distinct,
		Min:          c.Min,
		Max:          c.Max,
		HasMinMax:    c.HasMinMax,
		Histogram:    c.Histogram,
	}
}

func toFloat64(value any) (float64, bool) {
	switch val := value.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint8:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	default:
		return 0, false
	}
}
