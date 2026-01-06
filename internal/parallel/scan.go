// Package parallel provides parallel query execution infrastructure.
// This file implements parallel table scanning with filter and projection pushdown.
package parallel

import (
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// RowGroupMeta describes a row group for parallel scanning.
// Each row group represents a contiguous range of rows that can be
// scanned independently by different workers.
type RowGroupMeta struct {
	// ID is the unique identifier for this row group.
	ID int
	// StartRow is the first row index in this row group.
	StartRow uint64
	// RowCount is the number of rows in this row group.
	RowCount uint64
	// SizeBytes is the approximate size of this row group in bytes (optional).
	SizeBytes int64
	// Compressed indicates if the row group data is compressed.
	Compressed bool
}

// EndRow returns the exclusive end row index for this row group.
func (r RowGroupMeta) EndRow() uint64 {
	return r.StartRow + r.RowCount
}

// TableDataReader is an interface for reading table data.
// This abstraction decouples the parallel scan from the storage layer,
// allowing different storage backends to be used.
type TableDataReader interface {
	// ReadRowGroup reads data from a specific row group with optional column projection.
	// tableOID: the table's object identifier
	// rowGroupID: the ID of the row group to read
	// projections: column indices to read (empty means all columns)
	// Returns a DataChunk containing the requested data.
	ReadRowGroup(tableOID uint64, rowGroupID int, projections []int) (*storage.DataChunk, error)

	// GetRowGroupMeta returns metadata about all row groups in a table.
	GetRowGroupMeta(tableOID uint64) ([]RowGroupMeta, error)

	// GetColumnTypes returns the column types for the table.
	GetColumnTypes(tableOID uint64) ([]dukdb.Type, error)

	// GetColumnNames returns the column names for the table.
	GetColumnNames(tableOID uint64) ([]string, error)
}

// FilterExpr represents a filter expression for pushdown.
// This is a simplified interface that allows different expression types.
type FilterExpr interface {
	// Evaluate evaluates the filter for a single row.
	// Returns true if the row passes the filter.
	Evaluate(chunk *storage.DataChunk, rowIdx int) bool
}

// SimpleCompareFilter implements a simple comparison filter.
type SimpleCompareFilter struct {
	// ColumnIdx is the column index to filter on.
	ColumnIdx int
	// Op is the comparison operator ("=", "!=", "<", "<=", ">", ">=").
	Op string
	// Value is the value to compare against.
	Value any
}

// Evaluate evaluates the simple comparison filter.
func (f *SimpleCompareFilter) Evaluate(chunk *storage.DataChunk, rowIdx int) bool {
	val := chunk.GetValue(rowIdx, f.ColumnIdx)
	if val == nil {
		return false // NULL values don't match any comparison
	}

	return compareValues(val, f.Value, f.Op)
}

// AndFilter combines multiple filters with AND logic.
type AndFilter struct {
	Filters []FilterExpr
}

// Evaluate evaluates all filters with AND logic.
func (f *AndFilter) Evaluate(chunk *storage.DataChunk, rowIdx int) bool {
	for _, filter := range f.Filters {
		if !filter.Evaluate(chunk, rowIdx) {
			return false
		}
	}
	return true
}

// OrFilter combines multiple filters with OR logic.
type OrFilter struct {
	Filters []FilterExpr
}

// Evaluate evaluates all filters with OR logic.
func (f *OrFilter) Evaluate(chunk *storage.DataChunk, rowIdx int) bool {
	if len(f.Filters) == 0 {
		return true
	}
	for _, filter := range f.Filters {
		if filter.Evaluate(chunk, rowIdx) {
			return true
		}
	}
	return false
}

// ScanConfig configures parallel scan behavior.
type ScanConfig struct {
	// EnableFilterPushdown enables filter pushdown optimization.
	EnableFilterPushdown bool
	// EnableProjectionPushdown enables projection pushdown optimization.
	EnableProjectionPushdown bool
	// MorselConfig configures morsel sizing.
	MorselConfig MorselConfig
	// BatchSize is the maximum number of rows to process at once.
	BatchSize int
}

// DefaultScanConfig returns the default scan configuration.
func DefaultScanConfig() ScanConfig {
	return ScanConfig{
		EnableFilterPushdown:     true,
		EnableProjectionPushdown: true,
		MorselConfig:             DefaultMorselConfig(),
		BatchSize:                storage.StandardVectorSize,
	}
}

// ParallelTableScan implements ParallelSource for parallel table scanning.
// It partitions a table into morsels based on row groups and scans them
// in parallel with optional filter and projection pushdown.
type ParallelTableScan struct {
	// TableOID is the object identifier for the table.
	TableOID uint64
	// TableName is the name of the table being scanned.
	TableName string
	// Schema is the schema name (optional).
	Schema string
	// Columns is the list of all column names in the table.
	Columns []string
	// ColumnTypes is the list of all column types in the table.
	ColumnTypes []dukdb.Type
	// Projections contains the column indices to read (empty means all columns).
	Projections []int
	// Filter is the filter expression for pushdown (nil means no filter).
	Filter FilterExpr
	// RowGroups contains metadata for all row groups in the table.
	RowGroups []RowGroupMeta
	// DataReader is the interface for reading actual table data.
	DataReader TableDataReader
	// Config is the scan configuration.
	Config ScanConfig

	// mu protects mutable state during concurrent access.
	mu sync.RWMutex
	// morsels is the cached list of generated morsels.
	morsels []Morsel
	// morselGen is the morsel generator.
	morselGen *MorselGenerator
}

// NewParallelTableScan creates a new parallel table scan.
func NewParallelTableScan(
	tableOID uint64,
	tableName string,
	columns []string,
	columnTypes []dukdb.Type,
	dataReader TableDataReader,
) *ParallelTableScan {
	return &ParallelTableScan{
		TableOID:    tableOID,
		TableName:   tableName,
		Columns:     columns,
		ColumnTypes: columnTypes,
		DataReader:  dataReader,
		Config:      DefaultScanConfig(),
		morselGen:   NewMorselGenerator(),
	}
}

// NewParallelTableScanWithConfig creates a new parallel table scan with custom configuration.
func NewParallelTableScanWithConfig(
	tableOID uint64,
	tableName string,
	columns []string,
	columnTypes []dukdb.Type,
	dataReader TableDataReader,
	config ScanConfig,
) *ParallelTableScan {
	return &ParallelTableScan{
		TableOID:    tableOID,
		TableName:   tableName,
		Columns:     columns,
		ColumnTypes: columnTypes,
		DataReader:  dataReader,
		Config:      config,
		morselGen:   NewMorselGeneratorWithConfig(config.MorselConfig),
	}
}

// SetProjections sets the column projections for the scan.
// Only the specified columns will be read and returned.
func (s *ParallelTableScan) SetProjections(projections []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Projections = projections
	// Invalidate cached morsels since projections changed
	s.morsels = nil
}

// SetProjectionsByName sets column projections by column name.
func (s *ParallelTableScan) SetProjectionsByName(columnNames []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	projections := make([]int, 0, len(columnNames))
	for _, name := range columnNames {
		for i, col := range s.Columns {
			if col == name {
				projections = append(projections, i)
				break
			}
		}
	}
	s.Projections = projections
	s.morsels = nil
}

// SetFilter sets the filter expression for pushdown.
func (s *ParallelTableScan) SetFilter(filter FilterExpr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Filter = filter
}

// SetRowGroups sets the row group metadata.
func (s *ParallelTableScan) SetRowGroups(rowGroups []RowGroupMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RowGroups = rowGroups
	// Invalidate cached morsels since row groups changed
	s.morsels = nil
}

// GenerateMorsels generates morsels from the table's row groups.
// Implements the ParallelSource interface.
func (s *ParallelTableScan) GenerateMorsels() []Morsel {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Return cached morsels if available
	if s.morsels != nil {
		return s.morsels
	}

	// If no row groups are set, try to get them from the data reader
	if len(s.RowGroups) == 0 && s.DataReader != nil {
		if rowGroups, err := s.DataReader.GetRowGroupMeta(s.TableOID); err == nil {
			s.RowGroups = rowGroups
		}
	}

	// Convert RowGroupMeta to RowGroupInfo for morsel generation
	rowGroupInfos := make([]RowGroupInfo, len(s.RowGroups))
	for i, rg := range s.RowGroups {
		rowGroupInfos[i] = RowGroupInfo{
			StartRow: rg.StartRow,
			RowCount: rg.RowCount,
		}
	}

	// Generate morsels from row groups
	s.morsels = s.morselGen.GenerateMorselsFromRowGroups(s.TableOID, rowGroupInfos)

	// Prioritize larger morsels for better load balancing
	PrioritizeLargeMorsels(s.morsels)

	return s.morsels
}

// Scan reads data for the given morsel.
// Implements the ParallelSource interface.
func (s *ParallelTableScan) Scan(morsel Morsel) (*storage.DataChunk, error) {
	s.mu.RLock()
	projections := s.Projections
	filter := s.Filter
	config := s.Config
	dataReader := s.DataReader
	s.mu.RUnlock()

	if dataReader == nil {
		return nil, ErrNoDataReader
	}

	// Read the row group with projection pushdown if enabled
	var readProjections []int
	if config.EnableProjectionPushdown && len(projections) > 0 {
		readProjections = projections
	}

	chunk, err := dataReader.ReadRowGroup(s.TableOID, morsel.RowGroup, readProjections)
	if err != nil {
		return nil, err
	}

	if chunk == nil {
		return nil, nil
	}

	// Apply filter if present and pushdown is enabled
	if config.EnableFilterPushdown && filter != nil {
		chunk = ApplyFilter(chunk, filter)
	}

	// Apply projection if we didn't do it at read time
	if !config.EnableProjectionPushdown && len(projections) > 0 {
		chunk = ApplyProjection(chunk, projections)
	}

	return chunk, nil
}

// TotalRowCount returns the total number of rows across all row groups.
func (s *ParallelTableScan) TotalRowCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var total uint64
	for _, rg := range s.RowGroups {
		total += rg.RowCount
	}
	return total
}

// RowGroupCount returns the number of row groups.
func (s *ParallelTableScan) RowGroupCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.RowGroups)
}

// ProjectedColumnTypes returns the types of the projected columns.
func (s *ParallelTableScan) ProjectedColumnTypes() []dukdb.Type {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.Projections) == 0 {
		return s.ColumnTypes
	}

	types := make([]dukdb.Type, len(s.Projections))
	for i, idx := range s.Projections {
		if idx >= 0 && idx < len(s.ColumnTypes) {
			types[i] = s.ColumnTypes[idx]
		}
	}
	return types
}

// ProjectedColumnNames returns the names of the projected columns.
func (s *ParallelTableScan) ProjectedColumnNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.Projections) == 0 {
		return s.Columns
	}

	names := make([]string, len(s.Projections))
	for i, idx := range s.Projections {
		if idx >= 0 && idx < len(s.Columns) {
			names[i] = s.Columns[idx]
		}
	}
	return names
}

// ApplyFilter applies a filter expression to a DataChunk.
// Returns a new DataChunk containing only the rows that pass the filter.
func ApplyFilter(chunk *storage.DataChunk, filter FilterExpr) *storage.DataChunk {
	if chunk == nil || filter == nil {
		return chunk
	}

	count := chunk.Count()
	if count == 0 {
		return chunk
	}

	// Build selection vector for matching rows
	selection := storage.NewSelectionVector(count)
	matchCount := 0

	for i := 0; i < count; i++ {
		if filter.Evaluate(chunk, i) {
			selection.Set(matchCount, uint32(i))
			matchCount++
		}
	}

	if matchCount == 0 {
		// No rows match, return empty chunk
		return storage.NewDataChunkWithCapacity(chunk.Types(), 0)
	}

	if matchCount == count {
		// All rows match, return original chunk
		return chunk
	}

	selection.SetCount(matchCount)

	// Create new chunk with filtered rows
	filteredChunk := storage.NewDataChunkWithCapacity(chunk.Types(), matchCount)
	for i := 0; i < matchCount; i++ {
		srcRow := int(selection.Get(i))
		values := make([]any, chunk.ColumnCount())
		for j := 0; j < chunk.ColumnCount(); j++ {
			values[j] = chunk.GetValue(srcRow, j)
		}
		filteredChunk.AppendRow(values)
	}

	return filteredChunk
}

// ApplyFilterFunc applies a filter function to a DataChunk.
// The filter function receives the chunk and row index and returns true if the row passes.
func ApplyFilterFunc(chunk *storage.DataChunk, filterFunc func(chunk *storage.DataChunk, rowIdx int) bool) *storage.DataChunk {
	if chunk == nil || filterFunc == nil {
		return chunk
	}

	return ApplyFilter(chunk, &funcFilter{fn: filterFunc})
}

// funcFilter wraps a function as a FilterExpr.
type funcFilter struct {
	fn func(chunk *storage.DataChunk, rowIdx int) bool
}

func (f *funcFilter) Evaluate(chunk *storage.DataChunk, rowIdx int) bool {
	return f.fn(chunk, rowIdx)
}

// ApplyProjection applies column projection to a DataChunk.
// Returns a new DataChunk containing only the specified columns.
func ApplyProjection(chunk *storage.DataChunk, projections []int) *storage.DataChunk {
	if chunk == nil {
		return nil
	}

	if len(projections) == 0 {
		return chunk
	}

	// Get projected column types
	types := make([]dukdb.Type, len(projections))
	for i, idx := range projections {
		if idx >= 0 && idx < chunk.ColumnCount() {
			vec := chunk.GetVector(idx)
			if vec != nil {
				types[i] = vec.Type()
			}
		}
	}

	// Create new chunk with projected columns
	projectedChunk := storage.NewDataChunkWithCapacity(types, chunk.Count())

	for row := 0; row < chunk.Count(); row++ {
		values := make([]any, len(projections))
		for i, idx := range projections {
			if idx >= 0 && idx < chunk.ColumnCount() {
				values[i] = chunk.GetValue(row, idx)
			}
		}
		projectedChunk.AppendRow(values)
	}

	return projectedChunk
}

// ApplyFilterAndProjection applies both filter and projection efficiently.
// This is more efficient than calling ApplyFilter followed by ApplyProjection
// because it only copies matching rows once.
func ApplyFilterAndProjection(chunk *storage.DataChunk, filter FilterExpr, projections []int) *storage.DataChunk {
	if chunk == nil {
		return nil
	}

	// If no filter, just apply projection
	if filter == nil {
		return ApplyProjection(chunk, projections)
	}

	// If no projection, just apply filter
	if len(projections) == 0 {
		return ApplyFilter(chunk, filter)
	}

	count := chunk.Count()
	if count == 0 {
		return chunk
	}

	// Get projected column types
	types := make([]dukdb.Type, len(projections))
	for i, idx := range projections {
		if idx >= 0 && idx < chunk.ColumnCount() {
			vec := chunk.GetVector(idx)
			if vec != nil {
				types[i] = vec.Type()
			}
		}
	}

	// Create result chunk with estimated capacity
	resultChunk := storage.NewDataChunkWithCapacity(types, count)

	// Apply filter and projection in one pass
	for row := 0; row < count; row++ {
		if filter.Evaluate(chunk, row) {
			values := make([]any, len(projections))
			for i, idx := range projections {
				if idx >= 0 && idx < chunk.ColumnCount() {
					values[i] = chunk.GetValue(row, idx)
				}
			}
			resultChunk.AppendRow(values)
		}
	}

	return resultChunk
}

// compareValues compares two values with the given operator.
func compareValues(a, b any, op string) bool {
	// Handle nil cases
	if a == nil || b == nil {
		return false
	}

	// Try numeric comparison first
	if numResult, ok := compareNumeric(a, b, op); ok {
		return numResult
	}

	// Try string comparison
	if strResult, ok := compareStrings(a, b, op); ok {
		return strResult
	}

	// Default equality check
	switch op {
	case "=", "==":
		return a == b
	case "!=", "<>":
		return a != b
	default:
		return false
	}
}

// compareNumeric compares numeric values.
func compareNumeric(a, b any, op string) (bool, bool) {
	aFloat, aOk := toFloat64Val(a)
	bFloat, bOk := toFloat64Val(b)

	if !aOk || !bOk {
		return false, false
	}

	switch op {
	case "=", "==":
		return aFloat == bFloat, true
	case "!=", "<>":
		return aFloat != bFloat, true
	case "<":
		return aFloat < bFloat, true
	case "<=":
		return aFloat <= bFloat, true
	case ">":
		return aFloat > bFloat, true
	case ">=":
		return aFloat >= bFloat, true
	default:
		return false, false
	}
}

// compareStrings compares string values.
func compareStrings(a, b any, op string) (bool, bool) {
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)

	if !aOk || !bOk {
		return false, false
	}

	switch op {
	case "=", "==":
		return aStr == bStr, true
	case "!=", "<>":
		return aStr != bStr, true
	case "<":
		return aStr < bStr, true
	case "<=":
		return aStr <= bStr, true
	case ">":
		return aStr > bStr, true
	case ">=":
		return aStr >= bStr, true
	default:
		return false, false
	}
}

// toFloat64Val converts a value to float64 for comparison.
func toFloat64Val(v any) (float64, bool) {
	switch val := v.(type) {
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

// InMemoryTableReader is a simple in-memory implementation of TableDataReader.
// Useful for testing and small tables that fit in memory.
type InMemoryTableReader struct {
	mu          sync.RWMutex
	tables      map[uint64]*inMemoryTable
	rowGroupSize int
}

type inMemoryTable struct {
	columnNames []string
	columnTypes []dukdb.Type
	chunks      []*storage.DataChunk
}

// NewInMemoryTableReader creates a new in-memory table reader.
func NewInMemoryTableReader(rowGroupSize int) *InMemoryTableReader {
	if rowGroupSize <= 0 {
		rowGroupSize = storage.StandardVectorSize
	}
	return &InMemoryTableReader{
		tables:       make(map[uint64]*inMemoryTable),
		rowGroupSize: rowGroupSize,
	}
}

// RegisterTable registers a table with its column information.
func (r *InMemoryTableReader) RegisterTable(tableOID uint64, columnNames []string, columnTypes []dukdb.Type) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.tables[tableOID] = &inMemoryTable{
		columnNames: columnNames,
		columnTypes: columnTypes,
		chunks:      make([]*storage.DataChunk, 0),
	}
}

// AddChunk adds a data chunk to a table.
func (r *InMemoryTableReader) AddChunk(tableOID uint64, chunk *storage.DataChunk) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if table, ok := r.tables[tableOID]; ok {
		table.chunks = append(table.chunks, chunk)
	}
}

// ReadRowGroup implements TableDataReader.
func (r *InMemoryTableReader) ReadRowGroup(tableOID uint64, rowGroupID int, projections []int) (*storage.DataChunk, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	table, ok := r.tables[tableOID]
	if !ok {
		return nil, ErrTableNotFound
	}

	if rowGroupID < 0 || rowGroupID >= len(table.chunks) {
		return nil, ErrRowGroupNotFound
	}

	chunk := table.chunks[rowGroupID]

	// Apply projection if specified
	if len(projections) > 0 {
		return ApplyProjection(chunk, projections), nil
	}

	return chunk.Clone(), nil
}

// GetRowGroupMeta implements TableDataReader.
func (r *InMemoryTableReader) GetRowGroupMeta(tableOID uint64) ([]RowGroupMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	table, ok := r.tables[tableOID]
	if !ok {
		return nil, ErrTableNotFound
	}

	rowGroups := make([]RowGroupMeta, len(table.chunks))
	var startRow uint64
	for i, chunk := range table.chunks {
		rowGroups[i] = RowGroupMeta{
			ID:       i,
			StartRow: startRow,
			RowCount: uint64(chunk.Count()),
		}
		startRow += uint64(chunk.Count())
	}

	return rowGroups, nil
}

// GetColumnTypes implements TableDataReader.
func (r *InMemoryTableReader) GetColumnTypes(tableOID uint64) ([]dukdb.Type, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	table, ok := r.tables[tableOID]
	if !ok {
		return nil, ErrTableNotFound
	}

	return table.columnTypes, nil
}

// GetColumnNames implements TableDataReader.
func (r *InMemoryTableReader) GetColumnNames(tableOID uint64) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	table, ok := r.tables[tableOID]
	if !ok {
		return nil, ErrTableNotFound
	}

	return table.columnNames, nil
}
