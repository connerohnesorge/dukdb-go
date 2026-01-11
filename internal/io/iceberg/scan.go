// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements scan planning logic for Iceberg tables.
package iceberg

import (
	"context"
	"fmt"
	"time"
)

// ScanOptions contains options for scanning an Iceberg table.
type ScanOptions struct {
	// SelectedColumns specifies which columns to read (nil = all columns).
	SelectedColumns []string

	// SnapshotID specifies a specific snapshot to read (nil = current).
	SnapshotID *int64

	// Timestamp specifies a timestamp for time travel (nil = current).
	// If set, reads the snapshot that was current at this time.
	Timestamp *time.Time

	// Limit specifies the maximum number of rows to read (0 = unlimited).
	Limit int64

	// MaxRowsPerChunk limits the number of rows per DataChunk.
	// Default is storage.StandardVectorSize (2048).
	MaxRowsPerChunk int

	// PartitionFilters contains filter expressions for partition pruning.
	PartitionFilters []PartitionFilterExpr

	// ColumnFilters contains filter expressions for column statistics pruning.
	ColumnFilters []ColumnFilterExpr
}

// DefaultScanOptions returns the default scan options.
func DefaultScanOptions() *ScanOptions {
	return &ScanOptions{
		SelectedColumns: nil, // All columns
		MaxRowsPerChunk: 2048,
	}
}

// PartitionFilterExpr represents a filter expression for partition pruning.
type PartitionFilterExpr struct {
	// FieldName is the partition field name.
	FieldName string
	// Operator is the comparison operator ("=", "<", ">", "<=", ">=", "!=", "IN").
	Operator string
	// Value is the filter value (or slice for IN operator).
	Value any
}

// ColumnFilterExpr represents a filter expression for column statistics pruning.
type ColumnFilterExpr struct {
	// ColumnName is the column name.
	ColumnName string
	// Operator is the comparison operator.
	Operator string
	// Value is the filter value.
	Value any
}

// ScanPlan represents the result of scan planning.
// It contains the selected snapshot, data files to read, and projection info.
type ScanPlan struct {
	// Snapshot is the selected snapshot to read.
	Snapshot *Snapshot

	// DataFiles is the list of data files to read after pruning.
	DataFiles []*DataFile

	// DeleteFiles is the list of delete files to apply (TODO: implement).
	DeleteFiles []*DataFile

	// ColumnProjection contains the columns to read.
	ColumnProjection []ColumnInfo

	// PartitionSpec is the partition spec for the selected snapshot.
	PartitionSpec *PartitionSpec

	// EstimatedRowCount is the estimated number of rows to read.
	EstimatedRowCount int64

	// TotalRowCount is the total rows before pruning.
	TotalRowCount int64
}

// ScanPlanner creates scan plans for Iceberg tables.
type ScanPlanner struct {
	metadata       *TableMetadata
	manifestReader *ManifestReader
	schemaMapper   *SchemaMapper
}

// NewScanPlanner creates a new ScanPlanner for the given table metadata.
func NewScanPlanner(metadata *TableMetadata, manifestReader *ManifestReader) *ScanPlanner {
	return &ScanPlanner{
		metadata:       metadata,
		manifestReader: manifestReader,
		schemaMapper:   NewSchemaMapper(),
	}
}

// CreateScanPlan creates a scan plan based on the given options.
func (p *ScanPlanner) CreateScanPlan(ctx context.Context, opts *ScanOptions) (*ScanPlan, error) {
	if opts == nil {
		opts = DefaultScanOptions()
	}

	// Step 1: Select the appropriate snapshot
	snapshot, err := p.selectSnapshot(opts)
	if err != nil {
		return nil, err
	}

	if snapshot == nil {
		// No snapshots - return empty plan but with schema projection
		columnProjection, err := p.calculateColumnProjection(opts.SelectedColumns)
		if err != nil {
			return nil, err
		}
		return &ScanPlan{
			Snapshot:          nil,
			DataFiles:         []*DataFile{},
			DeleteFiles:       []*DataFile{},
			ColumnProjection:  columnProjection,
			EstimatedRowCount: 0,
			TotalRowCount:     0,
		}, nil
	}

	// Step 2: Get partition spec for the snapshot
	partSpec, err := NewPartitionSpec(p.metadata.PartitionSpec())
	if err != nil {
		return nil, fmt.Errorf("failed to parse partition spec: %w", err)
	}

	// Step 3: Read manifest list
	manifests, err := p.manifestReader.ReadManifestList(ctx, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest list: %w", err)
	}

	// Step 4: Apply partition pruning to manifests
	dataManifests := p.filterDataManifests(manifests)
	prunedManifests := p.applyPartitionPruningToManifests(dataManifests, opts.PartitionFilters, partSpec)

	// Step 5: Read data files from manifests
	dataFiles, err := p.readDataFilesFromManifests(ctx, prunedManifests)
	if err != nil {
		return nil, fmt.Errorf("failed to read data files: %w", err)
	}

	// Step 6: Apply partition pruning to data files
	prunedDataFiles := p.applyPartitionPruningToFiles(dataFiles, opts.PartitionFilters, partSpec)

	// Step 7: Apply column statistics pruning
	prunedDataFiles = p.applyColumnStatsPruning(prunedDataFiles, opts.ColumnFilters)

	// Step 8: Calculate column projection
	columnProjection, err := p.calculateColumnProjection(opts.SelectedColumns)
	if err != nil {
		return nil, err
	}

	// Step 9: Read delete manifests (for future implementation)
	deleteManifests := p.filterDeleteManifests(manifests)
	deleteFiles, err := p.readDataFilesFromManifests(ctx, deleteManifests)
	if err != nil {
		// Don't fail on delete file errors - just log and continue
		deleteFiles = []*DataFile{}
	}

	// Step 10: Calculate row counts
	estimatedRows := p.calculateEstimatedRows(prunedDataFiles)
	totalRows := p.calculateTotalRows(dataFiles)

	return &ScanPlan{
		Snapshot:          snapshot,
		DataFiles:         prunedDataFiles,
		DeleteFiles:       deleteFiles,
		ColumnProjection:  columnProjection,
		PartitionSpec:     partSpec,
		EstimatedRowCount: estimatedRows,
		TotalRowCount:     totalRows,
	}, nil
}

// selectSnapshot selects the appropriate snapshot based on options.
func (p *ScanPlanner) selectSnapshot(opts *ScanOptions) (*Snapshot, error) {
	selector := NewSnapshotSelector(p.metadata)

	if opts.SnapshotID != nil {
		return selector.SnapshotByID(*opts.SnapshotID)
	}

	if opts.Timestamp != nil {
		return selector.SnapshotAsOfTimestamp(*opts.Timestamp, true)
	}

	return selector.CurrentSnapshot(), nil
}

// filterDataManifests returns only data manifests (not delete manifests).
func (p *ScanPlanner) filterDataManifests(manifests []*ManifestFile) []*ManifestFile {
	result := make([]*ManifestFile, 0, len(manifests))
	for _, mf := range manifests {
		if mf.IsDataManifest() {
			result = append(result, mf)
		}
	}
	return result
}

// filterDeleteManifests returns only delete manifests.
func (p *ScanPlanner) filterDeleteManifests(manifests []*ManifestFile) []*ManifestFile {
	result := make([]*ManifestFile, 0)
	for _, mf := range manifests {
		if mf.IsDeleteManifest() {
			result = append(result, mf)
		}
	}
	return result
}

// applyPartitionPruningToManifests filters manifests based on partition filters.
// This is a coarse-grained pruning based on manifest-level partition summaries.
func (p *ScanPlanner) applyPartitionPruningToManifests(
	manifests []*ManifestFile,
	filters []PartitionFilterExpr,
	spec *PartitionSpec,
) []*ManifestFile {
	if len(filters) == 0 || spec.IsUnpartitioned() {
		return manifests
	}

	// For now, return all manifests - manifest-level partition summaries
	// require additional metadata that may not always be present.
	// TODO: Implement manifest-level partition pruning when partition summaries are available.
	return manifests
}

// readDataFilesFromManifests reads all data files from the given manifests.
func (p *ScanPlanner) readDataFilesFromManifests(ctx context.Context, manifests []*ManifestFile) ([]*DataFile, error) {
	var allFiles []*DataFile

	for _, manifest := range manifests {
		files, err := p.manifestReader.ReadDataFiles(ctx, manifest)
		if err != nil {
			return nil, err
		}
		allFiles = append(allFiles, files...)
	}

	return allFiles, nil
}

// applyPartitionPruningToFiles filters data files based on partition filters.
func (p *ScanPlanner) applyPartitionPruningToFiles(
	files []*DataFile,
	filters []PartitionFilterExpr,
	spec *PartitionSpec,
) []*DataFile {
	if len(filters) == 0 || spec.IsUnpartitioned() {
		return files
	}

	result := make([]*DataFile, 0, len(files))
	for _, file := range files {
		if p.fileMatchesPartitionFilters(file, filters, spec) {
			result = append(result, file)
		}
	}

	return result
}

// fileMatchesPartitionFilters checks if a file's partition data matches the filters.
func (p *ScanPlanner) fileMatchesPartitionFilters(
	file *DataFile,
	filters []PartitionFilterExpr,
	spec *PartitionSpec,
) bool {
	if file.PartitionData == nil {
		// No partition data - include the file
		return true
	}

	for _, filter := range filters {
		partValue, ok := file.PartitionData[filter.FieldName]
		if !ok {
			// Partition field not in file's partition data
			continue
		}

		if !p.evaluatePartitionFilter(partValue, filter) {
			return false
		}
	}

	return true
}

// evaluatePartitionFilter evaluates a single partition filter against a partition value.
func (p *ScanPlanner) evaluatePartitionFilter(partValue any, filter PartitionFilterExpr) bool {
	// Handle nil partition value
	if partValue == nil {
		return filter.Operator == "!=" || filter.Operator == "IS NOT NULL"
	}

	switch filter.Operator {
	case "=", "==":
		return compareValues(partValue, filter.Value) == 0
	case "!=", "<>":
		return compareValues(partValue, filter.Value) != 0
	case "<":
		return compareValues(partValue, filter.Value) < 0
	case "<=":
		return compareValues(partValue, filter.Value) <= 0
	case ">":
		return compareValues(partValue, filter.Value) > 0
	case ">=":
		return compareValues(partValue, filter.Value) >= 0
	case "IN":
		if values, ok := filter.Value.([]any); ok {
			for _, v := range values {
				if compareValues(partValue, v) == 0 {
					return true
				}
			}
			return false
		}
		return false
	default:
		// Unknown operator - don't prune
		return true
	}
}

// compareValues compares two values and returns -1, 0, or 1.
func compareValues(a, b any) int {
	// Handle same type comparisons
	switch av := a.(type) {
	case int:
		if bv, ok := b.(int); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case int32:
		if bv, ok := b.(int32); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case int64:
		if bv, ok := b.(int64); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case float64:
		if bv, ok := b.(float64); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	}

	// Default: convert to string for comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

// applyColumnStatsPruning filters files based on column statistics (min/max bounds).
func (p *ScanPlanner) applyColumnStatsPruning(files []*DataFile, filters []ColumnFilterExpr) []*DataFile {
	if len(filters) == 0 {
		return files
	}

	schema := p.metadata.CurrentSchema()
	if schema == nil {
		return files
	}

	result := make([]*DataFile, 0, len(files))
	for _, file := range files {
		if p.fileMatchesColumnFilters(file, filters, schema) {
			result = append(result, file)
		}
	}

	return result
}

// fileMatchesColumnFilters checks if a file's column stats might match the filters.
func (p *ScanPlanner) fileMatchesColumnFilters(file *DataFile, filters []ColumnFilterExpr, schema any) bool {
	for _, filter := range filters {
		// Find column ID by name
		colID := p.findColumnID(filter.ColumnName)
		if colID < 0 {
			continue
		}

		// Check if we can prune based on min/max bounds
		if file.LowerBounds != nil && file.UpperBounds != nil {
			lowerBound := file.LowerBounds[colID]
			upperBound := file.UpperBounds[colID]

			if !p.boundsMatchFilter(lowerBound, upperBound, filter) {
				return false
			}
		}

		// Check null counts for IS NOT NULL filter
		if filter.Operator == "IS NOT NULL" && file.NullValueCounts != nil {
			if nullCount, ok := file.NullValueCounts[colID]; ok {
				if valueCount, ok := file.ValueCounts[colID]; ok {
					if nullCount == valueCount {
						// All values are null - doesn't match IS NOT NULL
						return false
					}
				}
			}
		}
	}

	return true
}

// findColumnID finds the column ID for a column name in the current schema.
func (p *ScanPlanner) findColumnID(columnName string) int {
	schema := p.metadata.CurrentSchema()
	if schema == nil {
		return -1
	}

	for _, field := range schema.Fields() {
		if field.Name == columnName {
			return field.ID
		}
	}

	return -1
}

// boundsMatchFilter checks if min/max bounds might match a filter.
// Returns true if we cannot prune (might match), false if we can prune (definitely doesn't match).
func (p *ScanPlanner) boundsMatchFilter(lowerBound, upperBound []byte, filter ColumnFilterExpr) bool {
	// For now, skip complex bound checking - this requires proper deserialization
	// of the binary bounds based on column type.
	// TODO: Implement proper bound deserialization and comparison.
	return true
}

// calculateColumnProjection calculates which columns to read.
func (p *ScanPlanner) calculateColumnProjection(selectedColumns []string) ([]ColumnInfo, error) {
	schema := p.metadata.CurrentSchema()
	if schema == nil {
		return []ColumnInfo{}, nil
	}

	if len(selectedColumns) == 0 || (len(selectedColumns) == 1 && selectedColumns[0] == "*") {
		// Select all columns
		return p.schemaMapper.MapSchemaToColumnInfo(schema)
	}

	// Select specific columns
	return p.schemaMapper.ProjectSchema(schema, selectedColumns)
}

// calculateEstimatedRows calculates the estimated number of rows from data files.
func (p *ScanPlanner) calculateEstimatedRows(files []*DataFile) int64 {
	var total int64
	for _, file := range files {
		total += file.RecordCount
	}
	return total
}

// calculateTotalRows calculates the total row count from all files.
func (p *ScanPlanner) calculateTotalRows(files []*DataFile) int64 {
	var total int64
	for _, file := range files {
		total += file.RecordCount
	}
	return total
}

// FileScanTask represents a task to scan a single data file.
type FileScanTask struct {
	// DataFile is the file to scan.
	DataFile *DataFile
	// Columns is the columns to read.
	Columns []ColumnInfo
	// Start is the starting row offset (for split tasks).
	Start int64
	// Length is the number of rows to read (0 = all).
	Length int64
	// ResidualFilter contains filters that couldn't be pushed to storage.
	ResidualFilter []ColumnFilterExpr
}

// CreateFileTasks creates scan tasks for each file in the plan.
func (plan *ScanPlan) CreateFileTasks() []FileScanTask {
	tasks := make([]FileScanTask, 0, len(plan.DataFiles))

	for _, file := range plan.DataFiles {
		tasks = append(tasks, FileScanTask{
			DataFile: file,
			Columns:  plan.ColumnProjection,
			Start:    0,
			Length:   0, // All rows
		})
	}

	return tasks
}

// SplitFileTasks splits large files into multiple tasks based on split offsets.
func (plan *ScanPlan) SplitFileTasks(maxRowsPerTask int64) []FileScanTask {
	tasks := make([]FileScanTask, 0)

	for _, file := range plan.DataFiles {
		if maxRowsPerTask <= 0 || file.RecordCount <= maxRowsPerTask || len(file.SplitOffsets) == 0 {
			// Single task for small files or no split info
			tasks = append(tasks, FileScanTask{
				DataFile: file,
				Columns:  plan.ColumnProjection,
				Start:    0,
				Length:   0,
			})
			continue
		}

		// Create tasks based on split offsets
		offsets := append([]int64{0}, file.SplitOffsets...)
		for i := 0; i < len(offsets); i++ {
			start := offsets[i]
			var length int64
			if i+1 < len(offsets) {
				length = offsets[i+1] - start
			} else {
				length = 0 // Read to end
			}

			tasks = append(tasks, FileScanTask{
				DataFile: file,
				Columns:  plan.ColumnProjection,
				Start:    start,
				Length:   length,
			})
		}
	}

	return tasks
}
