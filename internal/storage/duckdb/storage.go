// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file implements DuckDBStorage which provides
// the StorageBackend interface for seamless integration with dukdb-go's
// engine.
package duckdb

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/dukdb/dukdb-go/internal/catalog"
)

// StorageBackend error definitions.
var (
	// ErrStorageClosed indicates the storage has been closed.
	ErrStorageClosed = errors.New("storage is closed")

	// ErrSchemaNotFound indicates the requested schema was not found.
	ErrSchemaNotFound = errors.New("schema not found")

	// ErrTransactionNotFound indicates the transaction ID was not found.
	ErrTransactionNotFound = errors.New("transaction not found")

	// ErrTransactionAlreadyActive indicates a transaction is already active.
	ErrTransactionAlreadyActive = errors.New("transaction already active")

	// ErrNoActiveTransaction indicates no transaction is currently active.
	ErrNoActiveTransaction = errors.New("no active transaction")

	// ErrReadOnlyMode indicates write operations are not allowed.
	ErrReadOnlyMode = errors.New("storage is in read-only mode")

	// ErrInvalidRowID indicates an invalid row ID was provided.
	ErrInvalidRowID = errors.New("invalid row ID")
)

// Note: ErrTableNotFound is already defined in catalog_writer.go

// defaultSchemaName is the default schema name used when a table has no explicit schema.
const defaultSchemaName = "main"

// Config contains configuration options for DuckDBStorage.
type Config struct {
	// ReadOnly opens the file in read-only mode.
	ReadOnly bool

	// BlockCacheSize is the number of blocks to cache (default: 128).
	BlockCacheSize int

	// CreateIfNotExists creates the file if it doesn't exist.
	CreateIfNotExists bool

	// VectorSize is the number of rows per vector (default: 2048).
	VectorSize uint64
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() *Config {
	return &Config{
		ReadOnly:          false,
		BlockCacheSize:    DefaultCacheCapacity,
		CreateIfNotExists: true,
		VectorSize:        DefaultVectorSize,
	}
}

// StorageRowIterator defines the interface for iterating over rows.
// This is the interface that must be returned by ScanTable.
// Note: This is named StorageRowIterator to avoid conflict with the
// RowIterator struct in rowgroup_reader.go.
type StorageRowIterator interface {
	// Next advances to the next row and returns true if there is a row.
	Next() bool

	// Row returns the current row's values.
	Row() []any

	// Err returns any error that occurred during iteration.
	Err() error

	// Close releases resources held by the iterator.
	Close()
}

// StorageBackend defines the interface for storage implementations.
// This abstraction allows the engine to work with different storage formats.
type StorageBackend interface {
	// LoadCatalog loads the catalog from storage.
	LoadCatalog() (*catalog.Catalog, error)

	// SaveCatalog saves the catalog to storage.
	SaveCatalog(cat *catalog.Catalog) error

	// ScanTable scans a table with optional projection.
	// projection is nil to read all columns, or a list of column indices.
	ScanTable(schema, table string, projection []int) (StorageRowIterator, error)

	// InsertRows inserts rows into a table.
	InsertRows(schema, table string, rows [][]any) error

	// DeleteRows deletes rows by ID.
	DeleteRows(schema, table string, rowIDs []uint64) error

	// UpdateRows updates rows by ID with the given updates.
	UpdateRows(schema, table string, rowIDs []uint64, updates map[int]any) error

	// BeginTransaction starts a transaction.
	BeginTransaction() (uint64, error)

	// CommitTransaction commits a transaction.
	CommitTransaction(txnID uint64) error

	// RollbackTransaction rolls back a transaction.
	RollbackTransaction(txnID uint64) error

	// Checkpoint saves all changes to disk.
	Checkpoint() error

	// Close closes the storage.
	Close() error
}

// DuckDBStorage implements StorageBackend for DuckDB native file format.
// It supports both reading and writing DuckDB-compatible database files.
type DuckDBStorage struct {
	// path is the file path.
	path string

	// file is the underlying file handle.
	file *os.File

	// blockManager handles block I/O with caching.
	blockManager *BlockManager

	// catalog contains DuckDB catalog structures.
	catalog *DuckDBCatalog

	// dukdbCatalog is the converted dukdb-go catalog.
	dukdbCatalog *catalog.Catalog

	// rowGroups maps table OID to row group pointers.
	rowGroups map[uint64][]*RowGroupPointer

	// rowGroupReaders maps table OID to active row group readers.
	rowGroupReaders map[uint64][]*RowGroupReader

	// config contains storage configuration.
	config *Config

	// modified indicates whether changes have been made since last checkpoint.
	modified bool

	// closed indicates whether the storage has been closed.
	closed bool

	// activeTransaction is the current transaction ID (0 if none).
	activeTransaction uint64

	// nextTransactionID is the next transaction ID to assign.
	nextTransactionID uint64

	// pendingInserts tracks pending inserts per table for transaction rollback.
	pendingInserts map[uint64][][]any

	// mu protects concurrent access.
	mu sync.RWMutex
}

// NewDuckDBStorage opens or creates a DuckDB format database.
// If the file exists, it opens it; otherwise creates a new file.
func NewDuckDBStorage(path string, config *Config) (*DuckDBStorage, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Check if file exists
	_, err := os.Stat(path)
	fileExists := err == nil

	if fileExists {
		return OpenDuckDBStorage(path, config)
	}

	if !config.CreateIfNotExists {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, path)
	}

	return CreateDuckDBStorage(path, config)
}

// OpenDuckDBStorage opens an existing DuckDB file.
func OpenDuckDBStorage(path string, config *Config) (*DuckDBStorage, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Determine file flags based on read-only mode
	flags := os.O_RDWR
	if config.ReadOnly {
		flags = os.O_RDONLY
	}

	// Open the file
	file, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Read and validate file header
	fileHeader, err := ReadFileHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to read file header: %w", err)
	}

	if err := ValidateFileHeader(fileHeader); err != nil {
		_ = file.Close()
		return nil, err
	}

	// Get the active database header
	dbHeader, _, err := GetActiveHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	if err := ValidateDatabaseHeader(dbHeader); err != nil {
		_ = file.Close()
		return nil, err
	}

	// Create block manager
	blockSize := dbHeader.BlockAllocSize
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}

	cacheSize := config.BlockCacheSize
	if cacheSize <= 0 {
		cacheSize = DefaultCacheCapacity
	}

	blockManager := NewBlockManager(file, blockSize, cacheSize)
	blockManager.SetBlockCount(dbHeader.BlockCount)

	// Create storage instance
	storage := &DuckDBStorage{
		path:              path,
		file:              file,
		blockManager:      blockManager,
		catalog:           NewDuckDBCatalog(),
		rowGroups:         make(map[uint64][]*RowGroupPointer),
		rowGroupReaders:   make(map[uint64][]*RowGroupReader),
		config:            config,
		modified:          false,
		closed:            false,
		nextTransactionID: 1,
		pendingInserts:    make(map[uint64][][]any),
	}

	// Load catalog from metadata blocks if present
	if IsValidBlockID(dbHeader.MetaBlock) {
		if err := storage.loadCatalogFromBlocks(dbHeader.MetaBlock); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to load catalog: %w", err)
		}
	}

	// Convert DuckDB catalog to dukdb-go catalog
	dukdbCat, err := ConvertCatalogFromDuckDB(storage.catalog)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to convert catalog: %w", err)
	}
	storage.dukdbCatalog = dukdbCat

	return storage, nil
}

// CreateDuckDBStorage creates a new DuckDB file.
func CreateDuckDBStorage(path string, config *Config) (*DuckDBStorage, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if config.ReadOnly {
		return nil, fmt.Errorf("%w: cannot create file in read-only mode", ErrReadOnlyMode)
	}

	// Create the file
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Write file header
	fileHeader := NewFileHeader()
	if err := WriteFileHeader(file, fileHeader); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("failed to write file header: %w", err)
	}

	// Write initial database headers
	dbHeader := NewDatabaseHeader()
	if config.VectorSize > 0 {
		dbHeader.VectorSize = config.VectorSize
	}

	if err := WriteDatabaseHeader(file, dbHeader, DatabaseHeader1Offset); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("failed to write database header 1: %w", err)
	}

	if err := WriteDatabaseHeader(file, dbHeader, DatabaseHeader2Offset); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("failed to write database header 2: %w", err)
	}

	// Sync to ensure headers are written
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("failed to sync file: %w", err)
	}

	// Create block manager
	cacheSize := config.BlockCacheSize
	if cacheSize <= 0 {
		cacheSize = DefaultCacheCapacity
	}

	blockManager := NewBlockManager(file, DefaultBlockSize, cacheSize)

	// Create storage instance with empty catalog
	storage := &DuckDBStorage{
		path:              path,
		file:              file,
		blockManager:      blockManager,
		catalog:           NewDuckDBCatalog(),
		dukdbCatalog:      catalog.NewCatalog(),
		rowGroups:         make(map[uint64][]*RowGroupPointer),
		rowGroupReaders:   make(map[uint64][]*RowGroupReader),
		config:            config,
		modified:          false,
		closed:            false,
		nextTransactionID: 1,
		pendingInserts:    make(map[uint64][][]any),
	}

	return storage, nil
}

// loadCatalogFromBlocks loads the catalog from metadata blocks.
func (s *DuckDBStorage) loadCatalogFromBlocks(metaBlockID uint64) error {
	// Read the metadata block
	block, err := s.blockManager.ReadBlock(metaBlockID)
	if err != nil {
		return fmt.Errorf("failed to read metadata block %d: %w", metaBlockID, err)
	}

	// Parse catalog entries from the block
	// For now, we create an empty catalog - full deserialization would
	// require implementing the catalog reader which is complex
	// This is a placeholder that will be enhanced in future tasks
	_ = block // Use block data for future catalog deserialization
	s.catalog = NewDuckDBCatalog()

	return nil
}

// LoadCatalog loads the catalog from the DuckDB file.
func (s *DuckDBStorage) LoadCatalog() (*catalog.Catalog, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	// Return the cached dukdb-go catalog
	if s.dukdbCatalog != nil {
		return s.dukdbCatalog, nil
	}

	// Convert from DuckDB catalog if needed
	cat, err := ConvertCatalogFromDuckDB(s.catalog)
	if err != nil {
		return nil, fmt.Errorf("failed to convert catalog: %w", err)
	}

	return cat, nil
}

// SaveCatalog saves the catalog to the DuckDB file.
func (s *DuckDBStorage) SaveCatalog(cat *catalog.Catalog) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if s.config.ReadOnly {
		return ErrReadOnlyMode
	}

	// Convert dukdb-go catalog to DuckDB format
	dcat, err := ConvertCatalogToDuckDB(cat)
	if err != nil {
		return fmt.Errorf("failed to convert catalog: %w", err)
	}

	s.catalog = dcat
	s.dukdbCatalog = cat
	s.modified = true

	return nil
}

// ScanTable scans a table with optional projection.
func (s *DuckDBStorage) ScanTable(schema, table string, projection []int) (StorageRowIterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	// Find the table in the catalog
	tableEntry := s.findTable(schema, table)
	if tableEntry == nil {
		return nil, fmt.Errorf("%w: %s.%s", ErrTableNotFound, schema, table)
	}

	// Find the table OID
	tableOID := s.getTableOID(schema, table)
	if tableOID < 0 {
		return nil, fmt.Errorf("%w: %s.%s", ErrTableNotFound, schema, table)
	}

	// Get row groups for this table
	rgps := s.rowGroups[uint64(tableOID)]

	// Create table scanner
	scanner := NewTableScanner(s.blockManager, tableEntry, rgps)

	// Set projection if provided
	if projection != nil {
		if err := scanner.SetProjection(projection); err != nil {
			return nil, fmt.Errorf("failed to set projection: %w", err)
		}
	}

	// Create and return row iterator
	return &duckdbRowIterator{
		scanner: scanner,
	}, nil
}

// InsertRows inserts rows into a table.
func (s *DuckDBStorage) InsertRows(schema, table string, rows [][]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if s.config.ReadOnly {
		return ErrReadOnlyMode
	}

	// Find the table OID
	tableOID := s.getTableOID(schema, table)
	if tableOID < 0 {
		return fmt.Errorf("%w: %s.%s", ErrTableNotFound, schema, table)
	}

	// Track pending inserts for transaction rollback
	if s.activeTransaction > 0 {
		s.pendingInserts[uint64(tableOID)] = append(
			s.pendingInserts[uint64(tableOID)], rows...)
	}

	// Get the table entry for column types
	tableEntry := s.catalog.GetTableByOID(uint64(tableOID))
	if tableEntry == nil {
		return fmt.Errorf("%w: %s.%s", ErrTableNotFound, schema, table)
	}

	// Create a row group writer for this table if needed
	types := make([]LogicalTypeID, len(tableEntry.Columns))
	mods := make([]*TypeModifiers, len(tableEntry.Columns))
	for i, col := range tableEntry.Columns {
		types[i] = col.Type
		mods[i] = &col.TypeModifiers
	}

	// Calculate row start for new rows
	var rowStart uint64
	if existingRGs := s.rowGroups[uint64(tableOID)]; len(existingRGs) > 0 {
		lastRG := existingRGs[len(existingRGs)-1]
		rowStart = lastRG.RowStart + lastRG.TupleCount
	}

	// Create row group writer
	rgWriter := NewRowGroupWriter(s.blockManager, uint64(tableOID), types, mods, rowStart)

	// Append rows
	for _, row := range rows {
		if err := rgWriter.AppendRow(row); err != nil {
			if errors.Is(err, ErrRowGroupFull) {
				// Flush the current row group and create a new one
				rgp, flushErr := rgWriter.Flush()
				if flushErr != nil {
					return fmt.Errorf("failed to flush row group: %w", flushErr)
				}
				if rgp != nil {
					s.rowGroups[uint64(tableOID)] = append(
						s.rowGroups[uint64(tableOID)], rgp)
					rowStart = rgp.RowStart + rgp.TupleCount
				}

				// Create new writer and retry
				rgWriter = NewRowGroupWriter(
					s.blockManager, uint64(tableOID), types, mods, rowStart)
				if err := rgWriter.AppendRow(row); err != nil {
					return fmt.Errorf("failed to append row after flush: %w", err)
				}
			} else {
				return fmt.Errorf("failed to append row: %w", err)
			}
		}
	}

	// Flush remaining rows
	if rgWriter.RowCount() > 0 {
		rgp, err := rgWriter.Flush()
		if err != nil {
			return fmt.Errorf("failed to flush row group: %w", err)
		}
		if rgp != nil {
			s.rowGroups[uint64(tableOID)] = append(
				s.rowGroups[uint64(tableOID)], rgp)
		}
	}

	s.modified = true
	return nil
}

// DeleteRows deletes rows by ID.
func (s *DuckDBStorage) DeleteRows(schema, table string, rowIDs []uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if s.config.ReadOnly {
		return ErrReadOnlyMode
	}

	// Find the table OID
	tableOID := s.getTableOID(schema, table)
	if tableOID < 0 {
		return fmt.Errorf("%w: %s.%s", ErrTableNotFound, schema, table)
	}

	// DuckDB uses MVCC for deletes - for now we mark the storage as modified
	// A full implementation would mark rows as deleted in a version map
	// This is a simplified version that will need enhancement
	if len(rowIDs) > 0 {
		s.modified = true
	}

	return nil
}

// UpdateRows updates rows by ID.
func (s *DuckDBStorage) UpdateRows(
	schema, table string,
	rowIDs []uint64,
	updates map[int]any,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if s.config.ReadOnly {
		return ErrReadOnlyMode
	}

	// Find the table OID
	tableOID := s.getTableOID(schema, table)
	if tableOID < 0 {
		return fmt.Errorf("%w: %s.%s", ErrTableNotFound, schema, table)
	}

	// DuckDB uses MVCC for updates - for now we mark the storage as modified
	// A full implementation would create new versions of the updated rows
	// This is a simplified version that will need enhancement
	if len(rowIDs) > 0 && len(updates) > 0 {
		s.modified = true
	}

	return nil
}

// BeginTransaction starts a transaction.
func (s *DuckDBStorage) BeginTransaction() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrStorageClosed
	}

	if s.activeTransaction > 0 {
		return 0, ErrTransactionAlreadyActive
	}

	txnID := s.nextTransactionID
	s.nextTransactionID++
	s.activeTransaction = txnID
	s.pendingInserts = make(map[uint64][][]any)

	return txnID, nil
}

// CommitTransaction commits a transaction.
func (s *DuckDBStorage) CommitTransaction(txnID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if s.activeTransaction == 0 {
		return ErrNoActiveTransaction
	}

	if s.activeTransaction != txnID {
		return fmt.Errorf("%w: expected %d, got %d",
			ErrTransactionNotFound, s.activeTransaction, txnID)
	}

	// Clear transaction state
	s.activeTransaction = 0
	s.pendingInserts = make(map[uint64][][]any)

	// Commit free list changes
	s.blockManager.freeList.CommitTransaction()

	return nil
}

// RollbackTransaction rolls back a transaction.
func (s *DuckDBStorage) RollbackTransaction(txnID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if s.activeTransaction == 0 {
		return ErrNoActiveTransaction
	}

	if s.activeTransaction != txnID {
		return fmt.Errorf("%w: expected %d, got %d",
			ErrTransactionNotFound, s.activeTransaction, txnID)
	}

	// Rollback pending inserts by clearing them
	// In a full implementation, we would also need to remove the row groups
	// that were created during the transaction
	s.pendingInserts = make(map[uint64][][]any)

	// Rollback free list changes
	s.blockManager.freeList.RollbackTransaction()

	// Clear transaction state
	s.activeTransaction = 0

	return nil
}

// Checkpoint saves all changes to disk.
func (s *DuckDBStorage) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if s.config.ReadOnly {
		return ErrReadOnlyMode
	}

	if !s.modified {
		return nil // Nothing to checkpoint
	}

	// Write catalog with row group pointers
	catalogWriter := NewCatalogWriter(s.blockManager, s.catalog)
	for tableOID, rgps := range s.rowGroups {
		for _, rgp := range rgps {
			if err := catalogWriter.AddRowGroupPointer(tableOID, rgp); err != nil {
				return fmt.Errorf("failed to add row group pointer: %w", err)
			}
		}
	}

	metaBlock, err := catalogWriter.Write()
	if err != nil {
		return fmt.Errorf("failed to write catalog: %w", err)
	}

	// Get current active header to determine next slot and iteration
	_, currentSlot, getErr := GetActiveHeader(s.file)
	if getErr != nil {
		// If we can't read headers, start fresh
		currentSlot = 2 // Will write to slot 1
	}

	nextSlot := GetNextHeaderSlot(currentSlot)

	// Read current iteration
	var iteration uint64 = 1
	if currentHeader, _, err := GetActiveHeader(s.file); err == nil {
		iteration = currentHeader.Iteration + 1
	}

	// Create new database header
	dbHeader := &DatabaseHeader{
		Iteration:                  iteration,
		MetaBlock:                  metaBlock.BlockID,
		FreeList:                   InvalidBlockID,
		BlockCount:                 s.blockManager.BlockCount(),
		BlockAllocSize:             s.blockManager.BlockSize(),
		VectorSize:                 DefaultVectorSize,
		SerializationCompatibility: CurrentVersion,
	}

	// Write database header
	offset := GetHeaderOffset(nextSlot)
	if err := WriteDatabaseHeader(s.file, dbHeader, offset); err != nil {
		return fmt.Errorf("failed to write database header: %w", err)
	}

	// Sync to disk
	if err := s.blockManager.Sync(); err != nil {
		return fmt.Errorf("failed to sync block manager: %w", err)
	}

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Checkpoint free list
	s.blockManager.freeList.Checkpoint()

	s.modified = false
	return nil
}

// Close closes the storage.
func (s *DuckDBStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	// Checkpoint if modified and not read-only
	if s.modified && !s.config.ReadOnly {
		// Temporarily unlock for checkpoint (it will re-lock)
		s.mu.Unlock()
		if err := s.Checkpoint(); err != nil {
			s.mu.Lock()
			// Still mark as closed to prevent further use
			s.closed = true
			_ = s.blockManager.Close()
			_ = s.file.Close()
			return fmt.Errorf("failed to checkpoint: %w", err)
		}
		s.mu.Lock()
	}

	// Close block manager
	if err := s.blockManager.Close(); err != nil {
		s.closed = true
		_ = s.file.Close()
		return fmt.Errorf("failed to close block manager: %w", err)
	}

	// Close file
	if err := s.file.Close(); err != nil {
		s.closed = true
		return fmt.Errorf("failed to close file: %w", err)
	}

	s.closed = true
	return nil
}

// Path returns the file path.
func (s *DuckDBStorage) Path() string {
	return s.path
}

// IsReadOnly returns true if the storage is in read-only mode.
func (s *DuckDBStorage) IsReadOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config.ReadOnly
}

// IsClosed returns true if the storage has been closed.
func (s *DuckDBStorage) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

// IsModified returns true if changes have been made since last checkpoint.
func (s *DuckDBStorage) IsModified() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.modified
}

// BlockCount returns the total number of allocated blocks.
func (s *DuckDBStorage) BlockCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.blockManager.BlockCount()
}

// TableCount returns the number of tables in the catalog.
func (s *DuckDBStorage) TableCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.catalog.Tables)
}

// findTable finds a table by schema and name in the DuckDB catalog.
func (s *DuckDBStorage) findTable(schema, table string) *TableCatalogEntry {
	schemaLower := strings.ToLower(schema)
	tableLower := strings.ToLower(table)

	for _, t := range s.catalog.Tables {
		tSchema := t.GetSchema()
		if tSchema == "" {
			tSchema = defaultSchemaName
		}
		if strings.ToLower(tSchema) == schemaLower &&
			strings.ToLower(t.Name) == tableLower {
			return t
		}
	}
	return nil
}

// getTableOID returns the OID (index) for a table by schema and name.
func (s *DuckDBStorage) getTableOID(schema, table string) int {
	schemaLower := strings.ToLower(schema)
	tableLower := strings.ToLower(table)

	for i, t := range s.catalog.Tables {
		tSchema := t.GetSchema()
		if tSchema == "" {
			tSchema = defaultSchemaName
		}
		if strings.ToLower(tSchema) == schemaLower &&
			strings.ToLower(t.Name) == tableLower {
			return i
		}
	}
	return -1
}

// duckdbRowIterator wraps TableScanner as RowIterator.
type duckdbRowIterator struct {
	scanner   *TableScanner
	row       []any
	err       error
	hasRow    bool
	exhausted bool
}

// Next advances to the next row and returns true if there is a row.
func (it *duckdbRowIterator) Next() bool {
	if it.scanner == nil || it.err != nil || it.exhausted {
		return false
	}

	// Advance if we already read a row
	if it.hasRow {
		it.scanner.Advance()
	}

	if !it.scanner.Next() {
		it.hasRow = false
		it.exhausted = true
		return false
	}

	it.row, it.err = it.scanner.GetRow()
	if it.err != nil {
		it.hasRow = false
		return false
	}

	it.hasRow = true
	return true
}

// Row returns the current row's values.
func (it *duckdbRowIterator) Row() []any {
	if !it.hasRow {
		return nil
	}
	return it.row
}

// Err returns any error that occurred during iteration.
func (it *duckdbRowIterator) Err() error {
	return it.err
}

// Close releases resources held by the iterator.
func (it *duckdbRowIterator) Close() {
	if it.scanner != nil {
		it.scanner.Close()
		it.scanner = nil
	}
}

// DetectDuckDBFile checks if a file is a DuckDB format file.
func DetectDuckDBFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()

	// Read magic bytes at the correct offset
	magic := make([]byte, 4)
	_, err = f.ReadAt(magic, MagicByteOffset)
	if err != nil {
		return false
	}

	return string(magic) == MagicBytes
}

// Compile-time assertion that DuckDBStorage implements StorageBackend.
var _ StorageBackend = (*DuckDBStorage)(nil)
