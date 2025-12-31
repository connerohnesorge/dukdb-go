package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/coder/quartz"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// CheckpointType defines the type of checkpoint.
type CheckpointType int

const (
	// CheckpointFull writes all data to the checkpoint.
	CheckpointFull CheckpointType = iota
	// CheckpointConcurrent allows concurrent reads during checkpoint.
	CheckpointConcurrent
)

// WALAction defines what to do with the WAL after checkpoint.
type WALAction int

const (
	// WALDelete deletes the WAL after checkpoint.
	WALDelete WALAction = iota
	// WALKeep keeps the WAL after checkpoint.
	WALKeep
)

// DefaultCheckpointThreshold is the default WAL size threshold for auto-checkpoint (1GB).
const DefaultCheckpointThreshold uint64 = 1024 * 1024 * 1024

// CheckpointManager manages WAL checkpoints.
type CheckpointManager struct {
	wal       *Writer
	catalog   *catalog.Catalog
	storage   *storage.Storage
	walPath   string
	threshold uint64
	clock     quartz.Clock
	mu        sync.Mutex
}

// NewCheckpointManager creates a new checkpoint manager.
func NewCheckpointManager(
	wal *Writer,
	cat *catalog.Catalog,
	store *storage.Storage,
	clock quartz.Clock,
) *CheckpointManager {
	return &CheckpointManager{
		wal:       wal,
		catalog:   cat,
		storage:   store,
		walPath:   wal.Path(),
		threshold: DefaultCheckpointThreshold,
		clock:     clock,
	}
}

// SetThreshold sets the auto-checkpoint threshold in bytes.
func (cm *CheckpointManager) SetThreshold(bytes uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.threshold = bytes
}

// Threshold returns the current auto-checkpoint threshold.
func (cm *CheckpointManager) Threshold() uint64 {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.threshold
}

// Checkpoint performs a checkpoint, writing all data to a new WAL file.
func (cm *CheckpointManager) Checkpoint() error {
	return cm.CheckpointWithOptions(CheckpointFull, WALDelete)
}

// CheckpointWithOptions performs a checkpoint with the specified options.
func (cm *CheckpointManager) CheckpointWithOptions(cpType CheckpointType, walAction WALAction) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Get current iteration
	iteration := cm.wal.Iteration()

	// Write checkpoint marker to current WAL
	entry := NewCheckpointEntry(iteration+1, cm.clock.Now())
	if err := cm.wal.WriteEntry(entry); err != nil {
		return fmt.Errorf("failed to write checkpoint entry: %w", err)
	}

	// Sync current WAL
	if err := cm.wal.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	// Create checkpoint WAL file
	ckptPath := cm.walPath + ".ckpt"
	ckptWriter, err := NewWriter(ckptPath, cm.clock)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint WAL: %w", err)
	}

	// Write all catalog entries
	if err := cm.writeCatalogCheckpoint(ckptWriter); err != nil {
		_ = ckptWriter.Close()
		_ = os.Remove(ckptPath)

		return fmt.Errorf("failed to write catalog checkpoint: %w", err)
	}

	// Write all data entries
	if err := cm.writeDataCheckpoint(ckptWriter); err != nil {
		_ = ckptWriter.Close()
		_ = os.Remove(ckptPath)

		return fmt.Errorf("failed to write data checkpoint: %w", err)
	}

	// Write checkpoint complete marker
	completeEntry := NewCheckpointEntry(iteration+1, cm.clock.Now())
	if err := ckptWriter.WriteEntry(completeEntry); err != nil {
		_ = ckptWriter.Close()
		_ = os.Remove(ckptPath)

		return fmt.Errorf("failed to write checkpoint complete entry: %w", err)
	}

	// Sync checkpoint WAL
	if err := ckptWriter.Sync(); err != nil {
		_ = ckptWriter.Close()
		_ = os.Remove(ckptPath)

		return fmt.Errorf("failed to sync checkpoint WAL: %w", err)
	}

	// Close checkpoint writer
	if err := ckptWriter.Close(); err != nil {
		_ = os.Remove(ckptPath)

		return fmt.Errorf("failed to close checkpoint WAL: %w", err)
	}

	// Sync directory to ensure file is durable
	if err := syncDir(filepath.Dir(cm.walPath)); err != nil {
		return fmt.Errorf("failed to sync directory: %w", err)
	}

	// Handle WAL action
	if walAction == WALDelete {
		// Close current WAL
		if err := cm.wal.Close(); err != nil {
			return fmt.Errorf("failed to close WAL: %w", err)
		}

		// Remove old WAL
		if err := os.Remove(cm.walPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove old WAL: %w", err)
		}

		// Rename checkpoint WAL to main WAL
		if err := os.Rename(ckptPath, cm.walPath); err != nil {
			return fmt.Errorf("failed to rename checkpoint WAL: %w", err)
		}

		// Reopen WAL
		newWal, err := NewWriter(cm.walPath, cm.clock)
		if err != nil {
			return fmt.Errorf("failed to reopen WAL: %w", err)
		}
		cm.wal = newWal
	}

	// Sync directory again after rename
	if err := syncDir(filepath.Dir(cm.walPath)); err != nil {
		return fmt.Errorf("failed to sync directory after rename: %w", err)
	}

	return nil
}

// MaybeAutoCheckpoint performs a checkpoint if the WAL exceeds the threshold.
func (cm *CheckpointManager) MaybeAutoCheckpoint() error {
	cm.mu.Lock()
	bytesWritten := cm.wal.BytesWritten()
	threshold := cm.threshold
	cm.mu.Unlock()

	if bytesWritten >= threshold {
		return cm.Checkpoint()
	}

	return nil
}

// writeCatalogCheckpoint writes all catalog entries to the checkpoint WAL.
func (cm *CheckpointManager) writeCatalogCheckpoint(w *Writer) error {
	// Write schemas
	// Note: For now, we only support the "main" schema
	// In the future, we should iterate over all schemas

	// Write tables
	for _, tableDef := range cm.catalog.ListTables() {
		columns := make([]ColumnDef, len(tableDef.Columns))
		for i, col := range tableDef.Columns {
			columns[i] = ColumnDef{
				Name:       col.Name,
				Type:       col.Type,
				Nullable:   col.Nullable,
				HasDefault: col.HasDefault,
			}
		}

		entry := &CreateTableEntry{
			Schema:  tableDef.Schema,
			Name:    tableDef.Name,
			Columns: columns,
		}

		if err := w.WriteEntry(entry); err != nil {
			return fmt.Errorf("failed to write table entry: %w", err)
		}
	}

	return nil
}

// writeDataCheckpoint writes all data entries to the checkpoint WAL.
func (cm *CheckpointManager) writeDataCheckpoint(w *Writer) error {
	// Start a pseudo-transaction for the checkpoint
	txnID := uint64(0) // Checkpoint transaction ID

	beginEntry := NewTxnBeginEntry(txnID, cm.clock.Now())
	if err := w.WriteEntry(beginEntry); err != nil {
		return fmt.Errorf("failed to write txn begin entry: %w", err)
	}

	// Write all table data
	for name, table := range cm.storage.Tables() {
		if err := cm.writeTableData(w, name, table, txnID); err != nil {
			return fmt.Errorf("failed to write table data: %w", err)
		}
	}

	commitEntry := NewTxnCommitEntry(txnID, cm.clock.Now())
	if err := w.WriteEntry(commitEntry); err != nil {
		return fmt.Errorf("failed to write txn commit entry: %w", err)
	}

	return nil
}

// writeTableData writes all data for a table to the checkpoint WAL.
func (cm *CheckpointManager) writeTableData(w *Writer, name string, table *storage.Table, txnID uint64) error {
	// Scan all rows in the table
	scanner := table.Scan()
	var allRows [][]any

	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		// Extract rows from chunk
		for row := 0; row < chunk.Count(); row++ {
			values := make([]any, chunk.ColumnCount())
			for col := 0; col < chunk.ColumnCount(); col++ {
				values[col] = chunk.GetValue(row, col)
			}
			allRows = append(allRows, values)
		}
	}

	if len(allRows) == 0 {
		return nil
	}

	// Write insert entry with all rows
	// Note: For large tables, we should batch this into multiple entries
	entry := NewInsertEntry(txnID, "main", name, allRows)
	if err := w.WriteEntry(entry); err != nil {
		return fmt.Errorf("failed to write insert entry: %w", err)
	}

	return nil
}

// syncDir syncs a directory to ensure durability.
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()

	return dir.Sync()
}

// WAL returns the underlying WAL writer.
func (cm *CheckpointManager) WAL() *Writer {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.wal
}

// SetWAL sets the WAL writer (used after recovery).
func (cm *CheckpointManager) SetWAL(wal *Writer) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.wal = wal
	cm.walPath = wal.Path()
}

// Compile-time check that InsertEntry implements TxnEntry
var _ TxnEntry = (*InsertEntry)(nil)
var _ TxnEntry = (*DeleteEntry)(nil)
var _ TxnEntry = (*UpdateEntry)(nil)
var _ TxnEntry = (*TxnBeginEntry)(nil)
var _ TxnEntry = (*TxnCommitEntry)(nil)

// Ensure dukdb import is used
var _ dukdb.Type
