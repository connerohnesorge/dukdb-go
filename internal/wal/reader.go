package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"

	"github.com/coder/quartz"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// ErrChecksumMismatch indicates a checksum validation failure.
var ErrChecksumMismatch = errors.New("WAL checksum mismatch")

// ErrTornWrite indicates a partial/torn write was detected.
var ErrTornWrite = errors.New("WAL torn write detected")

// Reader reads WAL entries from a file.
type Reader struct {
	file     *os.File
	reader   *bufio.Reader
	checksum *crc64.Table
	clock    quartz.Clock
	header   *FileHeader
	position int64
}

// NewReader creates a new WAL reader.
func NewReader(path string, clock quartz.Clock) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	r := &Reader{
		file:     file,
		reader:   bufio.NewReader(file),
		checksum: CRC64Table,
		clock:    clock,
	}

	// Read file header
	r.header = &FileHeader{}
	if err := r.header.Deserialize(r.reader); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to read WAL header: %w", err)
	}
	r.position = HeaderSize

	return r, nil
}

// Header returns the WAL file header.
func (r *Reader) Header() *FileHeader {
	return r.header
}

// ReadEntry reads the next WAL entry.
func (r *Reader) ReadEntry() (Entry, error) {
	// Read entry header
	var size uint64
	if err := binary.Read(r.reader, binary.LittleEndian, &size); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read entry size: %w", err)
	}

	var checksumValue uint64
	if err := binary.Read(r.reader, binary.LittleEndian, &checksumValue); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrTornWrite
		}
		return nil, fmt.Errorf("failed to read entry checksum: %w", err)
	}

	var entryType EntryType
	if err := binary.Read(r.reader, binary.LittleEndian, &entryType); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrTornWrite
		}
		return nil, fmt.Errorf("failed to read entry type: %w", err)
	}

	// Read entry data
	data := make([]byte, size)
	if _, err := io.ReadFull(r.reader, data); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrTornWrite
		}
		return nil, fmt.Errorf("failed to read entry data: %w", err)
	}

	// Verify checksum
	checksum := crc64.New(r.checksum)
	_ = binary.Write(checksum, binary.LittleEndian, size)
	_, _ = checksum.Write([]byte{byte(entryType)})
	_, _ = checksum.Write(data)
	if checksum.Sum64() != checksumValue {
		return nil, fmt.Errorf("%w at position %d", ErrChecksumMismatch, r.position)
	}

	r.position += EntryHeaderSize + int64(size)

	// Deserialize entry based on type
	entry, err := r.deserializeEntry(entryType, data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize entry: %w", err)
	}

	return entry, nil
}

// deserializeEntry deserializes an entry based on its type.
func (r *Reader) deserializeEntry(entryType EntryType, data []byte) (Entry, error) {
	reader := bytes.NewReader(data)

	switch entryType {
	// Catalog entries
	case EntryCreateTable:
		entry := &CreateTableEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryDropTable:
		entry := &DropTableEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryCreateSchema:
		entry := &CreateSchemaEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryDropSchema:
		entry := &DropSchemaEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryCreateView:
		entry := &CreateViewEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryDropView:
		entry := &DropViewEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryCreateIndex:
		entry := &CreateIndexEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryDropIndex:
		entry := &DropIndexEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	// Data entries
	case EntryInsert:
		entry := &InsertEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryDelete:
		entry := &DeleteEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryUpdate:
		entry := &UpdateEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryUseTable:
		entry := &UseTableEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	// Control entries
	case EntryTxnBegin:
		entry := &TxnBeginEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryTxnCommit:
		entry := &TxnCommitEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryCheckpoint:
		entry := &CheckpointEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryFlush:
		entry := &FlushEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	case EntryVersion:
		entry := &VersionEntry{}
		if err := entry.Deserialize(reader); err != nil {
			return nil, err
		}
		return entry, nil

	default:
		return nil, fmt.Errorf("unknown entry type: %d", entryType)
	}
}

// Close closes the WAL reader.
func (r *Reader) Close() error {
	return r.file.Close()
}

// Position returns the current read position.
func (r *Reader) Position() int64 {
	return r.position
}

// ReadAll reads all entries from the WAL.
func (r *Reader) ReadAll() ([]Entry, error) {
	var entries []Entry
	for {
		entry, err := r.ReadEntry()
		if err == io.EOF {
			break
		}
		if errors.Is(err, ErrTornWrite) {
			// Torn write at end is acceptable - just stop reading
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// Recover replays committed transactions from the WAL.
func (r *Reader) Recover(cat *catalog.Catalog, store *storage.Storage) error {
	// Track transactions
	activeTxns := make(map[uint64][]Entry)
	committedTxns := make(map[uint64]bool)
	var catalogEntries []Entry

	// Read all entries
	for {
		entry, err := r.ReadEntry()
		if err == io.EOF {
			break
		}
		if errors.Is(err, ErrTornWrite) {
			// Torn write at end is acceptable - just stop reading
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read WAL entry: %w", err)
		}

		switch e := entry.(type) {
		case *TxnBeginEntry:
			activeTxns[e.TxnID()] = []Entry{}

		case *TxnCommitEntry:
			committedTxns[e.TxnID()] = true

		case TxnEntry:
			// Buffer entry in transaction
			txnID := e.TxnID()
			if _, ok := activeTxns[txnID]; ok {
				activeTxns[txnID] = append(activeTxns[txnID], entry)
			}

		case *CreateTableEntry, *DropTableEntry, *CreateSchemaEntry, *DropSchemaEntry,
			*CreateViewEntry, *DropViewEntry, *CreateIndexEntry, *DropIndexEntry:
			// Catalog entries are applied immediately (not transactional for now)
			catalogEntries = append(catalogEntries, entry)

		case *CheckpointEntry:
			// Checkpoint marker - we can clear committed transactions
			// as they are already persisted

		default:
			// Other entries (flush, version) are control entries
		}
	}

	// Replay catalog entries first
	for _, entry := range catalogEntries {
		if err := r.replayCatalogEntry(entry, cat, store); err != nil {
			return fmt.Errorf("failed to replay catalog entry: %w", err)
		}
	}

	// Replay only committed transactions
	for txnID, entries := range activeTxns {
		if committedTxns[txnID] {
			for _, entry := range entries {
				if err := r.replayDataEntry(entry, cat, store); err != nil {
					return fmt.Errorf("failed to replay data entry: %w", err)
				}
			}
		}
	}

	return nil
}

// replayCatalogEntry replays a catalog entry.
func (r *Reader) replayCatalogEntry(entry Entry, cat *catalog.Catalog, store *storage.Storage) error {
	switch e := entry.(type) {
	case *CreateSchemaEntry:
		_, err := cat.CreateSchema(e.Name)
		if err != nil {
			// Schema may already exist from previous recovery
			return nil
		}

	case *DropSchemaEntry:
		_ = cat.DropSchema(e.Name)

	case *CreateTableEntry:
		columns := make([]*catalog.ColumnDef, len(e.Columns))
		for i, col := range e.Columns {
			columns[i] = catalog.NewColumnDef(col.Name, col.Type).WithNullable(col.Nullable)
		}
		tableDef := catalog.NewTableDef(e.Name, columns)

		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}

		if err := cat.CreateTableInSchema(schemaName, tableDef); err != nil {
			// Table may already exist
			return nil
		}

		// Also create storage table
		columnTypes := make([]dukdb.Type, len(e.Columns))
		for i, col := range e.Columns {
			columnTypes[i] = col.Type
		}
		_, _ = store.CreateTable(e.Name, columnTypes)

	case *DropTableEntry:
		schemaName := e.Schema
		if schemaName == "" {
			schemaName = "main"
		}
		_ = cat.DropTableInSchema(schemaName, e.Name)
		_ = store.DropTable(e.Name)

	case *CreateViewEntry:
		// Views are not yet implemented in the storage layer
		return nil

	case *DropViewEntry:
		// Views are not yet implemented in the storage layer
		return nil

	case *CreateIndexEntry:
		// Indexes are not yet implemented in the storage layer
		return nil

	case *DropIndexEntry:
		// Indexes are not yet implemented in the storage layer
		return nil
	}

	return nil
}

// replayDataEntry replays a data entry.
func (r *Reader) replayDataEntry(entry Entry, cat *catalog.Catalog, store *storage.Storage) error {
	switch e := entry.(type) {
	case *InsertEntry:
		table, ok := store.GetTable(e.Table)
		if !ok {
			return fmt.Errorf("table not found: %s", e.Table)
		}
		for _, row := range e.Values {
			if err := table.AppendRow(row); err != nil {
				return fmt.Errorf("failed to append row: %w", err)
			}
		}

	case *DeleteEntry:
		// Delete is not yet implemented in the storage layer
		// For now, we just ignore deletes during recovery
		return nil

	case *UpdateEntry:
		// Update is not yet implemented in the storage layer
		// For now, we just ignore updates during recovery
		return nil
	}

	return nil
}
