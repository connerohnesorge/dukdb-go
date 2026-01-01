// Package persistence provides database file persistence for dukdb-go.
package persistence

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// File format constants
const (
	// MagicNumber is the file signature for dukdb-go files
	MagicNumber = "DUKDBGO\x00"
	// Version is the current file format version
	Version = uint32(1)
	// HeaderSize is the fixed size of the file header in bytes
	HeaderSize = 64
	// FooterSize is the fixed size of the file footer in bytes
	FooterSize = 32
	// RowGroupMagic is the magic number for row group blocks
	RowGroupMagic = "ROWG"
)

var (
	// ErrInvalidMagic indicates the file has an invalid magic number
	ErrInvalidMagic = errors.New(
		"invalid magic number",
	)
	// ErrUnsupportedVersion indicates the file format version is not supported
	ErrUnsupportedVersion = errors.New(
		"unsupported file format version",
	)
	// ErrChecksumMismatch indicates the file checksum does not match
	ErrChecksumMismatch = errors.New(
		"checksum mismatch",
	)
	// ErrCorruptedFile indicates the file is corrupted
	ErrCorruptedFile = errors.New(
		"corrupted file",
	)
)

// Header represents the database file header (64 bytes)
type Header struct {
	Magic            [8]byte  // "DUKDBGO\x00"
	Version          uint32   // File format version
	Flags            uint32   // Reserved flags
	CatalogOffset    int64    // Offset to catalog data
	BlockIndexOffset int64    // Offset to block index
	BlockCount       uint32   // Number of data blocks
	Reserved         [28]byte // Reserved for future use
}

// Footer represents the database file footer (32 bytes)
type Footer struct {
	Checksum [32]byte // SHA-256 of header + blocks + catalog + block index
}

// BlockInfo contains metadata about a data block
type BlockInfo struct {
	TableName  string   // Table this block belongs to
	RowGroupID int      // Row group index within the table
	Offset     int64    // Byte offset in file
	Size       int64    // Block size in bytes
	Checksum   [32]byte // SHA-256 of block data
}

// Metadata contains the complete file layout information
type Metadata struct {
	Version       uint32
	CatalogOffset int64
	BlockIndex    []BlockInfo
}

// FileManager handles database file I/O
type FileManager struct {
	path       string
	file       *os.File
	header     *Header
	metadata   *Metadata
	blocks     []BlockInfo
	dataOffset int64 // Current write position for data blocks
}

// CreateFile creates a new database file at the given path
func CreateFile(
	path string,
) (*FileManager, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create file: %w",
			err,
		)
	}

	fm := &FileManager{
		path:   path,
		file:   file,
		header: newHeader(),
		metadata: &Metadata{
			Version:    Version,
			BlockIndex: make([]BlockInfo, 0),
		},
		blocks:     make([]BlockInfo, 0),
		dataOffset: HeaderSize, // Data starts after header
	}

	// Write initial header (will be updated during finalize)
	if err := fm.writeHeader(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)

		return nil, err
	}

	return fm, nil
}

// OpenFile opens an existing database file at the given path
func OpenFile(path string) (*FileManager, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to open file: %w",
			err,
		)
	}

	fm := &FileManager{
		path: path,
		file: file,
	}

	// Read and validate header
	header, err := fm.readHeader()
	if err != nil {
		_ = file.Close()

		return nil, err
	}
	fm.header = header

	// Read block index
	blocks, err := fm.readBlockIndex()
	if err != nil {
		_ = file.Close()

		return nil, err
	}
	fm.blocks = blocks

	fm.metadata = &Metadata{
		Version:       header.Version,
		CatalogOffset: header.CatalogOffset,
		BlockIndex:    blocks,
	}

	return fm, nil
}

// Close closes the file
func (fm *FileManager) Close() error {
	if fm.file != nil {
		return fm.file.Close()
	}

	return nil
}

// Path returns the file path
func (fm *FileManager) Path() string {
	return fm.path
}

// DataBlocks returns the list of data block info
func (fm *FileManager) DataBlocks() []BlockInfo {
	return fm.blocks
}

// newHeader creates a new header with default values
func newHeader() *Header {
	h := &Header{
		Version: Version,
	}
	copy(h.Magic[:], MagicNumber)

	return h
}

// writeHeader writes the header to the file
func (fm *FileManager) writeHeader() error {
	if _, err := fm.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf(
			"failed to seek to header: %w",
			err,
		)
	}

	buf := new(bytes.Buffer)

	// Write magic (8 bytes)
	if _, err := buf.Write(fm.header.Magic[:]); err != nil {
		return err
	}

	// Write version (4 bytes, little-endian)
	if err := binary.Write(buf, binary.LittleEndian, fm.header.Version); err != nil {
		return err
	}

	// Write flags (4 bytes, little-endian)
	if err := binary.Write(buf, binary.LittleEndian, fm.header.Flags); err != nil {
		return err
	}

	// Write catalog offset (8 bytes, little-endian)
	if err := binary.Write(buf, binary.LittleEndian, fm.header.CatalogOffset); err != nil {
		return err
	}

	// Write block index offset (8 bytes, little-endian)
	if err := binary.Write(buf, binary.LittleEndian, fm.header.BlockIndexOffset); err != nil {
		return err
	}

	// Write block count (4 bytes, little-endian)
	if err := binary.Write(buf, binary.LittleEndian, fm.header.BlockCount); err != nil {
		return err
	}

	// Write reserved (28 bytes)
	if _, err := buf.Write(fm.header.Reserved[:]); err != nil {
		return err
	}

	// Verify header size
	if buf.Len() != HeaderSize {
		return fmt.Errorf(
			"header size mismatch: got %d, expected %d",
			buf.Len(),
			HeaderSize,
		)
	}

	if _, err := fm.file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf(
			"failed to write header: %w",
			err,
		)
	}

	return nil
}

// readHeader reads and validates the file header
func (fm *FileManager) readHeader() (*Header, error) {
	if _, err := fm.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf(
			"failed to seek to header: %w",
			err,
		)
	}

	headerBuf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(fm.file, headerBuf); err != nil {
		return nil, fmt.Errorf(
			"failed to read header: %w",
			err,
		)
	}

	reader := bytes.NewReader(headerBuf)
	header := &Header{}

	// Read magic
	if _, err := io.ReadFull(reader, header.Magic[:]); err != nil {
		return nil, err
	}
	if string(header.Magic[:]) != MagicNumber {
		return nil, ErrInvalidMagic
	}

	// Read version
	if err := binary.Read(reader, binary.LittleEndian, &header.Version); err != nil {
		return nil, err
	}
	if header.Version > Version {
		return nil, ErrUnsupportedVersion
	}

	// Read flags
	if err := binary.Read(reader, binary.LittleEndian, &header.Flags); err != nil {
		return nil, err
	}

	// Read catalog offset
	if err := binary.Read(reader, binary.LittleEndian, &header.CatalogOffset); err != nil {
		return nil, err
	}

	// Read block index offset
	if err := binary.Read(reader, binary.LittleEndian, &header.BlockIndexOffset); err != nil {
		return nil, err
	}

	// Read block count
	if err := binary.Read(reader, binary.LittleEndian, &header.BlockCount); err != nil {
		return nil, err
	}

	// Read reserved
	if _, err := io.ReadFull(reader, header.Reserved[:]); err != nil {
		return nil, err
	}

	return header, nil
}

// WriteCatalog writes the catalog data to the file
func (fm *FileManager) WriteCatalog(
	data []byte,
) error {
	// Compress catalog data with gzip
	var buf bytes.Buffer
	gzWriter, err := gzip.NewWriterLevel(
		&buf,
		gzip.DefaultCompression,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create gzip writer: %w",
			err,
		)
	}

	if _, err := gzWriter.Write(data); err != nil {
		_ = gzWriter.Close()

		return fmt.Errorf(
			"failed to write catalog data: %w",
			err,
		)
	}

	if err := gzWriter.Close(); err != nil {
		return fmt.Errorf(
			"failed to close gzip writer: %w",
			err,
		)
	}

	// Seek to current data offset (after all blocks)
	if _, err := fm.file.Seek(fm.dataOffset, io.SeekStart); err != nil {
		return fmt.Errorf(
			"failed to seek to catalog position: %w",
			err,
		)
	}

	// Record catalog offset
	fm.header.CatalogOffset = fm.dataOffset

	// Write compressed catalog
	if _, err := fm.file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf(
			"failed to write catalog: %w",
			err,
		)
	}

	// Update data offset
	fm.dataOffset += int64(buf.Len())

	return nil
}

// ReadCatalog reads and decompresses the catalog data from the file
func (fm *FileManager) ReadCatalog() ([]byte, error) {
	if fm.header.CatalogOffset == 0 {
		return nil, ErrCorruptedFile
	}

	if _, err := fm.file.Seek(fm.header.CatalogOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf(
			"failed to seek to catalog: %w",
			err,
		)
	}

	// Calculate catalog size (from catalog offset to block index offset)
	catalogSize := fm.header.BlockIndexOffset - fm.header.CatalogOffset
	if catalogSize <= 0 {
		return nil, ErrCorruptedFile
	}

	compressedData := make([]byte, catalogSize)
	if _, err := io.ReadFull(fm.file, compressedData); err != nil {
		return nil, fmt.Errorf(
			"failed to read catalog: %w",
			err,
		)
	}

	// Decompress
	gzReader, err := gzip.NewReader(
		bytes.NewReader(compressedData),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create gzip reader: %w",
			err,
		)
	}
	defer func() {
		if closeErr := gzReader.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close gzip reader: %w", closeErr)
		}
	}()

	data, err := io.ReadAll(gzReader)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to decompress catalog: %w",
			err,
		)
	}

	return data, nil
}

// WriteBlock writes a data block to the file
func (fm *FileManager) WriteBlock(
	tableName string,
	rgID int,
	data []byte,
) error {
	// Calculate checksum
	checksum := sha256.Sum256(data)

	// Seek to current data offset
	if _, err := fm.file.Seek(fm.dataOffset, io.SeekStart); err != nil {
		return fmt.Errorf(
			"failed to seek to block position: %w",
			err,
		)
	}

	// Write block data
	if _, err := fm.file.Write(data); err != nil {
		return fmt.Errorf(
			"failed to write block: %w",
			err,
		)
	}

	// Record block info
	blockInfo := BlockInfo{
		TableName:  tableName,
		RowGroupID: rgID,
		Offset:     fm.dataOffset,
		Size:       int64(len(data)),
		Checksum:   checksum,
	}
	fm.blocks = append(fm.blocks, blockInfo)

	// Update data offset
	fm.dataOffset += int64(len(data))
	fm.header.BlockCount = uint32(len(fm.blocks))

	return nil
}

// ReadBlock reads a data block from the file
func (fm *FileManager) ReadBlock(
	info BlockInfo,
) ([]byte, error) {
	if _, err := fm.file.Seek(info.Offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf(
			"failed to seek to block: %w",
			err,
		)
	}

	data := make([]byte, info.Size)
	if _, err := io.ReadFull(fm.file, data); err != nil {
		return nil, fmt.Errorf(
			"failed to read block: %w",
			err,
		)
	}

	// Verify checksum
	checksum := sha256.Sum256(data)
	if checksum != info.Checksum {
		return nil, ErrChecksumMismatch
	}

	return data, nil
}

// writeBlockIndex writes the block index to the file
func (fm *FileManager) writeBlockIndex() error {
	// Record block index offset
	fm.header.BlockIndexOffset = fm.dataOffset

	// Seek to current position
	if _, err := fm.file.Seek(fm.dataOffset, io.SeekStart); err != nil {
		return fmt.Errorf(
			"failed to seek to block index position: %w",
			err,
		)
	}

	buf := new(bytes.Buffer)

	// Write entry count
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(fm.blocks))); err != nil {
		return err
	}

	// Write each block entry
	for _, block := range fm.blocks {
		// Write table name (varint length + UTF-8)
		if err := writeString(buf, block.TableName); err != nil {
			return err
		}

		// Write row group ID
		if err := binary.Write(buf, binary.LittleEndian, uint32(block.RowGroupID)); err != nil {
			return err
		}

		// Write offset
		if err := binary.Write(buf, binary.LittleEndian, block.Offset); err != nil {
			return err
		}

		// Write size
		if err := binary.Write(buf, binary.LittleEndian, block.Size); err != nil {
			return err
		}

		// Write checksum
		if _, err := buf.Write(block.Checksum[:]); err != nil {
			return err
		}
	}

	if _, err := fm.file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf(
			"failed to write block index: %w",
			err,
		)
	}

	fm.dataOffset += int64(buf.Len())

	return nil
}

// readBlockIndex reads the block index from the file
func (fm *FileManager) readBlockIndex() ([]BlockInfo, error) {
	if fm.header.BlockIndexOffset == 0 {
		return []BlockInfo{}, nil
	}

	if _, err := fm.file.Seek(fm.header.BlockIndexOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf(
			"failed to seek to block index: %w",
			err,
		)
	}

	// Read entry count
	var count uint32
	if err := binary.Read(fm.file, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf(
			"failed to read block count: %w",
			err,
		)
	}

	blocks := make([]BlockInfo, count)

	for i := range count {
		// Read table name
		tableName, err := readString(fm.file)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to read table name: %w",
				err,
			)
		}
		blocks[i].TableName = tableName

		// Read row group ID
		var rgID uint32
		if err := binary.Read(fm.file, binary.LittleEndian, &rgID); err != nil {
			return nil, fmt.Errorf(
				"failed to read row group ID: %w",
				err,
			)
		}
		blocks[i].RowGroupID = int(rgID)

		// Read offset
		if err := binary.Read(fm.file, binary.LittleEndian, &blocks[i].Offset); err != nil {
			return nil, fmt.Errorf(
				"failed to read block offset: %w",
				err,
			)
		}

		// Read size
		if err := binary.Read(fm.file, binary.LittleEndian, &blocks[i].Size); err != nil {
			return nil, fmt.Errorf(
				"failed to read block size: %w",
				err,
			)
		}

		// Read checksum
		if _, err := io.ReadFull(fm.file, blocks[i].Checksum[:]); err != nil {
			return nil, fmt.Errorf(
				"failed to read block checksum: %w",
				err,
			)
		}
	}

	return blocks, nil
}

// writeFooter writes the footer with the file checksum
func (fm *FileManager) writeFooter(
	checksum [32]byte,
) error {
	if _, err := fm.file.Seek(fm.dataOffset, io.SeekStart); err != nil {
		return fmt.Errorf(
			"failed to seek to footer position: %w",
			err,
		)
	}

	if _, err := fm.file.Write(checksum[:]); err != nil {
		return fmt.Errorf(
			"failed to write footer: %w",
			err,
		)
	}

	return nil
}

// Finalize finalizes the file by writing the block index, footer, and updating the header
func (fm *FileManager) Finalize() error {
	// Write block index
	if err := fm.writeBlockIndex(); err != nil {
		return err
	}

	// Update and rewrite header FIRST (so checksum includes correct header)
	if err := fm.writeHeader(); err != nil {
		return err
	}

	// Calculate checksum of everything before footer (now includes correct header)
	checksum, err := fm.calculateChecksum()
	if err != nil {
		return err
	}

	// Write footer
	if err := fm.writeFooter(checksum); err != nil {
		return err
	}

	// Sync to disk
	return fm.file.Sync()
}

// calculateChecksum calculates SHA-256 of header + data blocks + catalog + block index
func (fm *FileManager) calculateChecksum() ([32]byte, error) {
	h := sha256.New()

	// Seek to beginning
	if _, err := fm.file.Seek(0, io.SeekStart); err != nil {
		return [32]byte{}, err
	}

	// Hash everything from start to current position (before footer)
	if _, err := io.CopyN(h, fm.file, fm.dataOffset); err != nil {
		return [32]byte{}, err
	}

	var checksum [32]byte
	copy(checksum[:], h.Sum(nil))

	return checksum, nil
}

// VerifyFile verifies the integrity of a database file
func VerifyFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf(
			"failed to open file: %w",
			err,
		)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close file: %w", closeErr)
		}
	}()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	if fileSize < HeaderSize+FooterSize {
		return ErrCorruptedFile
	}

	// Read header
	headerBuf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(file, headerBuf); err != nil {
		return err
	}

	// Validate magic
	if string(headerBuf[:8]) != MagicNumber {
		return ErrInvalidMagic
	}

	// Read footer checksum
	if _, err := file.Seek(-FooterSize, io.SeekEnd); err != nil {
		return err
	}

	var storedChecksum [32]byte
	if _, err := io.ReadFull(file, storedChecksum[:]); err != nil {
		return err
	}

	// Calculate checksum of everything before footer
	h := sha256.New()
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	dataSize := fileSize - FooterSize
	if _, err := io.CopyN(h, file, dataSize); err != nil {
		return err
	}

	calculatedChecksum := h.Sum(nil)
	if !bytes.Equal(
		calculatedChecksum,
		storedChecksum[:],
	) {
		return ErrChecksumMismatch
	}

	return nil
}

// writeString writes a string with varint length prefix
func writeString(w io.Writer, s string) error {
	data := []byte(s)
	if err := writeVarint(w, uint64(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)

	return err
}

// readString reads a string with varint length prefix
func readString(r io.Reader) (string, error) {
	length, err := readVarint(r)
	if err != nil {
		return "", err
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}

	return string(data), nil
}

// writeVarint writes a variable-length integer
func writeVarint(w io.Writer, v uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, v)
	_, err := w.Write(buf[:n])

	return err
}

// readVarint reads a variable-length integer
func readVarint(r io.Reader) (uint64, error) {
	var result uint64
	var shift uint
	for {
		b := make([]byte, 1)
		if _, err := r.Read(b); err != nil {
			return 0, err
		}
		result |= uint64(b[0]&0x7F) << shift
		if b[0]&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return 0, errors.New(
				"varint overflow",
			)
		}
	}

	return result, nil
}

// CatalogJSON represents the JSON structure for catalog export/import
type CatalogJSON struct {
	Version int                    `json:"version"`
	Schemas map[string]*SchemaJSON `json:"schemas"`
}

// SchemaJSON represents a schema in JSON format
type SchemaJSON struct {
	Name   string                `json:"name"`
	Tables map[string]*TableJSON `json:"tables"`
}

// TableJSON represents a table in JSON format
type TableJSON struct {
	Name       string       `json:"name"`
	Schema     string       `json:"schema"`
	Columns    []ColumnJSON `json:"columns"`
	PrimaryKey []int        `json:"primary_key,omitempty"`
}

// ColumnJSON represents a column in JSON format
type ColumnJSON struct {
	Name         string    `json:"name"`
	Type         int       `json:"type"`
	Nullable     bool      `json:"nullable"`
	HasDefault   bool      `json:"has_default"`
	DefaultValue any       `json:"default_value,omitempty"`
	TypeInfo     *TypeJSON `json:"type_info,omitempty"`
}

// TypeJSON represents extended type information in JSON format
type TypeJSON struct {
	Precision   int          `json:"precision,omitempty"`
	Scale       int          `json:"scale,omitempty"`
	ElementType *ColumnJSON  `json:"element_type,omitempty"`
	ArraySize   int          `json:"array_size,omitempty"`
	Fields      []ColumnJSON `json:"fields,omitempty"`
	KeyType     *ColumnJSON  `json:"key_type,omitempty"`
	ValueType   *ColumnJSON  `json:"value_type,omitempty"`
	EnumValues  []string     `json:"enum_values,omitempty"`
}

// MarshalCatalog marshals catalog data to JSON bytes
func MarshalCatalog(
	catalog *CatalogJSON,
) ([]byte, error) {
	return json.Marshal(catalog)
}

// UnmarshalCatalog unmarshals JSON bytes to catalog data
func UnmarshalCatalog(
	data []byte,
) (*CatalogJSON, error) {
	var catalog CatalogJSON
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, err
	}

	return &catalog, nil
}
