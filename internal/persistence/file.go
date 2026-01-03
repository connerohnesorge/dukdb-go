package persistence

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// File format constants
const (
	RowGroupMagic = "ROWG"
)

// FileFormat represents the detected file format type
type FileFormat int

const (
	// FormatUnknown indicates the file format could not be determined
	FormatUnknown FileFormat = iota
	// FormatDuckDB indicates a DuckDB-compatible format (DUCK magic at offset 8)
	FormatDuckDB
)

// String returns a human-readable name for the file format
func (f FileFormat) String() string {
	switch f {
	case FormatDuckDB:
		return "DuckDB"
	case FormatUnknown:
		return "Unknown"
	default:
		return "Unknown"
	}
}

var (
	ErrInvalidMagic       = errors.New("invalid magic number")
	ErrUnsupportedVersion = errors.New("unsupported file format version")
	ErrChecksumMismatch   = errors.New("checksum mismatch")
	ErrCorruptedFile      = errors.New("corrupted file")
	ErrUnknownFormat      = errors.New("unknown file format")
)

type BlockInfo struct {
	TableName  string
	RowGroupID int
	Offset     int64
	Size       int64
	Checksum   [32]byte
}

type Metadata struct {
	Version        uint64
	CatalogOffset  int64
	BlockIndex     []BlockInfo
	IsDuckDBFormat bool
	MainHeader     *MainHeader
	ActiveHeader   *DatabaseHeader
}

type FileManager struct {
	path       string
	file       *os.File
	metadata   *Metadata
	blocks     []BlockInfo
	dataOffset int64
}

// DetectFileFormat detects the file format by reading the magic number.
// It checks for DuckDB format (DUCK at offset 8).
// Returns FormatUnknown if the format is not detected.
func DetectFileFormat(path string) (FileFormat, error) {
	file, err := os.Open(path)
	if err != nil {
		return FormatUnknown, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	// Read 12 bytes to check magic number at offset 8
	buf := make([]byte, 12)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return FormatUnknown, fmt.Errorf("failed to read header: %w", err)
	}

	// File too small to have DuckDB magic number (needs at least 12 bytes)
	if n < 12 {
		return FormatUnknown, nil
	}

	// Check for DuckDB magic number at offset 8 (4 bytes: DUCK)
	if string(buf[8:12]) == DuckDBMagicNumber {
		return FormatDuckDB, nil
	}

	return FormatUnknown, nil
}

// DetectFormat is an alias for DetectFileFormat for backwards compatibility
func DetectFormat(path string) (FileFormat, error) {
	return DetectFileFormat(path)
}

func CreateFile(path string) (*FileManager, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	fm := &FileManager{
		path:     path,
		file:     file,
		metadata: &Metadata{
			Version:        DuckDBVersion,
			BlockIndex:     make([]BlockInfo, 0),
			IsDuckDBFormat: true,
		},
		blocks:     make([]BlockInfo, 0),
		dataOffset: int64(DuckDBHeaderSize * 3),
	}

	// Create and write main header
	mainHeader := &MainHeader{}
	copy(mainHeader.Magic[:], DuckDBMagicNumber)
	mainHeader.Version = DuckDBVersion

	// Write main header at offset 0
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := writeMainHeader(file, mainHeader); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}

	// Write initial database header at offset DuckDBHeaderSize
	initialHeader := &DatabaseHeader{
		Iteration:                  1,
		MetaBlock:                  0,
		FreeList:                   0,
		BlockCount:                 0,
		BlockAllocSize:             4096,
		VectorSize:                 1024,
		SerializationCompatibility: 1,
	}
	if err := WriteDatabaseHeader(file, initialHeader); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}

	// Write second database header (copy of first) at offset DuckDBHeaderSize * 2
	if err := WriteDatabaseHeader(file, initialHeader); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}

	fm.metadata.MainHeader = mainHeader
	fm.metadata.ActiveHeader = initialHeader

	if err := file.Truncate(int64(DuckDBHeaderSize * 3)); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}

	return fm, nil
}

func OpenFile(path string) (*FileManager, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	fm := &FileManager{
		path:   path,
		file:   file,
		blocks: make([]BlockInfo, 0),
	}

	// Read first 12 bytes to check magic number
	buf := make([]byte, 12)
	if _, err := io.ReadFull(file, buf); err != nil {
		_ = file.Close()
		return nil, err
	}

	// Check for DuckDB magic number at offset 8
	if string(buf[8:12]) != DuckDBMagicNumber {
		_ = file.Close()
		return nil, ErrInvalidMagic
	}

	// Read main header
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	mainH, err := ReadMainHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	// Read both database headers
	if _, err := file.Seek(int64(DuckDBHeaderSize), io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	h1, errH1 := ReadDatabaseHeader(file, mainH.Version)

	if _, err := file.Seek(int64(DuckDBHeaderSize)*2, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	h2, errH2 := ReadDatabaseHeader(file, mainH.Version)

	// Choose the active header
	var activeH *DatabaseHeader
	if errH1 == nil && errH2 == nil {
		if h1.Iteration > h2.Iteration {
			activeH = h1
		} else {
			activeH = h2
		}
	} else if errH1 == nil {
		activeH = h1
	} else if errH2 == nil {
		activeH = h2
	} else {
		_ = file.Close()
		return nil, errors.New("no valid database header found")
	}

	fm.metadata = &Metadata{
		Version:        mainH.Version,
		IsDuckDBFormat: true,
		MainHeader:     mainH,
		ActiveHeader:   activeH,
	}
	fm.dataOffset = int64(DuckDBHeaderSize * 3)

	return fm, nil
}

func (fm *FileManager) Close() error {
	if fm.file != nil {
		return fm.file.Close()
	}
	return nil
}

func (fm *FileManager) Path() string {
	return fm.path
}

func (fm *FileManager) DataBlocks() []BlockInfo {
	return fm.blocks
}


func (fm *FileManager) WriteCatalog(data []byte) error {
	// Write catalog to meta block location
	if fm.metadata.ActiveHeader == nil {
		return ErrCorruptedFile
	}

	// Write catalog at the current data offset
	if _, err := fm.file.Seek(fm.dataOffset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to catalog position: %w", err)
	}

	// Store the catalog offset in MetaBlock
	fm.metadata.ActiveHeader.MetaBlock = uint64(fm.dataOffset)

	if _, err := fm.file.Write(data); err != nil {
		return fmt.Errorf("failed to write catalog: %w", err)
	}

	fm.dataOffset += int64(len(data))

	return nil
}

func (fm *FileManager) ReadCatalog() ([]byte, error) {
	if fm.metadata.ActiveHeader == nil || fm.metadata.ActiveHeader.MetaBlock == 0 {
		return nil, ErrCorruptedFile
	}
	catalogOffset := int64(fm.metadata.ActiveHeader.MetaBlock)

	if _, err := fm.file.Seek(catalogOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to catalog: %w", err)
	}

	// Read catalog from MetaBlock offset until EOF
	stat, err := fm.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := stat.Size()

	remaining := fileSize - catalogOffset
	if remaining <= 0 {
		return nil, ErrCorruptedFile
	}

	data := make([]byte, remaining)
	if _, err := io.ReadFull(fm.file, data); err != nil {
		return nil, fmt.Errorf("failed to read catalog: %w", err)
	}

	return data, nil
}

func (fm *FileManager) WriteBlock(tableName string, rgID int, data []byte) error {
	// DuckDB format uses its own storage mechanism
	// This method is kept for backward compatibility but does nothing
	return nil
}

func (fm *FileManager) ReadBlock(info BlockInfo) ([]byte, error) {
	// DuckDB format uses its own storage mechanism
	// This method is kept for backward compatibility but returns nil
	return nil, nil
}


func (fm *FileManager) Finalize() error {
	// Update the active header and write both headers
	if fm.metadata.ActiveHeader == nil {
		return ErrCorruptedFile
	}

	// Increment iteration for the active header
	fm.metadata.ActiveHeader.Iteration++

	// Write the updated active header to the appropriate slot
	// Database headers are at Block 1 (4096) and Block 2 (8192)
	// Iteration N goes to slot (N-1) % 2
	slotIndex := (fm.metadata.ActiveHeader.Iteration - 1) % 2
	headerOffset := int64(DuckDBHeaderSize) + int64(DuckDBHeaderSize)*int64(slotIndex)

	if _, err := fm.file.Seek(headerOffset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to header: %w", err)
	}

	if err := WriteDatabaseHeader(fm.file, fm.metadata.ActiveHeader); err != nil {
		return fmt.Errorf("failed to write database header: %w", err)
	}

	return fm.file.Sync()
}

func VerifyFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close file: %w", closeErr)
		}
	}()

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	// Verify minimum file size
	if fileSize < DuckDBHeaderSize*3 {
		return ErrCorruptedFile
	}

	// Read first 12 bytes to check magic number
	magicBuf := make([]byte, 12)
	if _, err := io.ReadFull(file, magicBuf); err != nil {
		return err
	}

	// Check for DuckDB format
	if string(magicBuf[8:]) != DuckDBMagicNumber {
		return ErrInvalidMagic
	}

	// Seek back to beginning before reading full header
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to header: %w", err)
	}

	// Read and verify main header
	mainH, err := ReadMainHeader(file)
	if err != nil {
		return err
	}

	// Verify at least one database header is valid
	_, err = file.Seek(DuckDBHeaderSize, io.SeekStart)
	if err != nil {
		return err
	}

	_, errH1 := ReadDatabaseHeader(file, mainH.Version)
	if errH1 != nil {
		// Try second header
		_, err = file.Seek(DuckDBHeaderSize*2, io.SeekStart)
		if err != nil {
			return err
		}
		_, errH2 := ReadDatabaseHeader(file, mainH.Version)
		if errH2 != nil {
			return errors.New("no valid database header found")
		}
	}

	return nil
}
