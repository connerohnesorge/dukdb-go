package persistence

import (
	"fmt"
	"io"
	"os"
)

// ReadActiveDatabaseHeader reads the headers from the file and returns the active DatabaseHeader
func ReadActiveDatabaseHeader(path string) (*DatabaseHeader, *MainHeader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("failed to seek to main header: %w", err)
	}
	mainHeader, err := ReadMainHeader(file)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read main header: %w", err)
	}

	if _, err := file.Seek(int64(DuckDBHeaderSize), io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("failed to seek to header 1: %w", err)
	}
	h1, errH1 := ReadDatabaseHeader(file, mainHeader.Version)

	if _, err := file.Seek(int64(DuckDBHeaderSize)*2, io.SeekStart); err != nil {
		// If we can't seek to H2, check if we have H1
		if errH1 == nil {
			return h1, mainHeader, nil
		}
		return nil, nil, fmt.Errorf("failed to seek to header 2: %w", err)
	}
	h2, errH2 := ReadDatabaseHeader(file, mainHeader.Version)

	if errH1 != nil && errH2 != nil {
		return nil, nil, fmt.Errorf("failed to read both database headers: %v, %v", errH1, errH2)
	}

	if errH1 != nil {
		return h2, mainHeader, nil
	}
	if errH2 != nil {
		return h1, mainHeader, nil
	}

	if h1.Iteration > h2.Iteration {
		return h1, mainHeader, nil
	}

	return h2, mainHeader, nil
}

// WriteActiveDatabaseHeader writes the new DatabaseHeader to the inactive block and syncs
func WriteActiveDatabaseHeader(path string, newHeader *DatabaseHeader) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file for writing: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	// Need to read current headers to determine which one is active,
	// so we can overwrite the OTHER one.
	// But `ReadActiveDatabaseHeader` closes the file.
	// We should reuse logic or implement it here.

	// Re-implement read logic briefly to find active index
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	mainHeader, err := ReadMainHeader(file)
	if err != nil {
		return err
	}

	if _, err := file.Seek(int64(DuckDBHeaderSize), io.SeekStart); err != nil {
		return err
	}
	h1, errH1 := ReadDatabaseHeader(file, mainHeader.Version)

	if _, err := file.Seek(int64(DuckDBHeaderSize)*2, io.SeekStart); err != nil {
		return err
	}
	h2, errH2 := ReadDatabaseHeader(file, mainHeader.Version)

	var targetOffset int64
	if errH1 != nil && errH2 != nil {
		// Both failed? Initialize H1.
		targetOffset = int64(DuckDBHeaderSize)
	} else if errH1 != nil {
		// H1 invalid, write to H1
		targetOffset = int64(DuckDBHeaderSize)
	} else if errH2 != nil {
		// H2 invalid, write to H2
		targetOffset = int64(DuckDBHeaderSize) * 2
	} else {
		// Both valid, overwrite the OLDER one
		if h1.Iteration > h2.Iteration {
			// H1 is newer, overwrite H2
			targetOffset = int64(DuckDBHeaderSize) * 2
		} else {
			// H2 is newer, overwrite H1
			targetOffset = int64(DuckDBHeaderSize)
		}
	}

	if _, err := file.Seek(targetOffset, io.SeekStart); err != nil {
		return err
	}

	if err := WriteDatabaseHeader(file, newHeader); err != nil {
		return err
	}

	return file.Sync()
}
