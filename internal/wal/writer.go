package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"sync"

	"github.com/coder/quartz"
)

// Writer writes WAL entries to a file.
type Writer struct {
	file         *os.File
	buffer       *bufio.Writer
	checksum     *crc64.Table
	clock        quartz.Clock
	iteration    uint64
	bytesWritten uint64
	mu           sync.Mutex
}

// NewWriter creates a new WAL writer.
func NewWriter(
	path string,
	clock quartz.Clock,
) (*Writer, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to open WAL file: %w",
			err,
		)
	}

	w := &Writer{
		file:     file,
		buffer:   bufio.NewWriter(file),
		checksum: CRC64Table,
		clock:    clock,
	}

	// Check if file is empty (new WAL)
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()

		return nil, fmt.Errorf(
			"failed to stat WAL file: %w",
			err,
		)
	}

	if stat.Size() == 0 {
		// Write file header for new WAL
		header := &FileHeader{
			Magic:          MagicBytes,
			Version:        CurrentVersion,
			HeaderSize:     HeaderSize,
			SequenceNumber: 0,
		}
		if err := header.Serialize(w.buffer); err != nil {
			_ = file.Close()

			return nil, fmt.Errorf(
				"failed to write WAL header: %w",
				err,
			)
		}
		w.bytesWritten = HeaderSize
	} else {
		// Read existing header to get sequence number
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			_ = file.Close()

			return nil, fmt.Errorf("failed to seek to WAL header: %w", err)
		}
		header := &FileHeader{}
		if err := header.Deserialize(file); err != nil {
			_ = file.Close()

			return nil, fmt.Errorf("failed to read WAL header: %w", err)
		}
		w.iteration = header.SequenceNumber
		w.bytesWritten = uint64(stat.Size())

		// Seek back to end for appending
		if _, err := file.Seek(0, io.SeekEnd); err != nil {
			_ = file.Close()

			return nil, fmt.Errorf("failed to seek to WAL end: %w", err)
		}
	}

	return w, nil
}

// WriteEntry writes a WAL entry to the file using DuckDB format.
func (w *Writer) WriteEntry(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Serialize entry payload to buffer
	var buf bytes.Buffer
	if err := entry.Serialize(&buf); err != nil {
		return fmt.Errorf(
			"failed to serialize entry: %w",
			err,
		)
	}
	payload := buf.Bytes()

	// Increment sequence number for this entry
	w.iteration++

	// Create DuckDB entry header (16 bytes)
	header := &EntryHeader{
		Type:           entry.Type(),
		Flags:          EntryFlagChecksum, // Always use checksum for durability
		Length:         uint32(len(payload)),
		SequenceNumber: uint32(w.iteration),
	}

	// Serialize entry header (16 bytes)
	if err := header.Serialize(w.buffer); err != nil {
		return fmt.Errorf(
			"failed to write entry header: %w",
			err,
		)
	}

	// Calculate and write checksum (CRC32, 4 bytes) AFTER header, BEFORE payload
	// This allows the reader to validate before reading the full payload
	checksum := header.CalculateChecksum(payload)
	if err := binary.Write(w.buffer, binary.LittleEndian, checksum); err != nil {
		return fmt.Errorf(
			"failed to write entry checksum: %w",
			err,
		)
	}

	// Write entry payload
	if _, err := w.buffer.Write(payload); err != nil {
		return fmt.Errorf(
			"failed to write entry payload: %w",
			err,
		)
	}

	w.bytesWritten += uint64(EntryHeaderSize + 4 + len(payload)) // header + checksum + payload

	return nil
}

// Sync flushes the buffer and syncs the file to disk.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buffer.Flush(); err != nil {
		return fmt.Errorf(
			"failed to flush WAL buffer: %w",
			err,
		)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf(
			"failed to sync WAL file: %w",
			err,
		)
	}

	return nil
}

// Close closes the WAL writer.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buffer.Flush(); err != nil {
		return fmt.Errorf(
			"failed to flush WAL buffer: %w",
			err,
		)
	}

	return w.file.Close()
}

// BytesWritten returns the total bytes written to the WAL.
func (w *Writer) BytesWritten() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.bytesWritten
}

// Iteration returns the current checkpoint iteration.
func (w *Writer) Iteration() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.iteration
}

// SetIteration sets the checkpoint iteration.
func (w *Writer) SetIteration(iteration uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.iteration = iteration
}

// Path returns the path to the WAL file.
func (w *Writer) Path() string {
	return w.file.Name()
}

// Reset resets the WAL writer by truncating the file and writing a new header.
func (w *Writer) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Truncate file
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf(
			"failed to truncate WAL file: %w",
			err,
		)
	}

	// Seek to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf(
			"failed to seek to WAL start: %w",
			err,
		)
	}

	// Reset buffer
	w.buffer.Reset(w.file)

	// Write new header with incremented sequence number
	w.iteration++
	header := &FileHeader{
		Magic:          MagicBytes,
		Version:        CurrentVersion,
		HeaderSize:     HeaderSize,
		SequenceNumber: w.iteration,
	}
	if err := header.Serialize(w.buffer); err != nil {
		return fmt.Errorf(
			"failed to write WAL header: %w",
			err,
		)
	}

	w.bytesWritten = HeaderSize

	// Flush header
	if err := w.buffer.Flush(); err != nil {
		return fmt.Errorf(
			"failed to flush WAL header: %w",
			err,
		)
	}

	return nil
}
