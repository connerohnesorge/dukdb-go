// Package arrow provides Apache Arrow IPC file reading and writing capabilities for dukdb-go.
// This file defines the BatchIterator interface for streaming record batch processing.
package arrow

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// BatchIterator provides a streaming interface for reading Arrow record batches.
// It follows the standard Go iterator pattern with Next()/Record() methods.
// Implementations include Reader (file format) and StreamReader (stream format).
//
// Usage:
//
//	iter := reader.Iterator()
//	for iter.Next() {
//	    record := iter.Record()
//	    // Process record batch...
//	}
//	if err := iter.Err(); err != nil {
//	    // Handle error
//	}
type BatchIterator interface {
	// Next advances to the next record batch.
	// Returns true if a record batch is available, false otherwise.
	// When Next returns false, call Err() to check for errors.
	Next() bool

	// Record returns the current record batch.
	// Returns nil if Next() has not been called or returned false.
	// The returned RecordBatch is valid until the next call to Next().
	// Callers should NOT call Release() on the returned record.
	Record() arrow.RecordBatch

	// Schema returns the Arrow schema for the record batches.
	// This can be called before the first call to Next().
	Schema() *arrow.Schema

	// Err returns any error that occurred during iteration.
	// Should be called after Next() returns false.
	Err() error
}

// ReaderIterator adapts a Reader to the BatchIterator interface.
// It provides sequential iteration over record batches in an Arrow IPC file.
type ReaderIterator struct {
	reader       *Reader
	currentBatch int
	record       arrow.RecordBatch
	err          error
}

// Iterator returns a BatchIterator for sequential access to record batches.
// The returned iterator starts at the first record batch.
func (r *Reader) Iterator() BatchIterator {
	return &ReaderIterator{
		reader:       r,
		currentBatch: 0,
		record:       nil,
		err:          nil,
	}
}

// Next advances to the next record batch.
// Returns true if a record batch is available, false otherwise.
func (ri *ReaderIterator) Next() bool {
	// Release previous record
	if ri.record != nil {
		ri.record.Release()
		ri.record = nil
	}

	// Check bounds
	if ri.currentBatch >= ri.reader.NumRecordBatches() {
		return false
	}

	// Read next record batch
	record, err := ri.reader.RecordBatchAt(ri.currentBatch)
	if err != nil {
		ri.err = err
		return false
	}

	ri.record = record
	ri.currentBatch++

	return true
}

// Record returns the current record batch.
// Returns nil if Next() has not been called or returned false.
func (ri *ReaderIterator) Record() arrow.RecordBatch {
	return ri.record
}

// Schema returns the Arrow schema for the record batches.
func (ri *ReaderIterator) Schema() *arrow.Schema {
	return ri.reader.ArrowSchema()
}

// Err returns any error that occurred during iteration.
func (ri *ReaderIterator) Err() error {
	return ri.err
}

// Release releases any resources held by the iterator.
// Should be called when done iterating.
func (ri *ReaderIterator) Release() {
	if ri.record != nil {
		ri.record.Release()
		ri.record = nil
	}
}

// StreamReaderIterator wraps StreamReader to implement BatchIterator.
// StreamReader already implements the iterator pattern, so this is a thin wrapper.
type StreamReaderIterator struct {
	reader *StreamReader
}

// Iterator returns a BatchIterator for the StreamReader.
// Note: StreamReader already supports Next()/Record(), this provides
// interface compatibility.
func (r *StreamReader) Iterator() BatchIterator {
	return &StreamReaderIterator{reader: r}
}

// Next advances to the next record batch.
func (si *StreamReaderIterator) Next() bool {
	return si.reader.Next()
}

// Record returns the current record batch.
func (si *StreamReaderIterator) Record() arrow.RecordBatch {
	return si.reader.Record()
}

// Schema returns the Arrow schema for the record batches.
func (si *StreamReaderIterator) Schema() *arrow.Schema {
	return si.reader.ArrowSchema()
}

// Err returns any error that occurred during iteration.
func (si *StreamReaderIterator) Err() error {
	return si.reader.Err()
}

// Verify interface implementations at compile time.
var (
	_ BatchIterator = (*ReaderIterator)(nil)
	_ BatchIterator = (*StreamReaderIterator)(nil)
)
