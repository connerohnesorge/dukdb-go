// Package storage provides columnar storage for the native Go DuckDB implementation.
package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	dukdb "github.com/dukdb/dukdb-go"
)

// RowGroupMagic is the magic number for serialized row groups
const RowGroupMagicBytes = "ROWG"

// ExportRowGroup serializes a row group to bytes
func (t *Table) ExportRowGroup(rg *RowGroup) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write magic (4 bytes)
	if _, err := buf.WriteString(RowGroupMagicBytes); err != nil {
		return nil, err
	}

	// Write row count (4 bytes, uint32 LE)
	if err := binary.Write(buf, binary.LittleEndian, uint32(rg.count)); err != nil {
		return nil, err
	}

	// Write column count (2 bytes, uint16 LE)
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(rg.columns))); err != nil {
		return nil, err
	}

	// Reserved (6 bytes)
	reserved := make([]byte, 6)
	if _, err := buf.Write(reserved); err != nil {
		return nil, err
	}

	// Write each column vector
	for i, vec := range rg.columns {
		if err := writeVector(buf, vec, t.columnTypes[i]); err != nil {
			return nil, fmt.Errorf("failed to write column %d: %w", i, err)
		}
	}

	return buf.Bytes(), nil
}

// ImportRowGroup deserializes a row group from bytes
func (t *Table) ImportRowGroup(data []byte) (*RowGroup, error) {
	r := bytes.NewReader(data)

	// Read and verify magic
	magic := make([]byte, 4)
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, err
	}
	if string(magic) != RowGroupMagicBytes {
		return nil, fmt.Errorf("invalid row group magic: %s", string(magic))
	}

	// Read row count
	var rowCount uint32
	if err := binary.Read(r, binary.LittleEndian, &rowCount); err != nil {
		return nil, err
	}

	// Read column count
	var colCount uint16
	if err := binary.Read(r, binary.LittleEndian, &colCount); err != nil {
		return nil, err
	}

	// Skip reserved bytes
	reserved := make([]byte, 6)
	if _, err := io.ReadFull(r, reserved); err != nil {
		return nil, err
	}

	// Create new row group
	rg := NewRowGroup(t.columnTypes, int(rowCount))
	rg.count = int(rowCount)

	// Read each column vector
	for i := range colCount {
		vec, err := readVector(r, t.columnTypes[i], int(rowCount))
		if err != nil {
			return nil, fmt.Errorf("failed to read column %d: %w", i, err)
		}
		rg.columns[i] = vec
	}

	return rg, nil
}

// writeVector writes a vector to a byte buffer
func writeVector(buf *bytes.Buffer, vec *Vector, colType dukdb.Type) error {
	// Write type (1 byte)
	if err := buf.WriteByte(byte(colType)); err != nil {
		return err
	}

	// Write validity mask
	if err := writeValidityMask(buf, vec.validity); err != nil {
		return err
	}

	// Write data based on type
	switch colType {
	case dukdb.TYPE_BOOLEAN:
		return writeBoolArray(buf, vec.data.([]bool), vec.count)
	case dukdb.TYPE_TINYINT:
		return writeInt8Array(buf, vec.data.([]int8), vec.count)
	case dukdb.TYPE_SMALLINT:
		return writeInt16Array(buf, vec.data.([]int16), vec.count)
	case dukdb.TYPE_INTEGER:
		return writeInt32Array(buf, vec.data.([]int32), vec.count)
	case dukdb.TYPE_BIGINT:
		return writeInt64Array(buf, vec.data.([]int64), vec.count)
	case dukdb.TYPE_UTINYINT:
		return writeUint8Array(buf, vec.data.([]uint8), vec.count)
	case dukdb.TYPE_USMALLINT:
		return writeUint16Array(buf, vec.data.([]uint16), vec.count)
	case dukdb.TYPE_UINTEGER:
		return writeUint32Array(buf, vec.data.([]uint32), vec.count)
	case dukdb.TYPE_UBIGINT:
		return writeUint64Array(buf, vec.data.([]uint64), vec.count)
	case dukdb.TYPE_FLOAT:
		return writeFloat32Array(buf, vec.data.([]float32), vec.count)
	case dukdb.TYPE_DOUBLE:
		return writeFloat64Array(buf, vec.data.([]float64), vec.count)
	case dukdb.TYPE_VARCHAR:
		return writeStringArray(buf, vec.data.([]string), vec.count)
	case dukdb.TYPE_BLOB:
		return writeBlobArray(buf, vec.data.([][]byte), vec.count)
	case dukdb.TYPE_DATE:
		// Date stored as days since epoch (int32)
		return writeInt32Array(buf, vec.data.([]int32), vec.count)
	case dukdb.TYPE_TIME:
		// Time stored as microseconds since midnight (int64)
		return writeInt64Array(buf, vec.data.([]int64), vec.count)
	case dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS, dukdb.TYPE_TIMESTAMP_TZ:
		// Timestamps stored as int64
		return writeInt64Array(buf, vec.data.([]int64), vec.count)
	default:
		// For other types, use generic any slice handling
		return writeAnyArray(buf, vec.data.([]any), vec.count)
	}
}

// readVector reads a vector from a byte reader
func readVector(r io.Reader, colType dukdb.Type, count int) (*Vector, error) {
	// Read type (1 byte)
	typeByte := make([]byte, 1)
	if _, err := io.ReadFull(r, typeByte); err != nil {
		return nil, err
	}

	// Read validity mask
	validity, err := readValidityMask(r, count)
	if err != nil {
		return nil, err
	}

	// Create vector
	vec := NewVector(colType, count)
	vec.validity = validity
	vec.count = count

	// Read data based on type
	switch colType {
	case dukdb.TYPE_BOOLEAN:
		data, err := readBoolArray(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_TINYINT:
		data, err := readInt8Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_SMALLINT:
		data, err := readInt16Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_INTEGER:
		data, err := readInt32Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_BIGINT:
		data, err := readInt64Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_UTINYINT:
		data, err := readUint8Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_USMALLINT:
		data, err := readUint16Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_UINTEGER:
		data, err := readUint32Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_UBIGINT:
		data, err := readUint64Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_FLOAT:
		data, err := readFloat32Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_DOUBLE:
		data, err := readFloat64Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_VARCHAR:
		data, err := readStringArray(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_BLOB:
		data, err := readBlobArray(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_DATE:
		data, err := readInt32Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_TIME:
		data, err := readInt64Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	case dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_S, dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS, dukdb.TYPE_TIMESTAMP_TZ:
		data, err := readInt64Array(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	default:
		// For other types, use generic any slice handling
		data, err := readAnyArray(r, count)
		if err != nil {
			return nil, err
		}
		vec.data = data
	}

	return vec, nil
}

// writeValidityMask writes a validity mask to a byte buffer
func writeValidityMask(buf *bytes.Buffer, v *ValidityMask) error {
	// Write count
	if err := binary.Write(buf, binary.LittleEndian, uint32(v.count)); err != nil {
		return err
	}

	// Write mask data
	for _, m := range v.mask {
		if err := binary.Write(buf, binary.LittleEndian, m); err != nil {
			return err
		}
	}

	return nil
}

// readValidityMask reads a validity mask from a byte reader
func readValidityMask(r io.Reader, count int) (*ValidityMask, error) {
	// Read count
	var maskCount uint32
	if err := binary.Read(r, binary.LittleEndian, &maskCount); err != nil {
		return nil, err
	}

	// Calculate number of uint64 entries
	entries := (int(maskCount) + 63) / 64
	mask := make([]uint64, entries)

	// Read mask data
	for i := range mask {
		if err := binary.Read(r, binary.LittleEndian, &mask[i]); err != nil {
			return nil, err
		}
	}

	return &ValidityMask{
		mask:  mask,
		count: int(maskCount),
	}, nil
}

// Boolean array serialization
func writeBoolArray(buf *bytes.Buffer, data []bool, count int) error {
	for i := range count {
		b := byte(0)
		if data[i] {
			b = 1
		}
		if err := buf.WriteByte(b); err != nil {
			return err
		}
	}

	return nil
}

func readBoolArray(r io.Reader, count int) ([]bool, error) {
	data := make([]bool, count)
	for i := range count {
		b := make([]byte, 1)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		data[i] = b[0] != 0
	}

	return data, nil
}

// Int8 array serialization
func writeInt8Array(buf *bytes.Buffer, data []int8, count int) error {
	for i := range count {
		if err := binary.Write(buf, binary.LittleEndian, data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readInt8Array(r io.Reader, count int) ([]int8, error) {
	data := make([]int8, count)
	for i := range count {
		if err := binary.Read(r, binary.LittleEndian, &data[i]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Int16 array serialization
func writeInt16Array(buf *bytes.Buffer, data []int16, count int) error {
	for i := range count {
		if err := binary.Write(buf, binary.LittleEndian, data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readInt16Array(r io.Reader, count int) ([]int16, error) {
	data := make([]int16, count)
	for i := range count {
		if err := binary.Read(r, binary.LittleEndian, &data[i]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Int32 array serialization
func writeInt32Array(buf *bytes.Buffer, data []int32, count int) error {
	for i := range count {
		if err := binary.Write(buf, binary.LittleEndian, data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readInt32Array(r io.Reader, count int) ([]int32, error) {
	data := make([]int32, count)
	for i := range count {
		if err := binary.Read(r, binary.LittleEndian, &data[i]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Int64 array serialization
func writeInt64Array(buf *bytes.Buffer, data []int64, count int) error {
	for i := range count {
		if err := binary.Write(buf, binary.LittleEndian, data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readInt64Array(r io.Reader, count int) ([]int64, error) {
	data := make([]int64, count)
	for i := range count {
		if err := binary.Read(r, binary.LittleEndian, &data[i]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Uint8 array serialization
func writeUint8Array(buf *bytes.Buffer, data []uint8, count int) error {
	for i := range count {
		if err := buf.WriteByte(data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readUint8Array(r io.Reader, count int) ([]uint8, error) {
	data := make([]uint8, count)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return data, nil
}

// Uint16 array serialization
func writeUint16Array(buf *bytes.Buffer, data []uint16, count int) error {
	for i := range count {
		if err := binary.Write(buf, binary.LittleEndian, data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readUint16Array(r io.Reader, count int) ([]uint16, error) {
	data := make([]uint16, count)
	for i := range count {
		if err := binary.Read(r, binary.LittleEndian, &data[i]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Uint32 array serialization
func writeUint32Array(buf *bytes.Buffer, data []uint32, count int) error {
	for i := range count {
		if err := binary.Write(buf, binary.LittleEndian, data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readUint32Array(r io.Reader, count int) ([]uint32, error) {
	data := make([]uint32, count)
	for i := range count {
		if err := binary.Read(r, binary.LittleEndian, &data[i]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Uint64 array serialization
func writeUint64Array(buf *bytes.Buffer, data []uint64, count int) error {
	for i := range count {
		if err := binary.Write(buf, binary.LittleEndian, data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readUint64Array(r io.Reader, count int) ([]uint64, error) {
	data := make([]uint64, count)
	for i := range count {
		if err := binary.Read(r, binary.LittleEndian, &data[i]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Float32 array serialization
func writeFloat32Array(buf *bytes.Buffer, data []float32, count int) error {
	for i := range count {
		if err := binary.Write(buf, binary.LittleEndian, data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readFloat32Array(r io.Reader, count int) ([]float32, error) {
	data := make([]float32, count)
	for i := range count {
		if err := binary.Read(r, binary.LittleEndian, &data[i]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Float64 array serialization
func writeFloat64Array(buf *bytes.Buffer, data []float64, count int) error {
	for i := range count {
		if err := binary.Write(buf, binary.LittleEndian, data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readFloat64Array(r io.Reader, count int) ([]float64, error) {
	data := make([]float64, count)
	for i := range count {
		if err := binary.Read(r, binary.LittleEndian, &data[i]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// String array serialization
func writeStringArray(buf *bytes.Buffer, data []string, count int) error {
	for i := range count {
		strBytes := []byte(data[i])
		// Write length as varint
		if err := writeVarint(buf, uint64(len(strBytes))); err != nil {
			return err
		}
		// Write string bytes
		if _, err := buf.Write(strBytes); err != nil {
			return err
		}
	}

	return nil
}

func readStringArray(r io.Reader, count int) ([]string, error) {
	data := make([]string, count)
	for i := range count {
		// Read length as varint
		length, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		// Read string bytes
		strBytes := make([]byte, length)
		if _, err := io.ReadFull(r, strBytes); err != nil {
			return nil, err
		}
		data[i] = string(strBytes)
	}

	return data, nil
}

// Blob array serialization
func writeBlobArray(buf *bytes.Buffer, data [][]byte, count int) error {
	for i := range count {
		// Write length as varint
		if err := writeVarint(buf, uint64(len(data[i]))); err != nil {
			return err
		}
		// Write blob bytes
		if _, err := buf.Write(data[i]); err != nil {
			return err
		}
	}

	return nil
}

func readBlobArray(r io.Reader, count int) ([][]byte, error) {
	data := make([][]byte, count)
	for i := range count {
		// Read length as varint
		length, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		// Read blob bytes
		data[i] = make([]byte, length)
		if _, err := io.ReadFull(r, data[i]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// Generic any array serialization (fallback for complex types)
func writeAnyArray(buf *bytes.Buffer, data []any, count int) error {
	for i := range count {
		// Write value as string representation
		var strVal string
		if data[i] != nil {
			strVal = fmt.Sprintf("%v", data[i])
		}
		strBytes := []byte(strVal)
		// Write length as varint
		if err := writeVarint(buf, uint64(len(strBytes))); err != nil {
			return err
		}
		// Write string bytes
		if _, err := buf.Write(strBytes); err != nil {
			return err
		}
	}

	return nil
}

func readAnyArray(r io.Reader, count int) ([]any, error) {
	data := make([]any, count)
	for i := range count {
		// Read length as varint
		length, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		// Read string bytes
		strBytes := make([]byte, length)
		if _, err := io.ReadFull(r, strBytes); err != nil {
			return nil, err
		}
		data[i] = string(strBytes)
	}

	return data, nil
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
			return 0, fmt.Errorf("varint overflow")
		}
	}

	return result, nil
}

// RowGroups returns all row groups in the table
func (t *Table) RowGroups() []*RowGroup {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.rowGroups
}

// AddRowGroup adds a row group to the table
func (t *Table) AddRowGroup(rg *RowGroup) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rowGroups = append(t.rowGroups, rg)
	t.totalRows += int64(rg.count)
}
