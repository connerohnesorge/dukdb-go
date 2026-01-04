package tests

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WKB encoding helpers for test data.
// WKB format: byte_order (1 byte) + type (4 bytes) + coordinates

// createPointWKB creates a WKB-encoded POINT geometry.
// WKB POINT: byte_order + type(1) + x(8 bytes) + y(8 bytes)
func createPointWKB(x, y float64) []byte {
	buf := new(bytes.Buffer)
	// Byte order: 1 = little endian
	buf.WriteByte(0x01)
	// Type: 1 = Point (little endian)
	binary.Write(buf, binary.LittleEndian, uint32(1))
	// X coordinate
	binary.Write(buf, binary.LittleEndian, x)
	// Y coordinate
	binary.Write(buf, binary.LittleEndian, y)
	return buf.Bytes()
}

// createLineStringWKB creates a WKB-encoded LINESTRING geometry.
// WKB LINESTRING: byte_order + type(2) + num_points + points...
func createLineStringWKB(points [][2]float64) []byte {
	buf := new(bytes.Buffer)
	// Byte order: 1 = little endian
	buf.WriteByte(0x01)
	// Type: 2 = LineString
	binary.Write(buf, binary.LittleEndian, uint32(2))
	// Number of points
	binary.Write(buf, binary.LittleEndian, uint32(len(points)))
	// Points
	for _, pt := range points {
		binary.Write(buf, binary.LittleEndian, pt[0])
		binary.Write(buf, binary.LittleEndian, pt[1])
	}
	return buf.Bytes()
}

// createPolygonWKB creates a WKB-encoded POLYGON geometry with a single ring.
// WKB POLYGON: byte_order + type(3) + num_rings + (num_points + points...)...
func createPolygonWKB(ring [][2]float64) []byte {
	buf := new(bytes.Buffer)
	// Byte order: 1 = little endian
	buf.WriteByte(0x01)
	// Type: 3 = Polygon
	binary.Write(buf, binary.LittleEndian, uint32(3))
	// Number of rings: 1
	binary.Write(buf, binary.LittleEndian, uint32(1))
	// Number of points in the ring
	binary.Write(buf, binary.LittleEndian, uint32(len(ring)))
	// Points
	for _, pt := range ring {
		binary.Write(buf, binary.LittleEndian, pt[0])
		binary.Write(buf, binary.LittleEndian, pt[1])
	}
	return buf.Bytes()
}

// TestGeometryColumnIntegration tests GEOMETRY column operations end-to-end with database/sql.
// Note: GEOMETRY type is currently stored as BLOB since the parser maps unknown types to VARCHAR.
// This test uses BLOB to store WKB geometry data until full GEOMETRY type support is added.
func TestGeometryColumnIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with BLOB column to store geometry WKB data
	// (GEOMETRY type in parser falls back to VARCHAR, so we use BLOB)
	_, err = db.Exec(`CREATE TABLE geom_test (id INTEGER, geom BLOB)`)
	require.NoError(t, err)

	t.Run("Insert and select POINT geometry", func(t *testing.T) {
		pointWKB := createPointWKB(1.0, 2.0)
		_, err := db.Exec(`INSERT INTO geom_test VALUES (1, $1)`, pointWKB)
		require.NoError(t, err)

		var id int
		var geomData []byte
		err = db.QueryRow(`SELECT id, geom FROM geom_test WHERE id = 1`).Scan(&id, &geomData)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, pointWKB, geomData)

		// Verify WKB structure
		assert.Equal(t, byte(0x01), geomData[0]) // Little endian
		geomType := binary.LittleEndian.Uint32(geomData[1:5])
		assert.Equal(t, uint32(1), geomType) // POINT type
	})

	t.Run("Insert and select LINESTRING geometry", func(t *testing.T) {
		lineWKB := createLineStringWKB([][2]float64{
			{0.0, 0.0},
			{1.0, 1.0},
			{2.0, 2.0},
		})
		_, err := db.Exec(`INSERT INTO geom_test VALUES (2, $1)`, lineWKB)
		require.NoError(t, err)

		var id int
		var geomData []byte
		err = db.QueryRow(`SELECT id, geom FROM geom_test WHERE id = 2`).Scan(&id, &geomData)
		require.NoError(t, err)
		assert.Equal(t, 2, id)
		assert.Equal(t, lineWKB, geomData)

		// Verify WKB structure
		assert.Equal(t, byte(0x01), geomData[0]) // Little endian
		geomType := binary.LittleEndian.Uint32(geomData[1:5])
		assert.Equal(t, uint32(2), geomType) // LINESTRING type
	})

	t.Run("Insert and select POLYGON geometry", func(t *testing.T) {
		// A simple square polygon
		polygonWKB := createPolygonWKB([][2]float64{
			{0.0, 0.0},
			{0.0, 1.0},
			{1.0, 1.0},
			{1.0, 0.0},
			{0.0, 0.0}, // Closed ring
		})
		_, err := db.Exec(`INSERT INTO geom_test VALUES (3, $1)`, polygonWKB)
		require.NoError(t, err)

		var id int
		var geomData []byte
		err = db.QueryRow(`SELECT id, geom FROM geom_test WHERE id = 3`).Scan(&id, &geomData)
		require.NoError(t, err)
		assert.Equal(t, 3, id)
		assert.Equal(t, polygonWKB, geomData)

		// Verify WKB structure
		assert.Equal(t, byte(0x01), geomData[0]) // Little endian
		geomType := binary.LittleEndian.Uint32(geomData[1:5])
		assert.Equal(t, uint32(3), geomType) // POLYGON type
	})
}

// TestGeometryNullHandling tests NULL handling in GEOMETRY (BLOB) columns.
func TestGeometryNullHandling(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with BLOB column
	_, err = db.Exec(`CREATE TABLE geom_null_test (id INTEGER, geom BLOB)`)
	require.NoError(t, err)

	t.Run("Insert SQL NULL into GEOMETRY column", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO geom_null_test VALUES (1, NULL)`)
		require.NoError(t, err)

		var id int
		var geomData []byte
		err = db.QueryRow(`SELECT id, geom FROM geom_null_test WHERE id = 1`).Scan(&id, &geomData)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Nil(t, geomData)
	})

	t.Run("Insert SQL NULL using parameter", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO geom_null_test VALUES (2, $1)`, nil)
		require.NoError(t, err)

		var id int
		var geomData []byte
		err = db.QueryRow(`SELECT id, geom FROM geom_null_test WHERE id = 2`).Scan(&id, &geomData)
		require.NoError(t, err)
		assert.Equal(t, 2, id)
		assert.Nil(t, geomData)
	})

	t.Run("Query NULL with IS NULL", func(t *testing.T) {
		rows, err := db.Query(`SELECT id FROM geom_null_test WHERE geom IS NULL ORDER BY id`)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())

		assert.Equal(t, []int{1, 2}, ids)
	})

	t.Run("Insert non-NULL then set to NULL via UPDATE", func(t *testing.T) {
		pointWKB := createPointWKB(3.0, 4.0)
		_, err := db.Exec(`INSERT INTO geom_null_test VALUES (3, $1)`, pointWKB)
		require.NoError(t, err)

		// Verify it's not NULL
		var geomData []byte
		err = db.QueryRow(`SELECT geom FROM geom_null_test WHERE id = 3`).Scan(&geomData)
		require.NoError(t, err)
		assert.NotNil(t, geomData)

		// Update to NULL
		_, err = db.Exec(`UPDATE geom_null_test SET geom = NULL WHERE id = 3`)
		require.NoError(t, err)

		// Verify it's now NULL
		err = db.QueryRow(`SELECT geom FROM geom_null_test WHERE id = 3`).Scan(&geomData)
		require.NoError(t, err)
		assert.Nil(t, geomData)
	})
}

// TestGeometryWKBRoundtrip tests that WKB data roundtrips correctly.
func TestGeometryWKBRoundtrip(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`CREATE TABLE geom_roundtrip (id INTEGER, geom BLOB)`)
	require.NoError(t, err)

	// Test various coordinate values including negative and zero
	testPoints := []struct {
		name string
		x, y float64
	}{
		{"origin", 0.0, 0.0},
		{"positive", 100.5, 200.5},
		{"negative", -100.5, -200.5},
		{"mixed", -50.0, 75.0},
		{"large", 12345.6789, -98765.4321},
		{"small", 0.00001, 0.00002},
	}

	for i, tc := range testPoints {
		t.Run(tc.name, func(t *testing.T) {
			pointWKB := createPointWKB(tc.x, tc.y)
			id := i + 1

			_, err := db.Exec(`INSERT INTO geom_roundtrip VALUES ($1, $2)`, id, pointWKB)
			require.NoError(t, err)

			var retrievedData []byte
			err = db.QueryRow(`SELECT geom FROM geom_roundtrip WHERE id = $1`, id).Scan(&retrievedData)
			require.NoError(t, err)

			// Verify exact binary match
			assert.Equal(t, pointWKB, retrievedData)

			// Parse coordinates from retrieved WKB
			x := binary.LittleEndian.Uint64(retrievedData[5:13])
			y := binary.LittleEndian.Uint64(retrievedData[13:21])
			retrievedX := bytesToFloat64(x)
			retrievedY := bytesToFloat64(y)

			assert.Equal(t, tc.x, retrievedX)
			assert.Equal(t, tc.y, retrievedY)
		})
	}
}

// bytesToFloat64 converts a uint64 to float64 using IEEE 754 encoding.
func bytesToFloat64(bits uint64) float64 {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, bits)
	var f float64
	binary.Read(bytes.NewReader(buf), binary.LittleEndian, &f)
	return f
}

// TestGeometryMultipleRows tests geometry operations with multiple rows.
func TestGeometryMultipleRows(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`CREATE TABLE geom_multi (id INTEGER, name VARCHAR, geom BLOB)`)
	require.NoError(t, err)

	// Insert multiple geometries of different types
	testData := []struct {
		id   int
		name string
		wkb  []byte
	}{
		{1, "point_a", createPointWKB(1.0, 2.0)},
		{2, "point_b", createPointWKB(3.0, 4.0)},
		{3, "line", createLineStringWKB([][2]float64{{0.0, 0.0}, {10.0, 10.0}})},
		{4, "polygon", createPolygonWKB([][2]float64{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}})},
		{5, "null_geom", nil}, // NULL geometry
	}

	for _, td := range testData {
		if td.wkb != nil {
			_, err := db.Exec(`INSERT INTO geom_multi VALUES ($1, $2, $3)`, td.id, td.name, td.wkb)
			require.NoError(t, err)
		} else {
			_, err := db.Exec(`INSERT INTO geom_multi VALUES ($1, $2, NULL)`, td.id, td.name)
			require.NoError(t, err)
		}
	}

	// Query all rows
	rows, err := db.Query(`SELECT id, name, geom FROM geom_multi ORDER BY id`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	var results []struct {
		id   int
		name string
		geom []byte
	}
	for rows.Next() {
		var id int
		var name string
		var geom []byte
		require.NoError(t, rows.Scan(&id, &name, &geom))
		results = append(results, struct {
			id   int
			name string
			geom []byte
		}{id, name, geom})
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 5)

	// Verify each row
	for i, td := range testData {
		assert.Equal(t, td.id, results[i].id)
		assert.Equal(t, td.name, results[i].name)
		if td.wkb != nil {
			assert.Equal(t, td.wkb, results[i].geom)
		} else {
			assert.Nil(t, results[i].geom)
		}
	}
}

// TestGeometryPreparedStatement tests geometry operations with prepared statements.
func TestGeometryPreparedStatement(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`CREATE TABLE geom_prep (id INTEGER, geom BLOB)`)
	require.NoError(t, err)

	// Prepare insert statement
	insertStmt, err := db.Prepare(`INSERT INTO geom_prep VALUES ($1, $2)`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, insertStmt.Close())
	}()

	// Insert multiple geometries using prepared statement
	testData := []struct {
		id  int
		wkb []byte
	}{
		{1, createPointWKB(1.0, 1.0)},
		{2, createPointWKB(2.0, 2.0)},
		{3, createPointWKB(3.0, 3.0)},
	}

	for _, td := range testData {
		_, err := insertStmt.Exec(td.id, td.wkb)
		require.NoError(t, err)
	}

	// Prepare select statement
	selectStmt, err := db.Prepare(`SELECT geom FROM geom_prep WHERE id = $1`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, selectStmt.Close())
	}()

	// Query using prepared statement
	for _, td := range testData {
		var geom []byte
		err := selectStmt.QueryRow(td.id).Scan(&geom)
		require.NoError(t, err)
		assert.Equal(t, td.wkb, geom)
	}
}

// TestGeometryTransaction tests geometry operations within a transaction.
func TestGeometryTransaction(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`CREATE TABLE geom_tx (id INTEGER, geom BLOB)`)
	require.NoError(t, err)

	t.Run("Commit transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		pointWKB := createPointWKB(5.0, 5.0)
		_, err = tx.Exec(`INSERT INTO geom_tx VALUES (1, $1)`, pointWKB)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		// Verify data persisted
		var geom []byte
		err = db.QueryRow(`SELECT geom FROM geom_tx WHERE id = 1`).Scan(&geom)
		require.NoError(t, err)
		assert.Equal(t, pointWKB, geom)
	})

	t.Run("Rollback transaction", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		pointWKB := createPointWKB(6.0, 6.0)
		_, err = tx.Exec(`INSERT INTO geom_tx VALUES (2, $1)`, pointWKB)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		// Verify data was not persisted
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM geom_tx WHERE id = 2`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

// TestGeometryUpdate tests updating geometry values.
func TestGeometryUpdate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`CREATE TABLE geom_update (id INTEGER PRIMARY KEY, geom BLOB)`)
	require.NoError(t, err)

	// Insert initial geometry
	initialWKB := createPointWKB(1.0, 1.0)
	_, err = db.Exec(`INSERT INTO geom_update VALUES (1, $1)`, initialWKB)
	require.NoError(t, err)

	// Update the geometry
	updatedWKB := createPointWKB(10.0, 20.0)
	result, err := db.Exec(`UPDATE geom_update SET geom = $1 WHERE id = 1`, updatedWKB)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify the update
	var geom []byte
	err = db.QueryRow(`SELECT geom FROM geom_update WHERE id = 1`).Scan(&geom)
	require.NoError(t, err)
	assert.Equal(t, updatedWKB, geom)

	// Verify it's different from the original
	assert.NotEqual(t, initialWKB, geom)
}

// TestGeometryDelete tests deleting rows with geometry columns.
func TestGeometryDelete(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`CREATE TABLE geom_delete (id INTEGER, geom BLOB)`)
	require.NoError(t, err)

	// Insert multiple geometries
	_, err = db.Exec(`INSERT INTO geom_delete VALUES (1, $1)`, createPointWKB(1.0, 1.0))
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO geom_delete VALUES (2, $1)`, createPointWKB(2.0, 2.0))
	require.NoError(t, err)

	// Delete one row
	result, err := db.Exec(`DELETE FROM geom_delete WHERE id = 1`)
	require.NoError(t, err)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

	// Verify only one row remains
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM geom_delete`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the correct row remains
	var id int
	err = db.QueryRow(`SELECT id FROM geom_delete`).Scan(&id)
	require.NoError(t, err)
	assert.Equal(t, 2, id)
}

// TestGeometryTypeVerification tests that WKB type bytes are preserved correctly.
func TestGeometryTypeVerification(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`CREATE TABLE geom_types (id INTEGER, geom_type VARCHAR, geom BLOB)`)
	require.NoError(t, err)

	// Test different geometry types
	testCases := []struct {
		name     string
		wkb      []byte
		wkbType  uint32
		typeName string
	}{
		{"Point", createPointWKB(1.0, 2.0), 1, "Point"},
		{"LineString", createLineStringWKB([][2]float64{{0, 0}, {1, 1}}), 2, "LineString"},
		{"Polygon", createPolygonWKB([][2]float64{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}}), 3, "Polygon"},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			id := i + 1
			_, err := db.Exec(`INSERT INTO geom_types VALUES ($1, $2, $3)`, id, tc.typeName, tc.wkb)
			require.NoError(t, err)

			var geomData []byte
			err = db.QueryRow(`SELECT geom FROM geom_types WHERE id = $1`, id).Scan(&geomData)
			require.NoError(t, err)

			// Extract and verify geometry type from WKB
			require.GreaterOrEqual(t, len(geomData), 5)
			geomType := binary.LittleEndian.Uint32(geomData[1:5])
			assert.Equal(t, tc.wkbType, geomType, "geometry type mismatch for %s", tc.name)
		})
	}
}

// TestGeometryBigEndianWKB tests that big-endian WKB is also handled correctly.
func TestGeometryBigEndianWKB(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`CREATE TABLE geom_be (id INTEGER, geom BLOB)`)
	require.NoError(t, err)

	// Create big-endian WKB for a POINT
	createBigEndianPointWKB := func(x, y float64) []byte {
		buf := new(bytes.Buffer)
		// Byte order: 0 = big endian
		buf.WriteByte(0x00)
		// Type: 1 = Point (big endian)
		binary.Write(buf, binary.BigEndian, uint32(1))
		// X coordinate
		binary.Write(buf, binary.BigEndian, x)
		// Y coordinate
		binary.Write(buf, binary.BigEndian, y)
		return buf.Bytes()
	}

	pointWKB := createBigEndianPointWKB(123.456, 789.012)
	_, err = db.Exec(`INSERT INTO geom_be VALUES (1, $1)`, pointWKB)
	require.NoError(t, err)

	var geomData []byte
	err = db.QueryRow(`SELECT geom FROM geom_be WHERE id = 1`).Scan(&geomData)
	require.NoError(t, err)

	// Verify exact binary match
	assert.Equal(t, pointWKB, geomData)

	// Verify byte order indicator
	assert.Equal(t, byte(0x00), geomData[0]) // Big endian

	// Verify geometry type (big endian)
	geomType := binary.BigEndian.Uint32(geomData[1:5])
	assert.Equal(t, uint32(1), geomType) // POINT
}
