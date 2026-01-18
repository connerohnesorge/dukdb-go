package tests

import (
	"database/sql"
	"math"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSTArea tests the ST_Area function at the SQL level.
func TestSTArea(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{
			name:     "unit square",
			query:    "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))",
			expected: 1.0,
		},
		{
			name:     "2x2 square",
			query:    "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'))",
			expected: 4.0,
		},
		{
			name:     "right triangle 3-4-5",
			query:    "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 0 4, 3 0, 0 0))'))",
			expected: 6.0,
		},
		{
			name:     "rectangle 3x5",
			query:    "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 0 5, 3 5, 3 0, 0 0))'))",
			expected: 15.0,
		},
		{
			name:     "point has zero area",
			query:    "SELECT ST_Area(ST_GeomFromText('POINT(5 5)'))",
			expected: 0.0,
		},
		{
			name:     "line has zero area",
			query:    "SELECT ST_Area(ST_GeomFromText('LINESTRING(0 0, 10 10)'))",
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result float64
			err := db.QueryRow(tt.query).Scan(&result)
			require.NoError(t, err)
			assert.InDelta(t, tt.expected, result, 0.0001)
		})
	}
}

// TestSTLength tests the ST_Length function at the SQL level.
func TestSTLength(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected float64
	}{
		{
			name:     "simple line 3-4-5",
			query:    "SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)'))",
			expected: 5.0,
		},
		{
			name:     "horizontal line",
			query:    "SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 10 0)'))",
			expected: 10.0,
		},
		{
			name:     "multi-segment line",
			query:    "SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 0, 3 4)'))",
			expected: 7.0, // 3 + 4 = 7
		},
		{
			name:     "polygon perimeter (unit square)",
			query:    "SELECT ST_Length(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))",
			expected: 4.0,
		},
		{
			name:     "polygon perimeter (3x4 rectangle)",
			query:    "SELECT ST_Length(ST_GeomFromText('POLYGON((0 0, 0 4, 3 4, 3 0, 0 0))'))",
			expected: 14.0,
		},
		{
			name:     "point has zero length",
			query:    "SELECT ST_Length(ST_GeomFromText('POINT(5 5)'))",
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result float64
			err := db.QueryRow(tt.query).Scan(&result)
			require.NoError(t, err)
			assert.InDelta(t, tt.expected, result, 0.0001)
		})
	}
}

// TestSTPerimeter tests the ST_Perimeter function (alias for ST_Length).
func TestSTPerimeter(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// ST_Perimeter should work like ST_Length for polygons
	var result float64
	err = db.QueryRow("SELECT ST_Perimeter(ST_GeomFromText('POLYGON((0 0, 0 3, 4 3, 4 0, 0 0))'))").
		Scan(&result)
	require.NoError(t, err)
	assert.InDelta(t, 14.0, result, 0.0001) // 3 + 4 + 3 + 4 = 14
}

// TestSTCentroid tests the ST_Centroid function at the SQL level.
func TestSTCentroid(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name      string
		query     string
		expectedX float64
		expectedY float64
	}{
		{
			name:      "point centroid is the point itself",
			query:     "SELECT ST_X(ST_Centroid(ST_GeomFromText('POINT(5 10)'))), ST_Y(ST_Centroid(ST_GeomFromText('POINT(5 10)')))",
			expectedX: 5.0,
			expectedY: 10.0,
		},
		{
			name:      "line centroid is midpoint",
			query:     "SELECT ST_X(ST_Centroid(ST_GeomFromText('LINESTRING(0 0, 10 0)'))), ST_Y(ST_Centroid(ST_GeomFromText('LINESTRING(0 0, 10 0)')))",
			expectedX: 5.0,
			expectedY: 0.0,
		},
		{
			name:      "unit square centroid",
			query:     "SELECT ST_X(ST_Centroid(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))), ST_Y(ST_Centroid(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')))",
			expectedX: 0.5,
			expectedY: 0.5,
		},
		{
			name:      "2x2 square centroid",
			query:     "SELECT ST_X(ST_Centroid(ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'))), ST_Y(ST_Centroid(ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))')))",
			expectedX: 1.0,
			expectedY: 1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var x, y float64
			err := db.QueryRow(tt.query).Scan(&x, &y)
			require.NoError(t, err)
			assert.InDelta(t, tt.expectedX, x, 0.0001)
			assert.InDelta(t, tt.expectedY, y, 0.0001)
		})
	}
}

// TestSTCentroidAsText tests centroid output as WKT.
func TestSTCentroidAsText(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	var wkt string
	err = db.QueryRow("SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))')))").
		Scan(&wkt)
	require.NoError(t, err)
	assert.Equal(t, "POINT(1 1)", wkt)
}

// TestGeometricAnalysisNullHandling tests NULL handling.
func TestGeometricAnalysisNullHandling(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// ST_Area with NULL should return NULL
	var areaResult sql.NullFloat64
	err = db.QueryRow("SELECT ST_Area(NULL)").Scan(&areaResult)
	require.NoError(t, err)
	assert.False(t, areaResult.Valid)

	// ST_Length with NULL should return NULL
	var lengthResult sql.NullFloat64
	err = db.QueryRow("SELECT ST_Length(NULL)").Scan(&lengthResult)
	require.NoError(t, err)
	assert.False(t, lengthResult.Valid)

	// ST_Centroid with NULL should return NULL
	var centroidResult sql.NullString
	err = db.QueryRow("SELECT ST_AsText(ST_Centroid(NULL))").Scan(&centroidResult)
	require.NoError(t, err)
	assert.False(t, centroidResult.Valid)
}

// TestGeometricAnalysisWithTable tests functions with table data.
func TestGeometricAnalysisWithTable(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a table with geometry data
	_, err = db.Exec(`CREATE TABLE shapes (id INTEGER, name VARCHAR, geom VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO shapes VALUES
		(1, 'unit_square', 'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),
		(2, 'rectangle', 'POLYGON((0 0, 0 4, 3 4, 3 0, 0 0))'),
		(3, 'line', 'LINESTRING(0 0, 3 4)')`)
	require.NoError(t, err)

	// Query areas
	rows, err := db.Query(
		`SELECT name, ST_Area(ST_GeomFromText(geom)) AS area FROM shapes ORDER BY id`,
	)
	require.NoError(t, err)
	defer rows.Close()

	expected := []struct {
		name string
		area float64
	}{
		{"unit_square", 1.0},
		{"rectangle", 12.0},
		{"line", 0.0},
	}

	i := 0
	for rows.Next() {
		var name string
		var area float64
		err := rows.Scan(&name, &area)
		require.NoError(t, err)
		assert.Equal(t, expected[i].name, name)
		assert.InDelta(t, expected[i].area, area, 0.0001)
		i++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, i)
}

// TestAreaPolygonWithHole tests polygon with hole.
func TestAreaPolygonWithHole(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// 10x10 square with 2x2 hole
	// Exterior: 100, Hole: 4, Net: 96
	var area float64
	err = db.QueryRow("SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (4 4, 4 6, 6 6, 6 4, 4 4))'))").
		Scan(&area)
	require.NoError(t, err)
	assert.InDelta(t, 96.0, area, 0.0001)
}

// TestComplexGeometricAnalysis tests more complex scenarios.
func TestComplexGeometricAnalysis(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Test that we can combine geometric analysis with other SQL features
	_, err = db.Exec(`CREATE TABLE polygons (id INTEGER, wkt VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO polygons VALUES
		(1, 'POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
		(2, 'POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))'),
		(3, 'POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))')`)
	require.NoError(t, err)

	// Query all areas to verify they're correct
	rows, err := db.Query(`SELECT id, ST_Area(ST_GeomFromText(wkt)) FROM polygons ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	expectedAreas := map[int]float64{1: 4.0, 2: 9.0, 3: 16.0}
	for rows.Next() {
		var id int
		var area float64
		err := rows.Scan(&id, &area)
		require.NoError(t, err)
		assert.InDelta(t, expectedAreas[id], area, 0.0001)
	}
	require.NoError(t, rows.Err())

	// Sum of all areas
	var totalArea float64
	err = db.QueryRow(`SELECT SUM(ST_Area(ST_GeomFromText(wkt))) FROM polygons`).Scan(&totalArea)
	require.NoError(t, err)
	assert.InDelta(t, 29.0, totalArea, 0.0001) // 4 + 9 + 16 = 29

	// Max area using MAX function
	var maxArea float64
	err = db.QueryRow(`SELECT MAX(ST_Area(ST_GeomFromText(wkt))) FROM polygons`).Scan(&maxArea)
	require.NoError(t, err)
	assert.InDelta(t, 16.0, maxArea, 0.0001)
}

// TestDeliverableExamples tests the exact SQL examples from the spec.
func TestDeliverableExamples(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Test: SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')); -- returns 1.0
	var area float64
	err = db.QueryRow("SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))").
		Scan(&area)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, area, 0.0001)

	// Test: SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)')); -- returns 5.0
	var length float64
	err = db.QueryRow("SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 3 4)'))").Scan(&length)
	require.NoError(t, err)
	assert.InDelta(t, 5.0, length, 0.0001)

	// Test: SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'))); -- returns 'POINT(1 1)'
	var wkt string
	err = db.QueryRow("SELECT ST_AsText(ST_Centroid(ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))')))").
		Scan(&wkt)
	require.NoError(t, err)
	assert.Equal(t, "POINT(1 1)", wkt)
}

// TestDiagonalLineLength tests diagonal line length calculation.
func TestDiagonalLineLength(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Test 45-degree diagonal: sqrt(2) for unit diagonal
	var length float64
	err = db.QueryRow("SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 1 1)'))").Scan(&length)
	require.NoError(t, err)
	assert.InDelta(t, math.Sqrt(2), length, 0.0001)

	// Test longer diagonal
	err = db.QueryRow("SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 10 10)'))").Scan(&length)
	require.NoError(t, err)
	assert.InDelta(t, math.Sqrt(200), length, 0.0001) // sqrt(10^2 + 10^2) = sqrt(200)
}
