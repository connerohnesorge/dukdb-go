package tests

import (
	"database/sql"
	"math"
	"strings"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSTMakePolygon tests creating polygons from closed linestrings.
func TestSTMakePolygon(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name        string
		query       string
		expectError bool
		validate    func(t *testing.T, result string)
	}{
		{
			name:  "unit square from linestring",
			query: "SELECT ST_AsText(ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)')))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POLYGON")
			},
		},
		{
			name:  "triangle from linestring",
			query: "SELECT ST_AsText(ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0, 5 0, 2.5 5, 0 0)')))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POLYGON")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.validate != nil && result.Valid {
				tt.validate(t, result.String)
			}
		})
	}
}

// TestSTBuffer tests the ST_Buffer function for creating buffer zones.
func TestSTBuffer(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name        string
		query       string
		expectError bool
		validate    func(t *testing.T, result string)
	}{
		{
			name:  "buffer point creates polygon",
			query: "SELECT ST_AsText(ST_Buffer(ST_Point(0, 0), 1))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POLYGON")
			},
		},
		{
			name:  "buffer linestring creates polygon",
			query: "SELECT ST_AsText(ST_Buffer(ST_GeomFromText('LINESTRING(0 0, 10 0)'), 1))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POLYGON")
			},
		},
		{
			name:  "buffer polygon expands it",
			query: "SELECT ST_AsText(ST_Buffer(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), 1))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POLYGON")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.validate != nil && result.Valid {
				tt.validate(t, result.String)
			}
		})
	}
}

// TestSTBuffer_AreaVerification verifies buffer creates correct area.
func TestSTBuffer_AreaVerification(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Buffer a point with radius 1 should have area approximately pi
	var area float64
	err = db.QueryRow("SELECT ST_Area(ST_Buffer(ST_Point(0, 0), 1))").Scan(&area)
	require.NoError(t, err)
	assert.InDelta(t, math.Pi, area, 0.1) // Some tolerance for polygon approximation
}

// TestSTIntersection tests the ST_Intersection function.
func TestSTIntersection(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name      string
		query     string
		expectNil bool
		validate  func(t *testing.T, result string)
	}{
		{
			name:  "overlapping polygons intersection",
			query: "SELECT ST_AsText(ST_Intersection(ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'), ST_GeomFromText('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POLYGON")
			},
		},
		{
			name:      "disjoint polygons - no intersection",
			query:     "SELECT CASE WHEN ST_Intersection(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), ST_GeomFromText('POLYGON((5 5, 5 6, 6 6, 6 5, 5 5))')) IS NULL THEN 'NULL' ELSE 'NOT_NULL' END",
			expectNil: true,
		},
		{
			name:  "point inside polygon",
			query: "SELECT ST_AsText(ST_Intersection(ST_Point(5, 5), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POINT")
			},
		},
		{
			name:      "point outside polygon",
			query:     "SELECT CASE WHEN ST_Intersection(ST_Point(15, 15), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')) IS NULL THEN 'NULL' ELSE 'NOT_NULL' END",
			expectNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			require.NoError(t, err)

			if tt.expectNil {
				// We're using CASE WHEN ... IS NULL, so result should be 'NULL'
				require.True(t, result.Valid)
				assert.Equal(
					t,
					"NULL",
					result.String,
					"expected NULL result for empty intersection",
				)
				return
			}

			require.True(t, result.Valid, "expected non-null result")
			if tt.validate != nil {
				tt.validate(t, result.String)
			}
		})
	}
}

// TestSTIntersection_AreaVerification verifies intersection produces correct area.
func TestSTIntersection_AreaVerification(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Two 2x2 squares overlapping in a 1x1 region
	var area float64
	err = db.QueryRow(`
		SELECT ST_Area(ST_Intersection(
			ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'),
			ST_GeomFromText('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')
		))
	`).Scan(&area)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, area, 0.0001) // Intersection should be 1x1 = 1.0
}

// TestSTUnion tests the ST_Union function.
func TestSTUnion(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		validate func(t *testing.T, result string)
	}{
		{
			name:  "disjoint polygons - multipolygon",
			query: "SELECT ST_AsText(ST_Union(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), ST_GeomFromText('POLYGON((5 5, 5 6, 6 6, 6 5, 5 5))')))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "MULTIPOLYGON")
			},
		},
		{
			name:  "overlapping polygons",
			query: "SELECT ST_AsText(ST_Union(ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'), ST_GeomFromText('POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))')))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POLYGON")
			},
		},
		{
			name:  "contained polygon",
			query: "SELECT ST_AsText(ST_Union(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POLYGON((2 2, 2 8, 8 8, 8 2, 2 2))')))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POLYGON")
			},
		},
		{
			name:  "identical points",
			query: "SELECT ST_AsText(ST_Union(ST_Point(5, 5), ST_Point(5, 5)))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POINT")
			},
		},
		{
			name:  "different points - multipoint",
			query: "SELECT ST_AsText(ST_Union(ST_Point(5, 5), ST_Point(6, 6)))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "MULTIPOINT")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			require.NoError(t, err)
			require.True(t, result.Valid)
			if tt.validate != nil {
				tt.validate(t, result.String)
			}
		})
	}
}

// TestSTDifference tests the ST_Difference function.
func TestSTDifference(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name      string
		query     string
		expectNil bool
		validate  func(t *testing.T, result string)
	}{
		{
			name:  "disjoint polygons - returns first",
			query: "SELECT ST_AsText(ST_Difference(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), ST_GeomFromText('POLYGON((5 5, 5 6, 6 6, 6 5, 5 5))')))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POLYGON")
			},
		},
		{
			name:      "inner contained by outer - empty result",
			query:     "SELECT CASE WHEN ST_Difference(ST_GeomFromText('POLYGON((2 2, 2 8, 8 8, 8 2, 2 2))'), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')) IS NULL THEN 'NULL' ELSE 'NOT_NULL' END",
			expectNil: true,
		},
		{
			name:  "outer minus inner - polygon with hole",
			query: "SELECT ST_AsText(ST_Difference(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POLYGON((2 2, 2 8, 8 8, 8 2, 2 2))')))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POLYGON")
			},
		},
		{
			name:      "point inside polygon - empty result",
			query:     "SELECT CASE WHEN ST_Difference(ST_Point(5, 5), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')) IS NULL THEN 'NULL' ELSE 'NOT_NULL' END",
			expectNil: true,
		},
		{
			name:  "point outside polygon - returns point",
			query: "SELECT ST_AsText(ST_Difference(ST_Point(15, 15), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')))",
			validate: func(t *testing.T, result string) {
				assert.Contains(t, result, "POINT")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			require.NoError(t, err)

			if tt.expectNil {
				// We're using CASE WHEN ... IS NULL, so result should be 'NULL'
				require.True(t, result.Valid)
				assert.Equal(t, "NULL", result.String, "expected NULL result for empty difference")
				return
			}

			require.True(t, result.Valid, "expected non-null result")
			if tt.validate != nil {
				tt.validate(t, result.String)
			}
		})
	}
}

// TestSTDifference_AreaVerification verifies difference produces correct area.
func TestSTDifference_AreaVerification(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// 10x10 square minus 6x6 inner square = 100 - 36 = 64
	var area float64
	err = db.QueryRow(`
		SELECT ST_Area(ST_Difference(
			ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
			ST_GeomFromText('POLYGON((2 2, 2 8, 8 8, 8 2, 2 2))')
		))
	`).Scan(&area)
	require.NoError(t, err)
	assert.InDelta(t, 64.0, area, 0.0001)
}

// TestSetOperationsWithTable tests set operations with data from tables.
func TestSetOperationsWithTable(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create table with geometry data
	_, err = db.Exec(`
		CREATE TABLE parcels (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			geom GEOMETRY
		)
	`)
	require.NoError(t, err)

	// Insert parcels
	_, err = db.Exec(
		`INSERT INTO parcels VALUES (1, 'Parcel A', ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))`,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO parcels VALUES (2, 'Parcel B', ST_GeomFromText('POLYGON((5 5, 5 15, 15 15, 15 5, 5 5))'))`,
	)
	require.NoError(t, err)

	// Test intersection of two parcels
	var intersection sql.NullString
	err = db.QueryRow(`
		SELECT ST_AsText(ST_Intersection(a.geom, b.geom))
		FROM parcels a, parcels b
		WHERE a.id = 1 AND b.id = 2
	`).Scan(&intersection)
	require.NoError(t, err)
	require.True(t, intersection.Valid)
	assert.Contains(t, intersection.String, "POLYGON")

	// Test union of two parcels
	var union sql.NullString
	err = db.QueryRow(`
		SELECT ST_AsText(ST_Union(a.geom, b.geom))
		FROM parcels a, parcels b
		WHERE a.id = 1 AND b.id = 2
	`).Scan(&union)
	require.NoError(t, err)
	require.True(t, union.Valid)
	assert.Contains(t, union.String, "POLYGON")
}

// TestBufferAndIntersection tests combining buffer and intersection.
func TestBufferAndIntersection(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create a point and buffer it, then intersect with a polygon
	var result sql.NullString
	err = db.QueryRow(`
		SELECT ST_AsText(ST_Intersection(
			ST_Buffer(ST_Point(5, 5), 3),
			ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))')
		))
	`).Scan(&result)
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Contains(t, result.String, "POLYGON")
}

// TestMakePolygonWithBuffer tests creating a polygon and buffering it.
func TestMakePolygonWithBuffer(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create polygon from linestring and buffer it
	var result sql.NullString
	err = db.QueryRow(`
		SELECT ST_AsText(ST_Buffer(
			ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0, 0 10, 10 10, 10 0, 0 0)')),
			1
		))
	`).Scan(&result)
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Contains(t, result.String, "POLYGON")
}

// TestSetOperationsNullHandling tests null handling in set operations.
func TestSetOperationsNullHandling(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "union with null",
			query: "SELECT ST_Union(NULL, ST_Point(0, 0))",
		},
		{
			name:  "intersection with null",
			query: "SELECT ST_Intersection(NULL, ST_Point(0, 0))",
		},
		{
			name:  "difference with null",
			query: "SELECT ST_Difference(NULL, ST_Point(0, 0))",
		},
		{
			name:  "buffer with null geometry",
			query: "SELECT ST_Buffer(NULL, 1)",
		},
		{
			name:  "makepolygon with null",
			query: "SELECT ST_MakePolygon(NULL)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			require.NoError(t, err)
			assert.False(t, result.Valid, "null input should produce null output")
		})
	}
}

// TestChainedSetOperations tests chaining multiple set operations.
func TestChainedSetOperations(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Chain: Create polygons, union them, then intersect with another
	var result sql.NullString
	err = db.QueryRow(`
		SELECT ST_AsText(ST_Intersection(
			ST_Union(
				ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'),
				ST_GeomFromText('POLYGON((3 0, 3 5, 8 5, 8 0, 3 0))')
			),
			ST_GeomFromText('POLYGON((2 2, 2 10, 10 10, 10 2, 2 2))')
		))
	`).Scan(&result)
	require.NoError(t, err)
	require.True(t, result.Valid)
	assert.Contains(t, result.String, "POLYGON")
}

// TestBufferCentroid tests that buffered geometry has correct centroid.
func TestBufferCentroid(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Buffer a point at (5, 5), centroid should still be approximately (5, 5)
	var centroidWKT string
	err = db.QueryRow(`
		SELECT ST_AsText(ST_Centroid(ST_Buffer(ST_Point(5, 5), 1)))
	`).Scan(&centroidWKT)
	require.NoError(t, err)

	// Extract coordinates from WKT (POINT(x y))
	assert.True(t, strings.HasPrefix(centroidWKT, "POINT"))
	assert.Contains(t, centroidWKT, "5")
}
