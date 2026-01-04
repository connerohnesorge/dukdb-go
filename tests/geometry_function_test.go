package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSTGeomFromText tests the ST_GeomFromText function.
func TestSTGeomFromText(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("parse POINT", func(t *testing.T) {
		var result float64
		err := db.QueryRow(`SELECT ST_X(ST_GeomFromText('POINT(1.5 2.5)'))`).Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 1.5, result, 0.0001)
	})

	t.Run("parse LINESTRING", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "LINESTRING", result)
	})

	t.Run("parse POLYGON", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT ST_GeometryType(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "POLYGON", result)
	})

	t.Run("NULL input returns NULL", func(t *testing.T) {
		var result sql.NullFloat64
		err := db.QueryRow(`SELECT ST_X(ST_GeomFromText(NULL))`).Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid)
	})
}

// TestSTAsText tests the ST_AsText function.
func TestSTAsText(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("POINT to WKT", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT ST_AsText(ST_GeomFromText('POINT(3 4)'))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "POINT(3 4)", result)
	})

	t.Run("LINESTRING to WKT", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT ST_AsText(ST_GeomFromText('LINESTRING(0 0, 1 1)'))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "LINESTRING(0 0, 1 1)", result)
	})

	t.Run("roundtrip preserves data", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT ST_AsText(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))", result)
	})
}

// TestSTGeometryType tests the ST_GeometryType function.
func TestSTGeometryType(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		wkt      string
		expected string
	}{
		{"POINT(0 0)", "POINT"},
		{"LINESTRING(0 0, 1 1)", "LINESTRING"},
		{"POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))", "POLYGON"},
		{"MULTIPOINT((0 0), (1 1))", "MULTIPOINT"},
		{"MULTILINESTRING((0 0, 1 1), (2 2, 3 3))", "MULTILINESTRING"},
		{"MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)))", "MULTIPOLYGON"},
		{"GEOMETRYCOLLECTION(POINT(0 0))", "GEOMETRYCOLLECTION"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			var result string
			err := db.QueryRow(`SELECT ST_GeometryType(ST_GeomFromText($1))`, tc.wkt).Scan(&result)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestSTX tests the ST_X function.
func TestSTX(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name     string
		wkt      string
		expected float64
	}{
		{"positive X", "POINT(1.5 2.5)", 1.5},
		{"negative X", "POINT(-3.7 4.2)", -3.7},
		{"zero X", "POINT(0 5)", 0.0},
		{"large X", "POINT(12345.6789 0)", 12345.6789},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var result float64
			err := db.QueryRow(`SELECT ST_X(ST_GeomFromText($1))`, tc.wkt).Scan(&result)
			require.NoError(t, err)
			assert.InDelta(t, tc.expected, result, 0.0001)
		})
	}
}

// TestSTY tests the ST_Y function.
func TestSTY(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	tests := []struct {
		name     string
		wkt      string
		expected float64
	}{
		{"positive Y", "POINT(1.5 2.5)", 2.5},
		{"negative Y", "POINT(3.7 -4.2)", -4.2},
		{"zero Y", "POINT(5 0)", 0.0},
		{"large Y", "POINT(0 98765.4321)", 98765.4321},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var result float64
			err := db.QueryRow(`SELECT ST_Y(ST_GeomFromText($1))`, tc.wkt).Scan(&result)
			require.NoError(t, err)
			assert.InDelta(t, tc.expected, result, 0.0001)
		})
	}
}

// TestSTPoint tests the ST_Point function.
func TestSTPoint(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("create point from coordinates", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT ST_AsText(ST_Point(3.0, 4.0))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "POINT(3 4)", result)
	})

	t.Run("verify X coordinate", func(t *testing.T) {
		var result float64
		err := db.QueryRow(`SELECT ST_X(ST_Point(5.5, 6.5))`).Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 5.5, result, 0.0001)
	})

	t.Run("verify Y coordinate", func(t *testing.T) {
		var result float64
		err := db.QueryRow(`SELECT ST_Y(ST_Point(5.5, 6.5))`).Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 6.5, result, 0.0001)
	})

	t.Run("negative coordinates", func(t *testing.T) {
		var x, y float64
		err := db.QueryRow(`SELECT ST_X(ST_Point(-1.5, -2.5)), ST_Y(ST_Point(-1.5, -2.5))`).Scan(&x, &y)
		require.NoError(t, err)
		assert.InDelta(t, -1.5, x, 0.0001)
		assert.InDelta(t, -2.5, y, 0.0001)
	})
}

// TestSTMakeLine tests the ST_MakeLine function.
func TestSTMakeLine(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("create line from two points", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT ST_AsText(ST_MakeLine(ST_Point(0, 0), ST_Point(1, 1)))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "LINESTRING(0 0, 1 1)", result)
	})

	t.Run("verify geometry type", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT ST_GeometryType(ST_MakeLine(ST_Point(0, 0), ST_Point(1, 1)))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "LINESTRING", result)
	})
}

// TestSTSRID tests the ST_SRID function.
func TestSTSRID(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("default SRID is 0", func(t *testing.T) {
		var result int64
		err := db.QueryRow(`SELECT ST_SRID(ST_GeomFromText('POINT(0 0)'))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result)
	})
}

// TestSTSetSRID tests the ST_SetSRID function.
func TestSTSetSRID(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("set SRID to 4326", func(t *testing.T) {
		var result int64
		err := db.QueryRow(`SELECT ST_SRID(ST_SetSRID(ST_GeomFromText('POINT(0 0)'), 4326))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(4326), result)
	})

	t.Run("geometry data preserved after SetSRID", func(t *testing.T) {
		var x, y float64
		err := db.QueryRow(`
			SELECT ST_X(ST_SetSRID(ST_GeomFromText('POINT(1.5 2.5)'), 4326)),
			       ST_Y(ST_SetSRID(ST_GeomFromText('POINT(1.5 2.5)'), 4326))
		`).Scan(&x, &y)
		require.NoError(t, err)
		assert.InDelta(t, 1.5, x, 0.0001)
		assert.InDelta(t, 2.5, y, 0.0001)
	})
}

// TestGeometryFunctionsIntegration tests the example SQL from the requirements.
func TestGeometryFunctionsIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("ST_X from WKT", func(t *testing.T) {
		var result float64
		err := db.QueryRow(`SELECT ST_X(ST_GeomFromText('POINT(1.5 2.5)'))`).Scan(&result)
		require.NoError(t, err)
		assert.InDelta(t, 1.5, result, 0.0001)
	})

	t.Run("ST_AsText from ST_Point", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT ST_AsText(ST_Point(3.0, 4.0))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "POINT(3 4)", result)
	})

	t.Run("ST_GeometryType from LINESTRING", func(t *testing.T) {
		var result string
		err := db.QueryRow(`SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(0 0, 1 1)'))`).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "LINESTRING", result)
	})
}
