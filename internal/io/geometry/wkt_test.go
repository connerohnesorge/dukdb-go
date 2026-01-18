package geometry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseWKT_Point(t *testing.T) {
	tests := []struct {
		name    string
		wkt     string
		wantX   float64
		wantY   float64
		wantErr bool
	}{
		{"basic point", "POINT(1 2)", 1.0, 2.0, false},
		{"point with decimals", "POINT(1.5 2.5)", 1.5, 2.5, false},
		{"point with negatives", "POINT(-1.5 -2.5)", -1.5, -2.5, false},
		{"point with spaces", "POINT( 1  2 )", 1.0, 2.0, false},
		{"point lowercase", "point(3 4)", 3.0, 4.0, false},
		{"point mixed case", "Point(5 6)", 5.0, 6.0, false},
		{"point with zero", "POINT(0 0)", 0.0, 0.0, false},
		{"point large coords", "POINT(12345.6789 -98765.4321)", 12345.6789, -98765.4321, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geom, err := ParseWKT(tt.wkt)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, geom)
			assert.Equal(t, GeometryPoint, geom.Type)

			x, err := geom.X()
			require.NoError(t, err)
			assert.InDelta(t, tt.wantX, x, 0.0001)

			y, err := geom.Y()
			require.NoError(t, err)
			assert.InDelta(t, tt.wantY, y, 0.0001)
		})
	}
}

func TestParseWKT_LineString(t *testing.T) {
	tests := []struct {
		name      string
		wkt       string
		wantType  GeometryType
		wantErr   bool
		errSubstr string
	}{
		{"basic linestring", "LINESTRING(0 0, 1 1, 2 2)", GeometryLineString, false, ""},
		{"linestring with decimals", "LINESTRING(0.5 0.5, 1.5 1.5)", GeometryLineString, false, ""},
		{"linestring lowercase", "linestring(0 0, 10 10)", GeometryLineString, false, ""},
		{"linestring too few points", "LINESTRING(0 0)", 0, true, "at least 2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geom, err := ParseWKT(tt.wkt)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errSubstr != "" {
					assert.Contains(t, err.Error(), tt.errSubstr)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, geom)
			assert.Equal(t, tt.wantType, geom.Type)
		})
	}
}

func TestParseWKT_Polygon(t *testing.T) {
	tests := []struct {
		name     string
		wkt      string
		wantType GeometryType
		wantErr  bool
	}{
		{"simple polygon", "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))", GeometryPolygon, false},
		{
			"polygon with hole",
			"POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2))",
			GeometryPolygon,
			false,
		},
		{"polygon lowercase", "polygon((0 0, 0 1, 1 1, 1 0, 0 0))", GeometryPolygon, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geom, err := ParseWKT(tt.wkt)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, geom)
			assert.Equal(t, tt.wantType, geom.Type)
		})
	}
}

func TestParseWKT_MultiPoint(t *testing.T) {
	tests := []struct {
		name     string
		wkt      string
		wantType GeometryType
		wantErr  bool
	}{
		{"multipoint with parens", "MULTIPOINT((0 0), (1 1))", GeometryMultiPoint, false},
		{"multipoint without parens", "MULTIPOINT(0 0, 1 1)", GeometryMultiPoint, false},
		{"multipoint mixed", "MULTIPOINT((0 0), (1 1), (2 2))", GeometryMultiPoint, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geom, err := ParseWKT(tt.wkt)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, geom)
			assert.Equal(t, tt.wantType, geom.Type)
		})
	}
}

func TestParseWKT_MultiLineString(t *testing.T) {
	tests := []struct {
		name     string
		wkt      string
		wantType GeometryType
		wantErr  bool
	}{
		{
			"basic multilinestring",
			"MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
			GeometryMultiLineString,
			false,
		},
		{
			"multilinestring with more points",
			"MULTILINESTRING((0 0, 1 1, 2 2), (3 3, 4 4))",
			GeometryMultiLineString,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geom, err := ParseWKT(tt.wkt)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, geom)
			assert.Equal(t, tt.wantType, geom.Type)
		})
	}
}

func TestParseWKT_MultiPolygon(t *testing.T) {
	tests := []struct {
		name     string
		wkt      string
		wantType GeometryType
		wantErr  bool
	}{
		{
			"basic multipolygon",
			"MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))",
			GeometryMultiPolygon,
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geom, err := ParseWKT(tt.wkt)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, geom)
			assert.Equal(t, tt.wantType, geom.Type)
		})
	}
}

func TestParseWKT_GeometryCollection(t *testing.T) {
	tests := []struct {
		name     string
		wkt      string
		wantType GeometryType
		wantErr  bool
	}{
		{
			"point and line",
			"GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1))",
			GeometryCollection,
			false,
		},
		{
			"mixed types",
			"GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1), POLYGON((0 0, 0 1, 1 1, 1 0, 0 0)))",
			GeometryCollection,
			false,
		},
		{"empty collection", "GEOMETRYCOLLECTION()", GeometryCollection, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geom, err := ParseWKT(tt.wkt)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, geom)
			assert.Equal(t, tt.wantType, geom.Type)
		})
	}
}

func TestParseWKT_Empty(t *testing.T) {
	tests := []struct {
		name     string
		wkt      string
		wantType GeometryType
	}{
		{"point empty", "POINT EMPTY", GeometryPoint},
		{"linestring empty", "LINESTRING EMPTY", GeometryLineString},
		{"polygon empty", "POLYGON EMPTY", GeometryPolygon},
		{"multipoint empty", "MULTIPOINT EMPTY", GeometryMultiPoint},
		{"multilinestring empty", "MULTILINESTRING EMPTY", GeometryMultiLineString},
		{"multipolygon empty", "MULTIPOLYGON EMPTY", GeometryMultiPolygon},
		{"geometrycollection empty", "GEOMETRYCOLLECTION EMPTY", GeometryCollection},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geom, err := ParseWKT(tt.wkt)
			require.NoError(t, err)
			require.NotNil(t, geom)
			assert.Equal(t, tt.wantType, geom.Type)
		})
	}
}

func TestFormatWKT_Roundtrip(t *testing.T) {
	tests := []struct {
		name       string
		wkt        string
		normalized string // expected WKT after roundtrip (may differ from input due to formatting)
	}{
		{"point", "POINT(1 2)", "POINT(1 2)"},
		{"point with decimals", "POINT(1.5 2.5)", "POINT(1.5 2.5)"},
		{"linestring", "LINESTRING(0 0, 1 1, 2 2)", "LINESTRING(0 0, 1 1, 2 2)"},
		{"polygon", "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))", "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			geom, err := ParseWKT(tt.wkt)
			require.NoError(t, err)

			wkt, err := FormatWKT(geom)
			require.NoError(t, err)
			assert.Equal(t, tt.normalized, wkt)
		})
	}
}

func TestParseWKT_Errors(t *testing.T) {
	tests := []struct {
		name string
		wkt  string
	}{
		{"empty string", ""},
		{"missing type", "(1 2)"},
		{"unknown type", "CIRCLE(1 2)"},
		{"unclosed paren", "POINT(1 2"},
		{"missing coords", "POINT()"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseWKT(tt.wkt)
			require.Error(t, err)
		})
	}
}

func TestGeometry_X_Y_Z(t *testing.T) {
	t.Run("X coordinate", func(t *testing.T) {
		geom, err := ParseWKT("POINT(1.5 2.5)")
		require.NoError(t, err)

		x, err := geom.X()
		require.NoError(t, err)
		assert.InDelta(t, 1.5, x, 0.0001)
	})

	t.Run("Y coordinate", func(t *testing.T) {
		geom, err := ParseWKT("POINT(1.5 2.5)")
		require.NoError(t, err)

		y, err := geom.Y()
		require.NoError(t, err)
		assert.InDelta(t, 2.5, y, 0.0001)
	})

	t.Run("X on non-point fails", func(t *testing.T) {
		geom, err := ParseWKT("LINESTRING(0 0, 1 1)")
		require.NoError(t, err)

		_, err = geom.X()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "POINT")
	})

	t.Run("Y on non-point fails", func(t *testing.T) {
		geom, err := ParseWKT("LINESTRING(0 0, 1 1)")
		require.NoError(t, err)

		_, err = geom.Y()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "POINT")
	})

	t.Run("Z on 2D point fails", func(t *testing.T) {
		geom, err := ParseWKT("POINT(1 2)")
		require.NoError(t, err)

		_, err = geom.Z()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not 3D")
	})
}

func TestGeometry_SRID(t *testing.T) {
	t.Run("default SRID is 0", func(t *testing.T) {
		geom, err := ParseWKT("POINT(1 2)")
		require.NoError(t, err)
		assert.Equal(t, int32(0), geom.GetSRID())
	})

	t.Run("WithSRID creates new geometry", func(t *testing.T) {
		geom, err := ParseWKT("POINT(1 2)")
		require.NoError(t, err)

		geom4326 := geom.WithSRID(4326)
		assert.Equal(t, int32(4326), geom4326.GetSRID())
		assert.Equal(t, int32(0), geom.GetSRID()) // Original unchanged
	})
}
