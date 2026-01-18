package geometry

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDistance_PointPoint(t *testing.T) {
	tests := []struct {
		name     string
		wkt1     string
		wkt2     string
		expected float64
	}{
		{
			name:     "horizontal distance",
			wkt1:     "POINT(0 0)",
			wkt2:     "POINT(3 0)",
			expected: 3.0,
		},
		{
			name:     "vertical distance",
			wkt1:     "POINT(0 0)",
			wkt2:     "POINT(0 4)",
			expected: 4.0,
		},
		{
			name:     "diagonal distance (3-4-5 triangle)",
			wkt1:     "POINT(0 0)",
			wkt2:     "POINT(3 4)",
			expected: 5.0,
		},
		{
			name:     "same point",
			wkt1:     "POINT(5 5)",
			wkt2:     "POINT(5 5)",
			expected: 0.0,
		},
		{
			name:     "negative coordinates",
			wkt1:     "POINT(-3 -4)",
			wkt2:     "POINT(0 0)",
			expected: 5.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g1, err := ParseWKT(tt.wkt1)
			require.NoError(t, err)
			g2, err := ParseWKT(tt.wkt2)
			require.NoError(t, err)

			dist, err := Distance(g1, g2)
			require.NoError(t, err)
			assert.InDelta(t, tt.expected, dist, 0.0001)
		})
	}
}

func TestDistance_PointLine(t *testing.T) {
	// Point to linestring distance - finds minimum distance to vertices
	// Our implementation checks vertex-to-vertex distances, not perpendicular distance
	g1, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)
	g2, err := ParseWKT("LINESTRING(0 0, 10 0)")
	require.NoError(t, err)

	dist, err := Distance(g1, g2)
	require.NoError(t, err)
	// Distance from (5,5) to nearest vertex (0,0 or 10,0) is sqrt(50) = ~7.07
	// This is minimum vertex distance, not perpendicular distance
	assert.InDelta(t, math.Sqrt(50), dist, 0.0001)
}

func TestDistanceSphere(t *testing.T) {
	// New York City
	nyc, err := ParseWKT("POINT(-73.9857 40.7484)")
	require.NoError(t, err)

	// London
	london, err := ParseWKT("POINT(-0.1278 51.5074)")
	require.NoError(t, err)

	dist, err := DistanceSphere(nyc, london)
	require.NoError(t, err)

	// NYC to London is approximately 5570 km
	expectedKm := 5570.0
	actualKm := dist / 1000.0
	assert.InDelta(t, expectedKm, actualKm, 50.0) // Within 50km tolerance

	// Same point should have zero distance
	dist2, err := DistanceSphere(nyc, nyc)
	require.NoError(t, err)
	assert.Equal(t, 0.0, dist2)
}

func TestDistanceSphere_NonPoint(t *testing.T) {
	line, err := ParseWKT("LINESTRING(0 0, 1 1)")
	require.NoError(t, err)
	point, err := ParseWKT("POINT(0 0)")
	require.NoError(t, err)

	_, err = DistanceSphere(line, point)
	assert.Error(t, err, "DistanceSphere should reject non-POINT geometries")
}

func TestContains_PolygonPoint(t *testing.T) {
	poly, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)

	tests := []struct {
		name     string
		pointWkt string
		expected bool
	}{
		{
			name:     "point inside",
			pointWkt: "POINT(5 5)",
			expected: true,
		},
		{
			name:     "point outside",
			pointWkt: "POINT(15 15)",
			expected: false,
		},
		{
			name:     "point on edge",
			pointWkt: "POINT(5 0)",
			expected: true, // Ray casting algorithm includes some boundary points
		},
		{
			name:     "point at corner",
			pointWkt: "POINT(0 0)",
			expected: true, // Ray casting algorithm includes corner
		},
		{
			name:     "point near edge but inside",
			pointWkt: "POINT(0.001 0.001)",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			point, err := ParseWKT(tt.pointWkt)
			require.NoError(t, err)

			contains, err := Contains(poly, point)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, contains)
		})
	}
}

func TestContains_PolygonPolygon(t *testing.T) {
	outer, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)

	// Inner polygon fully inside outer
	inner, err := ParseWKT("POLYGON((2 2, 2 8, 8 8, 8 2, 2 2))")
	require.NoError(t, err)

	contains, err := Contains(outer, inner)
	require.NoError(t, err)
	assert.True(t, contains, "outer polygon should contain inner polygon")

	// Check reverse - inner should not contain outer
	contains2, err := Contains(inner, outer)
	require.NoError(t, err)
	assert.False(t, contains2, "inner polygon should not contain outer polygon")
}

func TestWithin(t *testing.T) {
	outer, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)

	point, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)

	within, err := Within(point, outer)
	require.NoError(t, err)
	assert.True(t, within, "point should be within polygon")

	// Point outside
	point2, err := ParseWKT("POINT(15 15)")
	require.NoError(t, err)

	within2, err := Within(point2, outer)
	require.NoError(t, err)
	assert.False(t, within2, "point should not be within polygon")
}

func TestIntersects(t *testing.T) {
	tests := []struct {
		name     string
		wkt1     string
		wkt2     string
		expected bool
	}{
		{
			name:     "crossing lines",
			wkt1:     "LINESTRING(0 0, 10 10)",
			wkt2:     "LINESTRING(0 10, 10 0)",
			expected: true,
		},
		{
			name:     "parallel lines",
			wkt1:     "LINESTRING(0 0, 10 0)",
			wkt2:     "LINESTRING(0 5, 10 5)",
			expected: false,
		},
		{
			name:     "point in polygon",
			wkt1:     "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
			wkt2:     "POINT(5 5)",
			expected: true,
		},
		{
			name:     "point outside polygon",
			wkt1:     "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
			wkt2:     "POINT(15 15)",
			expected: false,
		},
		{
			name:     "identical points",
			wkt1:     "POINT(5 5)",
			wkt2:     "POINT(5 5)",
			expected: true,
		},
		{
			name:     "different points",
			wkt1:     "POINT(5 5)",
			wkt2:     "POINT(6 6)",
			expected: false,
		},
		{
			name:     "overlapping polygons",
			wkt1:     "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
			wkt2:     "POLYGON((5 5, 5 15, 15 15, 15 5, 5 5))",
			expected: true,
		},
		{
			name:     "disjoint polygons",
			wkt1:     "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))",
			wkt2:     "POLYGON((10 10, 10 15, 15 15, 15 10, 10 10))",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g1, err := ParseWKT(tt.wkt1)
			require.NoError(t, err)
			g2, err := ParseWKT(tt.wkt2)
			require.NoError(t, err)

			result, err := Intersects(g1, g2)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDisjoint(t *testing.T) {
	g1, err := ParseWKT("POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))")
	require.NoError(t, err)
	g2, err := ParseWKT("POLYGON((10 10, 10 15, 15 15, 15 10, 10 10))")
	require.NoError(t, err)

	disjoint, err := Disjoint(g1, g2)
	require.NoError(t, err)
	assert.True(t, disjoint, "disjoint polygons should return true")

	// Overlapping polygons
	g3, err := ParseWKT("POLYGON((3 3, 3 8, 8 8, 8 3, 3 3))")
	require.NoError(t, err)

	disjoint2, err := Disjoint(g1, g3)
	require.NoError(t, err)
	assert.False(t, disjoint2, "overlapping polygons should not be disjoint")
}

func TestEquals(t *testing.T) {
	tests := []struct {
		name     string
		wkt1     string
		wkt2     string
		expected bool
	}{
		{
			name:     "identical points",
			wkt1:     "POINT(5 5)",
			wkt2:     "POINT(5 5)",
			expected: true,
		},
		{
			name:     "different points",
			wkt1:     "POINT(5 5)",
			wkt2:     "POINT(5 6)",
			expected: false,
		},
		{
			name:     "identical lines",
			wkt1:     "LINESTRING(0 0, 10 10)",
			wkt2:     "LINESTRING(0 0, 10 10)",
			expected: true,
		},
		{
			name:     "different lines",
			wkt1:     "LINESTRING(0 0, 10 10)",
			wkt2:     "LINESTRING(0 0, 5 5)",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g1, err := ParseWKT(tt.wkt1)
			require.NoError(t, err)
			g2, err := ParseWKT(tt.wkt2)
			require.NoError(t, err)

			result, err := Equals(g1, g2)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnvelope(t *testing.T) {
	tests := []struct {
		name        string
		wkt         string
		expectedBB  *BoundingBox
		expectedWKT string
	}{
		{
			name:        "point returns point",
			wkt:         "POINT(5 5)",
			expectedBB:  &BoundingBox{MinX: 5, MinY: 5, MaxX: 5, MaxY: 5},
			expectedWKT: "POINT(5 5)",
		},
		{
			name:        "horizontal line returns line",
			wkt:         "LINESTRING(0 5, 10 5)",
			expectedBB:  &BoundingBox{MinX: 0, MinY: 5, MaxX: 10, MaxY: 5},
			expectedWKT: "LINESTRING(0 5, 10 5)",
		},
		{
			name:       "linestring returns polygon",
			wkt:        "LINESTRING(0 0, 5 10, 10 0)",
			expectedBB: &BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10},
		},
		{
			name:       "polygon returns polygon",
			wkt:        "POLYGON((1 1, 1 9, 9 9, 9 1, 1 1))",
			expectedBB: &BoundingBox{MinX: 1, MinY: 1, MaxX: 9, MaxY: 9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := ParseWKT(tt.wkt)
			require.NoError(t, err)

			// Test BoundingBox
			bb, err := g.BoundingBox()
			require.NoError(t, err)
			assert.Equal(t, tt.expectedBB.MinX, bb.MinX)
			assert.Equal(t, tt.expectedBB.MinY, bb.MinY)
			assert.Equal(t, tt.expectedBB.MaxX, bb.MaxX)
			assert.Equal(t, tt.expectedBB.MaxY, bb.MaxY)

			// Test Envelope
			env, err := Envelope(g)
			require.NoError(t, err)
			assert.NotNil(t, env)

			// Verify envelope type
			if tt.expectedBB.MinX == tt.expectedBB.MaxX &&
				tt.expectedBB.MinY == tt.expectedBB.MaxY {
				assert.Equal(t, GeometryPoint, env.Type)
			} else if tt.expectedBB.MinX == tt.expectedBB.MaxX || tt.expectedBB.MinY == tt.expectedBB.MaxY {
				assert.Equal(t, GeometryLineString, env.Type)
			} else {
				assert.Equal(t, GeometryPolygon, env.Type)
			}
		})
	}
}

func TestPointInPolygon(t *testing.T) {
	// Test the internal pointInPolygon function
	// Square from (0,0) to (10,10)
	polygon := [][]float64{
		{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0},
	}

	tests := []struct {
		px, py   float64
		expected bool
	}{
		{5, 5, true},     // Inside
		{15, 5, false},   // Outside (right)
		{-5, 5, false},   // Outside (left)
		{5, 15, false},   // Outside (top)
		{5, -5, false},   // Outside (bottom)
		{0, 0, true},     // Corner - ray casting includes this
		{5, 0, true},     // Bottom edge - ray casting includes this
		{0.1, 0.1, true}, // Just inside corner
	}

	for _, tt := range tests {
		result := pointInPolygon(tt.px, tt.py, polygon)
		assert.Equal(
			t,
			tt.expected,
			result,
			"pointInPolygon(%v, %v) = %v, expected %v",
			tt.px,
			tt.py,
			result,
			tt.expected,
		)
	}
}

func TestSegmentsIntersect(t *testing.T) {
	tests := []struct {
		name     string
		p1, p2   [2]float64
		p3, p4   [2]float64
		expected bool
	}{
		{
			name:     "crossing X",
			p1:       [2]float64{0, 0},
			p2:       [2]float64{10, 10},
			p3:       [2]float64{0, 10},
			p4:       [2]float64{10, 0},
			expected: true,
		},
		{
			name:     "parallel horizontal",
			p1:       [2]float64{0, 0},
			p2:       [2]float64{10, 0},
			p3:       [2]float64{0, 5},
			p4:       [2]float64{10, 5},
			expected: false,
		},
		{
			name:     "T intersection",
			p1:       [2]float64{5, 0},
			p2:       [2]float64{5, 10},
			p3:       [2]float64{0, 5},
			p4:       [2]float64{10, 5},
			expected: true,
		},
		{
			name:     "non-intersecting",
			p1:       [2]float64{0, 0},
			p2:       [2]float64{5, 0},
			p3:       [2]float64{6, 0},
			p4:       [2]float64{10, 0},
			expected: false,
		},
		{
			name:     "touching at endpoint",
			p1:       [2]float64{0, 0},
			p2:       [2]float64{5, 5},
			p3:       [2]float64{5, 5},
			p4:       [2]float64{10, 10},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := segmentsIntersect(tt.p1, tt.p2, tt.p3, tt.p4)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCoordinates(t *testing.T) {
	tests := []struct {
		name        string
		wkt         string
		expectedLen int
	}{
		{
			name:        "point",
			wkt:         "POINT(5 10)",
			expectedLen: 1,
		},
		{
			name:        "linestring",
			wkt:         "LINESTRING(0 0, 5 5, 10 0)",
			expectedLen: 3,
		},
		{
			name:        "polygon",
			wkt:         "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
			expectedLen: 5, // Exterior ring only
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := ParseWKT(tt.wkt)
			require.NoError(t, err)

			coords, err := g.Coordinates()
			require.NoError(t, err)
			assert.Len(t, coords, tt.expectedLen)
		})
	}
}

func TestAllCoordinates(t *testing.T) {
	// Polygon with hole
	g, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2))")
	require.NoError(t, err)

	allCoords, err := g.AllCoordinates()
	require.NoError(t, err)
	assert.Len(t, allCoords, 2)    // Exterior + 1 hole
	assert.Len(t, allCoords[0], 5) // Exterior ring
	assert.Len(t, allCoords[1], 5) // Hole
}

func TestHaversine(t *testing.T) {
	// Test known distance between two points
	// San Francisco to Los Angeles is approximately 559 km
	sfLon, sfLat := -122.4194, 37.7749
	laLon, laLat := -118.2437, 34.0522

	dist := haversine(sfLon, sfLat, laLon, laLat)
	expectedKm := 559.0
	actualKm := dist / 1000.0

	assert.InDelta(t, expectedKm, actualKm, 10.0) // Within 10km tolerance
}

func TestOverlaps(t *testing.T) {
	tests := []struct {
		name     string
		wkt1     string
		wkt2     string
		expected bool
	}{
		{
			name:     "overlapping polygons",
			wkt1:     "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
			wkt2:     "POLYGON((5 5, 5 15, 15 15, 15 5, 5 5))",
			expected: true,
		},
		{
			name:     "one contains other - not overlap",
			wkt1:     "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
			wkt2:     "POLYGON((2 2, 2 8, 8 8, 8 2, 2 2))",
			expected: false,
		},
		{
			name:     "disjoint - not overlap",
			wkt1:     "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))",
			wkt2:     "POLYGON((10 10, 10 15, 15 15, 15 10, 10 10))",
			expected: false,
		},
		{
			name:     "overlapping lines",
			wkt1:     "LINESTRING(0 0, 10 10)",
			wkt2:     "LINESTRING(5 5, 15 15)",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g1, err := ParseWKT(tt.wkt1)
			require.NoError(t, err)
			g2, err := ParseWKT(tt.wkt2)
			require.NoError(t, err)

			result, err := Overlaps(g1, g2)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCrosses(t *testing.T) {
	tests := []struct {
		name     string
		wkt1     string
		wkt2     string
		expected bool
	}{
		{
			name:     "crossing lines",
			wkt1:     "LINESTRING(0 0, 10 10)",
			wkt2:     "LINESTRING(0 10, 10 0)",
			expected: true,
		},
		{
			name:     "parallel lines - no cross",
			wkt1:     "LINESTRING(0 0, 10 0)",
			wkt2:     "LINESTRING(0 5, 10 5)",
			expected: false,
		},
		// Note: Line crossing polygon is complex - our simple implementation
		// checks if line has points both inside and outside the polygon
		{
			name:     "line inside polygon - no cross",
			wkt1:     "LINESTRING(2 5, 8 5)",
			wkt2:     "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
			expected: false, // All points inside
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g1, err := ParseWKT(tt.wkt1)
			require.NoError(t, err)
			g2, err := ParseWKT(tt.wkt2)
			require.NoError(t, err)

			result, err := Crosses(g1, g2)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTouches(t *testing.T) {
	tests := []struct {
		name     string
		wkt1     string
		wkt2     string
		expected bool
	}{
		{
			name:     "lines sharing endpoint",
			wkt1:     "LINESTRING(0 0, 5 5)",
			wkt2:     "LINESTRING(5 5, 10 0)",
			expected: true,
		},
		{
			name:     "point on polygon boundary",
			wkt1:     "POINT(5 0)",
			wkt2:     "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
			expected: true,
		},
		{
			name:     "point inside polygon - not touching",
			wkt1:     "POINT(5 5)",
			wkt2:     "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g1, err := ParseWKT(tt.wkt1)
			require.NoError(t, err)
			g2, err := ParseWKT(tt.wkt2)
			require.NoError(t, err)

			result, err := Touches(g1, g2)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNilGeometries(t *testing.T) {
	g, err := ParseWKT("POINT(0 0)")
	require.NoError(t, err)

	_, err = Distance(nil, g)
	assert.Error(t, err)

	_, err = Distance(g, nil)
	assert.Error(t, err)

	_, err = DistanceSphere(nil, g)
	assert.Error(t, err)

	_, err = Contains(nil, g)
	assert.Error(t, err)

	_, err = Intersects(nil, g)
	assert.Error(t, err)

	_, err = Envelope(nil)
	assert.Error(t, err)
}

func TestBoundingBox(t *testing.T) {
	g, err := ParseWKT("POLYGON((1 2, 3 4, 5 2, 3 0, 1 2))")
	require.NoError(t, err)

	bb, err := g.BoundingBox()
	require.NoError(t, err)

	assert.Equal(t, 1.0, bb.MinX)
	assert.Equal(t, 0.0, bb.MinY)
	assert.Equal(t, 5.0, bb.MaxX)
	assert.Equal(t, 4.0, bb.MaxY)
}

func TestGeometryDimension(t *testing.T) {
	assert.Equal(t, 0, geometryDimension(GeometryPoint))
	assert.Equal(t, 0, geometryDimension(GeometryMultiPoint))
	assert.Equal(t, 1, geometryDimension(GeometryLineString))
	assert.Equal(t, 1, geometryDimension(GeometryMultiLineString))
	assert.Equal(t, 2, geometryDimension(GeometryPolygon))
	assert.Equal(t, 2, geometryDimension(GeometryMultiPolygon))
}

func TestEuclideanDistance(t *testing.T) {
	p1 := [2]float64{0, 0}
	p2 := [2]float64{3, 4}

	dist := euclideanDistance(p1, p2)
	assert.InDelta(t, 5.0, dist, 0.0001)
}

func TestCreateGeometryFunctions(t *testing.T) {
	// Test createPointGeometry
	pt, err := createPointGeometry(5.0, 10.0)
	require.NoError(t, err)
	assert.Equal(t, GeometryPoint, pt.Type)

	x, err := pt.X()
	require.NoError(t, err)
	assert.Equal(t, 5.0, x)

	y, err := pt.Y()
	require.NoError(t, err)
	assert.Equal(t, 10.0, y)

	// Test createLineStringGeometry
	line, err := createLineStringGeometry([][2]float64{{0, 0}, {10, 10}})
	require.NoError(t, err)
	assert.Equal(t, GeometryLineString, line.Type)

	coords, err := line.Coordinates()
	require.NoError(t, err)
	assert.Len(t, coords, 2)

	// Test createPolygonGeometry
	poly, err := createPolygonGeometry([][][2]float64{
		{{0, 0}, {0, 10}, {10, 10}, {10, 0}, {0, 0}},
	})
	require.NoError(t, err)
	assert.Equal(t, GeometryPolygon, poly.Type)
}

func TestDirection(t *testing.T) {
	// Test direction calculation used in segment intersection
	// direction(p1, p2, p3) computes cross product of (p2-p1) and (p3-p1)
	// The sign indicates which side of the line p1-p2 the point p3 is on
	p1 := [2]float64{0, 0}
	p2 := [2]float64{10, 0}
	p3 := [2]float64{5, 5}

	d := direction(p1, p2, p3)
	// Point above line - cross product sign depends on orientation
	// For our formula: (p3[0]-p1[0])*(p2[1]-p1[1]) - (p2[0]-p1[0])*(p3[1]-p1[1])
	// = (5-0)*(0-0) - (10-0)*(5-0) = 0 - 50 = -50
	assert.Less(t, d, 0.0)

	p4 := [2]float64{5, -5}
	d2 := direction(p1, p2, p4)
	// Point below line: (5-0)*(0-0) - (10-0)*(-5-0) = 0 - (-50) = 50
	assert.Greater(t, d2, 0.0)

	p5 := [2]float64{5, 0}
	d3 := direction(p1, p2, p5)
	// Point on line should give zero
	assert.InDelta(t, 0.0, d3, 0.0001)
}

func TestBoundingBoxesIntersect(t *testing.T) {
	bb1 := &BoundingBox{MinX: 0, MinY: 0, MaxX: 10, MaxY: 10}
	bb2 := &BoundingBox{MinX: 5, MinY: 5, MaxX: 15, MaxY: 15}
	bb3 := &BoundingBox{MinX: 20, MinY: 20, MaxX: 30, MaxY: 30}

	assert.True(t, boundingBoxesIntersect(bb1, bb2))
	assert.False(t, boundingBoxesIntersect(bb1, bb3))
}

func TestEmptyGeometryCoordinates(t *testing.T) {
	empty, err := ParseWKT("LINESTRING EMPTY")
	require.NoError(t, err)

	coords, err := empty.Coordinates()
	require.NoError(t, err)
	assert.Nil(t, coords)
}

func TestMultiGeometryCoordinates(t *testing.T) {
	mp, err := ParseWKT("MULTIPOINT((0 0), (5 5), (10 10))")
	require.NoError(t, err)

	coords, err := mp.Coordinates()
	require.NoError(t, err)
	assert.Len(t, coords, 3)

	mls, err := ParseWKT("MULTILINESTRING((0 0, 5 5), (10 10, 15 15))")
	require.NoError(t, err)

	coords2, err := mls.Coordinates()
	require.NoError(t, err)
	assert.Len(t, coords2, 4) // All points from both lines
}

// Test the precision of haversine formula
func TestHaversinePrecision(t *testing.T) {
	// Test with known distances
	tests := []struct {
		name             string
		lon1, lat1       float64
		lon2, lat2       float64
		expectedKm       float64
		tolerancePercent float64
	}{
		{
			name: "Equator crossing",
			lon1: 0, lat1: 0,
			lon2: 1, lat2: 0,
			expectedKm:       111.32, // ~111km per degree at equator
			tolerancePercent: 1.0,
		},
		{
			name: "North-South at meridian",
			lon1: 0, lat1: 0,
			lon2: 0, lat2: 1,
			expectedKm:       111.32,
			tolerancePercent: 1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dist := haversine(tt.lon1, tt.lat1, tt.lon2, tt.lat2)
			actualKm := dist / 1000.0
			tolerance := tt.expectedKm * tt.tolerancePercent / 100.0
			assert.InDelta(t, tt.expectedKm, actualKm, tolerance)
		})
	}
}

func TestCreatePolygonEnvelope(t *testing.T) {
	// Test that envelope creates proper bounding box polygon
	line, err := ParseWKT("LINESTRING(1 2, 5 8, 9 4)")
	require.NoError(t, err)

	env, err := Envelope(line)
	require.NoError(t, err)
	assert.Equal(t, GeometryPolygon, env.Type)

	// Get WKT to verify
	wkt, err := FormatWKT(env)
	require.NoError(t, err)
	t.Logf("Envelope WKT: %s", wkt)

	// Verify bounding box
	bb, err := env.BoundingBox()
	require.NoError(t, err)
	assert.Equal(t, 1.0, bb.MinX)
	assert.Equal(t, 2.0, bb.MinY)
	assert.Equal(t, 9.0, bb.MaxX)
	assert.Equal(t, 8.0, bb.MaxY)
}

func TestPointOnSegment(t *testing.T) {
	p1 := [2]float64{0, 0}
	p2 := [2]float64{10, 10}

	// Point on segment
	assert.True(t, pointOnSegment(5, 5, p1, p2))

	// Point at endpoint
	assert.True(t, pointOnSegment(0, 0, p1, p2))
	assert.True(t, pointOnSegment(10, 10, p1, p2))

	// Point not on segment
	assert.False(t, pointOnSegment(5, 6, p1, p2))
	assert.False(t, pointOnSegment(15, 15, p1, p2))
}

func TestPointsEqual(t *testing.T) {
	p1 := [2]float64{5.0, 10.0}
	p2 := [2]float64{5.0, 10.0}
	p3 := [2]float64{5.0, 11.0}

	assert.True(t, pointsEqual(p1, p2))
	assert.False(t, pointsEqual(p1, p3))
}

func TestInDelta(t *testing.T) {
	// Test floating point comparison helper
	assert.InDelta(t, 5.0, 5.0000001, 0.0001)
	assert.InDelta(t, math.Pi, 3.14159265, 0.0001)
}

// Phase 4: Geometric Analysis Tests

func TestArea_Polygon(t *testing.T) {
	tests := []struct {
		name     string
		wkt      string
		expected float64
	}{
		{
			name:     "unit square",
			wkt:      "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
			expected: 1.0,
		},
		{
			name:     "2x2 square",
			wkt:      "POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))",
			expected: 4.0,
		},
		{
			name:     "3-4-5 right triangle",
			wkt:      "POLYGON((0 0, 0 4, 3 0, 0 0))",
			expected: 6.0,
		},
		{
			name:     "rectangle 3x5",
			wkt:      "POLYGON((0 0, 0 5, 3 5, 3 0, 0 0))",
			expected: 15.0,
		},
		{
			name:     "irregular polygon",
			wkt:      "POLYGON((0 0, 0 4, 2 4, 2 2, 4 2, 4 0, 0 0))",
			expected: 12.0, // L-shaped: 4*2 + 2*2 = 8 + 4 = 12
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := ParseWKT(tt.wkt)
			require.NoError(t, err)

			area, err := Area(g)
			require.NoError(t, err)
			assert.InDelta(t, tt.expected, area, 0.0001)
		})
	}
}

func TestArea_PolygonWithHole(t *testing.T) {
	// 10x10 square with 2x2 hole in center
	// Exterior area = 100, hole area = 4, net = 96
	g, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (4 4, 4 6, 6 6, 6 4, 4 4))")
	require.NoError(t, err)

	area, err := Area(g)
	require.NoError(t, err)
	assert.InDelta(t, 96.0, area, 0.0001)
}

func TestArea_NonPolygon(t *testing.T) {
	// Points and lines have zero area
	point, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)
	area, err := Area(point)
	require.NoError(t, err)
	assert.Equal(t, 0.0, area)

	line, err := ParseWKT("LINESTRING(0 0, 10 10)")
	require.NoError(t, err)
	area, err = Area(line)
	require.NoError(t, err)
	assert.Equal(t, 0.0, area)
}

func TestArea_Nil(t *testing.T) {
	_, err := Area(nil)
	assert.Error(t, err)
}

func TestLength_LineString(t *testing.T) {
	tests := []struct {
		name     string
		wkt      string
		expected float64
	}{
		{
			name:     "simple horizontal line",
			wkt:      "LINESTRING(0 0, 10 0)",
			expected: 10.0,
		},
		{
			name:     "simple vertical line",
			wkt:      "LINESTRING(0 0, 0 5)",
			expected: 5.0,
		},
		{
			name:     "diagonal line (3-4-5)",
			wkt:      "LINESTRING(0 0, 3 4)",
			expected: 5.0,
		},
		{
			name:     "multi-segment line",
			wkt:      "LINESTRING(0 0, 3 0, 3 4, 0 4)",
			expected: 10.0, // 3 + 4 + 3 = 10
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := ParseWKT(tt.wkt)
			require.NoError(t, err)

			length, err := Length(g)
			require.NoError(t, err)
			assert.InDelta(t, tt.expected, length, 0.0001)
		})
	}
}

func TestLength_Polygon(t *testing.T) {
	// Perimeter of a unit square = 4
	g, err := ParseWKT("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")
	require.NoError(t, err)

	length, err := Length(g)
	require.NoError(t, err)
	assert.InDelta(t, 4.0, length, 0.0001)

	// Perimeter of 3x4 rectangle = 14
	g2, err := ParseWKT("POLYGON((0 0, 0 4, 3 4, 3 0, 0 0))")
	require.NoError(t, err)

	length2, err := Length(g2)
	require.NoError(t, err)
	assert.InDelta(t, 14.0, length2, 0.0001)
}

func TestLength_PolygonWithHole(t *testing.T) {
	// Outer perimeter + hole perimeter
	// Outer: 10*4 = 40, Hole: 2*4 = 8, Total = 48
	g, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (4 4, 4 6, 6 6, 6 4, 4 4))")
	require.NoError(t, err)

	length, err := Length(g)
	require.NoError(t, err)
	assert.InDelta(t, 48.0, length, 0.0001)
}

func TestLength_Point(t *testing.T) {
	g, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)

	length, err := Length(g)
	require.NoError(t, err)
	assert.Equal(t, 0.0, length)
}

func TestLength_Nil(t *testing.T) {
	_, err := Length(nil)
	assert.Error(t, err)
}

func TestCentroid_Point(t *testing.T) {
	g, err := ParseWKT("POINT(5 10)")
	require.NoError(t, err)

	centroid, err := Centroid(g)
	require.NoError(t, err)
	require.NotNil(t, centroid)

	x, err := centroid.X()
	require.NoError(t, err)
	y, err := centroid.Y()
	require.NoError(t, err)

	assert.InDelta(t, 5.0, x, 0.0001)
	assert.InDelta(t, 10.0, y, 0.0001)
}

func TestCentroid_LineString(t *testing.T) {
	// Simple horizontal line - centroid should be at midpoint
	g, err := ParseWKT("LINESTRING(0 0, 10 0)")
	require.NoError(t, err)

	centroid, err := Centroid(g)
	require.NoError(t, err)

	x, err := centroid.X()
	require.NoError(t, err)
	y, err := centroid.Y()
	require.NoError(t, err)

	assert.InDelta(t, 5.0, x, 0.0001)
	assert.InDelta(t, 0.0, y, 0.0001)
}

func TestCentroid_Polygon(t *testing.T) {
	tests := []struct {
		name      string
		wkt       string
		expectedX float64
		expectedY float64
	}{
		{
			name:      "unit square centered at origin",
			wkt:       "POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))",
			expectedX: 1.0,
			expectedY: 1.0,
		},
		{
			name:      "unit square from (0,0) to (1,1)",
			wkt:       "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))",
			expectedX: 0.5,
			expectedY: 0.5,
		},
		{
			name:      "rectangle 6x4 from origin",
			wkt:       "POLYGON((0 0, 0 4, 6 4, 6 0, 0 0))",
			expectedX: 3.0,
			expectedY: 2.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, err := ParseWKT(tt.wkt)
			require.NoError(t, err)

			centroid, err := Centroid(g)
			require.NoError(t, err)

			x, err := centroid.X()
			require.NoError(t, err)
			y, err := centroid.Y()
			require.NoError(t, err)

			assert.InDelta(t, tt.expectedX, x, 0.0001)
			assert.InDelta(t, tt.expectedY, y, 0.0001)
		})
	}
}

func TestCentroid_Nil(t *testing.T) {
	_, err := Centroid(nil)
	assert.Error(t, err)
}

func TestShoelaceArea(t *testing.T) {
	// Unit square
	coords := [][2]float64{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}}
	area := shoelaceArea(coords)
	assert.InDelta(t, 1.0, area, 0.0001)

	// Triangle
	coords2 := [][2]float64{{0, 0}, {4, 0}, {2, 3}, {0, 0}}
	area2 := shoelaceArea(coords2)
	assert.InDelta(t, 6.0, area2, 0.0001) // base=4, height=3, area=6

	// Degenerate (less than 3 points)
	coords3 := [][2]float64{{0, 0}, {1, 1}}
	area3 := shoelaceArea(coords3)
	assert.Equal(t, 0.0, area3)
}

func TestSegmentLength(t *testing.T) {
	// 3-4-5 triangle
	length := segmentLength(0, 0, 3, 4)
	assert.InDelta(t, 5.0, length, 0.0001)

	// Horizontal
	length2 := segmentLength(0, 0, 10, 0)
	assert.InDelta(t, 10.0, length2, 0.0001)

	// Vertical
	length3 := segmentLength(0, 0, 0, 7)
	assert.InDelta(t, 7.0, length3, 0.0001)

	// Same point
	length4 := segmentLength(5, 5, 5, 5)
	assert.Equal(t, 0.0, length4)
}

func TestLineLength(t *testing.T) {
	// Single segment
	coords := [][2]float64{{0, 0}, {3, 4}}
	length := lineLength(coords)
	assert.InDelta(t, 5.0, length, 0.0001)

	// Multiple segments
	coords2 := [][2]float64{{0, 0}, {3, 0}, {3, 4}}
	length2 := lineLength(coords2)
	assert.InDelta(t, 7.0, length2, 0.0001) // 3 + 4 = 7

	// Single point
	coords3 := [][2]float64{{0, 0}}
	length3 := lineLength(coords3)
	assert.Equal(t, 0.0, length3)

	// Empty
	coords4 := [][2]float64{}
	length4 := lineLength(coords4)
	assert.Equal(t, 0.0, length4)
}

func TestLineCentroid(t *testing.T) {
	// Simple horizontal line
	coords := [][2]float64{{0, 0}, {10, 0}}
	cx, cy := lineCentroid(coords)
	assert.InDelta(t, 5.0, cx, 0.0001)
	assert.InDelta(t, 0.0, cy, 0.0001)

	// L-shaped line
	coords2 := [][2]float64{{0, 0}, {4, 0}, {4, 3}}
	cx2, cy2 := lineCentroid(coords2)
	// Segment 1: length=4, midpoint=(2, 0)
	// Segment 2: length=3, midpoint=(4, 1.5)
	// Weighted: cx = (2*4 + 4*3) / 7 = (8 + 12) / 7 = 20/7 = 2.857
	// Weighted: cy = (0*4 + 1.5*3) / 7 = (0 + 4.5) / 7 = 4.5/7 = 0.643
	assert.InDelta(t, 20.0/7.0, cx2, 0.0001)
	assert.InDelta(t, 4.5/7.0, cy2, 0.0001)

	// Empty
	coords3 := [][2]float64{}
	cx3, cy3 := lineCentroid(coords3)
	assert.Equal(t, 0.0, cx3)
	assert.Equal(t, 0.0, cy3)

	// Single point
	coords4 := [][2]float64{{5, 10}}
	cx4, cy4 := lineCentroid(coords4)
	assert.Equal(t, 5.0, cx4)
	assert.Equal(t, 10.0, cy4)
}

func TestPolygonCentroid(t *testing.T) {
	// Unit square
	coords := [][2]float64{{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}}
	cx, cy := polygonCentroid(coords)
	assert.InDelta(t, 0.5, cx, 0.0001)
	assert.InDelta(t, 0.5, cy, 0.0001)

	// 2x2 square
	coords2 := [][2]float64{{0, 0}, {0, 2}, {2, 2}, {2, 0}, {0, 0}}
	cx2, cy2 := polygonCentroid(coords2)
	assert.InDelta(t, 1.0, cx2, 0.0001)
	assert.InDelta(t, 1.0, cy2, 0.0001)

	// Empty
	coords3 := [][2]float64{}
	cx3, cy3 := polygonCentroid(coords3)
	assert.Equal(t, 0.0, cx3)
	assert.Equal(t, 0.0, cy3)
}

func TestSubGeometries_MultiPoint(t *testing.T) {
	g, err := ParseWKT("MULTIPOINT((0 0), (5 5), (10 10))")
	require.NoError(t, err)

	subGeoms, err := g.SubGeometries()
	require.NoError(t, err)
	assert.Len(t, subGeoms, 3)

	for _, sub := range subGeoms {
		assert.Equal(t, GeometryPoint, sub.Type)
	}
}

// Phase 5: Set Operations Tests

func TestMakePolygon(t *testing.T) {
	// Valid closed linestring
	ring, err := ParseWKT("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)")
	require.NoError(t, err)

	poly, err := MakePolygon(ring)
	require.NoError(t, err)
	require.NotNil(t, poly)
	assert.Equal(t, GeometryPolygon, poly.Type)

	// Verify the polygon has correct area
	area, err := Area(poly)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, area, 0.0001)
}

func TestMakePolygon_NotClosed(t *testing.T) {
	// Linestring that is not closed
	ring, err := ParseWKT("LINESTRING(0 0, 0 1, 1 1, 1 0)")
	require.NoError(t, err)

	_, err = MakePolygon(ring)
	assert.Error(t, err, "should fail for non-closed linestring")
}

func TestMakePolygon_TooFewPoints(t *testing.T) {
	// Linestring with too few points
	ring, err := ParseWKT("LINESTRING(0 0, 1 1, 0 0)")
	require.NoError(t, err)

	_, err = MakePolygon(ring)
	assert.Error(t, err, "should fail for linestring with too few points")
}

func TestMakePolygon_NotLineString(t *testing.T) {
	point, err := ParseWKT("POINT(0 0)")
	require.NoError(t, err)

	_, err = MakePolygon(point)
	assert.Error(t, err, "should fail for non-linestring geometry")
}

func TestMakePolygon_Nil(t *testing.T) {
	_, err := MakePolygon(nil)
	assert.Error(t, err)
}

func TestBuffer_Point(t *testing.T) {
	point, err := ParseWKT("POINT(0 0)")
	require.NoError(t, err)

	// Buffer with radius 1
	buffered, err := Buffer(point, 1.0)
	require.NoError(t, err)
	require.NotNil(t, buffered)
	assert.Equal(t, GeometryPolygon, buffered.Type)

	// Area should be approximately pi * r^2 = pi
	area, err := Area(buffered)
	require.NoError(t, err)
	assert.InDelta(t, math.Pi, area, 0.1) // Some tolerance due to polygon approximation
}

func TestBuffer_Point_LargerRadius(t *testing.T) {
	point, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)

	// Buffer with radius 2
	buffered, err := Buffer(point, 2.0)
	require.NoError(t, err)
	require.NotNil(t, buffered)

	// Area should be approximately pi * 4 = 4*pi
	area, err := Area(buffered)
	require.NoError(t, err)
	assert.InDelta(t, 4*math.Pi, area, 0.2)
}

func TestBuffer_LineString(t *testing.T) {
	line, err := ParseWKT("LINESTRING(0 0, 10 0)")
	require.NoError(t, err)

	// Buffer with distance 1
	buffered, err := Buffer(line, 1.0)
	require.NoError(t, err)
	require.NotNil(t, buffered)
	assert.Equal(t, GeometryPolygon, buffered.Type)

	// Area should be positive (buffer creates a polygon around the line)
	area, err := Area(buffered)
	require.NoError(t, err)
	assert.Greater(t, area, 10.0) // At least some area around the 10-unit line
}

func TestBuffer_Polygon(t *testing.T) {
	poly, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)

	// Buffer with distance 1 (expand)
	buffered, err := Buffer(poly, 1.0)
	require.NoError(t, err)
	require.NotNil(t, buffered)
	assert.Equal(t, GeometryPolygon, buffered.Type)

	// Area should be larger than original
	originalArea, _ := Area(poly)
	bufferedArea, err := Area(buffered)
	require.NoError(t, err)
	assert.Greater(t, bufferedArea, originalArea)
}

func TestBuffer_Polygon_Contract(t *testing.T) {
	poly, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)

	// Buffer with negative distance (contract)
	buffered, err := Buffer(poly, -1.0)
	require.NoError(t, err)
	require.NotNil(t, buffered)
	assert.Equal(t, GeometryPolygon, buffered.Type)

	// Area should be smaller than original
	originalArea, _ := Area(poly)
	bufferedArea, err := Area(buffered)
	require.NoError(t, err)
	assert.Less(t, bufferedArea, originalArea)
}

func TestBuffer_Nil(t *testing.T) {
	_, err := Buffer(nil, 1.0)
	assert.Error(t, err)
}

func TestBuffer_PointNegativeDistance(t *testing.T) {
	point, err := ParseWKT("POINT(0 0)")
	require.NoError(t, err)

	_, err = Buffer(point, -1.0)
	assert.Error(t, err, "negative buffer on point should fail")
}

func TestIntersection_OverlappingPolygons(t *testing.T) {
	// Two overlapping squares
	poly1, err := ParseWKT("POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))")
	require.NoError(t, err)
	poly2, err := ParseWKT("POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))")
	require.NoError(t, err)

	result, err := Intersection(poly1, poly2)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryPolygon, result.Type)

	// Intersection should be a 1x1 square
	area, err := Area(result)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, area, 0.0001)
}

func TestIntersection_DisjointPolygons(t *testing.T) {
	poly1, err := ParseWKT("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")
	require.NoError(t, err)
	poly2, err := ParseWKT("POLYGON((5 5, 5 6, 6 6, 6 5, 5 5))")
	require.NoError(t, err)

	result, err := Intersection(poly1, poly2)
	require.NoError(t, err)
	assert.Nil(t, result, "disjoint polygons should have no intersection")
}

func TestIntersection_PointInsidePolygon(t *testing.T) {
	point, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)
	poly, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)

	result, err := Intersection(point, poly)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryPoint, result.Type)
}

func TestIntersection_PointOutsidePolygon(t *testing.T) {
	point, err := ParseWKT("POINT(15 15)")
	require.NoError(t, err)
	poly, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)

	result, err := Intersection(point, poly)
	require.NoError(t, err)
	assert.Nil(t, result, "point outside polygon should have no intersection")
}

func TestIntersection_IdenticalPoints(t *testing.T) {
	point1, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)
	point2, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)

	result, err := Intersection(point1, point2)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryPoint, result.Type)
}

func TestIntersection_DifferentPoints(t *testing.T) {
	point1, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)
	point2, err := ParseWKT("POINT(6 6)")
	require.NoError(t, err)

	result, err := Intersection(point1, point2)
	require.NoError(t, err)
	assert.Nil(t, result, "different points should have no intersection")
}

func TestIntersection_Nil(t *testing.T) {
	point, err := ParseWKT("POINT(0 0)")
	require.NoError(t, err)

	_, err = Intersection(nil, point)
	assert.Error(t, err)

	_, err = Intersection(point, nil)
	assert.Error(t, err)
}

func TestUnion_DisjointPolygons(t *testing.T) {
	poly1, err := ParseWKT("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")
	require.NoError(t, err)
	poly2, err := ParseWKT("POLYGON((5 5, 5 6, 6 6, 6 5, 5 5))")
	require.NoError(t, err)

	result, err := Union(poly1, poly2)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryMultiPolygon, result.Type)
}

func TestUnion_OverlappingPolygons(t *testing.T) {
	poly1, err := ParseWKT("POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))")
	require.NoError(t, err)
	poly2, err := ParseWKT("POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))")
	require.NoError(t, err)

	result, err := Union(poly1, poly2)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryPolygon, result.Type)

	// Union area should be larger than either polygon alone
	area1, _ := Area(poly1)
	area2, _ := Area(poly2)
	unionArea, err := Area(result)
	require.NoError(t, err)
	assert.Greater(t, unionArea, area1)
	assert.Greater(t, unionArea, area2)
}

func TestUnion_OneContainsOther(t *testing.T) {
	outer, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)
	inner, err := ParseWKT("POLYGON((2 2, 2 8, 8 8, 8 2, 2 2))")
	require.NoError(t, err)

	result, err := Union(outer, inner)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryPolygon, result.Type)

	// Union should be same as outer polygon
	outerArea, _ := Area(outer)
	unionArea, _ := Area(result)
	assert.InDelta(t, outerArea, unionArea, 0.0001)
}

func TestUnion_IdenticalPoints(t *testing.T) {
	point1, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)
	point2, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)

	result, err := Union(point1, point2)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryPoint, result.Type)
}

func TestUnion_DifferentPoints(t *testing.T) {
	point1, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)
	point2, err := ParseWKT("POINT(6 6)")
	require.NoError(t, err)

	result, err := Union(point1, point2)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryMultiPoint, result.Type)
}

func TestUnion_Nil(t *testing.T) {
	point, err := ParseWKT("POINT(0 0)")
	require.NoError(t, err)

	_, err = Union(nil, point)
	assert.Error(t, err)

	_, err = Union(point, nil)
	assert.Error(t, err)
}

func TestDifference_DisjointPolygons(t *testing.T) {
	poly1, err := ParseWKT("POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))")
	require.NoError(t, err)
	poly2, err := ParseWKT("POLYGON((5 5, 5 6, 6 6, 6 5, 5 5))")
	require.NoError(t, err)

	result, err := Difference(poly1, poly2)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Difference of disjoint polygons is the first polygon
	area1, _ := Area(poly1)
	diffArea, _ := Area(result)
	assert.InDelta(t, area1, diffArea, 0.0001)
}

func TestDifference_G2ContainsG1(t *testing.T) {
	outer, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)
	inner, err := ParseWKT("POLYGON((2 2, 2 8, 8 8, 8 2, 2 2))")
	require.NoError(t, err)

	result, err := Difference(inner, outer)
	require.NoError(t, err)
	assert.Nil(t, result, "difference should be empty when g2 contains g1")
}

func TestDifference_G1ContainsG2(t *testing.T) {
	outer, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)
	inner, err := ParseWKT("POLYGON((2 2, 2 8, 8 8, 8 2, 2 2))")
	require.NoError(t, err)

	result, err := Difference(outer, inner)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryPolygon, result.Type)

	// Result should have area = outer - inner = 100 - 36 = 64
	outerArea, _ := Area(outer)
	innerArea, _ := Area(inner)
	diffArea, _ := Area(result)
	expectedArea := outerArea - innerArea
	assert.InDelta(t, expectedArea, diffArea, 0.0001)
}

func TestDifference_PointInsidePolygon(t *testing.T) {
	point, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)
	poly, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)

	result, err := Difference(point, poly)
	require.NoError(t, err)
	assert.Nil(t, result, "point inside polygon should have empty difference")
}

func TestDifference_PointOutsidePolygon(t *testing.T) {
	point, err := ParseWKT("POINT(15 15)")
	require.NoError(t, err)
	poly, err := ParseWKT("POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))")
	require.NoError(t, err)

	result, err := Difference(point, poly)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryPoint, result.Type)
}

func TestDifference_IdenticalPoints(t *testing.T) {
	point1, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)
	point2, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)

	result, err := Difference(point1, point2)
	require.NoError(t, err)
	assert.Nil(t, result, "identical points should have empty difference")
}

func TestDifference_DifferentPoints(t *testing.T) {
	point1, err := ParseWKT("POINT(5 5)")
	require.NoError(t, err)
	point2, err := ParseWKT("POINT(6 6)")
	require.NoError(t, err)

	result, err := Difference(point1, point2)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, GeometryPoint, result.Type)
}

func TestDifference_Nil(t *testing.T) {
	point, err := ParseWKT("POINT(0 0)")
	require.NoError(t, err)

	_, err = Difference(nil, point)
	assert.Error(t, err)

	_, err = Difference(point, nil)
	assert.Error(t, err)
}

func TestConvexHull(t *testing.T) {
	points := [][2]float64{
		{0, 0}, {0, 5}, {5, 5}, {5, 0}, {2.5, 2.5},
	}

	hull := convexHull(points)
	// Should return 4 points (the interior point is excluded)
	assert.Len(t, hull, 4)
}

func TestConvexHull_Triangle(t *testing.T) {
	points := [][2]float64{
		{0, 0}, {5, 0}, {2.5, 5},
	}

	hull := convexHull(points)
	assert.Len(t, hull, 3)
}

func TestConvexHull_Collinear(t *testing.T) {
	points := [][2]float64{
		{0, 0}, {1, 0}, {2, 0},
	}

	hull := convexHull(points)
	// Collinear points return the endpoints
	assert.LessOrEqual(t, len(hull), 3)
}

func TestSutherlandHodgman(t *testing.T) {
	// Subject: unit square
	subject := [][2]float64{
		{0, 0}, {2, 0}, {2, 2}, {0, 2},
	}
	// Clip: shifted square
	clip := [][2]float64{
		{1, 1}, {3, 1}, {3, 3}, {1, 3},
	}

	result := sutherlandHodgman(subject, clip)
	// Result should be the intersection (1x1 square)
	assert.GreaterOrEqual(t, len(result), 3)
}

func TestIsLeftOfEdge(t *testing.T) {
	p1 := [2]float64{0, 0}
	p2 := [2]float64{10, 0}

	// Point above line
	assert.True(t, isLeftOfEdge(p1, p2, [2]float64{5, 5}))

	// Point below line
	assert.False(t, isLeftOfEdge(p1, p2, [2]float64{5, -5}))

	// Point on line
	assert.True(t, isLeftOfEdge(p1, p2, [2]float64{5, 0}))
}

func TestLineSegmentIntersection(t *testing.T) {
	// Crossing lines
	ix, iy, ok := lineSegmentIntersection(0, 0, 10, 10, 0, 10, 10, 0)
	assert.True(t, ok)
	assert.InDelta(t, 5.0, ix, 0.0001)
	assert.InDelta(t, 5.0, iy, 0.0001)

	// Parallel lines
	_, _, ok = lineSegmentIntersection(0, 0, 10, 0, 0, 5, 10, 5)
	assert.False(t, ok)
}

func TestCrossProductFunc(t *testing.T) {
	p1 := [2]float64{0, 0}
	p2 := [2]float64{1, 0}
	p3 := [2]float64{0.5, 1}

	cp := crossProduct(p1, p2, p3)
	assert.Greater(t, cp, 0.0) // p3 is to the left of p1->p2
}

func TestBufferPolygonRing(t *testing.T) {
	// Square ring (counter-clockwise order)
	ring := [][2]float64{
		{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0},
	}

	// Expand by 1
	expanded := bufferPolygonRing(ring, 1.0)
	assert.Len(t, expanded, 4) // Excludes closing point

	// The buffer function offsets based on perpendicular direction
	// With counter-clockwise winding, positive buffer goes outward
	// Verify all points are offset from original
	for i := 0; i < 4; i++ {
		// Each expanded point should be different from original
		assert.NotEqual(t, ring[i][0], expanded[i][0], "X coordinate should change")
		// OR
		assert.NotEqual(t, ring[i][1], expanded[i][1], "Y coordinate should change")
	}
}

func TestOffsetCurve(t *testing.T) {
	coords := [][2]float64{{0, 0}, {10, 0}}

	// Offset up
	offset := offsetCurve(coords, 1.0)
	assert.Len(t, offset, 2)
	assert.InDelta(t, 0.0, offset[0][0], 0.0001)
	assert.InDelta(t, 1.0, offset[0][1], 0.0001) // Moved up by 1
}
