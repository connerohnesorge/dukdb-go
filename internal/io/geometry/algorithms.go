// Package geometry provides spatial algorithms for geometry types.
package geometry

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// earthRadiusMeters is the average Earth radius in meters.
const earthRadiusMeters = 6371008.8

// BoundingBox represents the minimum bounding rectangle of a geometry.
type BoundingBox struct {
	MinX, MinY, MaxX, MaxY float64
}

// Distance calculates the Euclidean distance between two geometries.
// For POINT-POINT: simple distance formula.
// For other types: minimum distance between closest points.
func Distance(g1, g2 *Geometry) (float64, error) {
	if g1 == nil || g2 == nil {
		return 0, fmt.Errorf("cannot calculate distance with nil geometry")
	}

	coords1, err := g1.Coordinates()
	if err != nil {
		return 0, fmt.Errorf("failed to get coordinates from first geometry: %w", err)
	}

	coords2, err := g2.Coordinates()
	if err != nil {
		return 0, fmt.Errorf("failed to get coordinates from second geometry: %w", err)
	}

	if len(coords1) == 0 || len(coords2) == 0 {
		return 0, fmt.Errorf("empty geometry")
	}

	// For POINT-POINT, calculate direct distance
	if g1.Type == GeometryPoint && g2.Type == GeometryPoint {
		return euclideanDistance(coords1[0], coords2[0]), nil
	}

	// For other geometry types, find minimum distance
	minDist := math.MaxFloat64
	for _, c1 := range coords1 {
		for _, c2 := range coords2 {
			d := euclideanDistance(c1, c2)
			if d < minDist {
				minDist = d
			}
		}
	}

	return minDist, nil
}

// euclideanDistance calculates Euclidean distance between two points.
func euclideanDistance(p1, p2 [2]float64) float64 {
	dx := p2[0] - p1[0]
	dy := p2[1] - p1[1]
	return math.Sqrt(dx*dx + dy*dy)
}

// DistanceSphere calculates the great-circle distance using the Haversine formula.
// Assumes coordinates are lon/lat in degrees.
// Returns distance in meters.
func DistanceSphere(g1, g2 *Geometry) (float64, error) {
	if g1 == nil || g2 == nil {
		return 0, fmt.Errorf("cannot calculate distance with nil geometry")
	}

	if g1.Type != GeometryPoint || g2.Type != GeometryPoint {
		return 0, fmt.Errorf("ST_Distance_Sphere only works with POINT geometries")
	}

	coords1, err := g1.Coordinates()
	if err != nil {
		return 0, fmt.Errorf("failed to get coordinates from first geometry: %w", err)
	}

	coords2, err := g2.Coordinates()
	if err != nil {
		return 0, fmt.Errorf("failed to get coordinates from second geometry: %w", err)
	}

	if len(coords1) == 0 || len(coords2) == 0 {
		return 0, fmt.Errorf("empty geometry")
	}

	// Coordinates are lon, lat
	lon1, lat1 := coords1[0][0], coords1[0][1]
	lon2, lat2 := coords2[0][0], coords2[0][1]

	return haversine(lon1, lat1, lon2, lat2), nil
}

// haversine calculates the great-circle distance between two points using the Haversine formula.
// Input coordinates are in degrees (lon, lat).
// Returns distance in meters.
func haversine(lon1, lat1, lon2, lat2 float64) float64 {
	// Convert to radians
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	lat1Rad := lat1 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusMeters * c
}

// Contains returns true if g1 completely contains g2.
// For POLYGON containing POINT: use point-in-polygon algorithm.
// For POLYGON containing POLYGON: all vertices of g2 inside g1.
func Contains(g1, g2 *Geometry) (bool, error) {
	if g1 == nil || g2 == nil {
		return false, fmt.Errorf("cannot check containment with nil geometry")
	}

	// POLYGON contains POINT
	if g1.Type == GeometryPolygon && g2.Type == GeometryPoint {
		rings, err := g1.AllCoordinates()
		if err != nil {
			return false, err
		}
		if len(rings) == 0 || len(rings[0]) == 0 {
			return false, nil
		}

		coords2, err := g2.Coordinates()
		if err != nil {
			return false, err
		}
		if len(coords2) == 0 {
			return false, nil
		}

		px, py := coords2[0][0], coords2[0][1]
		exteriorRing := make([][]float64, len(rings[0]))
		for i, pt := range rings[0] {
			exteriorRing[i] = []float64{pt[0], pt[1]}
		}

		// Check if point is inside exterior ring
		if !pointInPolygon(px, py, exteriorRing) {
			return false, nil
		}

		// Check if point is outside all holes (interior rings)
		for i := 1; i < len(rings); i++ {
			holeRing := make([][]float64, len(rings[i]))
			for j, pt := range rings[i] {
				holeRing[j] = []float64{pt[0], pt[1]}
			}
			if pointInPolygon(px, py, holeRing) {
				return false, nil // Point is in a hole
			}
		}

		return true, nil
	}

	// POLYGON contains POLYGON
	if g1.Type == GeometryPolygon && g2.Type == GeometryPolygon {
		rings1, err := g1.AllCoordinates()
		if err != nil {
			return false, err
		}
		if len(rings1) == 0 || len(rings1[0]) == 0 {
			return false, nil
		}

		coords2, err := g2.Coordinates()
		if err != nil {
			return false, err
		}

		exteriorRing := make([][]float64, len(rings1[0]))
		for i, pt := range rings1[0] {
			exteriorRing[i] = []float64{pt[0], pt[1]}
		}

		// Check that all points of g2 are inside g1
		for _, pt := range coords2 {
			if !pointInPolygon(pt[0], pt[1], exteriorRing) {
				return false, nil
			}
			// Also check holes
			for i := 1; i < len(rings1); i++ {
				holeRing := make([][]float64, len(rings1[i]))
				for j, p := range rings1[i] {
					holeRing[j] = []float64{p[0], p[1]}
				}
				if pointInPolygon(pt[0], pt[1], holeRing) {
					return false, nil
				}
			}
		}
		return true, nil
	}

	// POINT contains POINT - only if they are the same
	if g1.Type == GeometryPoint && g2.Type == GeometryPoint {
		coords1, err := g1.Coordinates()
		if err != nil {
			return false, err
		}
		coords2, err := g2.Coordinates()
		if err != nil {
			return false, err
		}
		if len(coords1) == 0 || len(coords2) == 0 {
			return false, nil
		}
		return coords1[0][0] == coords2[0][0] && coords1[0][1] == coords2[0][1], nil
	}

	// For other combinations, use bounding box containment as approximation
	bb1, err := g1.BoundingBox()
	if err != nil {
		return false, err
	}
	bb2, err := g2.BoundingBox()
	if err != nil {
		return false, err
	}

	return bb1.MinX <= bb2.MinX && bb1.MaxX >= bb2.MaxX &&
		bb1.MinY <= bb2.MinY && bb1.MaxY >= bb2.MaxY, nil
}

// Within is the inverse of Contains - g1 is within g2.
func Within(g1, g2 *Geometry) (bool, error) {
	return Contains(g2, g1)
}

// Intersects returns true if geometries share any space.
func Intersects(g1, g2 *Geometry) (bool, error) {
	if g1 == nil || g2 == nil {
		return false, fmt.Errorf("cannot check intersection with nil geometry")
	}

	// First check bounding boxes for quick rejection
	bb1, err := g1.BoundingBox()
	if err != nil {
		return false, err
	}
	bb2, err := g2.BoundingBox()
	if err != nil {
		return false, err
	}

	// If bounding boxes don't intersect, geometries don't intersect
	if !boundingBoxesIntersect(bb1, bb2) {
		return false, nil
	}

	// POINT-POINT intersection
	if g1.Type == GeometryPoint && g2.Type == GeometryPoint {
		coords1, _ := g1.Coordinates()
		coords2, _ := g2.Coordinates()
		if len(coords1) == 0 || len(coords2) == 0 {
			return false, nil
		}
		return coords1[0][0] == coords2[0][0] && coords1[0][1] == coords2[0][1], nil
	}

	// POLYGON-POINT intersection
	if g1.Type == GeometryPolygon && g2.Type == GeometryPoint {
		return Contains(g1, g2)
	}
	if g2.Type == GeometryPolygon && g1.Type == GeometryPoint {
		return Contains(g2, g1)
	}

	// LINESTRING-LINESTRING intersection
	if g1.Type == GeometryLineString && g2.Type == GeometryLineString {
		coords1, _ := g1.Coordinates()
		coords2, _ := g2.Coordinates()
		// Check all segment pairs
		for i := 0; i < len(coords1)-1; i++ {
			for j := 0; j < len(coords2)-1; j++ {
				if segmentsIntersect(coords1[i], coords1[i+1], coords2[j], coords2[j+1]) {
					return true, nil
				}
			}
		}
		return false, nil
	}

	// POLYGON-LINESTRING intersection
	if (g1.Type == GeometryPolygon && g2.Type == GeometryLineString) ||
		(g2.Type == GeometryPolygon && g1.Type == GeometryLineString) {
		var poly, line *Geometry
		if g1.Type == GeometryPolygon {
			poly, line = g1, g2
		} else {
			poly, line = g2, g1
		}

		lineCoords, _ := line.Coordinates()

		// Check if any line point is inside the polygon
		for _, pt := range lineCoords {
			ptGeom, _ := createPointGeometry(pt[0], pt[1])
			if contained, _ := Contains(poly, ptGeom); contained {
				return true, nil
			}
		}

		// Check if any line segment intersects polygon boundary
		polyRings, _ := poly.AllCoordinates()
		if len(polyRings) > 0 {
			ring := polyRings[0]
			for i := 0; i < len(lineCoords)-1; i++ {
				for j := 0; j < len(ring)-1; j++ {
					if segmentsIntersect(lineCoords[i], lineCoords[i+1], ring[j], ring[j+1]) {
						return true, nil
					}
				}
			}
		}
		return false, nil
	}

	// POLYGON-POLYGON intersection
	if g1.Type == GeometryPolygon && g2.Type == GeometryPolygon {
		// Check if any vertex of g2 is in g1
		coords2, _ := g2.Coordinates()
		for _, pt := range coords2 {
			ptGeom, _ := createPointGeometry(pt[0], pt[1])
			if contained, _ := Contains(g1, ptGeom); contained {
				return true, nil
			}
		}

		// Check if any vertex of g1 is in g2
		coords1, _ := g1.Coordinates()
		for _, pt := range coords1 {
			ptGeom, _ := createPointGeometry(pt[0], pt[1])
			if contained, _ := Contains(g2, ptGeom); contained {
				return true, nil
			}
		}

		// Check if any edges intersect
		rings1, _ := g1.AllCoordinates()
		rings2, _ := g2.AllCoordinates()
		if len(rings1) > 0 && len(rings2) > 0 {
			ring1, ring2 := rings1[0], rings2[0]
			for i := 0; i < len(ring1)-1; i++ {
				for j := 0; j < len(ring2)-1; j++ {
					if segmentsIntersect(ring1[i], ring1[i+1], ring2[j], ring2[j+1]) {
						return true, nil
					}
				}
			}
		}
		return false, nil
	}

	// Fallback: if bounding boxes intersect, assume geometries might intersect
	return true, nil
}

// Disjoint is the inverse of Intersects.
func Disjoint(g1, g2 *Geometry) (bool, error) {
	intersects, err := Intersects(g1, g2)
	if err != nil {
		return false, err
	}
	return !intersects, nil
}

// Touches returns true if geometries touch but interiors don't intersect.
func Touches(g1, g2 *Geometry) (bool, error) {
	if g1 == nil || g2 == nil {
		return false, fmt.Errorf("cannot check touch with nil geometry")
	}

	// Check if they intersect
	intersects, err := Intersects(g1, g2)
	if err != nil {
		return false, err
	}
	if !intersects {
		return false, nil
	}

	// For points, touching means they are the same point (handled by Intersects)
	if g1.Type == GeometryPoint && g2.Type == GeometryPoint {
		return false, nil // Two identical points don't "touch" in the spatial sense
	}

	// LINESTRING-LINESTRING touching: share endpoint but not interior
	if g1.Type == GeometryLineString && g2.Type == GeometryLineString {
		coords1, _ := g1.Coordinates()
		coords2, _ := g2.Coordinates()
		if len(coords1) < 2 || len(coords2) < 2 {
			return false, nil
		}

		// Get endpoints
		start1, end1 := coords1[0], coords1[len(coords1)-1]
		start2, end2 := coords2[0], coords2[len(coords2)-1]

		// Check if endpoints touch
		endpointsTouch := pointsEqual(start1, start2) || pointsEqual(start1, end2) ||
			pointsEqual(end1, start2) || pointsEqual(end1, end2)

		if endpointsTouch {
			// Verify interiors don't intersect
			for i := 0; i < len(coords1)-1; i++ {
				for j := 0; j < len(coords2)-1; j++ {
					if segmentsIntersectInterior(coords1[i], coords1[i+1], coords2[j], coords2[j+1]) {
						return false, nil
					}
				}
			}
			return true, nil
		}
		return false, nil
	}

	// For polygon-point, touching means point is on boundary
	if (g1.Type == GeometryPolygon && g2.Type == GeometryPoint) ||
		(g2.Type == GeometryPolygon && g1.Type == GeometryPoint) {
		var poly, point *Geometry
		if g1.Type == GeometryPolygon {
			poly, point = g1, g2
		} else {
			poly, point = g2, g1
		}

		coords, _ := point.Coordinates()
		if len(coords) == 0 {
			return false, nil
		}
		px, py := coords[0][0], coords[0][1]

		// Check if point is on polygon boundary
		rings, _ := poly.AllCoordinates()
		if len(rings) == 0 {
			return false, nil
		}
		return pointOnPolygonBoundary(px, py, rings[0]), nil
	}

	// Default case
	return false, nil
}

// Crosses returns true if geometries cross each other.
func Crosses(g1, g2 *Geometry) (bool, error) {
	if g1 == nil || g2 == nil {
		return false, fmt.Errorf("cannot check cross with nil geometry")
	}

	// Lines cross if they intersect at an interior point
	if g1.Type == GeometryLineString && g2.Type == GeometryLineString {
		coords1, _ := g1.Coordinates()
		coords2, _ := g2.Coordinates()

		for i := 0; i < len(coords1)-1; i++ {
			for j := 0; j < len(coords2)-1; j++ {
				if segmentsIntersectInterior(coords1[i], coords1[i+1], coords2[j], coords2[j+1]) {
					return true, nil
				}
			}
		}
		return false, nil
	}

	// Line crosses polygon if it passes through interior and exterior
	if (g1.Type == GeometryLineString && g2.Type == GeometryPolygon) ||
		(g2.Type == GeometryLineString && g1.Type == GeometryPolygon) {
		var line, poly *Geometry
		if g1.Type == GeometryLineString {
			line, poly = g1, g2
		} else {
			line, poly = g2, g1
		}

		lineCoords, _ := line.Coordinates()
		hasInside := false
		hasOutside := false

		for _, pt := range lineCoords {
			ptGeom, _ := createPointGeometry(pt[0], pt[1])
			if contained, _ := Contains(poly, ptGeom); contained {
				hasInside = true
			} else {
				hasOutside = true
			}
			if hasInside && hasOutside {
				return true, nil
			}
		}
		return false, nil
	}

	return false, nil
}

// Overlaps returns true if geometries overlap (same dimension, partial overlap).
func Overlaps(g1, g2 *Geometry) (bool, error) {
	if g1 == nil || g2 == nil {
		return false, fmt.Errorf("cannot check overlap with nil geometry")
	}

	// Must be same dimension for overlap
	dim1 := geometryDimension(g1.Type)
	dim2 := geometryDimension(g2.Type)
	if dim1 != dim2 {
		return false, nil
	}

	// Check they intersect
	intersects, err := Intersects(g1, g2)
	if err != nil {
		return false, err
	}
	if !intersects {
		return false, nil
	}

	// Check neither fully contains the other
	contains1, _ := Contains(g1, g2)
	contains2, _ := Contains(g2, g1)

	return !contains1 && !contains2, nil
}

// Equals returns true if geometries are spatially equal.
func Equals(g1, g2 *Geometry) (bool, error) {
	if g1 == nil || g2 == nil {
		return false, fmt.Errorf("cannot check equality with nil geometry")
	}

	if g1.Type != g2.Type {
		return false, nil
	}

	coords1, err := g1.Coordinates()
	if err != nil {
		return false, err
	}
	coords2, err := g2.Coordinates()
	if err != nil {
		return false, err
	}

	if len(coords1) != len(coords2) {
		return false, nil
	}

	for i := range coords1 {
		if !pointsEqual(coords1[i], coords2[i]) {
			return false, nil
		}
	}

	return true, nil
}

// Envelope returns the bounding box as a POLYGON geometry.
func Envelope(g *Geometry) (*Geometry, error) {
	if g == nil {
		return nil, fmt.Errorf("cannot compute envelope of nil geometry")
	}

	bb, err := g.BoundingBox()
	if err != nil {
		return nil, err
	}

	// If it's a point (minX == maxX && minY == maxY), return the point
	if bb.MinX == bb.MaxX && bb.MinY == bb.MaxY {
		return createPointGeometry(bb.MinX, bb.MinY)
	}

	// If it's a line (either minX == maxX or minY == maxY), return a line
	if bb.MinX == bb.MaxX || bb.MinY == bb.MaxY {
		return createLineStringGeometry([][2]float64{
			{bb.MinX, bb.MinY},
			{bb.MaxX, bb.MaxY},
		})
	}

	// Return a polygon (rectangle)
	return createPolygonGeometry([][][2]float64{{
		{bb.MinX, bb.MinY},
		{bb.MaxX, bb.MinY},
		{bb.MaxX, bb.MaxY},
		{bb.MinX, bb.MaxY},
		{bb.MinX, bb.MinY}, // Close the ring
	}})
}

// BoundingBox returns the minimum bounding rectangle of the geometry.
func (g *Geometry) BoundingBox() (*BoundingBox, error) {
	coords, err := g.Coordinates()
	if err != nil {
		return nil, err
	}
	if len(coords) == 0 {
		return nil, fmt.Errorf("empty geometry")
	}

	bb := &BoundingBox{
		MinX: coords[0][0],
		MinY: coords[0][1],
		MaxX: coords[0][0],
		MaxY: coords[0][1],
	}

	for _, coord := range coords {
		if coord[0] < bb.MinX {
			bb.MinX = coord[0]
		}
		if coord[0] > bb.MaxX {
			bb.MaxX = coord[0]
		}
		if coord[1] < bb.MinY {
			bb.MinY = coord[1]
		}
		if coord[1] > bb.MaxY {
			bb.MaxY = coord[1]
		}
	}

	return bb, nil
}

// Coordinates returns all coordinates as a slice of [x,y] pairs.
// For POINT: [[x,y]]
// For LINESTRING: [[x1,y1], [x2,y2], ...]
// For POLYGON: exterior ring coordinates (first ring).
func (g *Geometry) Coordinates() ([][2]float64, error) {
	if len(g.Data) < 5 {
		return nil, fmt.Errorf("invalid WKB data")
	}

	var order binary.ByteOrder
	if g.Data[0] == 0 {
		order = binary.BigEndian
	} else {
		order = binary.LittleEndian
	}

	switch g.Type {
	case GeometryPoint:
		return g.readPointCoords(order)
	case GeometryLineString:
		return g.readLineStringCoords(order)
	case GeometryPolygon:
		rings, err := g.readPolygonCoords(order)
		if err != nil {
			return nil, err
		}
		if len(rings) == 0 {
			return nil, nil
		}
		return rings[0], nil // Return exterior ring
	case GeometryMultiPoint:
		return g.readMultiPointCoords(order)
	case GeometryMultiLineString:
		return g.readMultiLineStringCoords(order)
	case GeometryMultiPolygon:
		return g.readMultiPolygonCoords(order)
	case GeometryCollection:
		// For collections, return nil - coordinate extraction not supported
		return nil, fmt.Errorf("coordinate extraction not supported for GEOMETRYCOLLECTION")
	}

	return nil, fmt.Errorf("unsupported geometry type: %d", g.Type)
}

// AllCoordinates returns all coordinates including holes for POLYGON.
func (g *Geometry) AllCoordinates() ([][][2]float64, error) {
	if len(g.Data) < 5 {
		return nil, fmt.Errorf("invalid WKB data")
	}

	var order binary.ByteOrder
	if g.Data[0] == 0 {
		order = binary.BigEndian
	} else {
		order = binary.LittleEndian
	}

	switch g.Type {
	case GeometryPoint:
		coords, err := g.readPointCoords(order)
		if err != nil {
			return nil, err
		}
		return [][][2]float64{coords}, nil
	case GeometryLineString:
		coords, err := g.readLineStringCoords(order)
		if err != nil {
			return nil, err
		}
		return [][][2]float64{coords}, nil
	case GeometryPolygon:
		return g.readPolygonCoords(order)
	case GeometryMultiPoint, GeometryMultiLineString, GeometryMultiPolygon, GeometryCollection:
		coords, err := g.Coordinates()
		if err != nil {
			return nil, err
		}
		return [][][2]float64{coords}, nil
	}

	return nil, fmt.Errorf("unsupported geometry type: %d", g.Type)
}

// readPointCoords reads POINT coordinates from WKB.
func (g *Geometry) readPointCoords(order binary.ByteOrder) ([][2]float64, error) {
	if len(g.Data) < 21 {
		return nil, fmt.Errorf("invalid POINT WKB data")
	}
	x := math.Float64frombits(order.Uint64(g.Data[5:13]))
	y := math.Float64frombits(order.Uint64(g.Data[13:21]))
	if math.IsNaN(x) || math.IsNaN(y) {
		return nil, nil // Empty point
	}
	return [][2]float64{{x, y}}, nil
}

// readLineStringCoords reads LINESTRING coordinates from WKB.
func (g *Geometry) readLineStringCoords(order binary.ByteOrder) ([][2]float64, error) {
	if len(g.Data) < 9 {
		return nil, fmt.Errorf("invalid LINESTRING WKB data")
	}
	numPoints := order.Uint32(g.Data[5:9])
	if numPoints == 0 {
		return nil, nil
	}

	coords := make([][2]float64, numPoints)
	offset := 9
	for i := uint32(0); i < numPoints; i++ {
		if offset+16 > len(g.Data) {
			return nil, fmt.Errorf("LINESTRING WKB data too short")
		}
		x := math.Float64frombits(order.Uint64(g.Data[offset : offset+8]))
		y := math.Float64frombits(order.Uint64(g.Data[offset+8 : offset+16]))
		coords[i] = [2]float64{x, y}
		offset += 16
	}
	return coords, nil
}

// readPolygonCoords reads POLYGON coordinates from WKB.
func (g *Geometry) readPolygonCoords(order binary.ByteOrder) ([][][2]float64, error) {
	if len(g.Data) < 9 {
		return nil, fmt.Errorf("invalid POLYGON WKB data")
	}
	numRings := order.Uint32(g.Data[5:9])
	if numRings == 0 {
		return nil, nil
	}

	rings := make([][][2]float64, numRings)
	offset := 9
	for i := uint32(0); i < numRings; i++ {
		if offset+4 > len(g.Data) {
			return nil, fmt.Errorf("POLYGON WKB data too short")
		}
		numPoints := order.Uint32(g.Data[offset : offset+4])
		offset += 4

		ring := make([][2]float64, numPoints)
		for j := uint32(0); j < numPoints; j++ {
			if offset+16 > len(g.Data) {
				return nil, fmt.Errorf("POLYGON WKB data too short")
			}
			x := math.Float64frombits(order.Uint64(g.Data[offset : offset+8]))
			y := math.Float64frombits(order.Uint64(g.Data[offset+8 : offset+16]))
			ring[j] = [2]float64{x, y}
			offset += 16
		}
		rings[i] = ring
	}
	return rings, nil
}

// readMultiPointCoords reads MULTIPOINT coordinates from WKB.
func (g *Geometry) readMultiPointCoords(order binary.ByteOrder) ([][2]float64, error) {
	if len(g.Data) < 9 {
		return nil, fmt.Errorf("invalid MULTIPOINT WKB data")
	}
	numPoints := order.Uint32(g.Data[5:9])
	if numPoints == 0 {
		return nil, nil
	}

	coords := make([][2]float64, 0, numPoints)
	offset := 9
	for i := uint32(0); i < numPoints; i++ {
		// Each point is a full WKB point (byte order + type + coords)
		if offset+21 > len(g.Data) {
			return nil, fmt.Errorf("MULTIPOINT WKB data too short")
		}
		// Skip byte order and type
		offset += 5
		x := math.Float64frombits(order.Uint64(g.Data[offset : offset+8]))
		y := math.Float64frombits(order.Uint64(g.Data[offset+8 : offset+16]))
		coords = append(coords, [2]float64{x, y})
		offset += 16
	}
	return coords, nil
}

// readMultiLineStringCoords reads MULTILINESTRING coordinates from WKB.
func (g *Geometry) readMultiLineStringCoords(order binary.ByteOrder) ([][2]float64, error) {
	if len(g.Data) < 9 {
		return nil, fmt.Errorf("invalid MULTILINESTRING WKB data")
	}
	numLineStrings := order.Uint32(g.Data[5:9])
	if numLineStrings == 0 {
		return nil, nil
	}

	var coords [][2]float64
	offset := 9
	for i := uint32(0); i < numLineStrings; i++ {
		// Each linestring is a full WKB linestring
		if offset+9 > len(g.Data) {
			return nil, fmt.Errorf("MULTILINESTRING WKB data too short")
		}
		// Skip byte order and type
		offset += 5
		numPoints := order.Uint32(g.Data[offset : offset+4])
		offset += 4

		for j := uint32(0); j < numPoints; j++ {
			if offset+16 > len(g.Data) {
				return nil, fmt.Errorf("MULTILINESTRING WKB data too short")
			}
			x := math.Float64frombits(order.Uint64(g.Data[offset : offset+8]))
			y := math.Float64frombits(order.Uint64(g.Data[offset+8 : offset+16]))
			coords = append(coords, [2]float64{x, y})
			offset += 16
		}
	}
	return coords, nil
}

// readMultiPolygonCoords reads MULTIPOLYGON coordinates from WKB.
func (g *Geometry) readMultiPolygonCoords(order binary.ByteOrder) ([][2]float64, error) {
	if len(g.Data) < 9 {
		return nil, fmt.Errorf("invalid MULTIPOLYGON WKB data")
	}
	numPolygons := order.Uint32(g.Data[5:9])
	if numPolygons == 0 {
		return nil, nil
	}

	var coords [][2]float64
	offset := 9
	for i := uint32(0); i < numPolygons; i++ {
		// Each polygon is a full WKB polygon
		if offset+9 > len(g.Data) {
			return nil, fmt.Errorf("MULTIPOLYGON WKB data too short")
		}
		// Skip byte order and type
		offset += 5
		numRings := order.Uint32(g.Data[offset : offset+4])
		offset += 4

		for j := uint32(0); j < numRings; j++ {
			if offset+4 > len(g.Data) {
				return nil, fmt.Errorf("MULTIPOLYGON WKB data too short")
			}
			numPoints := order.Uint32(g.Data[offset : offset+4])
			offset += 4

			for k := uint32(0); k < numPoints; k++ {
				if offset+16 > len(g.Data) {
					return nil, fmt.Errorf("MULTIPOLYGON WKB data too short")
				}
				x := math.Float64frombits(order.Uint64(g.Data[offset : offset+8]))
				y := math.Float64frombits(order.Uint64(g.Data[offset+8 : offset+16]))
				coords = append(coords, [2]float64{x, y})
				offset += 16
			}
		}
	}
	return coords, nil
}

// Helper functions

// pointInPolygon uses ray casting algorithm to check if a point is inside a polygon.
func pointInPolygon(px, py float64, polygon [][]float64) bool {
	inside := false
	n := len(polygon)
	if n == 0 {
		return false
	}

	j := n - 1
	for i := 0; i < n; i++ {
		xi, yi := polygon[i][0], polygon[i][1]
		xj, yj := polygon[j][0], polygon[j][1]

		if ((yi > py) != (yj > py)) && (px < (xj-xi)*(py-yi)/(yj-yi)+xi) {
			inside = !inside
		}
		j = i
	}
	return inside
}

// segmentsIntersect checks if two line segments intersect (including endpoints).
func segmentsIntersect(p1, p2, p3, p4 [2]float64) bool {
	d1 := direction(p3, p4, p1)
	d2 := direction(p3, p4, p2)
	d3 := direction(p1, p2, p3)
	d4 := direction(p1, p2, p4)

	if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
		((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
		return true
	}

	if d1 == 0 && onSegment(p3, p4, p1) {
		return true
	}
	if d2 == 0 && onSegment(p3, p4, p2) {
		return true
	}
	if d3 == 0 && onSegment(p1, p2, p3) {
		return true
	}
	if d4 == 0 && onSegment(p1, p2, p4) {
		return true
	}

	return false
}

// segmentsIntersectInterior checks if two line segments intersect at interior points.
func segmentsIntersectInterior(p1, p2, p3, p4 [2]float64) bool {
	d1 := direction(p3, p4, p1)
	d2 := direction(p3, p4, p2)
	d3 := direction(p1, p2, p3)
	d4 := direction(p1, p2, p4)

	return ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
		((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))
}

// direction computes the cross product of vectors (p2-p1) and (p3-p1).
func direction(p1, p2, p3 [2]float64) float64 {
	return (p3[0]-p1[0])*(p2[1]-p1[1]) - (p2[0]-p1[0])*(p3[1]-p1[1])
}

// onSegment checks if point p is on segment (p1, p2).
func onSegment(p1, p2, p [2]float64) bool {
	return math.Min(p1[0], p2[0]) <= p[0] && p[0] <= math.Max(p1[0], p2[0]) &&
		math.Min(p1[1], p2[1]) <= p[1] && p[1] <= math.Max(p1[1], p2[1])
}

// boundingBoxesIntersect checks if two bounding boxes intersect.
func boundingBoxesIntersect(bb1, bb2 *BoundingBox) bool {
	return bb1.MinX <= bb2.MaxX && bb1.MaxX >= bb2.MinX &&
		bb1.MinY <= bb2.MaxY && bb1.MaxY >= bb2.MinY
}

// pointsEqual checks if two points are equal.
func pointsEqual(p1, p2 [2]float64) bool {
	return p1[0] == p2[0] && p1[1] == p2[1]
}

// pointOnPolygonBoundary checks if a point is on the boundary of a polygon ring.
func pointOnPolygonBoundary(px, py float64, ring [][2]float64) bool {
	n := len(ring)
	for i := 0; i < n-1; i++ {
		if pointOnSegment(px, py, ring[i], ring[i+1]) {
			return true
		}
	}
	return false
}

// pointOnSegment checks if point (px, py) is on line segment (p1, p2).
func pointOnSegment(px, py float64, p1, p2 [2]float64) bool {
	// Check if point is in bounding box
	if px < math.Min(p1[0], p2[0]) || px > math.Max(p1[0], p2[0]) ||
		py < math.Min(p1[1], p2[1]) || py > math.Max(p1[1], p2[1]) {
		return false
	}

	// Check if point is on line (cross product should be zero)
	cross := (py-p1[1])*(p2[0]-p1[0]) - (px-p1[0])*(p2[1]-p1[1])
	return math.Abs(cross) < 1e-10
}

// geometryDimension returns the dimension of a geometry type.
func geometryDimension(gType GeometryType) int {
	switch gType {
	case GeometryPoint, GeometryMultiPoint:
		return 0
	case GeometryLineString, GeometryMultiLineString:
		return 1
	case GeometryPolygon, GeometryMultiPolygon:
		return 2
	case GeometryCollection:
		return -1 // Mixed dimension
	}
	return -1
}

// createPointGeometry creates a POINT geometry.
func createPointGeometry(x, y float64) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryPoint))
	binary.Write(buf, binary.LittleEndian, x)
	binary.Write(buf, binary.LittleEndian, y)

	return &Geometry{Type: GeometryPoint, Data: buf.Bytes()}, nil
}

// createLineStringGeometry creates a LINESTRING geometry.
func createLineStringGeometry(coords [][2]float64) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryLineString))
	binary.Write(buf, binary.LittleEndian, uint32(len(coords)))
	for _, c := range coords {
		binary.Write(buf, binary.LittleEndian, c[0])
		binary.Write(buf, binary.LittleEndian, c[1])
	}

	return &Geometry{Type: GeometryLineString, Data: buf.Bytes()}, nil
}

// createPolygonGeometry creates a POLYGON geometry.
func createPolygonGeometry(rings [][][2]float64) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryPolygon))
	binary.Write(buf, binary.LittleEndian, uint32(len(rings)))
	for _, ring := range rings {
		binary.Write(buf, binary.LittleEndian, uint32(len(ring)))
		for _, c := range ring {
			binary.Write(buf, binary.LittleEndian, c[0])
			binary.Write(buf, binary.LittleEndian, c[1])
		}
	}

	return &Geometry{Type: GeometryPolygon, Data: buf.Bytes()}, nil
}

// Area calculates the area of a geometry.
// For POLYGON: use Shoelace formula.
// For MULTIPOLYGON: sum of all polygon areas.
// For non-polygon types: returns 0.
func Area(g *Geometry) (float64, error) {
	if g == nil {
		return 0, fmt.Errorf("cannot calculate area of nil geometry")
	}

	switch g.Type {
	case GeometryPoint, GeometryMultiPoint, GeometryLineString, GeometryMultiLineString:
		// Non-polygon types have zero area
		return 0, nil

	case GeometryPolygon:
		rings, err := g.AllCoordinates()
		if err != nil {
			return 0, err
		}
		if len(rings) == 0 {
			return 0, nil
		}

		// Calculate exterior ring area
		area := shoelaceArea(rings[0])

		// Subtract hole areas
		for i := 1; i < len(rings); i++ {
			area -= shoelaceArea(rings[i])
		}

		return math.Abs(area), nil

	case GeometryMultiPolygon:
		// Sum areas of all polygons
		var totalArea float64
		subGeoms, err := g.SubGeometries()
		if err != nil {
			return 0, err
		}

		for _, subGeom := range subGeoms {
			a, err := Area(subGeom)
			if err != nil {
				return 0, err
			}
			totalArea += a
		}
		return totalArea, nil

	case GeometryCollection:
		// Sum areas of all geometries in collection
		var totalArea float64
		subGeoms, err := g.SubGeometries()
		if err != nil {
			return 0, err
		}

		for _, subGeom := range subGeoms {
			a, err := Area(subGeom)
			if err != nil {
				return 0, err
			}
			totalArea += a
		}
		return totalArea, nil
	}

	return 0, fmt.Errorf("unsupported geometry type for area: %d", g.Type)
}

// shoelaceArea calculates the area of a polygon ring using the Shoelace formula.
// The formula computes: 0.5 * |sum(x[i] * y[i+1] - x[i+1] * y[i])|
func shoelaceArea(coords [][2]float64) float64 {
	n := len(coords)
	if n < 3 {
		return 0
	}

	var area float64
	for i := 0; i < n; i++ {
		j := (i + 1) % n
		area += coords[i][0] * coords[j][1]
		area -= coords[j][0] * coords[i][1]
	}
	return math.Abs(area) / 2
}

// Length calculates the length of a geometry.
// For LINESTRING: sum of segment lengths.
// For MULTILINESTRING: sum of all linestring lengths.
// For POLYGON: perimeter (sum of all ring lengths).
// For POINT: returns 0.
func Length(g *Geometry) (float64, error) {
	if g == nil {
		return 0, fmt.Errorf("cannot calculate length of nil geometry")
	}

	switch g.Type {
	case GeometryPoint, GeometryMultiPoint:
		// Points have zero length
		return 0, nil

	case GeometryLineString:
		coords, err := g.Coordinates()
		if err != nil {
			return 0, err
		}
		return lineLength(coords), nil

	case GeometryMultiLineString:
		// Sum lengths of all linestrings
		var totalLength float64
		subGeoms, err := g.SubGeometries()
		if err != nil {
			return 0, err
		}

		for _, subGeom := range subGeoms {
			l, err := Length(subGeom)
			if err != nil {
				return 0, err
			}
			totalLength += l
		}
		return totalLength, nil

	case GeometryPolygon:
		// Perimeter: sum of all ring lengths
		rings, err := g.AllCoordinates()
		if err != nil {
			return 0, err
		}

		var totalLength float64
		for _, ring := range rings {
			totalLength += lineLength(ring)
		}
		return totalLength, nil

	case GeometryMultiPolygon:
		// Sum perimeters of all polygons
		var totalLength float64
		subGeoms, err := g.SubGeometries()
		if err != nil {
			return 0, err
		}

		for _, subGeom := range subGeoms {
			l, err := Length(subGeom)
			if err != nil {
				return 0, err
			}
			totalLength += l
		}
		return totalLength, nil

	case GeometryCollection:
		// Sum lengths of all geometries
		var totalLength float64
		subGeoms, err := g.SubGeometries()
		if err != nil {
			return 0, err
		}

		for _, subGeom := range subGeoms {
			l, err := Length(subGeom)
			if err != nil {
				return 0, err
			}
			totalLength += l
		}
		return totalLength, nil
	}

	return 0, fmt.Errorf("unsupported geometry type for length: %d", g.Type)
}

// lineLength calculates the total length of a sequence of coordinates.
func lineLength(coords [][2]float64) float64 {
	if len(coords) < 2 {
		return 0
	}

	var total float64
	for i := 0; i < len(coords)-1; i++ {
		total += segmentLength(coords[i][0], coords[i][1], coords[i+1][0], coords[i+1][1])
	}
	return total
}

// segmentLength calculates the length of a line segment.
func segmentLength(x1, y1, x2, y2 float64) float64 {
	dx := x2 - x1
	dy := y2 - y1
	return math.Sqrt(dx*dx + dy*dy)
}

// Centroid calculates the geometric center of a geometry.
// For POINT: returns the point itself.
// For LINESTRING: weighted average of midpoints.
// For POLYGON: centroid formula for polygons.
func Centroid(g *Geometry) (*Geometry, error) {
	if g == nil {
		return nil, fmt.Errorf("cannot calculate centroid of nil geometry")
	}

	switch g.Type {
	case GeometryPoint:
		// Return a copy of the point
		coords, err := g.Coordinates()
		if err != nil {
			return nil, err
		}
		if len(coords) == 0 {
			return nil, fmt.Errorf("empty point geometry")
		}
		return createPointGeometry(coords[0][0], coords[0][1])

	case GeometryMultiPoint:
		// Average of all points
		coords, err := g.Coordinates()
		if err != nil {
			return nil, err
		}
		if len(coords) == 0 {
			return nil, fmt.Errorf("empty multipoint geometry")
		}

		var sumX, sumY float64
		for _, c := range coords {
			sumX += c[0]
			sumY += c[1]
		}
		n := float64(len(coords))
		return createPointGeometry(sumX/n, sumY/n)

	case GeometryLineString:
		coords, err := g.Coordinates()
		if err != nil {
			return nil, err
		}
		cx, cy := lineCentroid(coords)
		return createPointGeometry(cx, cy)

	case GeometryMultiLineString:
		// Weighted centroid of all linestrings
		var totalLength float64
		var weightedX, weightedY float64

		subGeoms, err := g.SubGeometries()
		if err != nil {
			return nil, err
		}

		for _, subGeom := range subGeoms {
			coords, err := subGeom.Coordinates()
			if err != nil {
				return nil, err
			}
			length := lineLength(coords)
			cx, cy := lineCentroid(coords)
			weightedX += cx * length
			weightedY += cy * length
			totalLength += length
		}

		if totalLength == 0 {
			return createPointGeometry(0, 0)
		}
		return createPointGeometry(weightedX/totalLength, weightedY/totalLength)

	case GeometryPolygon:
		rings, err := g.AllCoordinates()
		if err != nil {
			return nil, err
		}
		if len(rings) == 0 || len(rings[0]) == 0 {
			return nil, fmt.Errorf("empty polygon geometry")
		}

		// Use the exterior ring for centroid calculation
		cx, cy := polygonCentroid(rings[0])
		return createPointGeometry(cx, cy)

	case GeometryMultiPolygon:
		// Weighted centroid based on area
		var totalArea float64
		var weightedX, weightedY float64

		subGeoms, err := g.SubGeometries()
		if err != nil {
			return nil, err
		}

		for _, subGeom := range subGeoms {
			area, err := Area(subGeom)
			if err != nil {
				return nil, err
			}
			centroid, err := Centroid(subGeom)
			if err != nil {
				return nil, err
			}
			cx, err := centroid.X()
			if err != nil {
				return nil, err
			}
			cy, err := centroid.Y()
			if err != nil {
				return nil, err
			}
			weightedX += cx * area
			weightedY += cy * area
			totalArea += area
		}

		if totalArea == 0 {
			return createPointGeometry(0, 0)
		}
		return createPointGeometry(weightedX/totalArea, weightedY/totalArea)

	case GeometryCollection:
		// Simple average of all centroids
		subGeoms, err := g.SubGeometries()
		if err != nil {
			return nil, err
		}

		if len(subGeoms) == 0 {
			return nil, fmt.Errorf("empty geometry collection")
		}

		var sumX, sumY float64
		for _, subGeom := range subGeoms {
			centroid, err := Centroid(subGeom)
			if err != nil {
				return nil, err
			}
			cx, err := centroid.X()
			if err != nil {
				return nil, err
			}
			cy, err := centroid.Y()
			if err != nil {
				return nil, err
			}
			sumX += cx
			sumY += cy
		}
		n := float64(len(subGeoms))
		return createPointGeometry(sumX/n, sumY/n)
	}

	return nil, fmt.Errorf("unsupported geometry type for centroid: %d", g.Type)
}

// lineCentroid calculates the centroid of a linestring using weighted average of midpoints.
func lineCentroid(coords [][2]float64) (float64, float64) {
	if len(coords) == 0 {
		return 0, 0
	}
	if len(coords) == 1 {
		return coords[0][0], coords[0][1]
	}

	var totalLength float64
	var weightedX, weightedY float64

	for i := 0; i < len(coords)-1; i++ {
		x1, y1 := coords[i][0], coords[i][1]
		x2, y2 := coords[i+1][0], coords[i+1][1]

		segLen := segmentLength(x1, y1, x2, y2)
		midX := (x1 + x2) / 2
		midY := (y1 + y2) / 2

		weightedX += midX * segLen
		weightedY += midY * segLen
		totalLength += segLen
	}

	if totalLength == 0 {
		// Degenerate linestring - all points the same
		return coords[0][0], coords[0][1]
	}

	return weightedX / totalLength, weightedY / totalLength
}

// polygonCentroid calculates the centroid of a polygon ring.
func polygonCentroid(coords [][2]float64) (float64, float64) {
	n := len(coords)
	if n == 0 {
		return 0, 0
	}

	var cx, cy, signedArea float64
	for i := 0; i < n; i++ {
		j := (i + 1) % n
		x0, y0 := coords[i][0], coords[i][1]
		x1, y1 := coords[j][0], coords[j][1]

		a := x0*y1 - x1*y0
		signedArea += a
		cx += (x0 + x1) * a
		cy += (y0 + y1) * a
	}

	signedArea *= 0.5
	if signedArea == 0 {
		// Degenerate polygon - return average of points
		var sumX, sumY float64
		for _, c := range coords {
			sumX += c[0]
			sumY += c[1]
		}
		return sumX / float64(n), sumY / float64(n)
	}

	cx /= (6 * signedArea)
	cy /= (6 * signedArea)
	return cx, cy
}

// SubGeometries returns the sub-geometries for MULTI* and GEOMETRYCOLLECTION types.
func (g *Geometry) SubGeometries() ([]*Geometry, error) {
	if len(g.Data) < 9 {
		return nil, fmt.Errorf("invalid WKB data")
	}

	var order binary.ByteOrder
	if g.Data[0] == 0 {
		order = binary.BigEndian
	} else {
		order = binary.LittleEndian
	}

	switch g.Type {
	case GeometryMultiPoint:
		return g.readMultiPointGeometries(order)
	case GeometryMultiLineString:
		return g.readMultiLineStringGeometries(order)
	case GeometryMultiPolygon:
		return g.readMultiPolygonGeometries(order)
	case GeometryCollection:
		return g.readGeometryCollectionGeometries(order)
	default:
		// For non-multi types, return self as single element
		return []*Geometry{g}, nil
	}
}

// readMultiPointGeometries extracts individual Point geometries from a MultiPoint.
func (g *Geometry) readMultiPointGeometries(order binary.ByteOrder) ([]*Geometry, error) {
	if len(g.Data) < 9 {
		return nil, fmt.Errorf("invalid MULTIPOINT WKB data")
	}
	numPoints := order.Uint32(g.Data[5:9])
	if numPoints == 0 {
		return nil, nil
	}

	geoms := make([]*Geometry, 0, numPoints)
	offset := 9
	for i := uint32(0); i < numPoints; i++ {
		if offset+21 > len(g.Data) {
			return nil, fmt.Errorf("MULTIPOINT WKB data too short")
		}
		// Each point is a full WKB geometry (21 bytes: 1 byte order + 4 type + 16 coords)
		pointData := g.Data[offset : offset+21]
		geoms = append(geoms, &Geometry{Type: GeometryPoint, Data: pointData})
		offset += 21
	}
	return geoms, nil
}

// readMultiLineStringGeometries extracts individual LineString geometries.
func (g *Geometry) readMultiLineStringGeometries(order binary.ByteOrder) ([]*Geometry, error) {
	if len(g.Data) < 9 {
		return nil, fmt.Errorf("invalid MULTILINESTRING WKB data")
	}
	numLineStrings := order.Uint32(g.Data[5:9])
	if numLineStrings == 0 {
		return nil, nil
	}

	geoms := make([]*Geometry, 0, numLineStrings)
	offset := 9
	for i := uint32(0); i < numLineStrings; i++ {
		if offset+9 > len(g.Data) {
			return nil, fmt.Errorf("MULTILINESTRING WKB data too short")
		}
		// Read number of points
		numPoints := order.Uint32(g.Data[offset+5 : offset+9])
		lineSize := 9 + int(numPoints)*16
		if offset+lineSize > len(g.Data) {
			return nil, fmt.Errorf("MULTILINESTRING WKB data too short")
		}
		lineData := g.Data[offset : offset+lineSize]
		geoms = append(geoms, &Geometry{Type: GeometryLineString, Data: lineData})
		offset += lineSize
	}
	return geoms, nil
}

// readMultiPolygonGeometries extracts individual Polygon geometries.
func (g *Geometry) readMultiPolygonGeometries(order binary.ByteOrder) ([]*Geometry, error) {
	if len(g.Data) < 9 {
		return nil, fmt.Errorf("invalid MULTIPOLYGON WKB data")
	}
	numPolygons := order.Uint32(g.Data[5:9])
	if numPolygons == 0 {
		return nil, nil
	}

	geoms := make([]*Geometry, 0, numPolygons)
	offset := 9
	for i := uint32(0); i < numPolygons; i++ {
		if offset+9 > len(g.Data) {
			return nil, fmt.Errorf("MULTIPOLYGON WKB data too short")
		}
		startOffset := offset
		// Skip byte order and type
		offset += 5
		numRings := order.Uint32(g.Data[offset : offset+4])
		offset += 4

		for j := uint32(0); j < numRings; j++ {
			if offset+4 > len(g.Data) {
				return nil, fmt.Errorf("MULTIPOLYGON WKB data too short")
			}
			numPoints := order.Uint32(g.Data[offset : offset+4])
			offset += 4 + int(numPoints)*16
		}

		polyData := g.Data[startOffset:offset]
		geoms = append(geoms, &Geometry{Type: GeometryPolygon, Data: polyData})
	}
	return geoms, nil
}

// readGeometryCollectionGeometries extracts geometries from a collection.
func (g *Geometry) readGeometryCollectionGeometries(order binary.ByteOrder) ([]*Geometry, error) {
	if len(g.Data) < 9 {
		return nil, fmt.Errorf("invalid GEOMETRYCOLLECTION WKB data")
	}
	numGeoms := order.Uint32(g.Data[5:9])
	if numGeoms == 0 {
		return nil, nil
	}

	// For geometry collections, we need to parse each sub-geometry
	// This is complex because sub-geometries have varying sizes
	// For now, return an error indicating this is not fully supported
	return nil, fmt.Errorf("GEOMETRYCOLLECTION sub-geometry extraction not fully implemented")
}

// ============================================================================
// Phase 5: Set Operations
// ============================================================================

// MakePolygon creates a POLYGON geometry from a closed LINESTRING.
// The linestring must be closed (first and last points must be the same).
func MakePolygon(ring *Geometry) (*Geometry, error) {
	if ring == nil {
		return nil, fmt.Errorf("cannot create polygon from nil geometry")
	}

	if ring.Type != GeometryLineString {
		return nil, fmt.Errorf("ST_MakePolygon requires a LINESTRING, got %s", ring.String())
	}

	coords, err := ring.Coordinates()
	if err != nil {
		return nil, fmt.Errorf("failed to get coordinates: %w", err)
	}

	if len(coords) < 4 {
		return nil, fmt.Errorf("polygon requires at least 4 points (3 unique + closing point)")
	}

	// Check if linestring is closed
	first := coords[0]
	last := coords[len(coords)-1]
	if first[0] != last[0] || first[1] != last[1] {
		return nil, fmt.Errorf("linestring is not closed (first point %v != last point %v)", first, last)
	}

	// Create polygon from the ring
	return createPolygonGeometry([][][2]float64{coords})
}

// Buffer creates a buffer zone around a geometry.
// For POINT: creates a circle approximation (polygon with n vertices).
// For LINESTRING: creates a buffered corridor.
// For POLYGON: expands/contracts the polygon.
// Positive distance expands, negative distance contracts.
func Buffer(g *Geometry, distance float64) (*Geometry, error) {
	if g == nil {
		return nil, fmt.Errorf("cannot buffer nil geometry")
	}

	switch g.Type {
	case GeometryPoint:
		return bufferPoint(g, distance, 32)
	case GeometryLineString:
		return bufferLineString(g, distance, 8)
	case GeometryPolygon:
		return bufferPolygon(g, distance)
	default:
		return nil, fmt.Errorf("buffer not supported for geometry type: %s", g.String())
	}
}

// bufferPoint creates a circle approximation around a point.
func bufferPoint(g *Geometry, distance float64, numSegments int) (*Geometry, error) {
	if distance <= 0 {
		// Zero or negative buffer on point results in empty geometry
		return nil, fmt.Errorf("cannot create buffer with non-positive distance on POINT")
	}

	coords, err := g.Coordinates()
	if err != nil {
		return nil, err
	}
	if len(coords) == 0 {
		return nil, fmt.Errorf("empty point geometry")
	}

	x, y := coords[0][0], coords[0][1]
	circleCoords := make([][2]float64, numSegments+1)
	for i := 0; i <= numSegments; i++ {
		angle := 2 * math.Pi * float64(i) / float64(numSegments)
		circleCoords[i] = [2]float64{
			x + distance*math.Cos(angle),
			y + distance*math.Sin(angle),
		}
	}

	return createPolygonGeometry([][][2]float64{circleCoords})
}

// bufferLineString creates a buffered corridor around a linestring.
func bufferLineString(g *Geometry, distance float64, quadSegs int) (*Geometry, error) {
	if distance <= 0 {
		return nil, fmt.Errorf("cannot create buffer with non-positive distance on LINESTRING")
	}

	coords, err := g.Coordinates()
	if err != nil {
		return nil, err
	}
	if len(coords) < 2 {
		return nil, fmt.Errorf("linestring needs at least 2 points")
	}

	// Build the buffer polygon by offsetting the line on both sides
	// This is a simplified implementation using offset curves
	leftSide := offsetCurve(coords, distance)
	rightSide := offsetCurve(coords, -distance)

	// Reverse the right side to form a closed polygon
	for i, j := 0, len(rightSide)-1; i < j; i, j = i+1, j-1 {
		rightSide[i], rightSide[j] = rightSide[j], rightSide[i]
	}

	// Create end caps (simplified - semicircles at each end)
	startCap := createEndCap(coords[0], coords[1], distance, quadSegs, true)
	endCap := createEndCap(coords[len(coords)-1], coords[len(coords)-2], distance, quadSegs, false)

	// Combine all parts into a single polygon ring
	ring := make([][2]float64, 0, len(leftSide)+len(rightSide)+len(startCap)+len(endCap))
	ring = append(ring, leftSide...)
	ring = append(ring, endCap...)
	ring = append(ring, rightSide...)
	ring = append(ring, startCap...)

	// Close the ring
	if len(ring) > 0 && (ring[0][0] != ring[len(ring)-1][0] || ring[0][1] != ring[len(ring)-1][1]) {
		ring = append(ring, ring[0])
	}

	return createPolygonGeometry([][][2]float64{ring})
}

// offsetCurve creates a parallel curve offset from the original by distance.
func offsetCurve(coords [][2]float64, distance float64) [][2]float64 {
	if len(coords) < 2 {
		return nil
	}

	result := make([][2]float64, len(coords))
	for i := range coords {
		var dx, dy float64
		if i == 0 {
			// First point - use direction to next point
			dx = coords[1][0] - coords[0][0]
			dy = coords[1][1] - coords[0][1]
		} else if i == len(coords)-1 {
			// Last point - use direction from previous point
			dx = coords[i][0] - coords[i-1][0]
			dy = coords[i][1] - coords[i-1][1]
		} else {
			// Middle points - average of directions
			dx1 := coords[i][0] - coords[i-1][0]
			dy1 := coords[i][1] - coords[i-1][1]
			dx2 := coords[i+1][0] - coords[i][0]
			dy2 := coords[i+1][1] - coords[i][1]
			dx = (dx1 + dx2) / 2
			dy = (dy1 + dy2) / 2
		}

		// Normalize and get perpendicular
		length := math.Sqrt(dx*dx + dy*dy)
		if length > 0 {
			// Perpendicular vector (rotate 90 degrees)
			perpX := -dy / length * distance
			perpY := dx / length * distance
			result[i] = [2]float64{coords[i][0] + perpX, coords[i][1] + perpY}
		} else {
			result[i] = coords[i]
		}
	}
	return result
}

// createEndCap creates a semicircular end cap for a buffered linestring.
func createEndCap(endPoint, prevPoint [2]float64, distance float64, quadSegs int, isStart bool) [][2]float64 {
	dx := endPoint[0] - prevPoint[0]
	dy := endPoint[1] - prevPoint[1]
	length := math.Sqrt(dx*dx + dy*dy)
	if length == 0 {
		return nil
	}

	// Direction angle
	angle := math.Atan2(dy, dx)
	if isStart {
		angle += math.Pi // Point in opposite direction for start cap
	}

	// Create semicircle points
	numPoints := quadSegs * 2
	cap := make([][2]float64, numPoints)
	startAngle := angle - math.Pi/2
	for i := 0; i < numPoints; i++ {
		a := startAngle + math.Pi*float64(i)/float64(numPoints-1)
		cap[i] = [2]float64{
			endPoint[0] + distance*math.Cos(a),
			endPoint[1] + distance*math.Sin(a),
		}
	}
	return cap
}

// bufferPolygon expands or contracts a polygon by the given distance.
func bufferPolygon(g *Geometry, distance float64) (*Geometry, error) {
	rings, err := g.AllCoordinates()
	if err != nil {
		return nil, err
	}
	if len(rings) == 0 {
		return nil, fmt.Errorf("empty polygon")
	}

	// Buffer the exterior ring
	exterior := rings[0]
	bufferedExterior := bufferPolygonRing(exterior, distance)

	// For simplicity, if contracting and result is invalid, return error
	if len(bufferedExterior) < 4 {
		return nil, fmt.Errorf("buffer resulted in degenerate polygon")
	}

	// Close the ring if needed
	if bufferedExterior[0][0] != bufferedExterior[len(bufferedExterior)-1][0] ||
		bufferedExterior[0][1] != bufferedExterior[len(bufferedExterior)-1][1] {
		bufferedExterior = append(bufferedExterior, bufferedExterior[0])
	}

	// Handle holes - they need to be buffered in the opposite direction
	bufferedRings := [][][2]float64{bufferedExterior}
	for i := 1; i < len(rings); i++ {
		hole := bufferPolygonRing(rings[i], -distance)
		if len(hole) >= 4 {
			if hole[0][0] != hole[len(hole)-1][0] || hole[0][1] != hole[len(hole)-1][1] {
				hole = append(hole, hole[0])
			}
			bufferedRings = append(bufferedRings, hole)
		}
	}

	return createPolygonGeometry(bufferedRings)
}

// bufferPolygonRing offsets all vertices of a polygon ring.
func bufferPolygonRing(coords [][2]float64, distance float64) [][2]float64 {
	n := len(coords)
	if n < 3 {
		return coords
	}

	// Remove closing point for calculation if present
	if coords[0][0] == coords[n-1][0] && coords[0][1] == coords[n-1][1] {
		n--
	}

	result := make([][2]float64, n)
	for i := 0; i < n; i++ {
		prev := (i - 1 + n) % n
		next := (i + 1) % n

		// Get edge directions
		dx1 := coords[i][0] - coords[prev][0]
		dy1 := coords[i][1] - coords[prev][1]
		dx2 := coords[next][0] - coords[i][0]
		dy2 := coords[next][1] - coords[i][1]

		// Normalize
		len1 := math.Sqrt(dx1*dx1 + dy1*dy1)
		len2 := math.Sqrt(dx2*dx2 + dy2*dy2)

		if len1 == 0 || len2 == 0 {
			result[i] = coords[i]
			continue
		}

		dx1, dy1 = dx1/len1, dy1/len1
		dx2, dy2 = dx2/len2, dy2/len2

		// Perpendicular directions (outward)
		perpX1, perpY1 := -dy1, dx1
		perpX2, perpY2 := -dy2, dx2

		// Average perpendicular direction
		avgPerpX := (perpX1 + perpX2) / 2
		avgPerpY := (perpY1 + perpY2) / 2

		// Normalize the average
		avgLen := math.Sqrt(avgPerpX*avgPerpX + avgPerpY*avgPerpY)
		if avgLen > 0 {
			avgPerpX /= avgLen
			avgPerpY /= avgLen
		}

		// Calculate the miter factor (to maintain proper distance at corners)
		dot := perpX1*perpX2 + perpY1*perpY2
		miterFactor := 1.0
		if dot < 0.99 { // Only adjust for non-parallel edges
			// Miter factor = 1 / cos(theta/2) where theta is angle between edges
			halfAngle := math.Acos(math.Max(-1, math.Min(1, dot))) / 2
			if halfAngle > 0 {
				miterFactor = 1.0 / math.Cos(halfAngle)
			}
			// Limit miter to avoid extreme spikes
			if miterFactor > 4 {
				miterFactor = 4
			}
		}

		result[i] = [2]float64{
			coords[i][0] + avgPerpX*distance*miterFactor,
			coords[i][1] + avgPerpY*distance*miterFactor,
		}
	}

	return result
}

// Intersection returns the shared area between two geometries.
// For polygons: uses Sutherland-Hodgman clipping algorithm.
func Intersection(g1, g2 *Geometry) (*Geometry, error) {
	if g1 == nil || g2 == nil {
		return nil, fmt.Errorf("cannot compute intersection with nil geometry")
	}

	// Handle POLYGON-POLYGON intersection
	if g1.Type == GeometryPolygon && g2.Type == GeometryPolygon {
		return polygonPolygonIntersection(g1, g2)
	}

	// Handle POINT-POLYGON intersection
	if g1.Type == GeometryPoint && g2.Type == GeometryPolygon {
		contains, err := Contains(g2, g1)
		if err != nil {
			return nil, err
		}
		if contains {
			return g1, nil
		}
		return nil, nil // Empty result
	}
	if g2.Type == GeometryPoint && g1.Type == GeometryPolygon {
		contains, err := Contains(g1, g2)
		if err != nil {
			return nil, err
		}
		if contains {
			return g2, nil
		}
		return nil, nil // Empty result
	}

	// Handle POINT-POINT intersection
	if g1.Type == GeometryPoint && g2.Type == GeometryPoint {
		eq, err := Equals(g1, g2)
		if err != nil {
			return nil, err
		}
		if eq {
			return g1, nil
		}
		return nil, nil // Empty result
	}

	return nil, fmt.Errorf("intersection not implemented for %s and %s", g1.String(), g2.String())
}

// polygonPolygonIntersection computes the intersection of two convex polygons.
// Uses Sutherland-Hodgman clipping algorithm.
func polygonPolygonIntersection(g1, g2 *Geometry) (*Geometry, error) {
	rings1, err := g1.AllCoordinates()
	if err != nil {
		return nil, err
	}
	rings2, err := g2.AllCoordinates()
	if err != nil {
		return nil, err
	}

	if len(rings1) == 0 || len(rings2) == 0 {
		return nil, nil
	}

	subject := rings1[0] // Exterior ring of first polygon
	clip := rings2[0]    // Exterior ring of second polygon

	// Remove closing point if present
	if len(subject) > 0 && subject[0][0] == subject[len(subject)-1][0] &&
		subject[0][1] == subject[len(subject)-1][1] {
		subject = subject[:len(subject)-1]
	}
	if len(clip) > 0 && clip[0][0] == clip[len(clip)-1][0] &&
		clip[0][1] == clip[len(clip)-1][1] {
		clip = clip[:len(clip)-1]
	}

	// Ensure both polygons are in consistent winding order (counter-clockwise)
	subject = ensureCounterClockwise(subject)
	clip = ensureCounterClockwise(clip)

	// Apply Sutherland-Hodgman algorithm
	result := sutherlandHodgman(subject, clip)

	if len(result) < 3 {
		return nil, nil // No intersection
	}

	// Close the ring
	result = append(result, result[0])

	return createPolygonGeometry([][][2]float64{result})
}

// ensureCounterClockwise ensures the polygon vertices are in counter-clockwise order.
func ensureCounterClockwise(coords [][2]float64) [][2]float64 {
	if len(coords) < 3 {
		return coords
	}

	// Calculate signed area using the shoelace formula
	// Positive area = counter-clockwise, negative = clockwise
	signedArea := 0.0
	for i := 0; i < len(coords); i++ {
		j := (i + 1) % len(coords)
		signedArea += coords[i][0] * coords[j][1]
		signedArea -= coords[j][0] * coords[i][1]
	}

	// If clockwise (negative area), reverse the order
	if signedArea < 0 {
		reversed := make([][2]float64, len(coords))
		for i, c := range coords {
			reversed[len(coords)-1-i] = c
		}
		return reversed
	}
	return coords
}

// sutherlandHodgman clips the subject polygon against the clip polygon.
// Both polygons should have vertices in counter-clockwise order.
func sutherlandHodgman(subject, clip [][2]float64) [][2]float64 {
	if len(subject) == 0 || len(clip) == 0 {
		return nil
	}

	output := make([][2]float64, len(subject))
	copy(output, subject)

	for i := range clip {
		if len(output) == 0 {
			return nil
		}

		input := output
		output = nil

		// Get clip edge
		p1 := clip[i]
		p2 := clip[(i+1)%len(clip)]

		for j := range input {
			current := input[j]
			next := input[(j+1)%len(input)]

			currentInside := isLeftOfEdge(p1, p2, current)
			nextInside := isLeftOfEdge(p1, p2, next)

			if currentInside {
				output = append(output, current)
				if !nextInside {
					// Exiting - add intersection point
					if ix, iy, ok := lineSegmentIntersection(
						p1[0], p1[1], p2[0], p2[1],
						current[0], current[1], next[0], next[1],
					); ok {
						output = append(output, [2]float64{ix, iy})
					}
				}
			} else if nextInside {
				// Entering - add intersection point
				if ix, iy, ok := lineSegmentIntersection(
					p1[0], p1[1], p2[0], p2[1],
					current[0], current[1], next[0], next[1],
				); ok {
					output = append(output, [2]float64{ix, iy})
				}
			}
		}
	}

	return output
}

// isLeftOfEdge returns true if point is on the left side of edge p1->p2.
// For counter-clockwise polygons, left side is inside.
func isLeftOfEdge(p1, p2, point [2]float64) bool {
	return (p2[0]-p1[0])*(point[1]-p1[1])-(p2[1]-p1[1])*(point[0]-p1[0]) >= 0
}

// lineSegmentIntersection finds the intersection point of two line segments.
// Returns the intersection point and true if segments intersect.
func lineSegmentIntersection(x1, y1, x2, y2, x3, y3, x4, y4 float64) (float64, float64, bool) {
	denom := (x1-x2)*(y3-y4) - (y1-y2)*(x3-x4)
	if math.Abs(denom) < 1e-10 {
		return 0, 0, false // Parallel lines
	}

	t := ((x1-x3)*(y3-y4) - (y1-y3)*(x3-x4)) / denom
	ix := x1 + t*(x2-x1)
	iy := y1 + t*(y2-y1)

	return ix, iy, true
}

// Union combines two geometries into one.
// For non-overlapping polygons: returns MULTIPOLYGON.
// For overlapping polygons: merges into single polygon (simplified).
func Union(g1, g2 *Geometry) (*Geometry, error) {
	if g1 == nil || g2 == nil {
		return nil, fmt.Errorf("cannot compute union with nil geometry")
	}

	// Handle POLYGON-POLYGON union
	if g1.Type == GeometryPolygon && g2.Type == GeometryPolygon {
		return polygonPolygonUnion(g1, g2)
	}

	// Handle POINT-POINT union
	if g1.Type == GeometryPoint && g2.Type == GeometryPoint {
		eq, err := Equals(g1, g2)
		if err != nil {
			return nil, err
		}
		if eq {
			return g1, nil
		}
		// Create MULTIPOINT
		return createMultiPointFromGeometries(g1, g2)
	}

	// Handle POINT-POLYGON union
	if (g1.Type == GeometryPoint && g2.Type == GeometryPolygon) ||
		(g2.Type == GeometryPoint && g1.Type == GeometryPolygon) {
		var point, poly *Geometry
		if g1.Type == GeometryPoint {
			point, poly = g1, g2
		} else {
			point, poly = g2, g1
		}

		contains, err := Contains(poly, point)
		if err != nil {
			return nil, err
		}
		if contains {
			return poly, nil // Point is inside polygon, union is just the polygon
		}
		// Point is outside - return geometry collection
		return createGeometryCollection(point, poly)
	}

	return nil, fmt.Errorf("union not implemented for %s and %s", g1.String(), g2.String())
}

// polygonPolygonUnion computes the union of two polygons.
func polygonPolygonUnion(g1, g2 *Geometry) (*Geometry, error) {
	// Check if they intersect
	intersects, err := Intersects(g1, g2)
	if err != nil {
		return nil, err
	}

	if !intersects {
		// Disjoint - return MULTIPOLYGON
		return createMultiPolygonFromGeometries(g1, g2)
	}

	// Check containment
	contains1, _ := Contains(g1, g2)
	if contains1 {
		return g1, nil // g1 contains g2, union is g1
	}
	contains2, _ := Contains(g2, g1)
	if contains2 {
		return g2, nil // g2 contains g1, union is g2
	}

	// Overlapping case - compute convex hull as approximation
	// A full polygon union is complex; we'll use convex hull for overlapping polygons
	return convexHullUnion(g1, g2)
}

// convexHullUnion creates the convex hull of two overlapping polygons.
// This is an approximation of the true union.
func convexHullUnion(g1, g2 *Geometry) (*Geometry, error) {
	coords1, err := g1.Coordinates()
	if err != nil {
		return nil, err
	}
	coords2, err := g2.Coordinates()
	if err != nil {
		return nil, err
	}

	// Combine all points
	allPoints := make([][2]float64, 0, len(coords1)+len(coords2))
	allPoints = append(allPoints, coords1...)
	allPoints = append(allPoints, coords2...)

	// Compute convex hull
	hull := convexHull(allPoints)

	if len(hull) < 3 {
		return nil, fmt.Errorf("convex hull resulted in degenerate polygon")
	}

	// Close the ring
	hull = append(hull, hull[0])

	return createPolygonGeometry([][][2]float64{hull})
}

// convexHull computes the convex hull of a set of points using Graham scan.
func convexHull(points [][2]float64) [][2]float64 {
	if len(points) < 3 {
		return points
	}

	// Find the point with lowest y (and leftmost if tie)
	pivot := 0
	for i := 1; i < len(points); i++ {
		if points[i][1] < points[pivot][1] ||
			(points[i][1] == points[pivot][1] && points[i][0] < points[pivot][0]) {
			pivot = i
		}
	}

	// Swap pivot to first position
	points[0], points[pivot] = points[pivot], points[0]

	// Sort by polar angle with respect to pivot
	pivotPoint := points[0]
	sortedPoints := make([][2]float64, len(points)-1)
	copy(sortedPoints, points[1:])

	// Sort by angle
	for i := 0; i < len(sortedPoints)-1; i++ {
		for j := i + 1; j < len(sortedPoints); j++ {
			angle1 := math.Atan2(sortedPoints[i][1]-pivotPoint[1], sortedPoints[i][0]-pivotPoint[0])
			angle2 := math.Atan2(sortedPoints[j][1]-pivotPoint[1], sortedPoints[j][0]-pivotPoint[0])
			if angle1 > angle2 {
				sortedPoints[i], sortedPoints[j] = sortedPoints[j], sortedPoints[i]
			}
		}
	}

	// Build hull using stack
	hull := make([][2]float64, 0, len(points))
	hull = append(hull, pivotPoint)

	for _, p := range sortedPoints {
		for len(hull) > 1 && crossProduct(hull[len(hull)-2], hull[len(hull)-1], p) <= 0 {
			hull = hull[:len(hull)-1]
		}
		hull = append(hull, p)
	}

	return hull
}

// crossProduct computes the cross product of vectors (p2-p1) and (p3-p1).
func crossProduct(p1, p2, p3 [2]float64) float64 {
	return (p2[0]-p1[0])*(p3[1]-p1[1]) - (p2[1]-p1[1])*(p3[0]-p1[0])
}

// Difference returns g1 minus g2 (area of g1 not covered by g2).
func Difference(g1, g2 *Geometry) (*Geometry, error) {
	if g1 == nil || g2 == nil {
		return nil, fmt.Errorf("cannot compute difference with nil geometry")
	}

	// Handle POLYGON-POLYGON difference
	if g1.Type == GeometryPolygon && g2.Type == GeometryPolygon {
		return polygonPolygonDifference(g1, g2)
	}

	// Handle POINT-POLYGON difference
	if g1.Type == GeometryPoint && g2.Type == GeometryPolygon {
		contains, err := Contains(g2, g1)
		if err != nil {
			return nil, err
		}
		if contains {
			return nil, nil // Point is inside polygon, difference is empty
		}
		return g1, nil // Point is outside, difference is the point
	}

	// Handle POINT-POINT difference
	if g1.Type == GeometryPoint && g2.Type == GeometryPoint {
		eq, err := Equals(g1, g2)
		if err != nil {
			return nil, err
		}
		if eq {
			return nil, nil // Same point, difference is empty
		}
		return g1, nil // Different points, difference is g1
	}

	return nil, fmt.Errorf("difference not implemented for %s and %s", g1.String(), g2.String())
}

// polygonPolygonDifference computes the difference of two polygons.
func polygonPolygonDifference(g1, g2 *Geometry) (*Geometry, error) {
	// Check if they intersect
	intersects, err := Intersects(g1, g2)
	if err != nil {
		return nil, err
	}

	if !intersects {
		return g1, nil // No intersection, difference is g1
	}

	// Check if g2 fully contains g1
	contains2, _ := Contains(g2, g1)
	if contains2 {
		return nil, nil // g2 contains g1, difference is empty
	}

	// Check if g1 fully contains g2 - result would be g1 with g2 as a hole
	contains1, _ := Contains(g1, g2)
	if contains1 {
		return createPolygonWithHole(g1, g2)
	}

	// Partial overlap - use clipping
	// Clip g1 by the complement of g2
	// This is a simplified approach using the non-intersecting parts
	return clipPolygonByExterior(g1, g2)
}

// createPolygonWithHole creates a polygon with an interior hole.
func createPolygonWithHole(outer, hole *Geometry) (*Geometry, error) {
	outerRings, err := outer.AllCoordinates()
	if err != nil {
		return nil, err
	}
	holeRings, err := hole.AllCoordinates()
	if err != nil {
		return nil, err
	}

	if len(outerRings) == 0 || len(holeRings) == 0 {
		return nil, fmt.Errorf("empty geometry")
	}

	// Create polygon with hole
	rings := make([][][2]float64, 0, len(outerRings)+1)
	rings = append(rings, outerRings[0]) // Exterior ring

	// Add the hole (needs to be in opposite winding order)
	holeCoords := holeRings[0]
	// Reverse to make it a hole (interior ring)
	reversedHole := make([][2]float64, len(holeCoords))
	for i, c := range holeCoords {
		reversedHole[len(holeCoords)-1-i] = c
	}
	rings = append(rings, reversedHole)

	return createPolygonGeometry(rings)
}

// clipPolygonByExterior clips g1 to keep only the parts outside g2.
// This is a simplified implementation.
func clipPolygonByExterior(g1, g2 *Geometry) (*Geometry, error) {
	rings1, err := g1.AllCoordinates()
	if err != nil {
		return nil, err
	}
	rings2, err := g2.AllCoordinates()
	if err != nil {
		return nil, err
	}

	if len(rings1) == 0 || len(rings2) == 0 {
		return g1, nil
	}

	// Get vertices of g1 that are outside g2
	exterior := rings1[0]
	clip := rings2[0]

	// Build polygon from vertices of g1 that are outside g2
	// This is a simplified approach - full polygon difference is complex
	outsideVertices := make([][2]float64, 0)
	for _, v := range exterior {
		pt, _ := createPointGeometry(v[0], v[1])
		inside, _ := Contains(g2, pt)
		if !inside {
			outsideVertices = append(outsideVertices, v)
		}
	}

	// Add intersection points
	for i := 0; i < len(exterior)-1; i++ {
		for j := 0; j < len(clip)-1; j++ {
			if ix, iy, ok := lineSegmentIntersection(
				exterior[i][0], exterior[i][1], exterior[i+1][0], exterior[i+1][1],
				clip[j][0], clip[j][1], clip[j+1][0], clip[j+1][1],
			); ok {
				outsideVertices = append(outsideVertices, [2]float64{ix, iy})
			}
		}
	}

	if len(outsideVertices) < 3 {
		return nil, nil // Degenerate result
	}

	// Use convex hull of outside vertices as approximation
	hull := convexHull(outsideVertices)
	if len(hull) < 3 {
		return nil, nil
	}

	hull = append(hull, hull[0])
	return createPolygonGeometry([][][2]float64{hull})
}

// createMultiPointFromGeometries creates a MULTIPOINT from point geometries.
func createMultiPointFromGeometries(points ...*Geometry) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryMultiPoint))
	binary.Write(buf, binary.LittleEndian, uint32(len(points)))

	for _, pt := range points {
		coords, err := pt.Coordinates()
		if err != nil {
			return nil, err
		}
		if len(coords) == 0 {
			continue
		}
		// Write point as WKB
		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryPoint))
		binary.Write(buf, binary.LittleEndian, coords[0][0])
		binary.Write(buf, binary.LittleEndian, coords[0][1])
	}

	return &Geometry{Type: GeometryMultiPoint, Data: buf.Bytes()}, nil
}

// createMultiPolygonFromGeometries creates a MULTIPOLYGON from polygon geometries.
func createMultiPolygonFromGeometries(polys ...*Geometry) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryMultiPolygon))
	binary.Write(buf, binary.LittleEndian, uint32(len(polys)))

	for _, poly := range polys {
		if poly.Type != GeometryPolygon {
			continue
		}
		// Write polygon WKB data (skip the header of original, add new header)
		rings, err := poly.AllCoordinates()
		if err != nil {
			return nil, err
		}

		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryPolygon))
		binary.Write(buf, binary.LittleEndian, uint32(len(rings)))
		for _, ring := range rings {
			binary.Write(buf, binary.LittleEndian, uint32(len(ring)))
			for _, c := range ring {
				binary.Write(buf, binary.LittleEndian, c[0])
				binary.Write(buf, binary.LittleEndian, c[1])
			}
		}
	}

	return &Geometry{Type: GeometryMultiPolygon, Data: buf.Bytes()}, nil
}

// createGeometryCollection creates a GEOMETRYCOLLECTION from geometries.
func createGeometryCollection(geoms ...*Geometry) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryCollection))
	binary.Write(buf, binary.LittleEndian, uint32(len(geoms)))

	for _, g := range geoms {
		// Write each geometry's WKB data
		buf.Write(g.Data)
	}

	return &Geometry{Type: GeometryCollection, Data: buf.Bytes()}, nil
}
