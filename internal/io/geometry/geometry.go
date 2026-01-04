// Package geometry provides geometry type support for spatial data.
package geometry

import (
	"encoding/binary"
	"fmt"
)

// GeometryType represents the type of geometry.
type GeometryType uint8

// Geometry type constants matching OGC Simple Features.
const (
	GeometryPoint           GeometryType = 1
	GeometryLineString      GeometryType = 2
	GeometryPolygon         GeometryType = 3
	GeometryMultiPoint      GeometryType = 4
	GeometryMultiLineString GeometryType = 5
	GeometryMultiPolygon    GeometryType = 6
	GeometryCollection      GeometryType = 7
)

// Geometry represents a spatial geometry object.
type Geometry struct {
	Type GeometryType
	Data []byte // WKB encoding
	Srid int32  // Spatial Reference ID
}

// ParseWKB parses Well-Known Binary data into a Geometry.
func ParseWKB(data []byte) (*Geometry, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("WKB data too short: need at least 5 bytes, got %d", len(data))
	}

	// First byte is byte order (0 = big endian, 1 = little endian)
	var order binary.ByteOrder
	if data[0] == 0 {
		order = binary.BigEndian
	} else {
		order = binary.LittleEndian
	}

	// Next 4 bytes are geometry type
	geomType := order.Uint32(data[1:5])

	return &Geometry{
		Type: GeometryType(geomType & 0xFF), // Lower 8 bits for base type
		Data: data,
		Srid: 0, // SRID can be extracted if present in extended WKB
	}, nil
}

// ParseWKT parses Well-Known Text into a Geometry.
// This is a simplified implementation that creates POINT geometries.
func ParseWKT(s string) (*Geometry, error) {
	// For now, return an error - full WKT parsing is complex
	// In a real implementation, you'd parse "POINT(x y)" etc.
	return nil, fmt.Errorf("WKT parsing not yet implemented: %s", s)
}

// String returns a string representation of the geometry type.
func (g *Geometry) String() string {
	typeNames := map[GeometryType]string{
		GeometryPoint:           "POINT",
		GeometryLineString:      "LINESTRING",
		GeometryPolygon:         "POLYGON",
		GeometryMultiPoint:      "MULTIPOINT",
		GeometryMultiLineString: "MULTILINESTRING",
		GeometryMultiPolygon:    "MULTIPOLYGON",
		GeometryCollection:      "GEOMETRYCOLLECTION",
	}
	if name, ok := typeNames[g.Type]; ok {
		return name
	}
	return fmt.Sprintf("GEOMETRY(%d)", g.Type)
}

// WKB returns the WKB bytes of the geometry.
func (g *Geometry) WKB() []byte {
	return g.Data
}
