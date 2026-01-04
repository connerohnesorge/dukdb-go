// Package geometry provides geometry type support for spatial data.
package geometry

import (
	"encoding/binary"
	"fmt"
	"math"
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

// ParseWKT is defined in wkt.go

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

// X returns the X coordinate of a POINT geometry.
func (g *Geometry) X() (float64, error) {
	if g.Type != GeometryPoint {
		return 0, fmt.Errorf("X() is only valid for POINT geometry, got %s", g.String())
	}
	if len(g.Data) < 21 {
		return 0, fmt.Errorf("POINT WKB data too short")
	}

	var order binary.ByteOrder
	if g.Data[0] == 0 {
		order = binary.BigEndian
	} else {
		order = binary.LittleEndian
	}

	x := order.Uint64(g.Data[5:13])
	return math.Float64frombits(x), nil
}

// Y returns the Y coordinate of a POINT geometry.
func (g *Geometry) Y() (float64, error) {
	if g.Type != GeometryPoint {
		return 0, fmt.Errorf("Y() is only valid for POINT geometry, got %s", g.String())
	}
	if len(g.Data) < 21 {
		return 0, fmt.Errorf("POINT WKB data too short")
	}

	var order binary.ByteOrder
	if g.Data[0] == 0 {
		order = binary.BigEndian
	} else {
		order = binary.LittleEndian
	}

	y := order.Uint64(g.Data[13:21])
	return math.Float64frombits(y), nil
}

// Z returns the Z coordinate of a POINT geometry (if 3D).
// Returns an error if the geometry is not a 3D POINT.
func (g *Geometry) Z() (float64, error) {
	if g.Type != GeometryPoint {
		return 0, fmt.Errorf("Z() is only valid for POINT geometry, got %s", g.String())
	}
	if len(g.Data) < 29 {
		return 0, fmt.Errorf("POINT does not have Z coordinate (not 3D)")
	}

	var order binary.ByteOrder
	if g.Data[0] == 0 {
		order = binary.BigEndian
	} else {
		order = binary.LittleEndian
	}

	z := order.Uint64(g.Data[21:29])
	return math.Float64frombits(z), nil
}

// GetSRID returns the Spatial Reference ID.
func (g *Geometry) GetSRID() int32 {
	return g.Srid
}

// WithSRID returns a new geometry with the specified SRID.
func (g *Geometry) WithSRID(srid int32) *Geometry {
	return &Geometry{
		Type: g.Type,
		Data: g.Data,
		Srid: srid,
	}
}
