// Package geometry provides geometry type support for spatial data.
package geometry

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// ParseWKT parses Well-Known Text into a Geometry.
// Supports: POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION
func ParseWKT(s string) (*Geometry, error) {
	parser := &wktParser{input: strings.TrimSpace(s)}
	return parser.parse()
}

// FormatWKT converts a Geometry to WKT string.
func FormatWKT(g *Geometry) (string, error) {
	if g == nil {
		return "", fmt.Errorf("cannot format nil geometry")
	}
	return formatWKT(g)
}

// wktParser handles parsing of WKT strings.
type wktParser struct {
	input string
	pos   int
}

func (p *wktParser) parse() (*Geometry, error) {
	p.skipWhitespace()

	typeName, err := p.readTypeName()
	if err != nil {
		return nil, err
	}

	p.skipWhitespace()

	// Check for EMPTY
	if p.hasPrefix("EMPTY") {
		p.advance(5)
		return p.createEmptyGeometry(typeName)
	}

	switch strings.ToUpper(typeName) {
	case "POINT":
		return p.parsePoint()
	case "LINESTRING":
		return p.parseLineString()
	case "POLYGON":
		return p.parsePolygon()
	case "MULTIPOINT":
		return p.parseMultiPoint()
	case "MULTILINESTRING":
		return p.parseMultiLineString()
	case "MULTIPOLYGON":
		return p.parseMultiPolygon()
	case "GEOMETRYCOLLECTION":
		return p.parseGeometryCollection()
	default:
		return nil, fmt.Errorf("unknown geometry type: %s", typeName)
	}
}

func (p *wktParser) readTypeName() (string, error) {
	start := p.pos
	for p.pos < len(p.input) && (unicode.IsLetter(rune(p.input[p.pos])) || p.input[p.pos] == '_') {
		p.pos++
	}
	if start == p.pos {
		return "", fmt.Errorf("expected geometry type name at position %d", p.pos)
	}
	return p.input[start:p.pos], nil
}

func (p *wktParser) skipWhitespace() {
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}

func (p *wktParser) hasPrefix(prefix string) bool {
	return strings.HasPrefix(strings.ToUpper(p.input[p.pos:]), strings.ToUpper(prefix))
}

func (p *wktParser) advance(n int) {
	p.pos += n
}

func (p *wktParser) expect(ch byte) error {
	p.skipWhitespace()
	if p.pos >= len(p.input) || p.input[p.pos] != ch {
		if p.pos >= len(p.input) {
			return fmt.Errorf("expected '%c' at position %d, but reached end of input", ch, p.pos)
		}
		return fmt.Errorf("expected '%c' at position %d, got '%c'", ch, p.pos, p.input[p.pos])
	}
	p.pos++
	return nil
}

func (p *wktParser) peek() byte {
	p.skipWhitespace()
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *wktParser) readNumber() (float64, error) {
	p.skipWhitespace()
	start := p.pos

	// Handle optional sign
	if p.pos < len(p.input) && (p.input[p.pos] == '-' || p.input[p.pos] == '+') {
		p.pos++
	}

	// Read digits before decimal
	for p.pos < len(p.input) && (unicode.IsDigit(rune(p.input[p.pos]))) {
		p.pos++
	}

	// Read decimal point and digits after
	if p.pos < len(p.input) && p.input[p.pos] == '.' {
		p.pos++
		for p.pos < len(p.input) && unicode.IsDigit(rune(p.input[p.pos])) {
			p.pos++
		}
	}

	// Read exponent
	if p.pos < len(p.input) && (p.input[p.pos] == 'e' || p.input[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.input) && (p.input[p.pos] == '-' || p.input[p.pos] == '+') {
			p.pos++
		}
		for p.pos < len(p.input) && unicode.IsDigit(rune(p.input[p.pos])) {
			p.pos++
		}
	}

	if start == p.pos {
		return 0, fmt.Errorf("expected number at position %d", p.pos)
	}

	return strconv.ParseFloat(p.input[start:p.pos], 64)
}

func (p *wktParser) readCoordinate() ([]float64, error) {
	coords := make([]float64, 0, 3) // Usually 2 or 3 coordinates

	// Read X
	x, err := p.readNumber()
	if err != nil {
		return nil, err
	}
	coords = append(coords, x)

	// Read Y
	y, err := p.readNumber()
	if err != nil {
		return nil, err
	}
	coords = append(coords, y)

	// Check for Z (optional third coordinate)
	p.skipWhitespace()
	if p.pos < len(p.input) {
		ch := p.input[p.pos]
		// If next char is a digit, minus sign, or plus sign, read Z
		if ch == '-' || ch == '+' || unicode.IsDigit(rune(ch)) {
			// Check if this looks like a number (not just the start of a new point)
			// Peek ahead to see if after this potential number there's a comma or paren
			savedPos := p.pos
			z, err := p.readNumber()
			if err == nil {
				p.skipWhitespace()
				// Accept Z only if followed by comma, closing paren, or end
				if p.pos >= len(p.input) || p.input[p.pos] == ',' || p.input[p.pos] == ')' {
					coords = append(coords, z)
				} else {
					// Not a Z coordinate, restore position
					p.pos = savedPos
				}
			} else {
				p.pos = savedPos
			}
		}
	}

	return coords, nil
}

func (p *wktParser) readCoordinateSequence() ([][]float64, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var coords [][]float64
	for {
		coord, err := p.readCoordinate()
		if err != nil {
			return nil, err
		}
		coords = append(coords, coord)

		p.skipWhitespace()
		if p.peek() == ')' {
			p.pos++
			break
		}
		if err := p.expect(','); err != nil {
			return nil, err
		}
	}

	return coords, nil
}

func (p *wktParser) parsePoint() (*Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	coord, err := p.readCoordinate()
	if err != nil {
		return nil, err
	}

	if err := p.expect(')'); err != nil {
		return nil, err
	}

	return p.buildPointWKB(coord)
}

func (p *wktParser) parseLineString() (*Geometry, error) {
	coords, err := p.readCoordinateSequence()
	if err != nil {
		return nil, err
	}

	if len(coords) < 2 {
		return nil, fmt.Errorf("LINESTRING requires at least 2 points, got %d", len(coords))
	}

	return p.buildLineStringWKB(coords)
}

func (p *wktParser) parsePolygon() (*Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var rings [][][]float64
	for {
		ring, err := p.readCoordinateSequence()
		if err != nil {
			return nil, err
		}
		rings = append(rings, ring)

		p.skipWhitespace()
		if p.peek() == ')' {
			p.pos++
			break
		}
		if err := p.expect(','); err != nil {
			return nil, err
		}
	}

	if len(rings) == 0 {
		return nil, fmt.Errorf("POLYGON requires at least one ring")
	}

	return p.buildPolygonWKB(rings)
}

func (p *wktParser) parseMultiPoint() (*Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var points [][]float64
	for {
		p.skipWhitespace()
		// MultiPoint can have points with or without parens: MULTIPOINT((0 0), (1 1)) or MULTIPOINT(0 0, 1 1)
		if p.peek() == '(' {
			p.pos++
			coord, err := p.readCoordinate()
			if err != nil {
				return nil, err
			}
			if err := p.expect(')'); err != nil {
				return nil, err
			}
			points = append(points, coord)
		} else {
			coord, err := p.readCoordinate()
			if err != nil {
				return nil, err
			}
			points = append(points, coord)
		}

		p.skipWhitespace()
		if p.peek() == ')' {
			p.pos++
			break
		}
		if err := p.expect(','); err != nil {
			return nil, err
		}
	}

	return p.buildMultiPointWKB(points)
}

func (p *wktParser) parseMultiLineString() (*Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var lineStrings [][][]float64
	for {
		coords, err := p.readCoordinateSequence()
		if err != nil {
			return nil, err
		}
		lineStrings = append(lineStrings, coords)

		p.skipWhitespace()
		if p.peek() == ')' {
			p.pos++
			break
		}
		if err := p.expect(','); err != nil {
			return nil, err
		}
	}

	return p.buildMultiLineStringWKB(lineStrings)
}

func (p *wktParser) parseMultiPolygon() (*Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var polygons [][][][]float64
	for {
		// Each polygon is wrapped in parens
		if err := p.expect('('); err != nil {
			return nil, err
		}

		var rings [][][]float64
		for {
			ring, err := p.readCoordinateSequence()
			if err != nil {
				return nil, err
			}
			rings = append(rings, ring)

			p.skipWhitespace()
			if p.peek() == ')' {
				p.pos++
				break
			}
			if err := p.expect(','); err != nil {
				return nil, err
			}
		}
		polygons = append(polygons, rings)

		p.skipWhitespace()
		if p.peek() == ')' {
			p.pos++
			break
		}
		if err := p.expect(','); err != nil {
			return nil, err
		}
	}

	return p.buildMultiPolygonWKB(polygons)
}

func (p *wktParser) parseGeometryCollection() (*Geometry, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var geometries []*Geometry
	for {
		p.skipWhitespace()

		// Check for empty collection
		if p.peek() == ')' {
			p.pos++
			break
		}

		// Parse nested geometry
		typeName, err := p.readTypeName()
		if err != nil {
			return nil, err
		}

		p.skipWhitespace()

		var geom *Geometry
		upperType := strings.ToUpper(typeName)

		if p.hasPrefix("EMPTY") {
			p.advance(5)
			geom, err = p.createEmptyGeometry(upperType)
		} else {
			switch upperType {
			case "POINT":
				geom, err = p.parsePoint()
			case "LINESTRING":
				geom, err = p.parseLineString()
			case "POLYGON":
				geom, err = p.parsePolygon()
			case "MULTIPOINT":
				geom, err = p.parseMultiPoint()
			case "MULTILINESTRING":
				geom, err = p.parseMultiLineString()
			case "MULTIPOLYGON":
				geom, err = p.parseMultiPolygon()
			case "GEOMETRYCOLLECTION":
				geom, err = p.parseGeometryCollection()
			default:
				return nil, fmt.Errorf("unknown geometry type in collection: %s", typeName)
			}
		}

		if err != nil {
			return nil, err
		}
		geometries = append(geometries, geom)

		p.skipWhitespace()
		if p.peek() == ')' {
			p.pos++
			break
		}
		if err := p.expect(','); err != nil {
			return nil, err
		}
	}

	return p.buildGeometryCollectionWKB(geometries)
}

func (p *wktParser) createEmptyGeometry(typeName string) (*Geometry, error) {
	// For empty geometries, we create a minimal valid WKB structure
	switch strings.ToUpper(typeName) {
	case "POINT":
		// Empty point with NaN coordinates
		buf := new(bytes.Buffer)
		buf.WriteByte(0x01) // Little endian
		binary.Write(buf, binary.LittleEndian, uint32(GeometryPoint))
		binary.Write(buf, binary.LittleEndian, math.NaN())
		binary.Write(buf, binary.LittleEndian, math.NaN())
		return &Geometry{Type: GeometryPoint, Data: buf.Bytes()}, nil
	case "LINESTRING":
		buf := new(bytes.Buffer)
		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryLineString))
		binary.Write(buf, binary.LittleEndian, uint32(0)) // 0 points
		return &Geometry{Type: GeometryLineString, Data: buf.Bytes()}, nil
	case "POLYGON":
		buf := new(bytes.Buffer)
		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryPolygon))
		binary.Write(buf, binary.LittleEndian, uint32(0)) // 0 rings
		return &Geometry{Type: GeometryPolygon, Data: buf.Bytes()}, nil
	case "MULTIPOINT":
		buf := new(bytes.Buffer)
		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryMultiPoint))
		binary.Write(buf, binary.LittleEndian, uint32(0)) // 0 points
		return &Geometry{Type: GeometryMultiPoint, Data: buf.Bytes()}, nil
	case "MULTILINESTRING":
		buf := new(bytes.Buffer)
		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryMultiLineString))
		binary.Write(buf, binary.LittleEndian, uint32(0)) // 0 linestrings
		return &Geometry{Type: GeometryMultiLineString, Data: buf.Bytes()}, nil
	case "MULTIPOLYGON":
		buf := new(bytes.Buffer)
		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryMultiPolygon))
		binary.Write(buf, binary.LittleEndian, uint32(0)) // 0 polygons
		return &Geometry{Type: GeometryMultiPolygon, Data: buf.Bytes()}, nil
	case "GEOMETRYCOLLECTION":
		buf := new(bytes.Buffer)
		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryCollection))
		binary.Write(buf, binary.LittleEndian, uint32(0)) // 0 geometries
		return &Geometry{Type: GeometryCollection, Data: buf.Bytes()}, nil
	default:
		return nil, fmt.Errorf("unknown geometry type: %s", typeName)
	}
}

// WKB building functions

func (p *wktParser) buildPointWKB(coord []float64) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryPoint))
	binary.Write(buf, binary.LittleEndian, coord[0]) // X
	binary.Write(buf, binary.LittleEndian, coord[1]) // Y

	return &Geometry{Type: GeometryPoint, Data: buf.Bytes()}, nil
}

func (p *wktParser) buildLineStringWKB(coords [][]float64) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryLineString))
	binary.Write(buf, binary.LittleEndian, uint32(len(coords)))
	for _, coord := range coords {
		binary.Write(buf, binary.LittleEndian, coord[0])
		binary.Write(buf, binary.LittleEndian, coord[1])
	}

	return &Geometry{Type: GeometryLineString, Data: buf.Bytes()}, nil
}

func (p *wktParser) buildPolygonWKB(rings [][][]float64) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryPolygon))
	binary.Write(buf, binary.LittleEndian, uint32(len(rings)))
	for _, ring := range rings {
		binary.Write(buf, binary.LittleEndian, uint32(len(ring)))
		for _, coord := range ring {
			binary.Write(buf, binary.LittleEndian, coord[0])
			binary.Write(buf, binary.LittleEndian, coord[1])
		}
	}

	return &Geometry{Type: GeometryPolygon, Data: buf.Bytes()}, nil
}

func (p *wktParser) buildMultiPointWKB(points [][]float64) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryMultiPoint))
	binary.Write(buf, binary.LittleEndian, uint32(len(points)))
	for _, coord := range points {
		// Each point is a full WKB Point
		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryPoint))
		binary.Write(buf, binary.LittleEndian, coord[0])
		binary.Write(buf, binary.LittleEndian, coord[1])
	}

	return &Geometry{Type: GeometryMultiPoint, Data: buf.Bytes()}, nil
}

func (p *wktParser) buildMultiLineStringWKB(lineStrings [][][]float64) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryMultiLineString))
	binary.Write(buf, binary.LittleEndian, uint32(len(lineStrings)))
	for _, coords := range lineStrings {
		// Each linestring is a full WKB LineString
		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryLineString))
		binary.Write(buf, binary.LittleEndian, uint32(len(coords)))
		for _, coord := range coords {
			binary.Write(buf, binary.LittleEndian, coord[0])
			binary.Write(buf, binary.LittleEndian, coord[1])
		}
	}

	return &Geometry{Type: GeometryMultiLineString, Data: buf.Bytes()}, nil
}

func (p *wktParser) buildMultiPolygonWKB(polygons [][][][]float64) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryMultiPolygon))
	binary.Write(buf, binary.LittleEndian, uint32(len(polygons)))
	for _, rings := range polygons {
		// Each polygon is a full WKB Polygon
		buf.WriteByte(0x01)
		binary.Write(buf, binary.LittleEndian, uint32(GeometryPolygon))
		binary.Write(buf, binary.LittleEndian, uint32(len(rings)))
		for _, ring := range rings {
			binary.Write(buf, binary.LittleEndian, uint32(len(ring)))
			for _, coord := range ring {
				binary.Write(buf, binary.LittleEndian, coord[0])
				binary.Write(buf, binary.LittleEndian, coord[1])
			}
		}
	}

	return &Geometry{Type: GeometryMultiPolygon, Data: buf.Bytes()}, nil
}

func (p *wktParser) buildGeometryCollectionWKB(geometries []*Geometry) (*Geometry, error) {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // Little endian
	binary.Write(buf, binary.LittleEndian, uint32(GeometryCollection))
	binary.Write(buf, binary.LittleEndian, uint32(len(geometries)))
	for _, geom := range geometries {
		buf.Write(geom.Data)
	}

	return &Geometry{Type: GeometryCollection, Data: buf.Bytes()}, nil
}

// formatWKT converts a Geometry to its WKT representation.
func formatWKT(g *Geometry) (string, error) {
	if len(g.Data) < 5 {
		return "", fmt.Errorf("invalid WKB data")
	}

	var order binary.ByteOrder
	if g.Data[0] == 0 {
		order = binary.BigEndian
	} else {
		order = binary.LittleEndian
	}

	reader := bytes.NewReader(g.Data[5:]) // Skip byte order and type

	switch g.Type {
	case GeometryPoint:
		return formatPointWKT(reader, order)
	case GeometryLineString:
		return formatLineStringWKT(reader, order)
	case GeometryPolygon:
		return formatPolygonWKT(reader, order)
	case GeometryMultiPoint:
		return formatMultiPointWKT(reader, order)
	case GeometryMultiLineString:
		return formatMultiLineStringWKT(reader, order)
	case GeometryMultiPolygon:
		return formatMultiPolygonWKT(reader, order)
	case GeometryCollection:
		return formatGeometryCollectionWKT(g.Data, order)
	default:
		return "", fmt.Errorf("unsupported geometry type: %d", g.Type)
	}
}

func formatPointWKT(r *bytes.Reader, order binary.ByteOrder) (string, error) {
	var x, y float64
	if err := binary.Read(r, order, &x); err != nil {
		return "", err
	}
	if err := binary.Read(r, order, &y); err != nil {
		return "", err
	}

	if math.IsNaN(x) && math.IsNaN(y) {
		return "POINT EMPTY", nil
	}

	return fmt.Sprintf("POINT(%s %s)", formatFloat(x), formatFloat(y)), nil
}

func formatLineStringWKT(r *bytes.Reader, order binary.ByteOrder) (string, error) {
	var numPoints uint32
	if err := binary.Read(r, order, &numPoints); err != nil {
		return "", err
	}

	if numPoints == 0 {
		return "LINESTRING EMPTY", nil
	}

	coords := make([]string, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		var x, y float64
		if err := binary.Read(r, order, &x); err != nil {
			return "", err
		}
		if err := binary.Read(r, order, &y); err != nil {
			return "", err
		}
		coords[i] = fmt.Sprintf("%s %s", formatFloat(x), formatFloat(y))
	}

	return fmt.Sprintf("LINESTRING(%s)", strings.Join(coords, ", ")), nil
}

func formatPolygonWKT(r *bytes.Reader, order binary.ByteOrder) (string, error) {
	var numRings uint32
	if err := binary.Read(r, order, &numRings); err != nil {
		return "", err
	}

	if numRings == 0 {
		return "POLYGON EMPTY", nil
	}

	rings := make([]string, numRings)
	for i := uint32(0); i < numRings; i++ {
		var numPoints uint32
		if err := binary.Read(r, order, &numPoints); err != nil {
			return "", err
		}

		coords := make([]string, numPoints)
		for j := uint32(0); j < numPoints; j++ {
			var x, y float64
			if err := binary.Read(r, order, &x); err != nil {
				return "", err
			}
			if err := binary.Read(r, order, &y); err != nil {
				return "", err
			}
			coords[j] = fmt.Sprintf("%s %s", formatFloat(x), formatFloat(y))
		}
		rings[i] = "(" + strings.Join(coords, ", ") + ")"
	}

	return fmt.Sprintf("POLYGON(%s)", strings.Join(rings, ", ")), nil
}

func formatMultiPointWKT(r *bytes.Reader, order binary.ByteOrder) (string, error) {
	var numPoints uint32
	if err := binary.Read(r, order, &numPoints); err != nil {
		return "", err
	}

	if numPoints == 0 {
		return "MULTIPOINT EMPTY", nil
	}

	points := make([]string, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		// Each point is a full WKB Point - skip byte order and type
		if _, err := r.ReadByte(); err != nil {
			return "", err
		}
		var pt uint32 // type
		if err := binary.Read(r, order, &pt); err != nil {
			return "", err
		}

		var x, y float64
		if err := binary.Read(r, order, &x); err != nil {
			return "", err
		}
		if err := binary.Read(r, order, &y); err != nil {
			return "", err
		}
		points[i] = fmt.Sprintf("(%s %s)", formatFloat(x), formatFloat(y))
	}

	return fmt.Sprintf("MULTIPOINT(%s)", strings.Join(points, ", ")), nil
}

func formatMultiLineStringWKT(r *bytes.Reader, order binary.ByteOrder) (string, error) {
	var numLineStrings uint32
	if err := binary.Read(r, order, &numLineStrings); err != nil {
		return "", err
	}

	if numLineStrings == 0 {
		return "MULTILINESTRING EMPTY", nil
	}

	lineStrings := make([]string, numLineStrings)
	for i := uint32(0); i < numLineStrings; i++ {
		// Each linestring is a full WKB LineString - skip byte order and type
		if _, err := r.ReadByte(); err != nil {
			return "", err
		}
		var lt uint32
		if err := binary.Read(r, order, &lt); err != nil {
			return "", err
		}

		var numPoints uint32
		if err := binary.Read(r, order, &numPoints); err != nil {
			return "", err
		}

		coords := make([]string, numPoints)
		for j := uint32(0); j < numPoints; j++ {
			var x, y float64
			if err := binary.Read(r, order, &x); err != nil {
				return "", err
			}
			if err := binary.Read(r, order, &y); err != nil {
				return "", err
			}
			coords[j] = fmt.Sprintf("%s %s", formatFloat(x), formatFloat(y))
		}
		lineStrings[i] = "(" + strings.Join(coords, ", ") + ")"
	}

	return fmt.Sprintf("MULTILINESTRING(%s)", strings.Join(lineStrings, ", ")), nil
}

func formatMultiPolygonWKT(r *bytes.Reader, order binary.ByteOrder) (string, error) {
	var numPolygons uint32
	if err := binary.Read(r, order, &numPolygons); err != nil {
		return "", err
	}

	if numPolygons == 0 {
		return "MULTIPOLYGON EMPTY", nil
	}

	polygons := make([]string, numPolygons)
	for i := uint32(0); i < numPolygons; i++ {
		// Each polygon is a full WKB Polygon - skip byte order and type
		if _, err := r.ReadByte(); err != nil {
			return "", err
		}
		var pt uint32
		if err := binary.Read(r, order, &pt); err != nil {
			return "", err
		}

		var numRings uint32
		if err := binary.Read(r, order, &numRings); err != nil {
			return "", err
		}

		rings := make([]string, numRings)
		for j := uint32(0); j < numRings; j++ {
			var numPoints uint32
			if err := binary.Read(r, order, &numPoints); err != nil {
				return "", err
			}

			coords := make([]string, numPoints)
			for k := uint32(0); k < numPoints; k++ {
				var x, y float64
				if err := binary.Read(r, order, &x); err != nil {
					return "", err
				}
				if err := binary.Read(r, order, &y); err != nil {
					return "", err
				}
				coords[k] = fmt.Sprintf("%s %s", formatFloat(x), formatFloat(y))
			}
			rings[j] = "(" + strings.Join(coords, ", ") + ")"
		}
		polygons[i] = "(" + strings.Join(rings, ", ") + ")"
	}

	return fmt.Sprintf("MULTIPOLYGON(%s)", strings.Join(polygons, ", ")), nil
}

func formatGeometryCollectionWKT(data []byte, order binary.ByteOrder) (string, error) {
	if len(data) < 9 {
		return "GEOMETRYCOLLECTION EMPTY", nil
	}

	r := bytes.NewReader(data[5:])
	var numGeometries uint32
	if err := binary.Read(r, order, &numGeometries); err != nil {
		return "", err
	}

	if numGeometries == 0 {
		return "GEOMETRYCOLLECTION EMPTY", nil
	}

	geometries := make([]string, numGeometries)
	for i := uint32(0); i < numGeometries; i++ {
		// Read the geometry header
		byteOrder, err := r.ReadByte()
		if err != nil {
			return "", err
		}

		var geomOrder binary.ByteOrder
		if byteOrder == 0 {
			geomOrder = binary.BigEndian
		} else {
			geomOrder = binary.LittleEndian
		}

		var geomType uint32
		if err := binary.Read(r, geomOrder, &geomType); err != nil {
			return "", err
		}

		gType := GeometryType(geomType & 0xFF)
		var wkt string

		switch gType {
		case GeometryPoint:
			wkt, err = formatPointWKT(r, geomOrder)
		case GeometryLineString:
			wkt, err = formatLineStringWKT(r, geomOrder)
		case GeometryPolygon:
			wkt, err = formatPolygonWKT(r, geomOrder)
		case GeometryMultiPoint:
			wkt, err = formatMultiPointWKT(r, geomOrder)
		case GeometryMultiLineString:
			wkt, err = formatMultiLineStringWKT(r, geomOrder)
		case GeometryMultiPolygon:
			wkt, err = formatMultiPolygonWKT(r, geomOrder)
		default:
			return "", fmt.Errorf("unsupported geometry type in collection: %d", gType)
		}

		if err != nil {
			return "", err
		}
		geometries[i] = wkt
	}

	return fmt.Sprintf("GEOMETRYCOLLECTION(%s)", strings.Join(geometries, ", ")), nil
}

// formatFloat formats a float64 for WKT output, removing unnecessary trailing zeros.
func formatFloat(f float64) string {
	// Use enough precision to avoid data loss
	s := strconv.FormatFloat(f, 'f', -1, 64)
	return s
}
