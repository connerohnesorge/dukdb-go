package dukdb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/dukdb/dukdb-go/internal/io/geometry"
)

// BenchmarkJSONParsing benchmarks JSON parsing operations.
// Tests small, medium, and large JSON documents to measure parsing performance.
func BenchmarkJSONParsing(b *testing.B) {
	// Small JSON (~50 bytes)
	smallJSON := `{"name":"Alice","age":30}`

	// Medium JSON (~500 bytes)
	mediumJSON := `{
		"users":[
			{"id":1,"name":"Alice","email":"alice@example.com","active":true},
			{"id":2,"name":"Bob","email":"bob@example.com","active":false},
			{"id":3,"name":"Charlie","email":"charlie@example.com","active":true}
		],
		"count":3,
		"page":1,
		"total_pages":10
	}`

	// Large JSON (~5KB)
	var users []string
	for i := range 100 {
		users = append(users, fmt.Sprintf(
			`{"id":%d,"name":"User%d","email":"user%d@example.com","active":%t,"score":%d}`,
			i, i, i, i%2 == 0, i*10,
		))
	}
	largeJSON := fmt.Sprintf(
		`{"users":[%s],"metadata":{"total":100,"generated":"2024-01-01T00:00:00Z"}}`,
		strings.Join(users, ","),
	)

	b.Run("SmallJSON/Parse", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			var result any
			_ = json.Unmarshal([]byte(smallJSON), &result)
		}
		b.ReportMetric(float64(len(smallJSON)), "bytes/op")
	})

	b.Run("SmallJSON/VectorSetGet", func(b *testing.B) {
		info, _ := NewJSONInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, smallJSON)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("MediumJSON/Parse", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			var result any
			_ = json.Unmarshal([]byte(mediumJSON), &result)
		}
		b.ReportMetric(float64(len(mediumJSON)), "bytes/op")
	})

	b.Run("MediumJSON/VectorSetGet", func(b *testing.B) {
		info, _ := NewJSONInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, mediumJSON)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("LargeJSON/Parse", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			var result any
			_ = json.Unmarshal([]byte(largeJSON), &result)
		}
		b.ReportMetric(float64(len(largeJSON)), "bytes/op")
	})

	b.Run("LargeJSON/VectorSetGet", func(b *testing.B) {
		info, _ := NewJSONInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, largeJSON)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("BulkJSON/VectorOps", func(b *testing.B) {
		info, _ := NewJSONInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		testData := []string{smallJSON, mediumJSON, largeJSON}

		b.ResetTimer()
		for range b.N {
			for row := range 1000 {
				jsonData := testData[row%len(testData)]
				_ = vec.setFn(vec, row, jsonData)
			}
			for row := range 1000 {
				_ = vec.getFn(vec, row)
			}
		}

		opsPerIter := float64(2000) // 1000 sets + 1000 gets
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/value")
	})
}

// createPointWKB creates WKB bytes for a POINT geometry with given coordinates.
func createPointWKB(x, y float64) []byte {
	wkb := make([]byte, 21)
	wkb[0] = 0x01                              // Little endian
	binary.LittleEndian.PutUint32(wkb[1:5], 1) // Point type
	binary.LittleEndian.PutUint64(wkb[5:13], uint64FromFloat64(x))
	binary.LittleEndian.PutUint64(wkb[13:21], uint64FromFloat64(y))
	return wkb
}

// createLineStringWKB creates WKB bytes for a LINESTRING geometry.
func createLineStringWKB(points [][2]float64) []byte {
	// Header (5 bytes) + num points (4 bytes) + points (16 bytes each)
	wkb := make([]byte, 9+len(points)*16)
	wkb[0] = 0x01                              // Little endian
	binary.LittleEndian.PutUint32(wkb[1:5], 2) // LineString type
	binary.LittleEndian.PutUint32(wkb[5:9], uint32(len(points)))

	offset := 9
	for _, pt := range points {
		binary.LittleEndian.PutUint64(wkb[offset:offset+8], uint64FromFloat64(pt[0]))
		binary.LittleEndian.PutUint64(wkb[offset+8:offset+16], uint64FromFloat64(pt[1]))
		offset += 16
	}
	return wkb
}

// createPolygonWKB creates WKB bytes for a POLYGON geometry with a single ring.
func createPolygonWKB(ring [][2]float64) []byte {
	// Header (5 bytes) + num rings (4 bytes) + num points (4 bytes) + points (16 bytes each)
	wkb := make([]byte, 13+len(ring)*16)
	wkb[0] = 0x01                              // Little endian
	binary.LittleEndian.PutUint32(wkb[1:5], 3) // Polygon type
	binary.LittleEndian.PutUint32(wkb[5:9], 1) // 1 ring
	binary.LittleEndian.PutUint32(wkb[9:13], uint32(len(ring)))

	offset := 13
	for _, pt := range ring {
		binary.LittleEndian.PutUint64(wkb[offset:offset+8], uint64FromFloat64(pt[0]))
		binary.LittleEndian.PutUint64(wkb[offset+8:offset+16], uint64FromFloat64(pt[1]))
		offset += 16
	}
	return wkb
}

// uint64FromFloat64 converts a float64 to its bit representation.
func uint64FromFloat64(f float64) uint64 {
	return math.Float64bits(f)
}

// BenchmarkWKBParsing benchmarks WKB geometry parsing operations.
func BenchmarkWKBParsing(b *testing.B) {
	// POINT geometry (21 bytes)
	pointWKB := createPointWKB(1.0, 2.0)

	// LINESTRING geometry with 10 points (169 bytes)
	linePoints := make([][2]float64, 10)
	for i := range linePoints {
		linePoints[i] = [2]float64{float64(i), float64(i * 2)}
	}
	lineStringWKB := createLineStringWKB(linePoints)

	// POLYGON geometry with 5-point ring (93 bytes)
	polygonRing := [][2]float64{
		{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}, // Closed ring
	}
	polygonWKB := createPolygonWKB(polygonRing)

	b.Run("POINT/Parse", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = geometry.ParseWKB(pointWKB)
		}
		b.ReportMetric(float64(len(pointWKB)), "bytes/op")
	})

	b.Run("POINT/VectorSetGet", func(b *testing.B) {
		info, _ := NewGeometryInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, pointWKB)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("LINESTRING/Parse", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = geometry.ParseWKB(lineStringWKB)
		}
		b.ReportMetric(float64(len(lineStringWKB)), "bytes/op")
	})

	b.Run("LINESTRING/VectorSetGet", func(b *testing.B) {
		info, _ := NewGeometryInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, lineStringWKB)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("POLYGON/Parse", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = geometry.ParseWKB(polygonWKB)
		}
		b.ReportMetric(float64(len(polygonWKB)), "bytes/op")
	})

	b.Run("POLYGON/VectorSetGet", func(b *testing.B) {
		info, _ := NewGeometryInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, polygonWKB)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("BulkGeometry/VectorOps", func(b *testing.B) {
		info, _ := NewGeometryInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		testData := [][]byte{pointWKB, lineStringWKB, polygonWKB}

		b.ResetTimer()
		for range b.N {
			for row := range 1000 {
				wkbData := testData[row%len(testData)]
				_ = vec.setFn(vec, row, wkbData)
			}
			for row := range 1000 {
				_ = vec.getFn(vec, row)
			}
		}

		opsPerIter := float64(2000) // 1000 sets + 1000 gets
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/value")
	})
}

// BenchmarkBignumOperations benchmarks BIGNUM set/get operations.
func BenchmarkBignumOperations(b *testing.B) {
	// Small number (fits in int64)
	smallNum := new(big.Int)
	smallNum.SetString("12345678901234567890", 10)

	// Medium number (requires arbitrary precision)
	mediumNum := new(big.Int)
	mediumNum.SetString("123456789012345678901234567890123456789012345678901234567890", 10)

	// Large number (very large arbitrary precision)
	largeNum := new(big.Int)
	largeNum.SetString(strings.Repeat("9", 200), 10) // 200-digit number

	b.Run("SmallBignum/SetGet", func(b *testing.B) {
		info, _ := NewBignumInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, smallNum)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("MediumBignum/SetGet", func(b *testing.B) {
		info, _ := NewBignumInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, mediumNum)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("LargeBignum/SetGet", func(b *testing.B) {
		info, _ := NewBignumInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, largeNum)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("Bignum/FromString", func(b *testing.B) {
		info, _ := NewBignumInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		numStr := "12345678901234567890123456789012345678901234567890"

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, numStr)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("Bignum/FromInt64", func(b *testing.B) {
		info, _ := NewBignumInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, int64(9223372036854775807))
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("BulkBignum/VectorOps", func(b *testing.B) {
		info, _ := NewBignumInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		testData := []*big.Int{smallNum, mediumNum, largeNum}

		b.ResetTimer()
		for range b.N {
			for row := range 1000 {
				num := testData[row%len(testData)]
				_ = vec.setFn(vec, row, num)
			}
			for row := range 1000 {
				_ = vec.getFn(vec, row)
			}
		}

		opsPerIter := float64(2000) // 1000 sets + 1000 gets
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/value")
	})
}

// BenchmarkJSONvsVARCHAR compares JSON column vs VARCHAR column performance.
func BenchmarkJSONvsVARCHAR(b *testing.B) {
	testJSON := `{"id":1,"name":"Test User","email":"test@example.com","active":true,"score":100}`

	b.Run("VARCHAR/SetGet", func(b *testing.B) {
		info, _ := NewTypeInfo(TYPE_VARCHAR)
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, testJSON)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("JSON/SetGet", func(b *testing.B) {
		info, _ := NewJSONInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			_ = vec.setFn(vec, 0, testJSON)
			_ = vec.getFn(vec, 0)
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/roundtrip")
	})

	b.Run("VARCHAR/BulkOps", func(b *testing.B) {
		info, _ := NewTypeInfo(TYPE_VARCHAR)
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			for row := range 1000 {
				_ = vec.setFn(vec, row, testJSON)
			}
			for row := range 1000 {
				_ = vec.getFn(vec, row)
			}
		}

		opsPerIter := float64(2000)
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/value")
	})

	b.Run("JSON/BulkOps", func(b *testing.B) {
		info, _ := NewJSONInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			for row := range 1000 {
				_ = vec.setFn(vec, row, testJSON)
			}
			for row := range 1000 {
				_ = vec.getFn(vec, row)
			}
		}

		opsPerIter := float64(2000)
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/value")
	})

	b.Run("VARCHAR/SetOnly", func(b *testing.B) {
		info, _ := NewTypeInfo(TYPE_VARCHAR)
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			for row := range 2000 {
				_ = vec.setFn(vec, row%VectorSize, testJSON)
			}
		}

		opsPerIter := float64(2000)
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/set")
	})

	b.Run("JSON/SetOnly", func(b *testing.B) {
		info, _ := NewJSONInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		b.ResetTimer()
		for range b.N {
			for row := range 2000 {
				_ = vec.setFn(vec, row%VectorSize, testJSON)
			}
		}

		opsPerIter := float64(2000)
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/set")
	})
}

// BenchmarkGeometryParsing benchmarks overall geometry parsing with various WKB sizes.
func BenchmarkGeometryParsing(b *testing.B) {
	// Simple POINT (21 bytes)
	pointWKB := createPointWKB(1.5, 2.5)

	// LINESTRING with 50 points (809 bytes)
	linePoints := make([][2]float64, 50)
	for i := range linePoints {
		linePoints[i] = [2]float64{float64(i) * 0.1, float64(i) * 0.2}
	}
	lineStringWKB := createLineStringWKB(linePoints)

	// Complex POLYGON with 100-point ring (1613 bytes)
	polygonRing := make([][2]float64, 100)
	for i := range polygonRing {
		angle := float64(i) * 0.0628 // ~100 points around a circle
		polygonRing[i] = [2]float64{10 * angle, 10 * angle}
	}
	polygonRing[99] = polygonRing[0] // Close the ring
	polygonWKB := createPolygonWKB(polygonRing)

	b.Run("Parse/Point", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = geometry.ParseWKB(pointWKB)
		}
		b.ReportMetric(float64(len(pointWKB)), "bytes/op")
	})

	b.Run("Parse/LineString50", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = geometry.ParseWKB(lineStringWKB)
		}
		b.ReportMetric(float64(len(lineStringWKB)), "bytes/op")
	})

	b.Run("Parse/Polygon100", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			_, _ = geometry.ParseWKB(polygonWKB)
		}
		b.ReportMetric(float64(len(polygonWKB)), "bytes/op")
	})

	b.Run("Vector/MixedGeometries", func(b *testing.B) {
		info, _ := NewGeometryInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		geometries := [][]byte{pointWKB, lineStringWKB, polygonWKB}

		b.ResetTimer()
		for range b.N {
			for row := range 300 {
				geomData := geometries[row%len(geometries)]
				_ = vec.setFn(vec, row, geomData)
			}
			for row := range 300 {
				_ = vec.getFn(vec, row)
			}
		}

		opsPerIter := float64(600) // 300 sets + 300 gets
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/value")
	})

	b.Run("DirectWKBAccess", func(b *testing.B) {
		info, _ := NewGeometryInfo()
		vec := newVector(VectorSize)
		_ = vec.init(info, 0)

		// Pre-populate with geometry
		_ = vec.setFn(vec, 0, pointWKB)

		b.ResetTimer()
		for range b.N {
			result := vec.getFn(vec, 0)
			if geom, ok := result.(*geometry.Geometry); ok {
				_ = geom.WKB() // Access the raw WKB bytes
			}
		}

		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
		b.ReportMetric(nsPerOp, "ns/access")
	})
}
