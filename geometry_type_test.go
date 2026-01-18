package dukdb

import (
	"testing"

	"github.com/dukdb/dukdb-go/internal/io/geometry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Valid WKB for POINT(1.0, 2.0)
// - Byte order: 0x01 (little endian)
// - Type: 0x01, 0x00, 0x00, 0x00 (Point = 1)
// - X: 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F (1.0 as float64)
// - Y: 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40 (2.0 as float64)
var validPointWKB = []byte{
	0x01,                   // Little endian
	0x01, 0x00, 0x00, 0x00, // Point type (1)
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // X = 1.0
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Y = 2.0
}

// TestGeometryTypeConstant tests that TYPE_GEOMETRY has the correct value.
func TestGeometryTypeConstant(t *testing.T) {
	t.Run("TYPE_GEOMETRY equals 60", func(t *testing.T) {
		assert.Equal(t, Type(60), TYPE_GEOMETRY)
	})

	t.Run("TYPE_GEOMETRY.String() returns GEOMETRY", func(t *testing.T) {
		assert.Equal(t, "GEOMETRY", TYPE_GEOMETRY.String())
	})

	t.Run("TYPE_GEOMETRY.Category() returns other", func(t *testing.T) {
		assert.Equal(t, "other", TYPE_GEOMETRY.Category())
	})
}

// TestGeometryTypeInfo tests the NewGeometryInfo function and TypeInfo interface.
func TestGeometryTypeInfo(t *testing.T) {
	t.Run("NewGeometryInfo returns correct TypeInfo", func(t *testing.T) {
		info, err := NewGeometryInfo()
		require.NoError(t, err)
		require.NotNil(t, info)
	})

	t.Run("InternalType returns TYPE_GEOMETRY", func(t *testing.T) {
		info, err := NewGeometryInfo()
		require.NoError(t, err)
		assert.Equal(t, TYPE_GEOMETRY, info.InternalType())
	})

	t.Run("SQLType returns GEOMETRY", func(t *testing.T) {
		info, err := NewGeometryInfo()
		require.NoError(t, err)
		assert.Equal(t, "GEOMETRY", info.SQLType())
	})

	t.Run("Details returns GeometryDetails", func(t *testing.T) {
		info, err := NewGeometryInfo()
		require.NoError(t, err)

		details := info.Details()
		require.NotNil(t, details)

		geomDetails, ok := details.(*GeometryDetails)
		require.True(t, ok, "Details should be *GeometryDetails")
		assert.NotNil(t, geomDetails)
	})
}

// TestGeometryVector tests vector operations for GEOMETRY type.
func TestGeometryVector(t *testing.T) {
	t.Run("create GEOMETRY vector", func(t *testing.T) {
		vec := newVector(10)
		vec.initGeometry()

		assert.Equal(t, TYPE_GEOMETRY, vec.Type)
		assert.NotNil(t, vec.dataSlice)
		_, ok := vec.dataSlice.([]*geometry.Geometry)
		assert.True(t, ok, "dataSlice should be []*geometry.Geometry")
	})

	t.Run("set and get geometry value using *geometry.Geometry", func(t *testing.T) {
		vec := newVector(10)
		vec.initGeometry()

		// Parse WKB to create a Geometry
		geom, err := geometry.ParseWKB(validPointWKB)
		require.NoError(t, err)

		// Set the geometry value
		err = vec.setFn(vec, 0, geom)
		require.NoError(t, err)

		// Get the geometry value
		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		resultGeom, ok := result.(*geometry.Geometry)
		require.True(t, ok, "result should be *geometry.Geometry")
		assert.Equal(t, geometry.GeometryPoint, resultGeom.Type)
		assert.Equal(t, validPointWKB, resultGeom.WKB())
	})

	t.Run("set and get geometry value using WKB byte slice", func(t *testing.T) {
		vec := newVector(10)
		vec.initGeometry()

		// Set geometry using WKB bytes directly
		err := vec.setFn(vec, 0, validPointWKB)
		require.NoError(t, err)

		// Get the geometry value
		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		resultGeom, ok := result.(*geometry.Geometry)
		require.True(t, ok, "result should be *geometry.Geometry")
		assert.Equal(t, geometry.GeometryPoint, resultGeom.Type)
		assert.Equal(t, validPointWKB, resultGeom.WKB())
	})

	t.Run("set NULL value", func(t *testing.T) {
		vec := newVector(10)
		vec.initGeometry()

		// Set a value first
		geom, err := geometry.ParseWKB(validPointWKB)
		require.NoError(t, err)
		err = vec.setFn(vec, 0, geom)
		require.NoError(t, err)

		// Now set it to NULL
		err = vec.setFn(vec, 0, nil)
		require.NoError(t, err)

		// Verify it is NULL
		assert.True(t, vec.isNull(0))

		// Get should return nil
		result := vec.getFn(vec, 0)
		assert.Nil(t, result)
	})

	t.Run("get NULL value returns nil", func(t *testing.T) {
		vec := newVector(10)
		vec.initGeometry()

		// Mark index 0 as NULL
		vec.setNull(0)

		// Get should return nil
		result := vec.getFn(vec, 0)
		assert.Nil(t, result)
	})

	t.Run("Reset clears GEOMETRY vector", func(t *testing.T) {
		vec := newVector(10)
		vec.initGeometry()

		// Set some geometry values
		geom, err := geometry.ParseWKB(validPointWKB)
		require.NoError(t, err)

		err = vec.setFn(vec, 0, geom)
		require.NoError(t, err)
		err = vec.setFn(vec, 1, geom)
		require.NoError(t, err)
		vec.setNull(2)

		// Reset the vector
		vec.Reset()

		// Verify data is cleared
		data := vec.dataSlice.([]*geometry.Geometry)
		assert.Nil(t, data[0])
		assert.Nil(t, data[1])
		assert.Nil(t, data[2])

		// Verify all entries are valid after reset
		assert.False(t, vec.isNull(0))
		assert.False(t, vec.isNull(1))
		assert.False(t, vec.isNull(2))
	})
}

// TestGeometryPackage tests the geometry package functionality.
func TestGeometryPackage(t *testing.T) {
	t.Run("ParseWKB with valid POINT geometry WKB", func(t *testing.T) {
		geom, err := geometry.ParseWKB(validPointWKB)
		require.NoError(t, err)
		require.NotNil(t, geom)

		assert.Equal(t, geometry.GeometryPoint, geom.Type)
		assert.Equal(t, validPointWKB, geom.Data)
	})

	t.Run("ParseWKB with invalid (too short) data returns error", func(t *testing.T) {
		// WKB must be at least 5 bytes (1 byte order + 4 type bytes)
		shortData := []byte{0x01, 0x01, 0x00, 0x00}
		_, err := geometry.ParseWKB(shortData)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too short")
	})

	t.Run("Geometry.String returns correct type name", func(t *testing.T) {
		geom, err := geometry.ParseWKB(validPointWKB)
		require.NoError(t, err)

		assert.Equal(t, "POINT", geom.String())
	})

	t.Run("Geometry.WKB returns the original WKB data", func(t *testing.T) {
		geom, err := geometry.ParseWKB(validPointWKB)
		require.NoError(t, err)

		assert.Equal(t, validPointWKB, geom.WKB())
	})

	t.Run("GeometryType constants have correct values", func(t *testing.T) {
		assert.Equal(t, geometry.GeometryType(1), geometry.GeometryPoint)
		assert.Equal(t, geometry.GeometryType(2), geometry.GeometryLineString)
		assert.Equal(t, geometry.GeometryType(3), geometry.GeometryPolygon)
		assert.Equal(t, geometry.GeometryType(4), geometry.GeometryMultiPoint)
		assert.Equal(t, geometry.GeometryType(5), geometry.GeometryMultiLineString)
		assert.Equal(t, geometry.GeometryType(6), geometry.GeometryMultiPolygon)
		assert.Equal(t, geometry.GeometryType(7), geometry.GeometryCollection)
	})
}

// TestGeometryVectorInit tests vector initialization via the init method.
func TestGeometryVectorInit(t *testing.T) {
	t.Run("init vector with GEOMETRY type via TypeInfo", func(t *testing.T) {
		info, err := NewGeometryInfo()
		require.NoError(t, err)

		vec := newVector(10)
		err = vec.init(info, 0)
		require.NoError(t, err)

		assert.Equal(t, TYPE_GEOMETRY, vec.Type)
	})
}

// TestGeometryVectorMultipleValues tests storing multiple geometry values.
func TestGeometryVectorMultipleValues(t *testing.T) {
	t.Run("store and retrieve multiple geometries", func(t *testing.T) {
		vec := newVector(10)
		vec.initGeometry()

		// Create multiple geometries
		geom1, err := geometry.ParseWKB(validPointWKB)
		require.NoError(t, err)

		// Create another valid WKB (same point, but we will use it as a second value)
		geom2, err := geometry.ParseWKB(validPointWKB)
		require.NoError(t, err)

		// Set values
		err = vec.setFn(vec, 0, geom1)
		require.NoError(t, err)

		err = vec.setFn(vec, 1, geom2)
		require.NoError(t, err)

		// Set a NULL
		err = vec.setFn(vec, 2, nil)
		require.NoError(t, err)

		// Verify values
		result0 := vec.getFn(vec, 0)
		require.NotNil(t, result0)
		resultGeom0, ok := result0.(*geometry.Geometry)
		require.True(t, ok)
		assert.Equal(t, geometry.GeometryPoint, resultGeom0.Type)

		result1 := vec.getFn(vec, 1)
		require.NotNil(t, result1)
		resultGeom1, ok := result1.(*geometry.Geometry)
		require.True(t, ok)
		assert.Equal(t, geometry.GeometryPoint, resultGeom1.Type)

		result2 := vec.getFn(vec, 2)
		assert.Nil(t, result2)
		assert.True(t, vec.isNull(2))
	})
}

// TestGeometrySetWithGeometryValue tests setting with geometry.Geometry value (not pointer).
func TestGeometrySetWithGeometryValue(t *testing.T) {
	t.Run("set geometry.Geometry value type", func(t *testing.T) {
		vec := newVector(10)
		vec.initGeometry()

		geom, err := geometry.ParseWKB(validPointWKB)
		require.NoError(t, err)

		// Set using the value (not pointer) - should also work
		err = vec.setFn(vec, 0, *geom)
		require.NoError(t, err)

		result := vec.getFn(vec, 0)
		require.NotNil(t, result)

		resultGeom, ok := result.(*geometry.Geometry)
		require.True(t, ok)
		assert.Equal(t, geometry.GeometryPoint, resultGeom.Type)
	})
}
