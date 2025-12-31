package dukdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVectorReset(t *testing.T) {
	tests := []struct {
		name      string
		setupFunc func(*vector)
		checkFunc func(*testing.T, *vector)
	}{
		{
			name: "reset integer vector",
			setupFunc: func(v *vector) {
				initNumericVec[int32](v, TYPE_INTEGER)
				setPrimitive(v, 0, int32(42))
				setPrimitive(v, 1, int32(100))
				v.setNull(2)
			},
			checkFunc: func(t *testing.T, v *vector) {
				// After reset, all values should be zero
				assert.Equal(t, int32(0), getPrimitive[int32](v, 0))
				assert.Equal(t, int32(0), getPrimitive[int32](v, 1))
				assert.Equal(t, int32(0), getPrimitive[int32](v, 2))
				// All entries should be valid
				assert.False(t, v.isNull(0))
				assert.False(t, v.isNull(1))
				assert.False(t, v.isNull(2))
			},
		},
		{
			name: "reset string vector",
			setupFunc: func(v *vector) {
				v.initVarchar()
				setPrimitive(v, 0, "hello")
				setPrimitive(v, 1, "world")
				v.setNull(2)
			},
			checkFunc: func(t *testing.T, v *vector) {
				// After reset, all values should be empty strings
				assert.Equal(t, "", getPrimitive[string](v, 0))
				assert.Equal(t, "", getPrimitive[string](v, 1))
				assert.Equal(t, "", getPrimitive[string](v, 2))
				// All entries should be valid
				assert.False(t, v.isNull(0))
				assert.False(t, v.isNull(1))
				assert.False(t, v.isNull(2))
			},
		},
		{
			name: "reset boolean vector",
			setupFunc: func(v *vector) {
				initBoolVec(v)
				setPrimitive(v, 0, true)
				setPrimitive(v, 1, false)
				v.setNull(2)
			},
			checkFunc: func(t *testing.T, v *vector) {
				// After reset, all values should be false
				assert.Equal(t, false, getPrimitive[bool](v, 0))
				assert.Equal(t, false, getPrimitive[bool](v, 1))
				assert.Equal(t, false, getPrimitive[bool](v, 2))
				// All entries should be valid
				assert.False(t, v.isNull(0))
				assert.False(t, v.isNull(1))
				assert.False(t, v.isNull(2))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newVector(10)
			tt.setupFunc(v)
			v.Reset()
			tt.checkFunc(t, v)
		})
	}
}

func TestVectorClose(t *testing.T) {
	tests := []struct {
		name      string
		setupFunc func(*vector)
	}{
		{
			name: "close integer vector",
			setupFunc: func(v *vector) {
				initNumericVec[int32](v, TYPE_INTEGER)
				setPrimitive(v, 0, int32(42))
			},
		},
		{
			name: "close string vector",
			setupFunc: func(v *vector) {
				v.initVarchar()
				setPrimitive(v, 0, "hello")
			},
		},
		{
			name: "close nested struct vector",
			setupFunc: func(v *vector) {
				// Create a simple struct type with one field
				intInfo, _ := NewTypeInfo(TYPE_INTEGER)
				entry, _ := NewStructEntry(intInfo, "field1")
				structInfo, _ := NewStructInfo(entry)
				_ = v.initStruct(structInfo, 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newVector(10)
			tt.setupFunc(v)
			v.Close()

			// After close, everything should be nil and capacity should be 0
			assert.Nil(t, v.dataSlice)
			assert.Nil(t, v.maskBits)
			assert.Nil(t, v.listOffsets)
			assert.Nil(t, v.childVectors)
			assert.Nil(t, v.namesDict)
			assert.Nil(t, v.tagDict)
			assert.Equal(t, 0, v.capacity)
		})
	}
}

func TestVectorResetWithNestedTypes(t *testing.T) {
	t.Run("reset list vector", func(t *testing.T) {
		v := newVector(10)
		intInfo, _ := NewTypeInfo(TYPE_INTEGER)
		listInfo, _ := NewListInfo(intInfo)
		err := v.initList(listInfo, 0)
		assert.NoError(t, err)

		// Set some values
		v.listOffsets[0] = 0
		v.listOffsets[1] = 2
		child := &v.childVectors[0]
		setPrimitive[int32](child, 0, int32(10))
		setPrimitive[int32](child, 1, int32(20))

		// Reset
		v.Reset()

		// Check that list offsets are zeroed
		for i := range v.listOffsets {
			assert.Equal(t, uint64(0), v.listOffsets[i])
		}

		// Check that child vector is also reset
		assert.Equal(t, int32(0), getPrimitive[int32](child, 0))
		assert.Equal(t, int32(0), getPrimitive[int32](child, 1))
		assert.False(t, child.isNull(0))
		assert.False(t, child.isNull(1))
	})
}
