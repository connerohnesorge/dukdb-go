package vector

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Define some example types for testing
	testTypeInteger VectorType = 4
	testTypeVarchar VectorType = 18
	testTypeDouble  VectorType = 11
)

func TestVectorPool_GetFromEmpty(t *testing.T) {
	t.Run("get vector from empty pool", func(t *testing.T) {
		pool := NewVectorPool()
		v := pool.Get(testTypeInteger, 2048)

		require.NotNil(t, v)
		require.NotNil(t, v.Mask)
		assert.Equal(t, testTypeInteger, v.Type())
		assert.Equal(t, uint64(2048), v.Capacity())
		assert.Equal(t, uint64(2048), v.Mask.Capacity())
	})

	t.Run("returned vector has all NULL validity", func(t *testing.T) {
		pool := NewVectorPool()
		v := pool.Get(testTypeInteger, 2048)

		// All entries should be NULL (validity mask cleared)
		for i := uint64(0); i < 2048; i++ {
			assert.False(t, v.Mask.IsValid(i), "entry %d should be NULL", i)
		}
		assert.Equal(t, uint64(0), v.Mask.CountValid())
	})

	t.Run("multiple gets create new vectors", func(t *testing.T) {
		pool := NewVectorPool()
		v1 := pool.Get(testTypeInteger, 2048)
		v2 := pool.Get(testTypeInteger, 2048)

		// Should be different instances
		assert.NotSame(t, v1, v2)
	})
}

func TestVectorPool_ReturnAndReuse(t *testing.T) {
	t.Run("return vector to pool", func(t *testing.T) {
		pool := NewVectorPool()
		v := pool.Get(testTypeInteger, 2048)

		// Modify the validity mask
		v.Mask.SetValid(0, true)
		v.Mask.SetValid(100, true)
		assert.Equal(t, uint64(2), v.Mask.CountValid())

		// Return to pool
		pool.Put(v)

		// Mask should be cleared
		assert.Equal(t, uint64(0), v.Mask.CountValid())
	})

	t.Run("reuse vector from pool", func(t *testing.T) {
		pool := NewVectorPool()

		// Get and return a vector
		v1 := pool.Get(testTypeInteger, 2048)
		v1.Mask.SetValid(0, true)
		pool.Put(v1)

		// Get another vector - sync.Pool may or may not reuse the same instance
		v2 := pool.Get(testTypeInteger, 2048)

		// Verify correct type and capacity
		assert.Equal(t, testTypeInteger, v2.Type())
		assert.Equal(t, uint64(2048), v2.Capacity())
		// Validity mask should be cleared regardless of instance
		assert.Equal(t, uint64(0), v2.Mask.CountValid())
	})

	t.Run("put nil vector does nothing", func(_ *testing.T) {
		pool := NewVectorPool()
		// Should not panic
		pool.Put(nil)
	})

	t.Run("put vector with nil mask does nothing", func(_ *testing.T) {
		pool := NewVectorPool()
		v := &PooledVector{
			Mask:     nil,
			typ:      testTypeInteger,
			capacity: 2048,
		}
		// Should not panic
		pool.Put(v)
	})
}

func TestVectorPool_TypeSpecific(t *testing.T) {
	t.Run("different types use different pools", func(t *testing.T) {
		pool := NewVectorPool()

		// Get vectors of different types
		vInt := pool.Get(testTypeInteger, 2048)
		vStr := pool.Get(testTypeVarchar, 2048)

		assert.Equal(t, testTypeInteger, vInt.Type())
		assert.Equal(t, testTypeVarchar, vStr.Type())

		// Modify validity masks to verify clearing works
		vInt.Mask.SetValid(0, true)
		vStr.Mask.SetValid(0, true)

		// Return them
		pool.Put(vInt)
		pool.Put(vStr)

		// Get again - sync.Pool may or may not reuse same instances
		vInt2 := pool.Get(testTypeInteger, 2048)
		vStr2 := pool.Get(testTypeVarchar, 2048)

		// Verify correct types are returned
		assert.Equal(t, testTypeInteger, vInt2.Type())
		assert.Equal(t, testTypeVarchar, vStr2.Type())
		// Verify validity masks are cleared
		assert.Equal(t, uint64(0), vInt2.Mask.CountValid())
		assert.Equal(t, uint64(0), vStr2.Mask.CountValid())
		// Verify different types return different vectors
		assert.NotSame(t, vInt2, vStr2)
	})

	t.Run("different capacities create new vectors", func(t *testing.T) {
		pool := NewVectorPool()

		// Get and return a 2048-capacity vector
		v1 := pool.Get(testTypeInteger, 2048)
		pool.Put(v1)

		// Get a different capacity - should create new
		v2 := pool.Get(testTypeInteger, 1024)
		assert.NotSame(t, v1, v2)
		assert.Equal(t, uint64(1024), v2.Capacity())
	})
}

func TestVectorPool_Concurrent(t *testing.T) {
	t.Run("concurrent get and put operations", func(t *testing.T) {
		pool := NewVectorPool()
		const numGoroutines = 100
		const iterations = 10

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()

				// Alternate between different types
				typ := testTypeInteger
				if id%2 == 0 {
					typ = testTypeVarchar
				}

				for j := 0; j < iterations; j++ {
					// Get vector
					v := pool.Get(typ, 2048)
					require.NotNil(t, v)
					require.NotNil(t, v.Mask)

					// Verify it's cleared
					assert.Equal(t, uint64(0), v.Mask.CountValid())

					// Modify it
					v.Mask.SetValid(uint64(id%2048), true)

					// Return to pool
					pool.Put(v)
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("concurrent operations on different types", func(t *testing.T) {
		pool := NewVectorPool()
		const numGoroutines = 50

		types := []VectorType{
			testTypeInteger,
			testTypeVarchar,
			testTypeDouble,
		}

		var wg sync.WaitGroup
		wg.Add(numGoroutines * len(types))

		for _, typ := range types {
			for i := 0; i < numGoroutines; i++ {
				go func(vType VectorType) {
					defer wg.Done()

					v := pool.Get(vType, 2048)
					require.NotNil(t, v)

					v.Mask.SetValid(0, true)
					pool.Put(v)
				}(typ)
			}
		}

		wg.Wait()
	})
}

func TestPooledVector_Accessors(t *testing.T) {
	t.Run("type and capacity accessors", func(t *testing.T) {
		pool := NewVectorPool()
		v := pool.Get(testTypeInteger, 2048)

		assert.Equal(t, testTypeInteger, v.Type())
		assert.Equal(t, uint64(2048), v.Capacity())
	})
}

// Benchmarks for performance validation

func BenchmarkVectorPool_GetPut(b *testing.B) {
	pool := NewVectorPool()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		v := pool.Get(testTypeInteger, 2048)
		pool.Put(v)
	}
}

func BenchmarkVectorPool_GetPutParallel(b *testing.B) {
	pool := NewVectorPool()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v := pool.Get(testTypeInteger, 2048)
			pool.Put(v)
		}
	})
}

func BenchmarkVectorPool_MultiType(b *testing.B) {
	pool := NewVectorPool()
	types := []VectorType{
		testTypeInteger,
		testTypeVarchar,
		testTypeDouble,
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		typ := types[i%len(types)]
		v := pool.Get(typ, 2048)
		pool.Put(v)
	}
}
