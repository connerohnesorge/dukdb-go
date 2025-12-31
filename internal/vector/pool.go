// Package vector provides low-level vector operations including validity masks and vector pooling.
package vector

import (
	"sync"
)

// VectorSize is the default vector size matching DuckDB's standard vector size.
const VectorSize = 2048

// VectorType represents a DuckDB type. This is defined as uint8 to match dukdb.Type.
// The caller is responsible for mapping dukdb.Type to this VectorType.
type VectorType uint8

// PooledVector wraps a ValidityMask with pool membership tracking.
// It holds a cleared validity mask ready for reuse.
type PooledVector struct {
	// Mask is the validity mask for this vector
	Mask *ValidityMask
	// typ is the type of this vector
	typ VectorType
	// capacity is the capacity of this vector
	capacity uint64
}

// Type returns the type of the pooled vector.
func (pv *PooledVector) Type() VectorType {
	return pv.typ
}

// Capacity returns the capacity of the pooled vector.
func (pv *PooledVector) Capacity() uint64 {
	return pv.capacity
}

// VectorPool manages a pool of reusable ValidityMask instances.
// It uses sync.Pool under the hood for efficient memory reuse and is thread-safe.
type VectorPool struct {
	// pools maps VectorType to a sync.Pool for that type
	pools map[VectorType]*sync.Pool
	mu    sync.RWMutex
}

// NewVectorPool creates a new VectorPool.
func NewVectorPool() *VectorPool {
	return &VectorPool{
		pools: make(map[VectorType]*sync.Pool),
	}
}

// Get retrieves a vector from the pool or creates a new one.
// The returned vector has a cleared validity mask (all NULL).
func (vp *VectorPool) Get(
	typ VectorType,
	capacity int,
) *PooledVector {
	capacityU64 := uint64(capacity)

	// Get or create pool for this type
	vp.mu.RLock()
	pool, exists := vp.pools[typ]
	vp.mu.RUnlock()

	if !exists {
		// Create a new pool for this type
		vp.mu.Lock()
		// Double-check after acquiring write lock
		pool, exists = vp.pools[typ]
		if !exists {
			pool = &sync.Pool{
				New: func() any {
					return &PooledVector{
						Mask: NewValidityMaskEmpty(
							capacityU64,
						),
						typ:      typ,
						capacity: capacityU64,
					}
				},
			}
			vp.pools[typ] = pool
		}
		vp.mu.Unlock()
	}

	// Try to get from pool
	if v := pool.Get(); v != nil {
		pv, ok := v.(*PooledVector)
		if ok && pv.capacity == capacityU64 {
			return pv
		}
		// Capacity mismatch or invalid type, create new one with correct capacity
	}

	// Create new vector with correct capacity
	return &PooledVector{
		Mask: NewValidityMaskEmpty(
			capacityU64,
		),
		typ:      typ,
		capacity: capacityU64,
	}
}

// Put returns a vector to the pool after clearing its validity mask.
// The validity mask is reset to all NULL before being returned to the pool.
func (vp *VectorPool) Put(v *PooledVector) {
	if v == nil || v.Mask == nil {
		return
	}

	// Clear the mask (set all to NULL)
	v.Mask.SetAllNull()

	// Get the pool for this type
	vp.mu.RLock()
	pool, exists := vp.pools[v.typ]
	vp.mu.RUnlock()

	if !exists {
		// Pool doesn't exist yet, create it
		vp.mu.Lock()
		// Double-check after acquiring write lock
		pool, exists = vp.pools[v.typ]
		if !exists {
			pool = &sync.Pool{
				New: func() any {
					return &PooledVector{
						Mask: NewValidityMaskEmpty(
							v.capacity,
						),
						typ:      v.typ,
						capacity: v.capacity,
					}
				},
			}
			vp.pools[v.typ] = pool
		}
		vp.mu.Unlock()
	}

	// Return to pool
	pool.Put(v)
}
