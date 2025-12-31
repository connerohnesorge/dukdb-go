package vector

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewValidityMask(t *testing.T) {
	t.Run("creates mask with correct capacity", func(t *testing.T) {
		mask := NewValidityMask(2048)
		assert.Equal(t, uint64(2048), mask.Capacity())
	})

	t.Run("initializes all entries as valid", func(t *testing.T) {
		mask := NewValidityMask(100)
		for i := range uint64(100) {
			assert.True(t, mask.IsValid(i), "entry %d should be valid", i)
		}
	})

	t.Run("creates correct number of words", func(t *testing.T) {
		// 64 bits per word
		mask := NewValidityMask(64)
		assert.Len(t, mask.bits, 1)

		mask = NewValidityMask(65)
		assert.Len(t, mask.bits, 2)

		mask = NewValidityMask(2048)
		assert.Len(t, mask.bits, 32) // 2048/64 = 32
	})
}

func TestNewValidityMaskEmpty(t *testing.T) {
	t.Run("creates mask with all entries NULL", func(t *testing.T) {
		mask := NewValidityMaskEmpty(100)
		for i := range uint64(100) {
			assert.False(t, mask.IsValid(i), "entry %d should be NULL", i)
		}
	})
}

func TestValidityMask_IsValid(t *testing.T) {
	mask := NewValidityMask(100)

	t.Run("returns true for valid entries", func(t *testing.T) {
		assert.True(t, mask.IsValid(0))
		assert.True(t, mask.IsValid(50))
		assert.True(t, mask.IsValid(99))
	})

	t.Run("returns false after SetNull", func(t *testing.T) {
		mask.SetNull(5)
		assert.False(t, mask.IsValid(5))
		assert.True(t, mask.IsValid(4))
		assert.True(t, mask.IsValid(6))
	})
}

func TestValidityMask_SetValid(t *testing.T) {
	mask := NewValidityMaskEmpty(100)

	t.Run("sets entry to valid", func(t *testing.T) {
		mask.SetValid(10, true)
		assert.True(t, mask.IsValid(10))
	})

	t.Run("sets entry to NULL", func(t *testing.T) {
		mask.SetValid(10, true)
		mask.SetValid(10, false)
		assert.False(t, mask.IsValid(10))
	})

	t.Run("handles word boundaries correctly", func(t *testing.T) {
		// Test at word boundaries (0, 63, 64, 65)
		mask := NewValidityMaskEmpty(100)
		mask.SetValid(0, true)
		mask.SetValid(63, true)
		mask.SetValid(64, true)
		mask.SetValid(65, true)

		assert.True(t, mask.IsValid(0))
		assert.True(t, mask.IsValid(63))
		assert.True(t, mask.IsValid(64))
		assert.True(t, mask.IsValid(65))
		assert.False(t, mask.IsValid(1))
		assert.False(t, mask.IsValid(62))
	})
}

func TestValidityMask_SetNull(t *testing.T) {
	mask := NewValidityMask(100)

	t.Run("marks entry as NULL", func(t *testing.T) {
		mask.SetNull(25)
		assert.False(t, mask.IsValid(25))
	})
}

func TestValidityMask_SetAllValid(t *testing.T) {
	t.Run("sets all entries to valid", func(t *testing.T) {
		mask := NewValidityMaskEmpty(100)
		mask.SetAllValid(100)
		for i := range uint64(100) {
			assert.True(t, mask.IsValid(i), "entry %d should be valid", i)
		}
	})

	t.Run("handles partial count", func(t *testing.T) {
		mask := NewValidityMaskEmpty(100)
		mask.SetAllValid(50)
		for i := range uint64(50) {
			assert.True(t, mask.IsValid(i), "entry %d should be valid", i)
		}
		for i := uint64(50); i < 100; i++ {
			assert.False(t, mask.IsValid(i), "entry %d should be NULL", i)
		}
	})

	t.Run("handles zero count", func(t *testing.T) {
		mask := NewValidityMaskEmpty(100)
		mask.SetAllValid(0)
		for i := range uint64(100) {
			assert.False(t, mask.IsValid(i))
		}
	})
}

func TestValidityMask_SetAllNull(t *testing.T) {
	t.Run("marks all entries as NULL", func(t *testing.T) {
		mask := NewValidityMask(100)
		mask.SetAllNull()
		for i := range uint64(100) {
			assert.False(t, mask.IsValid(i))
		}
	})
}

func TestValidityMask_CountValid(t *testing.T) {
	t.Run("counts all valid entries", func(t *testing.T) {
		mask := NewValidityMask(100)
		assert.Equal(t, uint64(100), mask.CountValid())
	})

	t.Run("counts after some NULLs", func(t *testing.T) {
		mask := NewValidityMask(100)
		mask.SetNull(10)
		mask.SetNull(20)
		mask.SetNull(30)
		assert.Equal(t, uint64(97), mask.CountValid())
	})

	t.Run("counts zero for all NULL", func(t *testing.T) {
		mask := NewValidityMaskEmpty(100)
		assert.Equal(t, uint64(0), mask.CountValid())
	})
}

func TestValidityMask_CountNull(t *testing.T) {
	t.Run("counts NULL entries", func(t *testing.T) {
		mask := NewValidityMask(100)
		mask.SetNull(10)
		mask.SetNull(20)
		assert.Equal(t, uint64(2), mask.CountNull())
	})
}

func TestValidityMask_HasNull(t *testing.T) {
	t.Run("returns false when all valid", func(t *testing.T) {
		mask := NewValidityMask(100)
		assert.False(t, mask.HasNull())
	})

	t.Run("returns true when any NULL", func(t *testing.T) {
		mask := NewValidityMask(100)
		mask.SetNull(50)
		assert.True(t, mask.HasNull())
	})
}

func TestValidityMask_AllValid(t *testing.T) {
	t.Run("returns true when all valid", func(t *testing.T) {
		mask := NewValidityMask(100)
		assert.True(t, mask.AllValid())
	})

	t.Run("returns false when any NULL", func(t *testing.T) {
		mask := NewValidityMask(100)
		mask.SetNull(50)
		assert.False(t, mask.AllValid())
	})
}

func TestValidityMask_Reset(t *testing.T) {
	t.Run("clears all entries to NULL", func(t *testing.T) {
		mask := NewValidityMask(100)
		mask.Reset()
		for i := range uint64(100) {
			assert.False(t, mask.IsValid(i))
		}
	})

	t.Run("preserves capacity", func(t *testing.T) {
		mask := NewValidityMask(100)
		mask.Reset()
		assert.Equal(t, uint64(100), mask.Capacity())
	})
}

func TestValidityMask_Clone(t *testing.T) {
	t.Run("creates independent copy", func(t *testing.T) {
		mask := NewValidityMask(100)
		mask.SetNull(50)

		clone := mask.Clone()

		// Verify clone has same values
		assert.Equal(t, mask.IsValid(50), clone.IsValid(50))
		assert.Equal(t, mask.Capacity(), clone.Capacity())

		// Modify original, clone should be unaffected
		mask.SetValid(50, true)
		assert.True(t, mask.IsValid(50))
		assert.False(t, clone.IsValid(50))
	})
}

func TestValidityMask_Bits(t *testing.T) {
	t.Run("returns underlying bits", func(t *testing.T) {
		mask := NewValidityMask(64)
		bits := mask.Bits()
		require.Len(t, bits, 1)
		assert.Equal(t, ^uint64(0), bits[0]) // All valid
	})
}

func TestValidityMask_SetBits(t *testing.T) {
	t.Run("sets bits from external data", func(t *testing.T) {
		mask := NewValidityMaskEmpty(64)
		mask.SetBits([]uint64{0b1010101010101010})
		assert.False(t, mask.IsValid(0))
		assert.True(t, mask.IsValid(1))
		assert.False(t, mask.IsValid(2))
		assert.True(t, mask.IsValid(3))
	})
}

// Benchmarks for performance validation

func BenchmarkValidityMask_IsValid(b *testing.B) {
	mask := NewValidityMask(2048)
	b.ResetTimer()
	for i := range b.N {
		_ = mask.IsValid(uint64(i % 2048))
	}
}

func BenchmarkValidityMask_SetValid(b *testing.B) {
	mask := NewValidityMask(2048)
	b.ResetTimer()
	for i := range b.N {
		mask.SetValid(uint64(i%2048), true)
	}
}

func BenchmarkValidityMask_CountValid(b *testing.B) {
	mask := NewValidityMask(2048)
	// Set some NULLs for realistic scenario
	for i := uint64(0); i < 2048; i += 10 {
		mask.SetNull(i)
	}
	b.ResetTimer()
	for range b.N {
		_ = mask.CountValid()
	}
}

func BenchmarkValidityMask_SetAllValid(b *testing.B) {
	mask := NewValidityMaskEmpty(2048)
	b.ResetTimer()
	for range b.N {
		mask.SetAllValid(2048)
	}
}
