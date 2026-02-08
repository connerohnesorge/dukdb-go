package stats

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"sync"
)

// HyperLogLog provides approximate distinct counting with fixed memory usage.
// This implementation focuses on low overhead for runtime statistics.
type HyperLogLog struct {
	mu        sync.Mutex
	precision uint8
	registers []uint8
}

// NewHyperLogLog creates a new sketch with the given precision.
// Precision must be in [4, 16]. Default precision is 10.
func NewHyperLogLog(precision uint8) *HyperLogLog {
	if precision < 4 {
		precision = 4
	}
	if precision > 16 {
		precision = 16
	}
	return &HyperLogLog{
		precision: precision,
		registers: make([]uint8, 1<<precision),
	}
}

// AddBytes adds an observation using raw bytes.
func (h *HyperLogLog) AddBytes(value []byte) {
	if h == nil {
		return
	}
	hashed := hashBytes64(value)
	h.addHash(hashed)
}

// AddString adds an observation using a string value.
func (h *HyperLogLog) AddString(value string) {
	if h == nil {
		return
	}
	hashed := hashBytes64([]byte(value))
	h.addHash(hashed)
}

// AddUint64 adds an observation using a uint64 value.
func (h *HyperLogLog) AddUint64(value uint64) {
	if h == nil {
		return
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	h.addHash(hashBytes64(buf[:]))
}

// Estimate returns the approximate distinct count.
func (h *HyperLogLog) Estimate() uint64 {
	if h == nil {
		return 0
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	m := float64(len(h.registers))
	if m == 0 {
		return 0
	}

	var sum float64
	zeroRegisters := 0
	for _, reg := range h.registers {
		sum += math.Pow(2, -float64(reg))
		if reg == 0 {
			zeroRegisters++
		}
	}

	alpha := h.alpha()
	rawEstimate := alpha * m * m / sum

	// Small range correction (linear counting)
	if rawEstimate <= 2.5*m && zeroRegisters > 0 {
		rawEstimate = m * math.Log(m/float64(zeroRegisters))
	}

	if rawEstimate < 0 {
		return 0
	}

	return uint64(rawEstimate + 0.5)
}

func (h *HyperLogLog) addHash(hashed uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	idx := hashed >> (64 - h.precision)
	w := hashed << h.precision
	rank := leadingZeros(w) + 1
	if rank > h.registers[idx] {
		h.registers[idx] = rank
	}
}

func (h *HyperLogLog) alpha() float64 {
	m := float64(len(h.registers))
	switch len(h.registers) {
	case 16:
		return 0.673
	case 32:
		return 0.697
	case 64:
		return 0.709
	default:
		return 0.7213 / (1 + 1.079/m)
	}
}

func leadingZeros(x uint64) uint8 {
	if x == 0 {
		return 64
	}
	var n uint8
	for (x & (1 << 63)) == 0 {
		n++
		x <<= 1
	}
	return n
}

func hashBytes64(value []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(value)
	return mix64(h.Sum64())
}

func mix64(value uint64) uint64 {
	value ^= value >> 33
	value *= 0xff51afd7ed558ccd
	value ^= value >> 33
	value *= 0xc4ceb9fe1a85ec53
	value ^= value >> 33
	return value
}
