// Package stats provides lightweight runtime statistics for adaptive optimization.
package stats

import "sync"

// Histogram is a lightweight equi-width histogram for numeric values.
// It prioritizes low overhead over perfect accuracy.
type Histogram struct {
	mu       sync.Mutex
	buckets  []uint64
	min      float64
	max      float64
	count    uint64
	initDone bool
	samples  []float64
	sampleSz int
}

// NewHistogram returns a histogram with the requested bucket count.
func NewHistogram(bucketCount int) *Histogram {
	if bucketCount < 1 {
		bucketCount = 16
	}
	sampleSz := bucketCount * 10
	if sampleSz < 32 {
		sampleSz = 32
	}
	return &Histogram{
		buckets:  make([]uint64, bucketCount),
		samples:  make([]float64, 0, sampleSz),
		sampleSz: sampleSz,
	}
}

// Observe adds a value to the histogram.
func (h *Histogram) Observe(value float64) {
	if h == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	h.count++
	if len(h.samples) < h.sampleSz {
		h.samples = append(h.samples, value)
	} else {
		idx := int(h.count % uint64(h.sampleSz))
		h.samples[idx] = value
	}

	if !h.initDone || h.count%uint64(h.sampleSz) == 0 {
		h.rebuild()
	}
}

// EstimateLessThan returns the estimated selectivity for values < threshold.
func (h *Histogram) EstimateLessThan(threshold float64) float64 {
	if h == nil {
		return 0.0
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.count == 0 || !h.initDone {
		return 0.0
	}

	if threshold <= h.min {
		return 0.0
	}
	if threshold >= h.max {
		return 1.0
	}

	idx := h.bucketIndex(threshold)
	var total uint64
	for i := 0; i < idx; i++ {
		total += h.buckets[i]
	}

	bucketMin, bucketMax := h.bucketBounds(idx)
	fraction := (threshold - bucketMin) / (bucketMax - bucketMin)
	if fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}

	total += uint64(float64(h.buckets[idx]) * fraction)

	return float64(total) / float64(h.count)
}

// EstimateBetween returns the estimated selectivity for min <= value <= max.
func (h *Histogram) EstimateBetween(min, max float64) float64 {
	if h == nil {
		return 0.0
	}
	if min > max {
		min, max = max, min
	}

	lessThanMax := h.EstimateLessThan(max)
	lessThanMin := h.EstimateLessThan(min)
	selectivity := lessThanMax - lessThanMin
	if selectivity < 0 {
		return 0.0
	}
	if selectivity > 1 {
		return 1.0
	}
	return selectivity
}

func (h *Histogram) bucketIndex(value float64) int {
	if len(h.buckets) == 1 || h.max == h.min {
		return 0
	}

	width := (h.max - h.min) / float64(len(h.buckets))
	idx := int((value - h.min) / width)
	if idx < 0 {
		return 0
	}
	if idx >= len(h.buckets) {
		return len(h.buckets) - 1
	}
	return idx
}

func (h *Histogram) bucketBounds(index int) (float64, float64) {
	if len(h.buckets) == 1 || h.max == h.min {
		return h.min, h.max
	}

	width := (h.max - h.min) / float64(len(h.buckets))
	bucketMin := h.min + float64(index)*width
	bucketMax := bucketMin + width
	return bucketMin, bucketMax
}

func (h *Histogram) rebuild() {
	if len(h.samples) == 0 {
		return
	}

	min := h.samples[0]
	max := h.samples[0]
	for _, value := range h.samples[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}

	h.min = min
	h.max = max
	h.initDone = true
	for i := range h.buckets {
		h.buckets[i] = 0
	}
	for _, value := range h.samples {
		idx := h.bucketIndex(value)
		h.buckets[idx]++
	}
}
