package optimizer

import "sync"

// CostCalibrator adjusts cost constants based on actual vs estimated costs.
// It uses an exponential moving average to avoid abrupt changes.
type CostCalibrator struct {
	mu           sync.Mutex
	cpuFactor    float64
	ioFactor     float64
	memoryFactor float64
	alpha        float64
	minFactor    float64
	maxFactor    float64
}

// NewCostCalibrator creates a new calibrator with conservative defaults.
func NewCostCalibrator() *CostCalibrator {
	return &CostCalibrator{
		cpuFactor:    1.0,
		ioFactor:     1.0,
		memoryFactor: 1.0,
		alpha:        0.1,
		minFactor:    0.5,
		maxFactor:    2.0,
	}
}

// RecordSample updates calibration factors using a single observation.
func (c *CostCalibrator) RecordSample(estimated, actual float64) {
	if c == nil || estimated <= 0 || actual <= 0 {
		return
	}

	ratio := actual / estimated
	if ratio < c.minFactor {
		ratio = c.minFactor
	}
	if ratio > c.maxFactor {
		ratio = c.maxFactor
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.cpuFactor = (1-c.alpha)*c.cpuFactor + c.alpha*ratio
	c.ioFactor = (1-c.alpha)*c.ioFactor + c.alpha*ratio
	c.memoryFactor = (1-c.alpha)*c.memoryFactor + c.alpha*ratio
}

// Apply returns updated cost constants with calibration factors applied.
func (c *CostCalibrator) Apply(constants CostConstants) CostConstants {
	if c == nil {
		return constants
	}

	c.mu.Lock()
	cpu := c.cpuFactor
	io := c.ioFactor
	memory := c.memoryFactor
	c.mu.Unlock()

	constants.CPUTupleCost *= cpu
	constants.CPUOperatorCost *= cpu
	constants.SeqPageCost *= io
	constants.RandomPageCost *= io
	constants.HashBuildCost *= memory
	constants.HashProbeCost *= memory
	return constants
}
