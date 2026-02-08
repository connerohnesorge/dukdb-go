package adaptive

// CardinalityObserver consumes cardinality observations for learning.
type CardinalityObserver interface {
	RecordObservation(operatorSig string, estimated, actual int64)
}

// CostCalibrator consumes cost samples for calibration.
type CostCalibrator interface {
	RecordCostSample(sample CostSample)
}

// CostSample represents estimated vs actual cost.
type CostSample struct {
	Estimated float64
	Actual    float64
}

// FeedbackLoop applies execution feedback to learning components.
type FeedbackLoop struct {
	cardinality CardinalityObserver
	calibrator  CostCalibrator
}

// NewFeedbackLoop creates a feedback loop.
func NewFeedbackLoop(cardinality CardinalityObserver, calibrator CostCalibrator) *FeedbackLoop {
	return &FeedbackLoop{cardinality: cardinality, calibrator: calibrator}
}

// Apply updates learning components with query stats.
func (f *FeedbackLoop) Apply(stats QueryStats) {
	if f == nil {
		return
	}

	for operator, opStats := range stats.Operators {
		if f.cardinality != nil && opStats.EstimatedRows > 0 {
			f.cardinality.RecordObservation(operator, opStats.EstimatedRows, opStats.ActualRows)
		}

		if f.calibrator != nil && opStats.EstimatedCost > 0 && opStats.ActualCost > 0 {
			f.calibrator.RecordCostSample(CostSample{Estimated: opStats.EstimatedCost, Actual: opStats.ActualCost})
		}
	}
}
