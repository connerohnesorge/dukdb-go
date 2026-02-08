package optimizer

import "testing"

type adaptiveScanPlan struct{}

func (p adaptiveScanPlan) PhysicalPlanType() string                      { return "PhysicalScan" }
func (p adaptiveScanPlan) PhysicalChildren() []PhysicalPlanNode          { return nil }
func (p adaptiveScanPlan) PhysicalOutputColumns() []PhysicalOutputColumn { return nil }
func (p adaptiveScanPlan) AdaptiveSignature() string                     { return "scan_users" }
func (p adaptiveScanPlan) ScanSchema() string                            { return "main" }
func (p adaptiveScanPlan) ScanTableName() string                         { return "users" }
func (p adaptiveScanPlan) ScanAlias() string                             { return "" }
func (p adaptiveScanPlan) ScanRowCount() float64                         { return 100 }
func (p adaptiveScanPlan) ScanPageCount() float64                        { return 10 }

func TestCostCalibratorAdjustsConstants(t *testing.T) {
	calibrator := NewCostCalibrator()
	constants := DefaultCostConstants()

	calibrator.RecordSample(10, 20)
	updated := calibrator.Apply(constants)

	if updated.CPUTupleCost <= constants.CPUTupleCost {
		t.Fatalf("expected CPU cost to increase")
	}
	if updated.SeqPageCost <= constants.SeqPageCost {
		t.Fatalf("expected IO cost to increase")
	}
}

func TestAdaptiveCorrectionApplied(t *testing.T) {
	model := NewCostModel(DefaultCostConstants(), nil)
	model.learner = NewCardinalityLearner(10, 1)
	model.learner.RecordObservation("scan_users", 100, 200)
	model.learner.RecordObservation("scan_users", 100, 200)

	cost := model.EstimateCost(adaptiveScanPlan{})
	if cost.OutputRows < 110 {
		t.Fatalf("expected corrected output rows, got %.0f", cost.OutputRows)
	}
}
