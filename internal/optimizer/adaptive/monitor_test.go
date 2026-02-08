package adaptive

import "testing"

type testCardinalityObserver struct {
	count int
}

func (t *testCardinalityObserver) RecordObservation(operatorSig string, estimated, actual int64) {
	t.count++
}

type testCostCalibrator struct {
	samples int
}

func (t *testCostCalibrator) RecordCostSample(sample CostSample) {
	t.samples++
}

func TestMonitorAndFeedback(t *testing.T) {
	monitor := NewMonitor(true)
	monitor.StartQuery("q1")
	monitor.StartOperator("q1", "scan", 100, 10)
	monitor.EndOperator("q1", "scan", 120, 12, 1000, 2048, 0)

	stats := monitor.FinishQuery("q1")
	if len(stats.Operators) != 1 {
		t.Fatalf("expected 1 operator")
	}

	observer := &testCardinalityObserver{}
	calibrator := &testCostCalibrator{}
	loop := NewFeedbackLoop(observer, calibrator)
	loop.Apply(stats)

	if observer.count != 1 {
		t.Fatalf("expected 1 cardinality observation")
	}
	if calibrator.samples != 1 {
		t.Fatalf("expected 1 cost sample")
	}
}

func TestReoptimizer(t *testing.T) {
	trigger := NewReoptimizer()
	if !trigger.ShouldReoptimize(100, 2000) {
		t.Fatalf("expected reoptimization trigger")
	}
	if trigger.ShouldReoptimize(100, 150) {
		t.Fatalf("did not expect reoptimization")
	}
}
