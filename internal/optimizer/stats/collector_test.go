package stats

import "testing"

func TestCollectorSnapshot(t *testing.T) {
	collector := NewCollector(CollectorOptions{HistogramBuckets: 8, HLLPrecision: 10, Enabled: true})
	collector.RecordRow("orders", map[string]any{"id": 1, "status": "open"})
	collector.RecordRow("orders", map[string]any{"id": 2, "status": "closed"})
	collector.RecordRow("orders", map[string]any{"id": 3, "status": nil})

	snapshot := collector.Snapshot("orders")
	if snapshot.RowCount != 3 {
		t.Fatalf("expected 3 rows, got %d", snapshot.RowCount)
	}

	status := snapshot.Columns["status"]
	if status.NullFraction == 0 {
		t.Fatalf("expected non-zero null fraction")
	}
	if status.Distinct == 0 {
		t.Fatalf("expected distinct estimate")
	}

	id := snapshot.Columns["id"]
	if !id.HasMinMax {
		t.Fatalf("expected min/max for id")
	}
	if id.Min != 1 || id.Max != 3 {
		t.Fatalf("unexpected min/max %.0f %.0f", id.Min, id.Max)
	}
}
