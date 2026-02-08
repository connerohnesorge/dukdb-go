package stats

import "testing"

func TestHyperLogLogEstimate(t *testing.T) {
	sketch := NewHyperLogLog(10)
	for i := 0; i < 1000; i++ {
		sketch.AddString(string(rune(i)))
	}

	estimate := sketch.Estimate()
	if estimate < 800 || estimate > 1200 {
		t.Fatalf("estimate out of range: %d", estimate)
	}
}
