package stats

import "testing"

func TestHistogramSelectivity(t *testing.T) {
	hist := NewHistogram(10)
	for i := 0; i < 100; i++ {
		hist.Observe(float64(i))
	}

	sel := hist.EstimateLessThan(50)
	if sel < 0.4 || sel > 0.6 {
		t.Fatalf("unexpected selectivity %.2f", sel)
	}

	between := hist.EstimateBetween(20, 40)
	if between < 0.15 || between > 0.35 {
		t.Fatalf("unexpected between selectivity %.2f", between)
	}
}
