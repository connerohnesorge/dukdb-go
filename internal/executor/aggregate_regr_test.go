package executor

import (
	"testing"
)

// regrAlmostEqual is a test helper for floating point comparisons with tolerance
func regrAlmostEqual(a, b float64) bool {
	return almostEqual(a, b)
}

func TestCollectNonNullPairs(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		wantLen int
	}{
		{
			name:    "all valid pairs",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{4.0, 5.0, 6.0},
			wantLen: 3,
		},
		{
			name:    "some nulls in Y",
			yValues: []any{1.0, nil, 3.0},
			xValues: []any{4.0, 5.0, 6.0},
			wantLen: 2,
		},
		{
			name:    "some nulls in X",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{nil, 5.0, 6.0},
			wantLen: 2,
		},
		{
			name:    "nulls in both",
			yValues: []any{nil, 2.0, nil},
			xValues: []any{4.0, nil, 6.0},
			wantLen: 0,
		},
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			wantLen: 0,
		},
		{
			name:    "mixed types",
			yValues: []any{int64(1), float64(2.5), int(3)},
			xValues: []any{int32(4), float32(5.5), int64(6)},
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yFloats, xFloats := collectNonNullPairs(tt.yValues, tt.xValues)
			if len(yFloats) != tt.wantLen || len(xFloats) != tt.wantLen {
				t.Errorf(
					"collectNonNullPairs() got len = (%d, %d), want %d",
					len(yFloats),
					len(xFloats),
					tt.wantLen,
				)
			}
		})
	}
}

func TestComputeCovarPop(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "single value",
			yValues: []any{5.0},
			xValues: []any{3.0},
			want:    0.0, // Covariance of single point is 0
		},
		{
			name:    "perfect positive correlation",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    2.0 / 3.0, // VAR_POP of [1,2,3]
		},
		{
			name:    "perfect negative correlation",
			yValues: []any{3.0, 2.0, 1.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    -2.0 / 3.0,
		},
		{
			name:    "zero covariance",
			yValues: []any{1.0, 1.0, 1.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeCovarPop(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeCovarPop() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeCovarPop() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeCovarPop() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeCovarPop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeCovarSamp(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "single value - needs N>=2",
			yValues: []any{5.0},
			xValues: []any{3.0},
			isNil:   true,
		},
		{
			name:    "two values",
			yValues: []any{1.0, 3.0},
			xValues: []any{1.0, 3.0},
			want:    2.0, // (1-2)(1-2) + (3-2)(3-2) = 2, divided by 1
		},
		{
			name:    "perfect positive correlation",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    1.0, // VAR_SAMP of [1,2,3]
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeCovarSamp(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeCovarSamp() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeCovarSamp() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeCovarSamp() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeCovarSamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeCorr(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "perfect positive correlation",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    1.0,
		},
		{
			name:    "perfect negative correlation",
			yValues: []any{3.0, 2.0, 1.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    -1.0,
		},
		{
			name:    "zero variance in X",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{5.0, 5.0, 5.0},
			isNil:   true,
		},
		{
			name:    "zero variance in Y",
			yValues: []any{5.0, 5.0, 5.0},
			xValues: []any{1.0, 2.0, 3.0},
			isNil:   true,
		},
		{
			name:    "no correlation - orthogonal",
			yValues: []any{1.0, -1.0, 1.0, -1.0},
			xValues: []any{1.0, 1.0, -1.0, -1.0},
			want:    0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeCorr(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeCorr() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeCorr() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeCorr() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeCorr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeRegrSlope(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "slope = 1 (y = x)",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    1.0,
		},
		{
			name:    "slope = 2 (y = 2x)",
			yValues: []any{2.0, 4.0, 6.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    2.0,
		},
		{
			name:    "slope = -1 (y = -x)",
			yValues: []any{3.0, 2.0, 1.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    -1.0,
		},
		{
			name:    "zero variance in X",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{5.0, 5.0, 5.0},
			isNil:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeRegrSlope(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeRegrSlope() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeRegrSlope() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeRegrSlope() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeRegrSlope() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeRegrIntercept(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "intercept = 0 (y = x)",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    0.0,
		},
		{
			name:    "intercept = 1 (y = x + 1)",
			yValues: []any{2.0, 3.0, 4.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    1.0,
		},
		{
			name:    "intercept = 5 (y = -x + 5)",
			yValues: []any{4.0, 3.0, 2.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    5.0,
		},
		{
			name:    "zero variance in X",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{5.0, 5.0, 5.0},
			isNil:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeRegrIntercept(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeRegrIntercept() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeRegrIntercept() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeRegrIntercept() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeRegrIntercept() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeRegrR2(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "perfect fit (R^2 = 1)",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    1.0,
		},
		{
			name:    "perfect negative fit (R^2 = 1)",
			yValues: []any{3.0, 2.0, 1.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    1.0,
		},
		{
			name:    "zero variance in X",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{5.0, 5.0, 5.0},
			isNil:   true,
		},
		{
			name:    "zero variance in Y - perfect fit",
			yValues: []any{5.0, 5.0, 5.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    1.0,
		},
		{
			name:    "no correlation (R^2 = 0)",
			yValues: []any{1.0, -1.0, 1.0, -1.0},
			xValues: []any{1.0, 1.0, -1.0, -1.0},
			want:    0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeRegrR2(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeRegrR2() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeRegrR2() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeRegrR2() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeRegrR2() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeRegrCount(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    int64
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			want:    0,
		},
		{
			name:    "all valid pairs",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{4.0, 5.0, 6.0},
			want:    3,
		},
		{
			name:    "some nulls",
			yValues: []any{1.0, nil, 3.0},
			xValues: []any{4.0, 5.0, nil},
			want:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeRegrCount(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeRegrCount() error = %v", err)
				return
			}
			if got.(int64) != tt.want {
				t.Errorf("computeRegrCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeRegrAvgX(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "simple average",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{2.0, 4.0, 6.0},
			want:    4.0,
		},
		{
			name:    "with null filtering",
			yValues: []any{nil, 2.0, 3.0},
			xValues: []any{2.0, 4.0, 6.0},
			want:    5.0, // (4 + 6) / 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeRegrAvgX(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeRegrAvgX() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeRegrAvgX() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeRegrAvgX() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeRegrAvgX() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeRegrAvgY(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "simple average",
			yValues: []any{2.0, 4.0, 6.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    4.0,
		},
		{
			name:    "with null filtering",
			yValues: []any{2.0, 4.0, 6.0},
			xValues: []any{nil, 2.0, 3.0},
			want:    5.0, // (4 + 6) / 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeRegrAvgY(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeRegrAvgY() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeRegrAvgY() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeRegrAvgY() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeRegrAvgY() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeRegrSXX(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "sum of squares",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    2.0, // (1-2)^2 + (2-2)^2 + (3-2)^2 = 1 + 0 + 1 = 2
		},
		{
			name:    "zero variance",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{5.0, 5.0, 5.0},
			want:    0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeRegrSXX(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeRegrSXX() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeRegrSXX() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeRegrSXX() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeRegrSXX() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeRegrSYY(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "sum of squares",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    2.0, // (1-2)^2 + (2-2)^2 + (3-2)^2 = 1 + 0 + 1 = 2
		},
		{
			name:    "zero variance",
			yValues: []any{5.0, 5.0, 5.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeRegrSYY(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeRegrSYY() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeRegrSYY() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeRegrSYY() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeRegrSYY() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComputeRegrSXY(t *testing.T) {
	tests := []struct {
		name    string
		yValues []any
		xValues []any
		want    any
		isNil   bool
	}{
		{
			name:    "empty input",
			yValues: []any{},
			xValues: []any{},
			isNil:   true,
		},
		{
			name:    "sum of cross-products - same values",
			yValues: []any{1.0, 2.0, 3.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    2.0, // (1-2)(1-2) + (2-2)(2-2) + (3-2)(3-2) = 1 + 0 + 1 = 2
		},
		{
			name:    "sum of cross-products - negative",
			yValues: []any{3.0, 2.0, 1.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    -2.0, // (3-2)(1-2) + (2-2)(2-2) + (1-2)(3-2) = -1 + 0 + (-1) = -2
		},
		{
			name:    "zero covariance",
			yValues: []any{5.0, 5.0, 5.0},
			xValues: []any{1.0, 2.0, 3.0},
			want:    0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeRegrSXY(tt.yValues, tt.xValues)
			if err != nil {
				t.Errorf("computeRegrSXY() error = %v", err)
				return
			}
			if tt.isNil {
				if got != nil {
					t.Errorf("computeRegrSXY() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("computeRegrSXY() = nil, want %v", tt.want)
				return
			}
			if !regrAlmostEqual(got.(float64), tt.want.(float64)) {
				t.Errorf("computeRegrSXY() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRegressionFormulas verifies that the regression formulas are consistent
// For a perfect linear relationship y = mx + b:
// - REGR_SLOPE should return m
// - REGR_INTERCEPT should return b
// - REGR_R2 should return 1.0
// - REGR_SXY / REGR_SXX should equal REGR_SLOPE
func TestRegressionFormulas(t *testing.T) {
	// y = 2x + 3
	xValues := []any{1.0, 2.0, 3.0, 4.0, 5.0}
	yValues := []any{5.0, 7.0, 9.0, 11.0, 13.0} // 2*1+3, 2*2+3, 2*3+3, 2*4+3, 2*5+3

	slope, _ := computeRegrSlope(yValues, xValues)
	if !regrAlmostEqual(slope.(float64), 2.0) {
		t.Errorf("slope = %v, want 2.0", slope)
	}

	intercept, _ := computeRegrIntercept(yValues, xValues)
	if !regrAlmostEqual(intercept.(float64), 3.0) {
		t.Errorf("intercept = %v, want 3.0", intercept)
	}

	r2, _ := computeRegrR2(yValues, xValues)
	if !regrAlmostEqual(r2.(float64), 1.0) {
		t.Errorf("r2 = %v, want 1.0", r2)
	}

	sxy, _ := computeRegrSXY(yValues, xValues)
	sxx, _ := computeRegrSXX(yValues, xValues)
	calculatedSlope := sxy.(float64) / sxx.(float64)
	if !regrAlmostEqual(calculatedSlope, 2.0) {
		t.Errorf("sxy/sxx = %v, want 2.0", calculatedSlope)
	}

	// Verify covariance relationship: COVAR_POP = SXY / N
	covarPop, _ := computeCovarPop(yValues, xValues)
	expectedCovar := sxy.(float64) / float64(len(xValues))
	if !regrAlmostEqual(covarPop.(float64), expectedCovar) {
		t.Errorf("covarPop = %v, want %v", covarPop, expectedCovar)
	}
}

// TestRegrNullHandling verifies that NULL values are properly skipped in regression functions
func TestRegrNullHandling(t *testing.T) {
	// Include some NULLs - only pairs (2,4), (3,6) should be used
	yValues := []any{nil, 2.0, 3.0, nil, nil}
	xValues := []any{1.0, 4.0, 6.0, 7.0, nil}

	count, _ := computeRegrCount(yValues, xValues)
	if count.(int64) != 2 {
		t.Errorf("count = %v, want 2", count)
	}

	avgX, _ := computeRegrAvgX(yValues, xValues)
	if !regrAlmostEqual(avgX.(float64), 5.0) { // (4+6)/2 = 5
		t.Errorf("avgX = %v, want 5.0", avgX)
	}

	avgY, _ := computeRegrAvgY(yValues, xValues)
	if !regrAlmostEqual(avgY.(float64), 2.5) { // (2+3)/2 = 2.5
		t.Errorf("avgY = %v, want 2.5", avgY)
	}
}
