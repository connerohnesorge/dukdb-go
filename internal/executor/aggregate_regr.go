// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"math"
)

// Regression and correlation aggregate computation functions.
// These functions implement regression analysis operations for dukdb-go.
// All functions handle NULL values by considering only pairs where BOTH values are non-NULL.
// Note: These functions take (Y, X) arguments where Y is the dependent variable and X is independent.

// collectNonNullPairs filters pairs where either value is NULL and converts to float64 slices.
// Returns two float64 slices of equal length containing only valid pairs.
func collectNonNullPairs(yValues []any, xValues []any) ([]float64, []float64) {
	if len(yValues) != len(xValues) {
		return nil, nil
	}

	yResult := make([]float64, 0, len(yValues))
	xResult := make([]float64, 0, len(xValues))

	for i := 0; i < len(yValues); i++ {
		yFloat, yOk := toFloat64ForStats(yValues[i])
		xFloat, xOk := toFloat64ForStats(xValues[i])

		// Only include pairs where BOTH values are non-NULL
		if yOk && xOk {
			yResult = append(yResult, yFloat)
			xResult = append(xResult, xFloat)
		}
	}

	return yResult, xResult
}

// computeCovarPop calculates population covariance of two variables.
// COVAR_POP(Y, X) = SUM((x - avgX)(y - avgY)) / N
// Returns nil for empty input.
func computeCovarPop(yValues []any, xValues []any) (any, error) {
	yFloats, xFloats := collectNonNullPairs(yValues, xValues)
	n := len(yFloats)
	if n == 0 {
		return nil, nil
	}

	// Calculate means
	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += xFloats[i]
		sumY += yFloats[i]
	}
	avgX := sumX / float64(n)
	avgY := sumY / float64(n)

	// Calculate covariance
	var covar float64
	for i := 0; i < n; i++ {
		covar += (xFloats[i] - avgX) * (yFloats[i] - avgY)
	}

	return covar / float64(n), nil
}

// computeCovarSamp calculates sample covariance of two variables.
// COVAR_SAMP(Y, X) = SUM((x - avgX)(y - avgY)) / (N - 1)
// Returns nil for empty input or when N < 2.
func computeCovarSamp(yValues []any, xValues []any) (any, error) {
	yFloats, xFloats := collectNonNullPairs(yValues, xValues)
	n := len(yFloats)
	if n < 2 {
		return nil, nil
	}

	// Calculate means
	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += xFloats[i]
		sumY += yFloats[i]
	}
	avgX := sumX / float64(n)
	avgY := sumY / float64(n)

	// Calculate covariance
	var covar float64
	for i := 0; i < n; i++ {
		covar += (xFloats[i] - avgX) * (yFloats[i] - avgY)
	}

	return covar / float64(n-1), nil
}

// computeCorr calculates the Pearson correlation coefficient.
// CORR(Y, X) = COVAR_POP(Y, X) / (STDDEV_POP(Y) * STDDEV_POP(X))
// Returns nil for empty input or when either variable has zero variance.
func computeCorr(yValues []any, xValues []any) (any, error) {
	yFloats, xFloats := collectNonNullPairs(yValues, xValues)
	n := len(yFloats)
	if n == 0 {
		return nil, nil
	}

	// Calculate means
	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += xFloats[i]
		sumY += yFloats[i]
	}
	avgX := sumX / float64(n)
	avgY := sumY / float64(n)

	// Calculate covariance and variances
	var covar, varX, varY float64
	for i := 0; i < n; i++ {
		diffX := xFloats[i] - avgX
		diffY := yFloats[i] - avgY
		covar += diffX * diffY
		varX += diffX * diffX
		varY += diffY * diffY
	}

	// Check for zero variance (division by zero)
	if varX == 0 || varY == 0 {
		return nil, nil
	}

	// Correlation = covariance / (stddev_x * stddev_y)
	// = (sum((x-avgX)(y-avgY))/N) / (sqrt(sum((x-avgX)^2)/N) * sqrt(sum((y-avgY)^2)/N))
	// = sum((x-avgX)(y-avgY)) / sqrt(sum((x-avgX)^2) * sum((y-avgY)^2))
	correlation := covar / math.Sqrt(varX*varY)

	return correlation, nil
}

// computeRegrSlope calculates the slope of the least-squares regression line.
// REGR_SLOPE(Y, X) = COVAR_POP(Y, X) / VAR_POP(X)
// Returns nil for empty input or when X has zero variance.
func computeRegrSlope(yValues []any, xValues []any) (any, error) {
	yFloats, xFloats := collectNonNullPairs(yValues, xValues)
	n := len(yFloats)
	if n == 0 {
		return nil, nil
	}

	// Calculate means
	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += xFloats[i]
		sumY += yFloats[i]
	}
	avgX := sumX / float64(n)
	avgY := sumY / float64(n)

	// Calculate covariance and variance of X
	var covar, varX float64
	for i := 0; i < n; i++ {
		diffX := xFloats[i] - avgX
		diffY := yFloats[i] - avgY
		covar += diffX * diffY
		varX += diffX * diffX
	}

	// Check for zero variance (division by zero)
	if varX == 0 {
		return nil, nil
	}

	// REGR_SLOPE = COVAR_POP / VAR_POP(X)
	// = (sum((x-avgX)(y-avgY))/N) / (sum((x-avgX)^2)/N)
	// = sum((x-avgX)(y-avgY)) / sum((x-avgX)^2)
	slope := covar / varX

	return slope, nil
}

// computeRegrIntercept calculates the y-intercept of the least-squares regression line.
// REGR_INTERCEPT(Y, X) = AVG(Y) - REGR_SLOPE(Y, X) * AVG(X)
// Returns nil for empty input or when X has zero variance.
func computeRegrIntercept(yValues []any, xValues []any) (any, error) {
	yFloats, xFloats := collectNonNullPairs(yValues, xValues)
	n := len(yFloats)
	if n == 0 {
		return nil, nil
	}

	// Calculate means
	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += xFloats[i]
		sumY += yFloats[i]
	}
	avgX := sumX / float64(n)
	avgY := sumY / float64(n)

	// Calculate covariance and variance of X for slope
	var covar, varX float64
	for i := 0; i < n; i++ {
		diffX := xFloats[i] - avgX
		diffY := yFloats[i] - avgY
		covar += diffX * diffY
		varX += diffX * diffX
	}

	// Check for zero variance (division by zero)
	if varX == 0 {
		return nil, nil
	}

	slope := covar / varX
	intercept := avgY - slope*avgX

	return intercept, nil
}

// computeRegrR2 calculates the coefficient of determination (R-squared).
// REGR_R2(Y, X) = CORR(Y, X)^2
// Returns nil for empty input or when either variable has zero variance.
// Special case: when VAR_POP(Y) = 0 and VAR_POP(X) > 0, returns 1.0
// When VAR_POP(X) = 0, returns nil.
func computeRegrR2(yValues []any, xValues []any) (any, error) {
	yFloats, xFloats := collectNonNullPairs(yValues, xValues)
	n := len(yFloats)
	if n == 0 {
		return nil, nil
	}

	// Calculate means
	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += xFloats[i]
		sumY += yFloats[i]
	}
	avgX := sumX / float64(n)
	avgY := sumY / float64(n)

	// Calculate covariance and variances
	var covar, varX, varY float64
	for i := 0; i < n; i++ {
		diffX := xFloats[i] - avgX
		diffY := yFloats[i] - avgY
		covar += diffX * diffY
		varX += diffX * diffX
		varY += diffY * diffY
	}

	// If VAR_POP(X) = 0, return nil
	if varX == 0 {
		return nil, nil
	}

	// If VAR_POP(Y) = 0 but VAR_POP(X) > 0, return 1.0
	// (all Y values are the same, perfect fit)
	if varY == 0 {
		return 1.0, nil
	}

	// R^2 = CORR^2
	correlation := covar / math.Sqrt(varX*varY)
	r2 := correlation * correlation

	return r2, nil
}

// computeRegrCount calculates the number of non-NULL pairs.
// REGR_COUNT(Y, X) = count of pairs where both Y and X are non-NULL
// Returns 0 for empty input.
func computeRegrCount(yValues []any, xValues []any) (any, error) {
	yFloats, _ := collectNonNullPairs(yValues, xValues)
	return int64(len(yFloats)), nil
}

// computeRegrAvgX calculates the average of X values in non-NULL pairs.
// REGR_AVGX(Y, X) = AVG(X) for pairs where both Y and X are non-NULL
// Returns nil for empty input.
func computeRegrAvgX(yValues []any, xValues []any) (any, error) {
	_, xFloats := collectNonNullPairs(yValues, xValues)
	n := len(xFloats)
	if n == 0 {
		return nil, nil
	}

	var sum float64
	for _, x := range xFloats {
		sum += x
	}

	return sum / float64(n), nil
}

// computeRegrAvgY calculates the average of Y values in non-NULL pairs.
// REGR_AVGY(Y, X) = AVG(Y) for pairs where both Y and X are non-NULL
// Returns nil for empty input.
func computeRegrAvgY(yValues []any, xValues []any) (any, error) {
	yFloats, _ := collectNonNullPairs(yValues, xValues)
	n := len(yFloats)
	if n == 0 {
		return nil, nil
	}

	var sum float64
	for _, y := range yFloats {
		sum += y
	}

	return sum / float64(n), nil
}

// computeRegrSXX calculates the sum of squares of the independent variable.
// REGR_SXX(Y, X) = SUM((X - AVG(X))^2) = N * VAR_POP(X)
// Returns nil for empty input.
func computeRegrSXX(yValues []any, xValues []any) (any, error) {
	_, xFloats := collectNonNullPairs(yValues, xValues)
	n := len(xFloats)
	if n == 0 {
		return nil, nil
	}

	// Calculate mean of X
	var sumX float64
	for _, x := range xFloats {
		sumX += x
	}
	avgX := sumX / float64(n)

	// Calculate sum of squared deviations
	var sxx float64
	for _, x := range xFloats {
		diff := x - avgX
		sxx += diff * diff
	}

	return sxx, nil
}

// computeRegrSYY calculates the sum of squares of the dependent variable.
// REGR_SYY(Y, X) = SUM((Y - AVG(Y))^2) = N * VAR_POP(Y)
// Returns nil for empty input.
func computeRegrSYY(yValues []any, xValues []any) (any, error) {
	yFloats, _ := collectNonNullPairs(yValues, xValues)
	n := len(yFloats)
	if n == 0 {
		return nil, nil
	}

	// Calculate mean of Y
	var sumY float64
	for _, y := range yFloats {
		sumY += y
	}
	avgY := sumY / float64(n)

	// Calculate sum of squared deviations
	var syy float64
	for _, y := range yFloats {
		diff := y - avgY
		syy += diff * diff
	}

	return syy, nil
}

// computeRegrSXY calculates the sum of cross-products of deviations.
// REGR_SXY(Y, X) = SUM((X - AVG(X)) * (Y - AVG(Y))) = N * COVAR_POP(Y, X)
// Returns nil for empty input.
func computeRegrSXY(yValues []any, xValues []any) (any, error) {
	yFloats, xFloats := collectNonNullPairs(yValues, xValues)
	n := len(yFloats)
	if n == 0 {
		return nil, nil
	}

	// Calculate means
	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += xFloats[i]
		sumY += yFloats[i]
	}
	avgX := sumX / float64(n)
	avgY := sumY / float64(n)

	// Calculate sum of cross-products
	var sxy float64
	for i := 0; i < n; i++ {
		sxy += (xFloats[i] - avgX) * (yFloats[i] - avgY)
	}

	return sxy, nil
}
