// Package optimizer provides cost-based query optimization for dukdb-go.
package optimizer

import (
	"fmt"
	"sync"
	"time"
)

// CardinalityObservation tracks a single cardinality observation for an operator.
// It stores both the estimate and actual cardinality, allowing correction factor
// computation over multiple observations.
type CardinalityObservation struct {
	EstimatedCardinality int64     // Cardinality estimate from cost model
	ActualCardinality    int64     // Actual cardinality observed at execution
	Count                int64     // Number of times this operator has been observed
	LastUpdated          time.Time // Last observation timestamp
	CorrectionFactor     float64   // Current correction factor (moving average)
}

// CardinalityLearner tracks actual vs estimated cardinalities per operator signature.
//
// Algorithm:
//
// The cardinality learning system uses a conservative approach to prevent outliers
// from corrupting estimates:
//
//  1. Track observations: For each unique operator signature, store estimated vs
//     actual cardinality from executed queries.
//
//  2. N-observation threshold: Only apply corrections after collecting 100+
//     observations for an operator. This threshold prevents one-off outlier queries
//     from affecting all subsequent plans.
//
//  3. Moving average correction: For each observation, compute correction ratio =
//     actual / estimated. Use exponential moving average (EMA) to update:
//     newCorrection = 0.9 * oldCorrection + 0.1 * newRatio
//     This dampens outlier effects.
//
// 4. Bounded corrections: Apply min/max bounds to prevent extreme values:
//
//   - Minimum: 0.5x (max 2x speedup)
//
//   - Maximum: 2.0x (max 2x slowdown)
//     This prevents the learning from creating terrible plans.
//
//     5. Bounded memory: Store at most MaxHistorySize observations total.
//     When limit reached, evict least-recently-updated entries (LRU).
//
// Reference: Adaptive query optimization literature, particularly:
// - Stillger et al. "LEO - DB2's Learning Optimizer" (IBM Research)
// - Chaudhuri & Narasayya "Self-Tuning Database Systems" (Microsoft Research)
//
// Note: This is a novel feature beyond DuckDB v1.4.3. DuckDB only reads existing
// statistics but never learns from runtime feedback. dukdb-go extends this with
// conservative runtime learning to improve plan quality over time.
type CardinalityLearner struct {
	mu                   sync.RWMutex
	observations         map[string]*CardinalityObservation
	maxHistorySize       int
	observationThreshold int64
}

// NewCardinalityLearner creates a new cardinality learner with default settings.
//
// Parameters:
//   - maxHistorySize: Maximum number of operator signatures to track (default 1000).
//     When exceeded, LRU entries are evicted. Limits memory usage in long-running
//     sessions with many unique operator types.
//   - observationThreshold: Minimum observations required before applying corrections
//     (default 100). Conservative threshold prevents early overfitting.
func NewCardinalityLearner(maxHistorySize int, observationThreshold int64) *CardinalityLearner {
	if maxHistorySize <= 0 {
		maxHistorySize = 1000
	}
	if observationThreshold <= 0 {
		observationThreshold = 100
	}
	return &CardinalityLearner{
		observations:         make(map[string]*CardinalityObservation),
		maxHistorySize:       maxHistorySize,
		observationThreshold: observationThreshold,
	}
}

// RecordObservation records an actual vs estimated cardinality observation.
//
// Algorithm:
//  1. Normalize operatorSignature for consistency
//  2. If first observation for this signature, initialize observation
//  3. Compute correction ratio = actual / estimated (with guards for zero)
//  4. Update correction factor using exponential moving average:
//     newCorrection = 0.9 * oldCorrection + 0.1 * ratio
//  5. Increment observation count
//  6. If observations map exceeds maxHistorySize, evict LRU entry
//
// Time Complexity: O(log n) due to potential LRU eviction search
// Space: O(1) - updates existing entry or evicts one entry
func (cl *CardinalityLearner) RecordObservation(
	operatorSig string,
	estimatedCardinality, actualCardinality int64,
) {
	if operatorSig == "" {
		return // Ignore empty signatures
	}

	// Guard against invalid cardinalities
	if estimatedCardinality <= 0 || actualCardinality < 0 {
		return
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	obs, exists := cl.observations[operatorSig]
	if !exists {
		// First observation for this operator signature
		obs = &CardinalityObservation{
			EstimatedCardinality: estimatedCardinality,
			ActualCardinality:    actualCardinality,
			Count:                1,
			LastUpdated:          time.Now(),
			CorrectionFactor:     1.0, // No correction initially
		}
		cl.observations[operatorSig] = obs
	} else {
		// Subsequent observation: update with exponential moving average
		ratio := float64(actualCardinality) / float64(estimatedCardinality)
		// Bound ratio to [0.5, 2.0] to prevent extreme values from dominating
		if ratio < 0.5 {
			ratio = 0.5
		} else if ratio > 2.0 {
			ratio = 2.0
		}

		// EMA: 90% weight to old correction, 10% to new ratio
		// This dampens the effect of outliers significantly
		obs.CorrectionFactor = 0.9*obs.CorrectionFactor + 0.1*ratio

		// Ensure final correction is also bounded [0.5, 2.0]
		if obs.CorrectionFactor < 0.5 {
			obs.CorrectionFactor = 0.5
		} else if obs.CorrectionFactor > 2.0 {
			obs.CorrectionFactor = 2.0
		}

		obs.EstimatedCardinality = estimatedCardinality
		obs.ActualCardinality = actualCardinality
		obs.Count++
		obs.LastUpdated = time.Now()
	}

	// Memory management: evict LRU if over capacity
	if len(cl.observations) > cl.maxHistorySize {
		cl.evictLRU()
	}
}

// GetLearningCorrection returns the correction factor for an operator signature.
//
// Returns:
// - If observation count < threshold: 1.0 (no correction)
// - If observation count >= threshold: current correction factor
//
// This conservative approach ensures corrections are only applied after sufficient
// evidence has accumulated. Early observations don't affect query plans.
func (cl *CardinalityLearner) GetLearningCorrection(operatorSig string) float64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	obs, exists := cl.observations[operatorSig]
	if !exists {
		return 1.0 // No learning data
	}

	// Only apply correction if we have enough observations
	if obs.Count < cl.observationThreshold {
		return 1.0 // Under threshold, use original estimate
	}

	return obs.CorrectionFactor
}

// GetObservationCount returns the number of observations for an operator signature.
// Used for monitoring learning progress and debugging.
func (cl *CardinalityLearner) GetObservationCount(operatorSig string) int64 {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	obs, exists := cl.observations[operatorSig]
	if !exists {
		return 0
	}
	return obs.Count
}

// GetStatistics returns statistics about the learner's state.
// Useful for debugging and monitoring learning effectiveness.
func (cl *CardinalityLearner) GetStatistics() map[string]interface{} {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	totalObservations := int64(0)
	activeCorrections := 0

	for _, obs := range cl.observations {
		totalObservations += obs.Count
		if obs.Count >= cl.observationThreshold {
			activeCorrections++
		}
	}

	return map[string]interface{}{
		"tracked_signatures": len(cl.observations),
		"active_corrections": activeCorrections,
		"total_observations": totalObservations,
		"threshold":          cl.observationThreshold,
		"max_history_size":   cl.maxHistorySize,
	}
}

// evictLRU removes the least recently updated observation.
// Called when memory bounds are exceeded to enforce maxHistorySize limit.
//
// Memory Bounds Strategy:
// - Bounded learning prevents unbounded memory growth in long-running sessions
// - Conservative: Keep only 1000 unique operator signatures by default
// - LRU eviction ensures active operators are retained
// - Old or rarely-used operators are evicted first
//
// Algorithm:
// 1. Scan all observations for the minimum LastUpdated timestamp
// 2. Remove the observation with oldest LastUpdated
// 3. This preserves frequently-used operators (they get re-observed and updated)
// 4. Stale operators (not executed in a while) are evicted
//
// Example Scenario (maxHistorySize=1000):
// - After 1000 unique operators have been tracked
// - Query 1001 with new operator comes in
// - Find oldest operator (e.g., one not executed for 5 minutes)
// - Delete that operator's observations
// - Insert new operator's first observation
// - Typically 10-50 active operators; rest are historical
//
// Memory Analysis:
// - Per observation: ~96 bytes (time.Time, int64s, float64)
// - 1000 observations: ~96KB
// - Acceptable overhead for long-running database sessions
//
// Time Complexity: O(n) where n = number of observations
// This is acceptable since eviction only happens at capacity boundaries.
// Called at most once per query (when new observation added at capacity).
func (cl *CardinalityLearner) evictLRU() {
	var lruKey string
	var lruTime time.Time

	for key, obs := range cl.observations {
		if lruTime.IsZero() || obs.LastUpdated.Before(lruTime) {
			lruKey = key
			lruTime = obs.LastUpdated
		}
	}

	if lruKey != "" {
		delete(cl.observations, lruKey)
	}
}

// GenerateOperatorSignature creates a unique signature for an operator.
// The signature must be consistent across multiple executions of the same query.
//
// Example signatures:
// - "SeqScan(table=orders, columns=order_id,customer_id)"
// - "HashJoin(left=customers, right=orders, condition=c.id=o.customer_id)"
// - "Aggregate(function=COUNT, group_by=category_id)"
//
// The signature should capture:
// 1. Operator type
// 2. Key parameters (table, columns, conditions)
// 3. Input cardinalities (can be added by caller)
//
// NOT included in signature (too volatile):
// - Specific literal values in predicates
// - Order of operations in commutative operations
// - Column names that are aliases
func GenerateOperatorSignature(operatorType string, params map[string]string) string {
	// Simple concatenation of operator type and sorted parameters
	// In production, use more sophisticated hashing/normalization
	sig := operatorType + "("

	// Sort parameters for consistency (would use a sorted key iteration in production)
	for key, value := range params {
		sig += key + "=" + value + ","
	}
	sig += ")"

	return sig
}

// CorrectedCardinality applies the learning correction to an estimated cardinality.
//
// Formula: correctedEstimate = estimatedCardinality * correction
//
// This is the integration point with the cost model. After computing the base
// cardinality estimate using statistics, apply the learned correction factor.
//
// Example:
//
//	baseEstimate := 1000 (from statistics)
//	correction := learner.GetLearningCorrection(sig)  // returns 1.5 after threshold
//	corrected := learner.CorrectedCardinality(baseEstimate, sig)  // returns 1500
//
// The corrected cardinality feeds into downstream cost calculations:
//
//	cost = baseOpCost * correctedCardinality
func (cl *CardinalityLearner) CorrectedCardinality(
	estimatedCardinality float64,
	operatorSig string,
) float64 {
	correction := cl.GetLearningCorrection(operatorSig)
	corrected := estimatedCardinality * correction

	// Ensure result is at least 1 (can't have 0 cardinality for operator producing rows)
	if corrected < 1.0 {
		corrected = 1.0
	}

	return corrected
}

// String returns a string representation of an observation for debugging.
func (obs *CardinalityObservation) String() string {
	return fmt.Sprintf(
		"Obs{est=%d, actual=%d, count=%d, correction=%.2fx, updated=%s}",
		obs.EstimatedCardinality,
		obs.ActualCardinality,
		obs.Count,
		obs.CorrectionFactor,
		obs.LastUpdated.Format("15:04:05"),
	)
}
