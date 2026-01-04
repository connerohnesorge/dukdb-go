// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"math/rand"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// Sampler provides methods for sampling rows from query results.
type Sampler struct {
	opts *binder.BoundSampleOptions
	rng  *rand.Rand
}

// NewSampler creates a new Sampler with the given options.
func NewSampler(opts *binder.BoundSampleOptions) *Sampler {
	var rng *rand.Rand
	if opts.Seed != nil {
		rng = rand.New(rand.NewSource(*opts.Seed))
	} else {
		rng = rand.New(rand.NewSource(rand.Int63()))
	}
	return &Sampler{
		opts: opts,
		rng:  rng,
	}
}

// SampleRows applies sampling to a slice of rows based on the configured method.
// For BERNOULLI/SYSTEM: filters rows probabilistically based on percentage.
// For RESERVOIR: returns exactly N rows using reservoir sampling algorithm.
func (s *Sampler) SampleRows(rows []map[string]any) []map[string]any {
	if s.opts == nil {
		return rows
	}

	switch s.opts.Method {
	case parser.SampleBernoulli, parser.SampleSystem:
		return s.sampleBernoulli(rows)
	case parser.SampleReservoir:
		return s.sampleReservoir(rows)
	default:
		return rows
	}
}

// sampleBernoulli implements Bernoulli sampling where each row has an
// independent probability of being included based on the percentage.
// SYSTEM sampling uses the same implementation (block-level sampling
// can use the same approach for simplicity in a pure Go implementation).
func (s *Sampler) sampleBernoulli(rows []map[string]any) []map[string]any {
	if len(rows) == 0 {
		return rows
	}

	probability := s.opts.Percentage / 100.0
	result := make([]map[string]any, 0, int(float64(len(rows))*probability)+1)

	for _, row := range rows {
		if s.rng.Float64() < probability {
			result = append(result, row)
		}
	}

	return result
}

// sampleReservoir implements reservoir sampling to select exactly N rows
// from the input with uniform probability.
//
// Algorithm R (Vitter, 1985):
// 1. Fill reservoir with first N rows
// 2. For each subsequent row i (i > N):
//    - Generate random j in [0, i)
//    - If j < N, replace reservoir[j] with row[i]
// 3. Return reservoir
func (s *Sampler) sampleReservoir(rows []map[string]any) []map[string]any {
	n := s.opts.Rows
	if n <= 0 || len(rows) == 0 {
		return []map[string]any{}
	}

	// If we have fewer rows than requested, return all
	if len(rows) <= n {
		return rows
	}

	// Initialize reservoir with first n rows
	reservoir := make([]map[string]any, n)
	for i := 0; i < n; i++ {
		reservoir[i] = rows[i]
	}

	// Process remaining rows
	for i := n; i < len(rows); i++ {
		// Generate random index in [0, i]
		j := s.rng.Intn(i + 1)
		// If j is in reservoir range, replace
		if j < n {
			reservoir[j] = rows[i]
		}
	}

	return reservoir
}

// executeSample executes a PhysicalSample plan node.
func (e *Executor) executeSample(
	ctx *ExecutionContext,
	plan *planner.PhysicalSample,
) (*ExecutionResult, error) {
	// Execute child plan first
	childResult, err := e.Execute(
		ctx.Context,
		plan.Child,
		ctx.Args,
	)
	if err != nil {
		return nil, err
	}

	// Create sampler with the bound options
	sampler := NewSampler(plan.Sample)

	// Apply sampling to the result rows
	sampledRows := sampler.SampleRows(childResult.Rows)

	return &ExecutionResult{
		Rows:    sampledRows,
		Columns: childResult.Columns,
	}, nil
}
