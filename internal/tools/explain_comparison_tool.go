// Package tools provides utility tools for testing and development.
//
// # EXPLAIN Comparison Tool
//
// Task 9.14: Compares EXPLAIN output between dukdb-go and DuckDB to validate
// query planning behavior matches expectations. This tool parses EXPLAIN plans
// and performs structural comparison.
package tools

import (
	"fmt"
	"strings"
)

// ExplainPlanNode represents a node in an EXPLAIN plan
type ExplainPlanNode struct {
	Name         string
	Properties   map[string]string
	Children     []*ExplainPlanNode
	RowEstimate  int64
	CostEstimate float64
}

// ExplainComparisonReport contains results of comparing two EXPLAIN plans
type ExplainComparisonReport struct {
	Query                 string
	DukdbPlan             *ExplainPlanNode
	DuckdbPlan            *ExplainPlanNode
	StructureMatches      bool
	CostDifference        float64
	CardinalityDifference float64
	Differences           []string
	Summary               string
}

// ExplainComparator compares EXPLAIN plans from two sources
type ExplainComparator struct {
	targetCostVariance float64 // Acceptable variance in cost estimates (e.g., 0.2 = 20%)
}

// NewExplainComparator creates a new EXPLAIN comparison tool
// targetCostVariance specifies acceptable cost difference (e.g., 0.2 for 20%)
func NewExplainComparator(targetCostVariance float64) *ExplainComparator {
	return &ExplainComparator{
		targetCostVariance: targetCostVariance,
	}
}

// ComparePlans compares two EXPLAIN plans and returns a detailed report
func (ec *ExplainComparator) ComparePlans(
	dukdbPlan, duckdbPlan *ExplainPlanNode,
) *ExplainComparisonReport {
	report := &ExplainComparisonReport{
		DukdbPlan:   dukdbPlan,
		DuckdbPlan:  duckdbPlan,
		Differences: []string{},
	}

	// Check structure match
	report.StructureMatches = ec.compareStructure(dukdbPlan, duckdbPlan)

	// Check cost estimates
	if dukdbPlan != nil && duckdbPlan != nil {
		report.CostDifference = ec.calculateCostDifference(
			dukdbPlan.CostEstimate,
			duckdbPlan.CostEstimate,
		)
		report.CardinalityDifference = ec.calculateCardinalityDifference(
			dukdbPlan.RowEstimate,
			duckdbPlan.RowEstimate,
		)
	}

	// Generate summary
	report.Summary = ec.generateSummary(report)

	return report
}

// compareStructure recursively compares the structure of two plans
func (ec *ExplainComparator) compareStructure(node1, node2 *ExplainPlanNode) bool {
	// Check if both are nil
	if node1 == nil && node2 == nil {
		return true
	}

	// Check if one is nil
	if node1 == nil || node2 == nil {
		return false
	}

	// Check operator names match
	if !strings.EqualFold(ec.normalizeOperatorName(node1.Name),
		ec.normalizeOperatorName(node2.Name)) {
		return false
	}

	// Check number of children match
	if len(node1.Children) != len(node2.Children) {
		return false
	}

	// Recursively check children
	for i := range node1.Children {
		if !ec.compareStructure(node1.Children[i], node2.Children[i]) {
			return false
		}
	}

	return true
}

// normalizeOperatorName normalizes operator names for comparison
func (ec *ExplainComparator) normalizeOperatorName(name string) string {
	// Remove spaces and convert to lowercase for comparison
	return strings.ToLower(strings.TrimSpace(name))
}

// calculateCostDifference calculates the relative difference between two costs
func (ec *ExplainComparator) calculateCostDifference(cost1, cost2 float64) float64 {
	if cost2 == 0 {
		return 0
	}
	return (cost1 - cost2) / cost2
}

// calculateCardinalityDifference calculates the relative difference between two cardinalities
func (ec *ExplainComparator) calculateCardinalityDifference(card1, card2 int64) float64 {
	if card2 == 0 {
		if card1 == 0 {
			return 0
		}
		return float64(card1)
	}
	diff := float64(card1-card2) / float64(card2)
	if diff < 0 {
		diff = -diff
	}
	return diff
}

// generateSummary generates a human-readable summary of the comparison
func (ec *ExplainComparator) generateSummary(report *ExplainComparisonReport) string {
	var sb strings.Builder

	sb.WriteString("EXPLAIN Plan Comparison Report\n")
	sb.WriteString("===============================\n")

	if report.StructureMatches {
		sb.WriteString("Structure: MATCH - Plan structures are equivalent\n")
	} else {
		sb.WriteString("Structure: MISMATCH - Plan structures differ\n")
	}

	// Cost analysis
	costWithinTolerance := report.CostDifference <= ec.targetCostVariance
	costStatus := "PASS"
	if !costWithinTolerance {
		costStatus = "FAIL"
	}
	sb.WriteString(
		fmt.Sprintf(
			"Cost Difference: %s (%.2f%% difference)\n",
			costStatus,
			report.CostDifference*100,
		),
	)

	// Cardinality analysis
	cardWithinTolerance := report.CardinalityDifference <= ec.targetCostVariance
	cardStatus := "PASS"
	if !cardWithinTolerance {
		cardStatus = "FAIL"
	}
	sb.WriteString(
		fmt.Sprintf(
			"Cardinality Difference: %s (%.2f%% difference)\n",
			cardStatus,
			report.CardinalityDifference*100,
		),
	)

	// Detailed differences
	if len(report.Differences) > 0 {
		sb.WriteString("\nDetailed Differences:\n")
		for _, diff := range report.Differences {
			sb.WriteString(fmt.Sprintf("  - %s\n", diff))
		}
	}

	return sb.String()
}

// FormatPlan formats a plan for display
func (ec *ExplainComparator) FormatPlan(node *ExplainPlanNode, indent string) string {
	if node == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(indent)
	sb.WriteString(node.Name)

	// Add properties if any
	if len(node.Properties) > 0 {
		sb.WriteString(" [")
		first := true
		for k, v := range node.Properties {
			if !first {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s=%s", k, v))
			first = false
		}
		sb.WriteString("]")
	}

	// Add estimates
	if node.RowEstimate > 0 {
		sb.WriteString(fmt.Sprintf(" (rows: %d, cost: %.2f)", node.RowEstimate, node.CostEstimate))
	}

	sb.WriteString("\n")

	// Format children
	for _, child := range node.Children {
		sb.WriteString(ec.FormatPlan(child, indent+"  "))
	}

	return sb.String()
}
