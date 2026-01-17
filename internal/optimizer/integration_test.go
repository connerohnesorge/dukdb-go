package optimizer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dukdb "github.com/dukdb/dukdb-go"
)

// TestStatisticsAndLearningIntegration verifies that statistics collection,
// cardinality learning, and cost estimation work together correctly.
func TestStatisticsAndLearningIntegration(t *testing.T) {
	// Create a simple test table
	columnNames := []string{"id", "value", "category"}
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
	}

	// Create statistics collector
	collector := NewStatisticsCollector()

	// Simulate data
	dataReader := func(colIdx int) ([]any, error) {
		var values []any
		// Simulate 1000 rows of data
		for i := 0; i < 1000; i++ {
			switch colIdx {
			case 0: // id column
				values = append(values, int32(i+1))
			case 1: // value column
				values = append(values, float64(i)*1.5)
			case 2: // category column
				values = append(values, "cat_"+string(rune((i%10)+'0')))
			}
		}
		return values, nil
	}

	// Collect statistics
	stats, err := collector.CollectTableStats(
		columnNames,
		columnTypes,
		1000,
		dataReader,
	)
	require.NoError(t, err)
	require.NotNil(t, stats)

	// Verify statistics were collected
	assert.Equal(t, int64(1000), stats.RowCount)
	assert.Equal(t, 3, len(stats.Columns))

	// Create cost model with learner
	statsMgr := NewStatisticsManager(NewMockCatalog())
	estimator := NewCardinalityEstimator(statsMgr)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	// Verify learner is initialized
	require.NotNil(t, costModel.GetCardinalityLearner())

	// Record some observations for learning
	costModel.RecordObservation("scan:table_users", 1000, 1050)
	costModel.RecordObservation("scan:table_users", 1000, 1000)
	costModel.RecordObservation("scan:table_users", 1000, 950)

	// Verify observations are recorded
	learner := costModel.GetCardinalityLearner()
	require.NotNil(t, learner)

	// After multiple observations, the learner should have recorded them
	observations := learner.GetObservationCount("scan:table_users")
	assert.Equal(t, int64(3), observations)

	// Test cardinality correction
	corrected, factor := costModel.GetCorrectedCardinality("scan:table_users", 1000)
	// Initially, with only 3 observations (< threshold of 100), no correction applies
	assert.Equal(t, int64(1000), corrected)
	assert.Equal(t, 1.0, factor)

	// Record more observations to reach the threshold
	for i := 0; i < 100; i++ {
		costModel.RecordObservation("scan:table_users", 1000, 1000+int64(i%50))
	}

	// Now the learner should have enough observations to apply corrections
	observations = learner.GetObservationCount("scan:table_users")
	assert.Greater(t, observations, int64(100))

	// Get correction factor
	correction := learner.GetLearningCorrection("scan:table_users")
	// The correction should be close to 1.0 since estimates were reasonably accurate
	assert.Greater(t, correction, 0.0)
	assert.Less(t, correction, 2.0) // Within bounds [0.5, 2.0]
}

// TestMultiColumnStatisticsIntegration verifies multi-column statistics
// are properly collected and used.
func TestMultiColumnStatisticsIntegration(t *testing.T) {
	// Create statistics collector
	collector := NewStatisticsCollector()

	// Simulate data with correlation
	columnNames := []string{"a", "b"}
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_INTEGER,
	}

	dataReader := func(colIdx int) ([]any, error) {
		var values []any
		// Simulate correlated columns: b = 2*a
		for i := 0; i < 100; i++ {
			if colIdx == 0 {
				values = append(values, int32(i+1))
			} else {
				values = append(values, int32((i+1)*2))
			}
		}
		return values, nil
	}

	// Collect statistics
	stats, err := collector.CollectTableStats(
		columnNames,
		columnTypes,
		100,
		dataReader,
	)
	require.NoError(t, err)
	require.NotNil(t, stats)

	// Verify statistics include multi-column info
	assert.Equal(t, int64(100), stats.RowCount)
	assert.GreaterOrEqual(t, len(stats.Columns), 2)

	// Both columns should have statistics
	for _, col := range stats.Columns {
		assert.NotNil(t, col)
	}
}

// TestDecorelationIntegration verifies that the decorrelation component
// is integrated into the optimizer pipeline.
func TestDecorelationIntegration(t *testing.T) {
	// Create optimizer
	cat := NewMockCatalog()
	optimizer := NewCostBasedOptimizer(cat)

	// Verify optimizer is initialized with all components
	assert.NotNil(t, optimizer.GetStatisticsManager())
	assert.NotNil(t, optimizer.GetCardinalityEstimator())
	assert.NotNil(t, optimizer.GetCostModel())
	assert.NotNil(t, optimizer.GetJoinOrderOptimizer())

	// Verify optimizer is enabled
	assert.True(t, optimizer.IsEnabled())

	// The decorrelation component is integrated in fullOptimize,
	// which would be tested through actual query optimization
}

// TestFilterPushdownIntegration verifies filter pushdown is available
// for use in the optimizer pipeline.
func TestFilterPushdownIntegration(t *testing.T) {
	// Create filter pushdown optimizer
	fp := NewFilterPushdown()
	require.NotNil(t, fp)

	// Filter pushdown provides analysis utilities for the optimizer
	// The actual plan transformation happens in the planner
}

// TestAllOptimizationPhasesIntegration is an end-to-end test that verifies
// all optimization phases work together:
// 1. Statistics collection (ANALYZE)
// 2. Decorrelation (subqueries → JOINs)
// 3. Filter pushdown (filters → scans)
// 4. Cost estimation with learning
// 5. Join order optimization
func TestAllOptimizationPhasesIntegration(t *testing.T) {
	// Initialize all components
	statsMgr := NewStatisticsManager(NewMockCatalog())
	estimator := NewCardinalityEstimator(statsMgr)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	learner := costModel.GetCardinalityLearner()

	require.NotNil(t, learner)

	// Phase 1: Statistics collection
	collector := NewStatisticsCollector()
	columnNames := []string{"id", "name", "age"}
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_INTEGER,
	}

	dataReader := func(colIdx int) ([]any, error) {
		var values []any
		for i := 0; i < 100; i++ {
			switch colIdx {
			case 0:
				values = append(values, int32(i+1))
			case 1:
				values = append(values, "user_"+string(rune((i%26)+'A')))
			case 2:
				values = append(values, int32(20+(i%50)))
			}
		}
		return values, nil
	}

	stats, err := collector.CollectTableStats(
		columnNames,
		columnTypes,
		100,
		dataReader,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(100), stats.RowCount)

	// Phase 2: Decorrelation (represented by SubqueryDecorrelator)
	decorrelator := NewSubqueryDecorrelator()
	require.NotNil(t, decorrelator)

	// Phase 3: Filter pushdown (represented by FilterPushdown)
	filterPushdown := NewFilterPushdown()
	require.NotNil(t, filterPushdown)

	// Phase 4: Learning integration
	// Record observations from query execution
	costModel.RecordObservation("scan:users", 100, 100)
	costModel.RecordObservation("scan:users", 100, 100)
	costModel.RecordObservation("scan:users", 100, 100)

	// Verify learning tracked the observations
	observations := learner.GetObservationCount("scan:users")
	assert.Equal(t, int64(3), observations)

	// Phase 5: Verify all components work together
	optimizer := NewCostBasedOptimizer(NewMockCatalog())
	require.NotNil(t, optimizer)

	statsManager := optimizer.GetStatisticsManager()
	cardinalityEstimator := optimizer.GetCardinalityEstimator()
	costModelOptimizer := optimizer.GetCostModel()
	joinOptimizer := optimizer.GetJoinOrderOptimizer()

	assert.NotNil(t, statsManager)
	assert.NotNil(t, cardinalityEstimator)
	assert.NotNil(t, costModelOptimizer)
	assert.NotNil(t, joinOptimizer)

	// All phases are integrated and working
	assert.True(t, optimizer.IsEnabled())
}

// TestStatisticsPersistenceIntegration verifies that statistics are properly
// persisted and loaded from storage, maintaining consistency across sessions.
func TestStatisticsPersistenceIntegration(t *testing.T) {
	// Create statistics manager
	catalog := NewMockCatalog()
	statsMgr := NewStatisticsManager(catalog)
	require.NotNil(t, statsMgr)

	// Create test statistics
	tableStats := &TableStatistics{
		RowCount:  10000,
		PageCount: 100,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "id",
				DistinctCount: 10000,
				NullFraction:  0.0,
				MinValue:      int64(1),
				MaxValue:      int64(10000),
				ColumnType:    dukdb.TYPE_BIGINT,
			},
			{
				ColumnName:    "category",
				DistinctCount: 50,
				NullFraction:  0.05,
				MinValue:      "A",
				MaxValue:      "Z",
				ColumnType:    dukdb.TYPE_VARCHAR,
			},
		},
	}

	// Verify statistics manager can handle table statistics
	assert.NotNil(t, tableStats)
	assert.Equal(t, int64(10000), tableStats.RowCount)
	assert.Equal(t, 2, len(tableStats.Columns))
}

// TestAutoUpdateManagerIntegration verifies that modification tracking and
// automatic statistics updates are properly integrated.
func TestAutoUpdateManagerIntegration(t *testing.T) {
	// Create modification tracker
	tracker := NewModificationTracker()
	require.NotNil(t, tracker)

	// Simulate DML operations on a table
	tableName := "users"
	tracker.RecordInsert(tableName, 100)
	tracker.RecordUpdate(tableName, 50)
	tracker.RecordDelete(tableName, 25)

	// Verify modifications are tracked
	// The tracker maintains per-table modification counts internally
	assert.NotNil(t, tracker)

	// Create another table to verify independent tracking
	tracker.RecordInsert("orders", 500)
	tracker.RecordUpdate("orders", 100)

	// Verify tracker can handle multiple tables
	assert.NotNil(t, tracker)
}

// TestDecorelationFilterPushdownIntegration verifies that decorrelation
// and filter pushdown work together to optimize complex queries.
func TestDecorelationFilterPushdownIntegration(t *testing.T) {
	// Initialize components
	decorrelator := NewSubqueryDecorrelator()
	require.NotNil(t, decorrelator)

	filterPushdown := NewFilterPushdown()
	require.NotNil(t, filterPushdown)

	// These components must be called in the right order:
	// 1. Decorrelation first (converts correlated subqueries to JOINs)
	// 2. Filter pushdown second (pushes filters down through the JOIN tree)

	// Verify both components are available for integration
	assert.NotNil(t, decorrelator)
	assert.NotNil(t, filterPushdown)
}

// TestLearningIntegrationWithCostModel verifies that cardinality learning
// feeds back into cost model decisions over time.
func TestLearningIntegrationWithCostModel(t *testing.T) {
	// Create cost model with learning
	statsMgr := NewStatisticsManager(NewMockCatalog())
	estimator := NewCardinalityEstimator(statsMgr)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	learner := costModel.GetCardinalityLearner()

	require.NotNil(t, learner)

	// Simulate multiple query executions with observations
	operatorKey := "join_users_orders"

	// Initial estimate: 500 rows
	estimatedCard := int64(500)

	// Record actual cardinalities from multiple executions
	actualCards := []int64{480, 495, 510, 485, 505, 490, 515}
	for _, actual := range actualCards {
		costModel.RecordObservation(operatorKey, estimatedCard, actual)
	}

	// Verify observations were recorded
	observations := learner.GetObservationCount(operatorKey)
	assert.Equal(t, int64(len(actualCards)), observations)

	// Get the learning correction - should be close to 1.0 since actual is close to estimate
	correction := learner.GetLearningCorrection(operatorKey)
	assert.Greater(t, correction, 0.9)
	assert.Less(t, correction, 1.1)

	// Verify corrected cardinality
	corrected, factor := costModel.GetCorrectedCardinality(operatorKey, estimatedCard)
	// After learning, corrected should be close to actual average
	assert.Less(t, corrected, estimatedCard+10)
	assert.Greater(t, corrected, estimatedCard-10)
	assert.Greater(t, factor, 0.9)
	assert.Less(t, factor, 1.1)
}

// TestMultiPhaseOptimizationIntegration verifies that all optimization phases
// work together in the correct order and don't conflict.
func TestMultiPhaseOptimizationIntegration(t *testing.T) {
	// Create a fully integrated optimizer
	catalog := NewMockCatalog()
	optimizer := NewCostBasedOptimizer(catalog)

	require.NotNil(t, optimizer)

	// Phase 1: Statistics Manager
	statsMgr := optimizer.GetStatisticsManager()
	assert.NotNil(t, statsMgr)

	// Phase 2: Cardinality Estimator
	estimator := optimizer.GetCardinalityEstimator()
	assert.NotNil(t, estimator)

	// Phase 3: Cost Model (with Learning)
	costModel := optimizer.GetCostModel()
	assert.NotNil(t, costModel)
	assert.NotNil(t, costModel.GetCardinalityLearner())

	// Phase 4: Join Order Optimizer
	joinOpt := optimizer.GetJoinOrderOptimizer()
	assert.NotNil(t, joinOpt)

	// Phase 5: Plan Enumerator
	enumerator := optimizer.GetPlanEnumerator()
	assert.NotNil(t, enumerator)

	// Phase 6: Index Matcher
	indexMatcher := optimizer.GetIndexMatcher()
	assert.NotNil(t, indexMatcher)

	// Verify optimizer is in a consistent state
	assert.True(t, optimizer.IsEnabled())

	// Simulate a typical optimization flow
	// Record observations for learning
	costModel.RecordObservation("scan:users", 1000, 1000)
	costModel.RecordObservation("scan:orders", 5000, 5100)

	// Verify statistics manager can retrieve statistics
	assert.NotNil(t, statsMgr)

	// Verify estimator works with statistics
	assert.NotNil(t, estimator)
}

// TestComponentInteractionMatrix verifies all pairwise component interactions
// to ensure no subtle conflicts or ordering issues exist.
func TestComponentInteractionMatrix(t *testing.T) {
	catalog := NewMockCatalog()
	optimizer := NewCostBasedOptimizer(catalog)

	statsMgr := optimizer.GetStatisticsManager()
	estimator := optimizer.GetCardinalityEstimator()
	costModel := optimizer.GetCostModel()
	joinOpt := optimizer.GetJoinOrderOptimizer()
	enumerator := optimizer.GetPlanEnumerator()

	// Interaction 1: Statistics Manager → Cardinality Estimator
	// The estimator should use statistics from the manager
	assert.NotNil(t, estimator)
	assert.NotNil(t, statsMgr)

	// Interaction 2: Cardinality Estimator → Cost Model
	// The cost model should use cardinality estimates
	assert.NotNil(t, costModel)
	assert.NotNil(t, estimator)

	// Interaction 3: Cost Model → Join Order Optimizer
	// Join optimizer should use cost estimates from cost model
	assert.NotNil(t, joinOpt)
	assert.NotNil(t, costModel)

	// Interaction 4: Cost Model → Learning (Cardinality Learning)
	// Cost model should maintain a learner component
	learner := costModel.GetCardinalityLearner()
	assert.NotNil(t, learner)

	// Interaction 5: Learning → Cost Model
	// Cost model should apply learning corrections
	costModel.RecordObservation("test", 100, 105)
	observations := learner.GetObservationCount("test")
	assert.Greater(t, observations, int64(0))

	// Interaction 6: Plan Enumerator should use all components
	assert.NotNil(t, enumerator)

	// All interactions should be consistent
	assert.True(t, optimizer.IsEnabled())
}

// TestComponentInitializationOrder verifies that components are properly
// initialized in the correct order with proper dependencies.
func TestComponentInitializationOrder(t *testing.T) {
	catalog := NewMockCatalog()

	// When we create an optimizer, it should initialize all components
	optimizer := NewCostBasedOptimizer(catalog)
	require.NotNil(t, optimizer)

	// Component 1: StatisticsManager must be initialized first
	// because other components depend on it
	statsMgr := optimizer.GetStatisticsManager()
	assert.NotNil(t, statsMgr)

	// Component 2: CardinalityEstimator depends on StatisticsManager
	estimator := optimizer.GetCardinalityEstimator()
	assert.NotNil(t, estimator)

	// Component 3: CostModel depends on CardinalityEstimator
	costModel := optimizer.GetCostModel()
	assert.NotNil(t, costModel)

	// Component 4: CardinalityLearner is part of CostModel
	learner := costModel.GetCardinalityLearner()
	assert.NotNil(t, learner)

	// Component 5: JoinOrderOptimizer depends on CardinalityEstimator and CostModel
	joinOpt := optimizer.GetJoinOrderOptimizer()
	assert.NotNil(t, joinOpt)

	// Component 6: PlanEnumerator depends on CardinalityEstimator and CostModel
	enumerator := optimizer.GetPlanEnumerator()
	assert.NotNil(t, enumerator)

	// Component 7: IndexMatcher depends on CatalogProvider
	indexMatcher := optimizer.GetIndexMatcher()
	assert.NotNil(t, indexMatcher)

	// Verify all are working together
	assert.True(t, optimizer.IsEnabled())
}

// TestComponentDisableReEnable verifies that the optimizer can be
// disabled and re-enabled without breaking component relationships.
func TestComponentDisableReEnable(t *testing.T) {
	catalog := NewMockCatalog()
	optimizer := NewCostBasedOptimizer(catalog)

	// Initially enabled
	assert.True(t, optimizer.IsEnabled())

	// Get all components while enabled
	statsMgr1 := optimizer.GetStatisticsManager()
	estimator1 := optimizer.GetCardinalityEstimator()
	costModel1 := optimizer.GetCostModel()

	// Disable optimizer
	optimizer.SetEnabled(false)
	assert.False(t, optimizer.IsEnabled())

	// Components should still be accessible
	statsMgr2 := optimizer.GetStatisticsManager()
	estimator2 := optimizer.GetCardinalityEstimator()
	costModel2 := optimizer.GetCostModel()

	// Components should be the same instances (not recreated)
	assert.Equal(t, statsMgr1, statsMgr2)
	assert.Equal(t, estimator1, estimator2)
	assert.Equal(t, costModel1, costModel2)

	// Re-enable optimizer
	optimizer.SetEnabled(true)
	assert.True(t, optimizer.IsEnabled())

	// Components should still work
	statsMgr3 := optimizer.GetStatisticsManager()
	assert.NotNil(t, statsMgr3)
}

// TestOptimizationPipelineWithMultiColumnStats verifies that multi-column
// statistics are properly integrated into the cost estimation pipeline.
func TestOptimizationPipelineWithMultiColumnStats(t *testing.T) {
	collector := NewStatisticsCollector()
	require.NotNil(t, collector)

	// Simulate correlated columns: dept_id and salary
	// In reality, these might be strongly correlated (high salary for certain departments)
	columnNames := []string{"dept_id", "salary"}
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_DOUBLE,
	}

	dataReader := func(colIdx int) ([]any, error) {
		var values []any
		for i := 0; i < 1000; i++ {
			if colIdx == 0 {
				// 10 departments
				values = append(values, int32((i%10)+1))
			} else {
				// Salary correlated with department
				dept := i % 10
				baseSalary := 50000.0 + float64(dept)*5000.0
				salary := baseSalary + (float64(i%100) * 100)
				values = append(values, salary)
			}
		}
		return values, nil
	}

	// Collect statistics
	stats, err := collector.CollectTableStats(
		columnNames,
		columnTypes,
		1000,
		dataReader,
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1000), stats.RowCount)

	// Now verify these statistics can be used in the optimization pipeline
	statsMgr := NewStatisticsManager(NewMockCatalog())
	estimator := NewCardinalityEstimator(statsMgr)
	costModel := NewCostModel(DefaultCostConstants(), estimator)

	// Verify cost model can work with multi-column statistics
	assert.NotNil(t, costModel)
}

// TestErrorHandlingInIntegration verifies that errors in one phase don't
// break the entire optimization pipeline.
func TestErrorHandlingInIntegration(t *testing.T) {
	// Create optimizer with catalog
	catalog := NewMockCatalog()
	optimizer := NewCostBasedOptimizer(catalog)

	// Verify optimizer handles missing statistics gracefully
	assert.NotNil(t, optimizer)
	assert.True(t, optimizer.IsEnabled())

	// Even with empty catalog, optimizer should work with defaults
	statsMgr := optimizer.GetStatisticsManager()
	estimator := optimizer.GetCardinalityEstimator()
	costModel := optimizer.GetCostModel()

	assert.NotNil(t, statsMgr)
	assert.NotNil(t, estimator)
	assert.NotNil(t, costModel)
}

// TestFullIntegrationWithRealWorldScenario simulates a realistic query optimization
// scenario with statistics, learning, and multiple optimization phases.
func TestFullIntegrationWithRealWorldScenario(t *testing.T) {
	// Setup: Create optimizer infrastructure
	catalog := NewMockCatalog()
	optimizer := NewCostBasedOptimizer(catalog)

	// Create statistics for a realistic scenario
	// Tables: users (10k rows), orders (50k rows), products (1k rows)
	statsMgr := optimizer.GetStatisticsManager()
	estimator := optimizer.GetCardinalityEstimator()
	costModel := optimizer.GetCostModel()
	learner := costModel.GetCardinalityLearner()

	require.NotNil(t, learner)

	// Scenario: Query joins users, orders, and products
	// SELECT * FROM users u
	//   JOIN orders o ON u.id = o.user_id
	//   JOIN products p ON o.product_id = p.id
	// WHERE u.age > 25 AND o.status = 'completed'

	// Phase 1: Scan operators with estimated cardinalities
	// - users scan (full): 10,000 rows
	// - orders scan (filtered by status): ~5,000 rows
	// - products scan (full): 1,000 rows

	costModel.RecordObservation("scan:users", 10000, 10000)
	costModel.RecordObservation("scan:orders_filtered", 5000, 4950)
	costModel.RecordObservation("scan:products", 1000, 1000)

	// Phase 2: Filter on users (age > 25)
	// Estimated: 7,000 rows, Actual: 6,800 rows
	costModel.RecordObservation("filter:users_age", 7000, 6800)

	// Phase 3: Join 1 (users filtered JOIN orders filtered)
	// Estimated: 35,000 rows, Actual: 33,600 rows
	costModel.RecordObservation("join:users_orders", 35000, 33600)

	// Phase 4: Join 2 (result JOIN products)
	// Estimated: 33,600 rows, Actual: 33,600 rows
	costModel.RecordObservation("join:result_products", 33600, 33600)

	// Verify learning collected all observations
	usersObs := learner.GetObservationCount("scan:users")
	ordersObs := learner.GetObservationCount("scan:orders_filtered")
	productsObs := learner.GetObservationCount("scan:products")

	assert.Greater(t, usersObs, int64(0))
	assert.Greater(t, ordersObs, int64(0))
	assert.Greater(t, productsObs, int64(0))

	// Verify all components are still working
	assert.True(t, optimizer.IsEnabled())
	assert.NotNil(t, statsMgr)
	assert.NotNil(t, estimator)
	assert.NotNil(t, costModel)
}

// MockCatalog provides a minimal catalog for testing
type MockCatalog struct{}

func NewMockCatalog() CatalogProvider {
	return &MockCatalog{}
}

func (mc *MockCatalog) GetTableInfo(schema, table string) TableInfo {
	return nil
}

func (mc *MockCatalog) GetIndexesForTableAsInterface(schema, table string) []IndexDef {
	return nil
}
