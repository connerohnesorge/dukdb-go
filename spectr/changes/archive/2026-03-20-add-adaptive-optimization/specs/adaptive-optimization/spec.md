# Adaptive Query Optimization - Delta Spec

## ADDED Requirements

### Requirement: Cardinality Learning

The system MUST learn from actual query execution to improve cardinality estimates for future similar queries.

#### Scenario: Track actual cardinality
- GIVEN a query executing
- WHEN each operator finishes
- THEN actual output cardinality is recorded
- AND compared to estimated cardinality

#### Scenario: Calculate correction factors
- GIVEN actual vs estimated cardinality for an operator
- WHEN query completes
- THEN correction factor = actual / estimated is stored
- AND labeled with operator type and predicate pattern

#### Scenario: Apply corrections to similar queries
- GIVEN a new query with similar structure to previously executed query
- WHEN query is planned
- THEN stored correction factors are retrieved
- AND applied to cardinality estimates for matching operators
- AND estimates are improved based on learning

#### Scenario: Confidence-weighted corrections
- GIVEN correction factors with varying confidence levels
- WHEN applied to new query
- THEN high-confidence corrections are weighted more
- AND low-confidence corrections are applied cautiously
- AND confidence increases as more observations accumulate

#### Scenario: Exponential moving average smoothing
- GIVEN multiple correction observations for same operator
- WHEN new observation arrives
- THEN EMA is updated: new_EMA = 0.8 * old_EMA + 0.2 * new_observation
- AND smoothing reduces impact of outliers
- AND recent observations have more weight

---

### Requirement: Query Signature and Matching

The system MUST identify similar queries to apply relevant learned corrections.

#### Scenario: Generate query signature
- GIVEN any query
- WHEN signature generation runs
- THEN signature captures plan shape
- AND signature captures filter predicates
- AND signature ignores constants in predicates

#### Scenario: Match queries with same signature
- GIVEN two queries with identical structure but different constants
- WHEN signatures are compared
- THEN queries are identified as similar
- AND corrections from first query apply to second

#### Scenario: Handle parameterized queries
- GIVEN prepared statement with parameters
- WHEN signature is generated
- THEN parameters don't affect signature
- AND same signature for multiple parameter values

#### Scenario: Distinguish genuinely different queries
- GIVEN two queries with different join orders or structure
- WHEN signatures are compared
- THEN queries have different signatures
- AND corrections don't cross-apply inappropriately

#### Scenario: Approximate matching with small differences
- GIVEN two queries that are almost identical
- WHEN similarity is evaluated
- THEN approximate match is found
- AND corrections are weighted by similarity score

---

### Requirement: Execution Profiling

The system MUST collect performance metrics during query execution.

#### Scenario: Collect actual cardinality per operator
- GIVEN query executing with profiling enabled
- WHEN each operator produces output
- THEN actual row count is recorded
- AND associated with operator type and predicate

#### Scenario: Collect execution time
- GIVEN operator executing
- WHEN operator completes
- THEN execution time is recorded
- AND CPU time and wall-clock time tracked

#### Scenario: Collect memory usage
- GIVEN operator allocating memory
- WHEN operator completes
- THEN peak memory usage is recorded
- AND memory allocations tracked

#### Scenario: Low-overhead profiling
- GIVEN profiling collection running
- WHEN query executes
- THEN profiling overhead is less than 5%
- AND profiling doesn't significantly impact performance

#### Scenario: Collect I/O statistics
- GIVEN query performing I/O
- WHEN query completes
- THEN I/O statistics are recorded
- AND cache hits/misses tracked if available

---

### Requirement: Correction Application

The system MUST apply learned corrections during query planning.

#### Scenario: Apply cardinality correction
- GIVEN learned correction factor of 2.5x for equality predicate
- WHEN planning new query with similar predicate
- THEN estimated cardinality is multiplied by 2.5
- AND improved estimate is used in cost calculation

#### Scenario: Apply selectivity correction
- GIVEN learned selectivity for filter
- WHEN planning new query with similar filter
- THEN selectivity is corrected instead of using statistics
- AND more accurate than statistics in this case

#### Scenario: Apply join selectivity correction
- GIVEN learned correction for join predicate
- WHEN planning new query with similar join
- THEN join selectivity estimate is corrected
- AND join plan selection improved

#### Scenario: Limit correction magnitude
- GIVEN correction factor that's unusually large (> 100x)
- WHEN applying correction
- THEN correction is capped at reasonable limit (e.g., 10x)
- AND avoids over-correction from outliers

#### Scenario: Decay corrections over time
- GIVEN correction factors from old queries
- WHEN new query is planned
- THEN corrections are weighted less if data has changed
- AND temporal decay factor is applied

---

### Requirement: Plan Anomaly Detection

The system MUST detect when actual execution differs significantly from plan.

#### Scenario: Detect cardinality anomaly
- GIVEN operator with estimated cardinality 1000, actual cardinality 100000
- WHEN query executes
- THEN anomaly is detected (ratio > 10x threshold)
- AND operator is marked for attention

#### Scenario: Trigger re-optimization on anomaly
- GIVEN significant cardinality anomaly detected mid-query
- WHEN anomaly threshold exceeded
- THEN query execution can continue OR optimizer can suggest better plan
- AND future similar queries benefit from learning

#### Scenario: Identify wrong plan decisions
- GIVEN query executing slower than alternative plan would
- WHEN execution completes with profiling data
- THEN anomaly analysis identifies plan decision issue
- AND future queries avoid same mistake

#### Scenario: Switch algorithms based on actual cardinality
- GIVEN hash join with estimated cardinality 10k, actual 1M
- WHEN anomaly detected during execution
- THEN algorithm can be switched to sort merge if beneficial
- AND execution completes with better performance

#### Scenario: Adjustable anomaly threshold
- GIVEN PRAGMA anomaly_threshold = 5 (5x instead of default 10x)
- WHEN query executes
- THEN anomalies are detected more aggressively
- AND learning is triggered more often

---

### Requirement: Cost Constant Learning

The system MUST self-tune cost model constants based on observed performance.

#### Scenario: Calculate CPU cost constant
- GIVEN observed execution time vs estimated cost
- WHEN query completes
- THEN CPU cost constant is adjusted
- AND actual_time / total_cost ≈ CPU_cost_constant

#### Scenario: Calculate I/O cost constant
- GIVEN I/O operations observed during execution
- WHEN query completes
- THEN I/O cost constant is adjusted
- AND reflects actual hardware I/O performance

#### Scenario: Calculate memory cost constant
- GIVEN memory allocation during execution
- WHEN query completes
- THEN memory cost constant adjusted if relevant
- AND reflects memory access characteristics

#### Scenario: Conservative adjustment strategy
- GIVEN cost constant adjustment proposed
- WHEN applying adjustment
- THEN only small incremental adjustments allowed (e.g., 5% max per query)
- AND prevents runaway adjustments

#### Scenario: Separate constants per hardware
- GIVEN database running on different hardware
- WHEN cost constants are learned
- THEN hardware-specific constants are maintained
- AND migration to new hardware doesn't break optimization

---

### Requirement: EXPLAIN ANALYZE Enhancement

The system MUST show estimation accuracy and learning in EXPLAIN output.

#### Scenario: Show estimated vs actual cardinality
- GIVEN EXPLAIN ANALYZE output
- WHEN displayed
- THEN each operator shows both estimated and actual rows
- AND format: "Rows: 1000 (estimated) / 50000 (actual)"

#### Scenario: Show estimation accuracy percentage
- GIVEN cardinality estimation
- WHEN EXPLAIN ANALYZE displayed
- THEN accuracy percentage shown: "Accuracy: 2%" or "Accuracy: 200%"
- AND highlights significant misestimates

#### Scenario: Show applied corrections
- GIVEN corrections applied during planning
- WHEN EXPLAIN ANALYZE displayed
- THEN corrections are shown: "Correction: 2.5x from previous learning"
- AND confidence level shown

#### Scenario: Highlight problem operators
- GIVEN operators with significant estimation errors
- WHEN EXPLAIN ANALYZE displayed
- THEN these operators are highlighted
- AND recommendations suggested for improvement

#### Scenario: Show cost adjustment impact
- GIVEN cost constants adjusted based on learning
- WHEN EXPLAIN ANALYZE displayed
- THEN impact shown: "CPU constant adjusted: 1.2x baseline"
- AND historical adjustment log available

#### Scenario: Show learning statistics
- GIVEN EXPLAIN ANALYZE in learning mode
- WHEN displayed
- THEN shows "Learned from 5 previous executions"
- AND confidence metrics provided

---

### Requirement: Configuration and Control

The system MUST provide options to control adaptive learning behavior.

#### Scenario: Enable/disable learning
- GIVEN PRAGMA adaptive_learning = false
- WHEN query is planned
- THEN no learning corrections are applied
- AND learning data is not collected

#### Scenario: Configure learning cache size
- GIVEN PRAGMA learning_cache_size = 10000
- WHEN learning cache fills up
- THEN LRU eviction removes oldest entries
- AND cache doesn't exceed memory limit

#### Scenario: Configure retention policy
- GIVEN PRAGMA learning_history_retention = 1000
- WHEN learning data accumulates
- THEN only most recent 1000 observations retained
- AND older observations are removed

#### Scenario: Configure correction weight
- GIVEN PRAGMA correction_weight = 0.5
- WHEN applying corrections
- THEN corrections are weighted at 50% (0.5 * correction + 0.5 * estimate)
- AND allows tuning confidence in learning

#### Scenario: Configure anomaly threshold
- GIVEN PRAGMA anomaly_threshold = 20 (20x instead of default 10x)
- WHEN query executes
- THEN only large anomalies trigger re-optimization
- AND conservative approach

---

### Requirement: Data Change Invalidation

The system MUST invalidate corrections when underlying data changes significantly.

#### Scenario: Track data modification ratio
- GIVEN modifications to table (inserts, updates, deletes)
- WHEN operations occur
- THEN modification count tracked
- AND ratio = modifications / table_size tracked

#### Scenario: Invalidate corrections on large changes
- GIVEN table modification ratio > 20%
- WHEN threshold exceeded
- THEN learned corrections for this table are invalidated
- AND next query uses fresh learning

#### Scenario: Incremental invalidation
- GIVEN corrections for individual operators
- WHEN data changes affect specific columns
- THEN only relevant corrections are invalidated
- AND other corrections remain valid

#### Scenario: ANALYZE CASCADE
- GIVEN command ANALYZE CASCADE
- WHEN executed
- THEN all learned corrections are refreshed
- AND fresh statistics collected for all tables
- AND optimizations reset and relearned

#### Scenario: Automatic re-learning
- GIVEN corrections invalidated due to data change
- WHEN next query executes
- THEN new learning observations accumulated
- AND corrections rebuilding automatically

---

### Requirement: Persistence of Learning Data

The system MUST save and restore learning corrections across database restarts.

#### Scenario: Save learning to catalog
- GIVEN learning data accumulated during session
- WHEN CHECKPOINT executed
- THEN learning corrections serialized
- AND stored in database catalog metadata

#### Scenario: Load learning on database open
- GIVEN database with persisted learning data
- WHEN database is opened
- THEN learning data is loaded
- AND immediately available for optimization

#### Scenario: Learning compatibility handling
- GIVEN database with old learning format
- WHEN database is opened with new version
- THEN learning data is migrated if format changed
- AND graceful fallback if migration fails

#### Scenario: Export learning statistics
- GIVEN accumulated learning data
- WHEN PRAGMA export_learning_stats executed
- THEN learning data exported to JSON/CSV format
- AND can be analyzed and debugged

#### Scenario: Import learning statistics
- GIVEN learning statistics from file
- WHEN PRAGMA import_learning_stats executed
- THEN statistics loaded into learning cache
- AND optimization benefits from imported data

---

### Requirement: Performance Improvement from Learning

The system MUST deliver measurable performance benefits through adaptive optimization.

#### Scenario: Improved plan for repeated query
- GIVEN query executed once, then repeated with different parameters
- WHEN second query is planned
- THEN learned corrections improve accuracy
- AND plan is better than first execution plan

#### Scenario: Faster execution as learning accumulates
- GIVEN workload with many similar queries
- WHEN queries execute over time
- THEN average execution time decreases
- AND system learns optimal strategy

#### Scenario: Anomaly recovery
- GIVEN query with bad initial plan due to estimation error
- WHEN anomaly detected and learning triggered
- THEN subsequent similar queries get better plans
- AND execution time 30-50% faster

#### Scenario: Self-tuning on new hardware
- GIVEN database migrated to new hardware
- WHEN queries start executing
- THEN cost constants automatically adjust
- AND optimization improves without manual tuning
