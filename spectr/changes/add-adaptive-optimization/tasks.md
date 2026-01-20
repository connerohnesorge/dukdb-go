## 1. Learning Infrastructure

- [ ] 1.1 Design cardinality learning cache structure
- [ ] 1.2 Implement learning cache with LRU eviction
- [ ] 1.3 Add query signature generation for cache key
- [ ] 1.4 Implement correction factor storage and retrieval
- [ ] 1.5 Design learning statistics serialization format
- [ ] 1.6 Write tests for learning cache basic operations

## 2. Execution Profile Collection

- [ ] 2.1 Add actual cardinality tracking to all operators
- [ ] 2.2 Implement execution timeline collection
- [ ] 2.3 Collect CPU time per operator
- [ ] 2.4 Collect memory usage per operator
- [ ] 2.5 Collect I/O statistics if applicable
- [ ] 2.6 Implement low-overhead profiling (minimal overhead)
- [ ] 2.7 Write tests for profile collection

## 3. Correction Factor Calculation

- [ ] 3.1 Calculate cardinality correction factors (actual / estimated)
- [ ] 3.2 Track correction factors for each operator type
- [ ] 3.3 Identify operators with high estimation error
- [ ] 3.4 Implement exponential moving average for stability
- [ ] 3.5 Add confidence levels to corrections
- [ ] 3.6 Handle edge cases (zero estimates, very small actual)
- [ ] 3.7 Write tests for correction factor calculation

## 4. Query Signature and Matching

- [ ] 4.1 Design query signature for similarity matching
- [ ] 4.2 Extract plan shape for matching
- [ ] 4.3 Extract predicate patterns
- [ ] 4.4 Implement approximate matching for parametric queries
- [ ] 4.5 Handle parameterized queries (prepared statements)
- [ ] 4.6 Write tests for signature generation and matching
- [ ] 4.7 Test matching accuracy on similar/different queries

## 5. Correction Application

- [ ] 5.1 Apply cardinality corrections during planning
- [ ] 5.2 Adjust join selectivity estimates
- [ ] 5.3 Adjust filter selectivity estimates
- [ ] 5.4 Adjust aggregate cardinality estimates
- [ ] 5.5 Apply temporal decay to older corrections
- [ ] 5.6 Limit correction magnitude to prevent over-correction
- [ ] 5.7 Write tests for correction application

## 6. Cost Adjustment Learning

- [ ] 6.1 Track actual cost vs estimated cost per execution
- [ ] 6.2 Calculate cost constant adjustments
- [ ] 6.3 Adjust CPU cost constant if estimates are off
- [ ] 6.4 Adjust I/O cost constant if estimates are off
- [ ] 6.5 Adjust memory cost constant if estimates are off
- [ ] 6.6 Implement conservative adjustment strategy
- [ ] 6.7 Write tests for cost adjustment

## 7. Plan Anomaly Detection

- [ ] 7.1 Identify when actual cardinality significantly differs from estimate
- [ ] 7.2 Trigger re-optimization if anomaly detected
- [ ] 7.3 Implement threshold for significant difference (e.g., > 10x)
- [ ] 7.4 Add adaptive operator selection based on actual cardinality
- [ ] 7.5 Switch algorithms mid-execution if beneficial
- [ ] 7.6 Write tests for anomaly detection

## 8. EXPLAIN ANALYZE Enhancement

- [ ] 8.1 Show estimated vs actual cardinality per operator
- [ ] 8.2 Show estimation accuracy percentage
- [ ] 8.3 Show execution time per operator
- [ ] 8.4 Show correction factors applied
- [ ] 8.5 Show confidence levels
- [ ] 8.6 Highlight operators with high estimation error
- [ ] 8.7 Show recommendations for improving estimates

## 9. Configuration and Control

- [ ] 9.1 Add PRAGMA to enable/disable learning
- [ ] 9.2 Add PRAGMA for learning cache size
- [ ] 9.3 Add PRAGMA for learning history retention
- [ ] 9.4 Add PRAGMA for correction factor weight
- [ ] 9.5 Add PRAGMA for anomaly detection threshold
- [ ] 9.6 Add diagnostic mode for learning visibility
- [ ] 9.7 Write tests for configuration options

## 10. Planner Integration

- [ ] 10.1 Look up and apply corrections before cost calculation
- [ ] 10.2 Track which corrections were applied in metadata
- [ ] 10.3 Use correction confidence in plan selection
- [ ] 10.4 Add learned corrections to EXPLAIN output
- [ ] 10.5 Handle missing corrections gracefully

## 11. Data Invalidation

- [ ] 11.1 Track data modification counts per table
- [ ] 11.2 Detect significant data changes
- [ ] 11.3 Invalidate corrections when data changes > 20%
- [ ] 11.4 Add ANALYZE CASCADE to refresh all corrections
- [ ] 11.5 Implement incremental invalidation
- [ ] 11.6 Write tests for invalidation logic

## 12. Persistence

- [ ] 12.1 Design serialization format for learning data
- [ ] 12.2 Implement save to catalog/metadata
- [ ] 12.3 Implement load from catalog on database open
- [ ] 12.4 Handle compatibility with old format
- [ ] 12.5 Add migration if format changes
- [ ] 12.6 Write tests for persistence

## 13. Testing and Validation

- [ ] 13.1 Test learning accuracy with synthetic queries
- [ ] 13.2 Test learning with realistic workloads
- [ ] 13.3 Test correction factor stability over time
- [ ] 13.4 Test anomaly detection on various scenarios
- [ ] 13.5 Test plan selection improves with learning
- [ ] 13.6 Test EXPLAIN ANALYZE output accuracy
- [ ] 13.7 Test concurrent query learning
- [ ] 13.8 Performance benchmark: impact on planning time
- [ ] 13.9 Equivalence checking against reference

## 14. Documentation

- [ ] 14.1 Document how learning works
- [ ] 14.2 Document best practices for enabling learning
- [ ] 14.3 Document troubleshooting guide
- [ ] 14.4 Create examples of learning improving performance
- [ ] 14.5 Document EXPLAIN ANALYZE interpretation
