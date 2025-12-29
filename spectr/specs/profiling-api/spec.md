# Profiling Api Specification

## Requirements

### Requirement: Profiling Info Retrieval

The package SHALL provide query profiling information.

#### Scenario: Get profiling after query
- GIVEN profiling enabled via PRAGMA
- WHEN executing a query and calling GetProfilingInfo
- THEN ProfilingInfo is returned with metrics

#### Scenario: Profiling not enabled
- GIVEN no profiling PRAGMA
- WHEN calling GetProfilingInfo
- THEN error or empty ProfilingInfo is returned

### Requirement: Profiling Metrics

The ProfilingInfo SHALL contain execution metrics.

#### Scenario: Root node metrics
- GIVEN executed query
- WHEN examining ProfilingInfo
- THEN Metrics contains "TOTAL_TIME" key

#### Scenario: Child nodes present
- GIVEN complex query with multiple operators
- WHEN examining ProfilingInfo
- THEN Children contains operator nodes

### Requirement: Profiling Tree Structure

The ProfilingInfo SHALL represent the query plan tree.

#### Scenario: Recursive tree structure
- GIVEN query with joins and filters
- WHEN examining ProfilingInfo tree
- THEN structure reflects operator hierarchy

#### Scenario: Operator type identification
- GIVEN profiling node
- WHEN examining Metrics
- THEN "OPERATOR_TYPE" identifies the operator

### Requirement: Per-Operator Metrics

Each operator node SHALL have execution metrics.

#### Scenario: Operator timing
- GIVEN operator profiling node
- WHEN examining Metrics
- THEN timing information is available

#### Scenario: Row count metrics
- GIVEN operator profiling node
- WHEN examining Metrics
- THEN row count information is available

