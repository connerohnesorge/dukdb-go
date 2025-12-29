# Aggregate Udf Specification

## Requirements

### Requirement: Aggregate Function Registration

The system SHALL support registration of user-defined aggregate functions.

#### Scenario: Register simple aggregate function
- GIVEN a database connection
- WHEN registering an aggregate function with name "my_sum"
- AND providing Init, Update, Combine, Finalize callbacks
- THEN registration succeeds
- AND function is callable in SQL queries

#### Scenario: Register with empty name fails
- GIVEN a database connection
- WHEN registering an aggregate function with empty name ""
- THEN registration fails with "name cannot be empty" error

#### Scenario: Register without Init callback fails
- GIVEN a database connection
- WHEN registering an aggregate function without Init callback
- THEN registration fails with "must have Init callback" error

#### Scenario: Register without Update callback fails
- GIVEN a database connection
- WHEN registering an aggregate function without Update or UpdateCtx
- THEN registration fails with "must have Update or UpdateCtx callback" error

#### Scenario: Register without Combine callback fails
- GIVEN a database connection
- WHEN registering an aggregate function without Combine
- THEN registration fails with "must have Combine callback" error

#### Scenario: Register without Finalize callback fails
- GIVEN a database connection
- WHEN registering an aggregate function without Finalize or FinalizeCtx
- THEN registration fails with "must have Finalize or FinalizeCtx callback" error

### Requirement: Aggregate Function Overloading

The system SHALL support function overloading by parameter types.

#### Scenario: Register multiple overloads
- GIVEN aggregate function "my_agg" registered for INTEGER input
- WHEN registering "my_agg" again for DOUBLE input
- THEN both overloads are registered
- AND correct overload is selected based on argument type

#### Scenario: Lookup exact type match
- GIVEN aggregate function "my_sum" registered for INTEGER
- WHEN calling "SELECT my_sum(int_col) FROM t"
- THEN INTEGER overload is selected

#### Scenario: Lookup with no match returns error
- GIVEN aggregate function "my_agg" registered for INTEGER only
- WHEN calling "SELECT my_agg(varchar_col) FROM t"
- THEN "unknown aggregate function" error is returned

### Requirement: Aggregate State Lifecycle

The system SHALL manage aggregate state through Init/Update/Combine/Finalize lifecycle.

#### Scenario: Init creates fresh state
- GIVEN an aggregate function with Init callback
- WHEN processing a new group
- THEN Init is called to create state
- AND state is associated with that group

#### Scenario: Update accumulates values
- GIVEN an aggregate function with state initialized
- WHEN processing row with value 5
- AND processing row with value 10
- THEN Update is called twice with values 5 and 10

#### Scenario: Combine merges parallel states
- GIVEN two execution states with accumulated values
- WHEN combining them
- THEN Combine callback merges source into target
- AND combined state reflects all values

#### Scenario: Finalize produces result
- GIVEN an aggregate state with accumulated values
- WHEN finalizing
- THEN Finalize callback is called
- AND final aggregate value is returned

### Requirement: Grouped Aggregation

The system SHALL support GROUP BY with aggregate UDFs.

#### Scenario: Single group column
- GIVEN table with columns (category, value)
- WHEN executing "SELECT category, my_avg(value) FROM t GROUP BY category"
- THEN separate state is maintained per category
- AND each group produces its own result

#### Scenario: Multiple group columns
- GIVEN table with columns (a, b, value)
- WHEN executing "SELECT a, b, my_sum(value) FROM t GROUP BY a, b"
- THEN separate state per (a, b) combination
- AND results reflect grouped aggregation

#### Scenario: No GROUP BY (global aggregation)
- GIVEN table with values
- WHEN executing "SELECT my_sum(value) FROM t"
- THEN single state is used for all rows
- AND single result is returned

### Requirement: NULL Value Handling

The system SHALL handle NULL values based on configuration.

#### Scenario: Default NULL handling skips NULLs
- GIVEN aggregate function with SpecialNullHandling=false
- WHEN processing rows with some NULL values
- THEN rows with NULL values are skipped
- AND Update is not called for NULL rows

#### Scenario: Special NULL handling passes NULLs
- GIVEN aggregate function with SpecialNullHandling=true
- WHEN processing rows with some NULL values
- THEN all rows are processed including NULLs
- AND Update receives NULL values

### Requirement: Context-Aware Callbacks

The system SHALL support context-aware callback variants.

#### Scenario: UpdateCtx receives context
- GIVEN aggregate function with UpdateCtx callback
- WHEN processing rows
- THEN UpdateCtx receives AggregateFuncContext
- AND context provides Clock() method

#### Scenario: FinalizeCtx receives context
- GIVEN aggregate function with FinalizeCtx callback
- WHEN finalizing
- THEN FinalizeCtx receives AggregateFuncContext
- AND context provides Clock() method

#### Scenario: Context provides group key
- GIVEN aggregate function with grouped aggregation
- WHEN UpdateCtx is called
- THEN context.GroupKey contains current group key values

### Requirement: Panic Recovery

The system SHALL recover from panics in user callbacks.

#### Scenario: Panic in Update converted to error
- GIVEN aggregate function whose Update panics
- WHEN processing rows
- THEN panic is recovered
- AND error "panic in aggregate Update" is returned

#### Scenario: Panic in Finalize converted to error
- GIVEN aggregate function whose Finalize panics
- WHEN finalizing
- THEN panic is recovered
- AND error "panic in aggregate Finalize" is returned

#### Scenario: Panic in Combine converted to error
- GIVEN aggregate function whose Combine panics
- WHEN combining states
- THEN panic is recovered
- AND error "panic in aggregate Combine" is returned

### Requirement: Deterministic Testing Support

The system SHALL support quartz.Clock injection for deterministic testing.

#### Scenario: Clock injection at execution state
- GIVEN AggregateExecutionState created with mock clock
- WHEN processing chunks
- THEN clock.Now() is called with appropriate tags

#### Scenario: ProcessChunk clock tags
- GIVEN mock clock with trap set
- WHEN ProcessChunk is called
- THEN clock is called at chunk_start and chunk_end

#### Scenario: CombineWith clock tags
- GIVEN mock clock with trap set
- WHEN CombineWith is called
- THEN clock is called at combine_start and combine_end

#### Scenario: Finalize clock tags
- GIVEN mock clock with trap set
- WHEN Finalize is called
- THEN clock is called at finalize_start and finalize_end

#### Scenario: Context.Now uses injected clock
- GIVEN AggregateFuncContext with mock clock
- WHEN calling context.Now()
- THEN mock clock time is returned

### Requirement: Thread-Safe Registry

The system SHALL provide thread-safe function registration and lookup.

#### Scenario: Concurrent registration
- GIVEN multiple goroutines registering functions
- WHEN registrations occur simultaneously
- THEN all registrations succeed without race conditions

#### Scenario: Concurrent lookup during registration
- GIVEN one goroutine registering functions
- AND another goroutine looking up functions
- WHEN operations occur simultaneously
- THEN lookups return consistent results
- AND no race conditions occur

### Requirement: State Cleanup

The system SHALL support state cleanup after finalization.

#### Scenario: Cleanup calls Destroy for each state
- GIVEN aggregate function with Destroy callback
- WHEN Cleanup() is called after Finalize()
- THEN Destroy is called for each group state
- AND group states are released for GC

#### Scenario: Cleanup without Destroy callback
- GIVEN aggregate function without Destroy callback
- WHEN Cleanup() is called
- THEN group states are released for GC
- AND no error occurs

#### Scenario: Destroy panic is recovered
- GIVEN aggregate function with Destroy that panics
- WHEN Cleanup() is called
- THEN panic is recovered
- AND cleanup continues for remaining states

