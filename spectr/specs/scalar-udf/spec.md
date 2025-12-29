# Scalar Udf Specification

## Requirements

### Requirement: Scalar UDF Registration

The package SHALL allow registration of user-defined scalar functions.

#### Scenario: Register simple scalar function
- GIVEN a ScalarFunc with INTEGER input and INTEGER output
- WHEN calling RegisterScalarUDF(conn, "my_double", func)
- THEN no error is returned
- AND function is available in SQL queries

#### Scenario: Register function with no executor
- GIVEN a ScalarFunc with nil RowExecutor and nil RowContextExecutor
- WHEN calling RegisterScalarUDF(conn, "broken", func)
- THEN error is returned containing "no executor"

#### Scenario: Register function with empty name
- GIVEN a ScalarFunc with valid configuration
- WHEN calling RegisterScalarUDF(conn, "", func)
- THEN error is returned containing "name required"

#### Scenario: Register function with nil interface
- GIVEN nil ScalarFunc
- WHEN calling RegisterScalarUDF(conn, "test", nil)
- THEN error is returned containing "function is nil"

### Requirement: Scalar UDF Execution

The registered scalar function SHALL be callable in SQL queries.

#### Scenario: Execute simple scalar UDF
- GIVEN registered function `my_double` that doubles integers
- WHEN executing "SELECT my_double(5)"
- THEN result contains 10

#### Scenario: Execute scalar UDF in expression
- GIVEN registered function `my_add(a, b)` that adds two integers
- WHEN executing "SELECT my_add(3, 4) + 1"
- THEN result contains 8

#### Scenario: Execute scalar UDF with table column
- GIVEN table `t` with column `x` containing [1, 2, 3]
- AND registered function `my_double`
- WHEN executing "SELECT my_double(x) FROM t"
- THEN result contains [2, 4, 6]

#### Scenario: Execute scalar UDF in WHERE clause
- GIVEN table `t` with values [1, 2, 3, 4, 5]
- AND registered function `is_even` returning boolean
- WHEN executing "SELECT x FROM t WHERE is_even(x)"
- THEN result contains [2, 4]

### Requirement: Scalar UDF NULL Handling

The scalar function SHALL handle NULL values according to configuration.

#### Scenario: Default NULL handling (NULL in = NULL out)
- GIVEN registered function with SpecialNullHandling = false
- WHEN executing function with NULL input
- THEN result is NULL
- AND user function is NOT called

#### Scenario: Special NULL handling
- GIVEN registered function with SpecialNullHandling = true
- WHEN executing function with NULL input
- THEN user function IS called with nil value
- AND function can return non-NULL result

#### Scenario: NULL handling with multiple arguments
- GIVEN registered function with 3 arguments
- AND SpecialNullHandling = false
- WHEN any one argument is NULL
- THEN result is NULL

### Requirement: Scalar UDF Type Support

The scalar function SHALL support all DuckDB types as inputs and outputs.

#### Scenario: VARCHAR input/output
- GIVEN registered function `my_upper(VARCHAR) -> VARCHAR`
- WHEN executing with string input
- THEN string result is returned correctly

#### Scenario: BOOLEAN input/output
- GIVEN registered function `my_not(BOOLEAN) -> BOOLEAN`
- WHEN executing with boolean input
- THEN boolean result is returned correctly

#### Scenario: Temporal type input/output
- GIVEN registered function `add_day(DATE) -> DATE`
- WHEN executing with date input
- THEN date result is returned correctly

#### Scenario: DECIMAL input/output
- GIVEN registered function `round_cents(DECIMAL) -> DECIMAL`
- WHEN executing with decimal input
- THEN decimal result with correct precision is returned

#### Scenario: LIST input/output
- GIVEN registered function `list_sum(LIST(INTEGER)) -> INTEGER`
- WHEN executing with list input [1, 2, 3]
- THEN result is 6

#### Scenario: STRUCT input/output
- GIVEN registered function taking STRUCT input
- WHEN executing with struct value
- THEN map[string]any is passed to user function

### Requirement: Scalar UDF Function Overloading

The package SHALL support multiple functions with the same name but different signatures.

#### Scenario: Register overloaded functions
- GIVEN RegisterScalarUDFSet with two functions:
  - `length(VARCHAR) -> INTEGER`
  - `length(LIST) -> INTEGER`
- WHEN executing "SELECT length('hello')"
- THEN VARCHAR version is called returning 5

#### Scenario: Overload resolution with LIST
- GIVEN same overloaded `length` functions
- WHEN executing "SELECT length([1, 2, 3])"
- THEN LIST version is called returning 3

#### Scenario: Overload ambiguity error
- GIVEN overlapping function signatures
- WHEN ambiguous call cannot be resolved
- THEN error is returned indicating ambiguous function call

### Requirement: Scalar UDF Variadic Support

The scalar function SHALL support variadic parameters.

#### Scenario: Variadic function with fixed prefix
- GIVEN function config with InputTypeInfos = [VARCHAR] and VariadicTypeInfo = INTEGER
- WHEN executing "SELECT my_func('format', 1, 2, 3)"
- THEN user function receives ["format", 1, 2, 3]

#### Scenario: Variadic with TYPE_ANY
- GIVEN function config with VariadicTypeInfo.Type = TYPE_ANY
- WHEN executing with mixed argument types
- THEN all arguments are passed to user function

#### Scenario: Empty variadic
- GIVEN variadic function
- WHEN executing with only fixed parameters
- THEN function executes with empty variadic portion

### Requirement: Scalar UDF Context Support

The scalar function SHALL support context-aware execution.

#### Scenario: Context executor receives context
- GIVEN function with RowContextExecutor
- WHEN query is executed
- THEN context is passed to user function

#### Scenario: Context cancellation stops execution
- GIVEN function with RowContextExecutor
- WHEN context is cancelled during execution
- THEN remaining rows are not processed
- AND context.Err() is returned

#### Scenario: Query timeout propagation
- GIVEN query with timeout
- WHEN scalar UDF is executed
- THEN context deadline is available in user function

### Requirement: Scalar UDF Volatile Functions

The scalar function SHALL support volatile (non-deterministic) functions.

#### Scenario: Volatile function called per row
- GIVEN function with Volatile = true (e.g., random())
- WHEN executing "SELECT my_random() FROM t" with 3 rows
- THEN function is called 3 times with potentially different results

#### Scenario: Non-volatile function optimized
- GIVEN function with Volatile = false
- WHEN executing with constant arguments
- THEN function may be called once and result reused

### Requirement: Scalar UDF Constant Folding

The scalar function SHALL support optional bind-time constant folding.

#### Scenario: ScalarBinder receives foldable arguments
- GIVEN function with ScalarBinder
- WHEN query contains "my_func('constant', column)"
- THEN ScalarBinder receives args[0].Foldable = true
- AND args[0].Value = "constant"
- AND args[1].Foldable = false

#### Scenario: ScalarBinder modifies context
- GIVEN ScalarBinder that stores precomputed data in context
- WHEN RowContextExecutor runs
- THEN modified context is available

### Requirement: Scalar UDF Error Handling

The scalar function SHALL properly handle errors from user functions.

#### Scenario: User function returns error
- GIVEN function that returns error for invalid input
- WHEN executing with invalid input
- THEN query fails with error message from user function

#### Scenario: User function panics
- GIVEN function that panics
- WHEN executing
- THEN panic is caught
- AND error is returned containing panic message

#### Scenario: Error includes context
- GIVEN function that returns error
- WHEN error occurs during execution
- THEN error message includes function name

### Requirement: Scalar UDF Result Type

The scalar function result type SHALL match the declared ResultTypeInfo.

#### Scenario: Correct result type
- GIVEN function declared as returning INTEGER
- WHEN function returns int32
- THEN result is correctly typed

#### Scenario: Result type conversion
- GIVEN function declared as returning BIGINT
- WHEN function returns int (Go's native int)
- THEN result is converted to int64

#### Scenario: Result type mismatch error
- GIVEN function declared as returning INTEGER
- WHEN function returns string
- THEN error is returned containing "type mismatch"

