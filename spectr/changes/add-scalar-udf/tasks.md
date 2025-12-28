## 1. Core Types and Interfaces

- [ ] 1.1 Create `scalar_udf.go` with ScalarFuncConfig struct
- [ ] 1.2 Define ScalarFuncExecutor struct with RowExecutor and RowContextExecutor
- [ ] 1.3 Define ScalarFunc interface with Config() and Executor() methods
- [ ] 1.4 Define ScalarUDFArg struct for bind-time argument metadata
- [ ] 1.5 Define function type aliases (RowExecutorFn, RowContextExecutorFn, ScalarBinderFn)

## 2. Function Registry

- [ ] 2.1 Create scalarFuncRegistry struct with thread-safe function storage
- [ ] 2.2 Implement registry lookup with type matching
- [ ] 2.3 Add registry field to Conn struct
- [ ] 2.4 Initialize registry on connection creation
- [ ] 2.5 Write tests for registry operations

## 3. Registration API

- [ ] 3.1 Implement RegisterScalarUDF function
- [ ] 3.2 Validate function configuration (name, types, executor)
- [ ] 3.3 Implement RegisterScalarUDFSet for function overloading
- [ ] 3.4 Handle duplicate function registration (replace or error)
- [ ] 3.5 Write tests for registration with various configurations

## 4. Type Matching

- [ ] 4.1 Implement type compatibility checking for function overloading
- [ ] 4.2 Support TYPE_ANY for generic functions
- [ ] 4.3 Handle variadic parameter matching
- [ ] 4.4 Implement best-match selection when multiple overloads match
- [ ] 4.5 Write tests for type matching edge cases

## 5. Binder Integration

- [ ] 5.1 Hook scalar UDF resolution into internal/binder
- [ ] 5.2 Create BoundScalarUDF expression type
- [ ] 5.3 Implement argument type checking at bind time
- [ ] 5.4 Return type inference from function config
- [ ] 5.5 Write tests for binding scalar UDF calls

## 6. Execution

- [ ] 6.1 Implement scalar UDF execution operator
- [ ] 6.2 Handle NULL input with default behavior (NULL in = NULL out)
- [ ] 6.3 Handle SpecialNullHandling flag for custom NULL logic
- [ ] 6.4 Implement value conversion between SQL and Go types
- [ ] 6.5 Add panic recovery wrapper for user functions
- [ ] 6.6 Write tests for execution with all primitive types

## 7. Context Support

- [ ] 7.1 Implement RowContextExecutor path with context propagation
- [ ] 7.2 Handle context cancellation during execution
- [ ] 7.3 Pass query timeout to user functions via context
- [ ] 7.4 Write tests for context cancellation

## 8. Constant Folding

- [ ] 8.1 Implement ScalarBinder callback during bind phase
- [ ] 8.2 Detect foldable arguments (constants)
- [ ] 8.3 Evaluate and cache constant results
- [ ] 8.4 Store bind-time context for execution phase
- [ ] 8.5 Write tests for constant folding optimization

## 9. Complex Types

- [ ] 9.1 Test scalar UDFs with LIST input/output
- [ ] 9.2 Test scalar UDFs with STRUCT input/output
- [ ] 9.3 Test scalar UDFs with MAP input/output
- [ ] 9.4 Test variadic functions with mixed types
- [ ] 9.5 Write integration tests for complex type scenarios

## 10. Volatile Functions

- [ ] 10.1 Implement Volatile flag handling
- [ ] 10.2 Prevent caching of volatile function results
- [ ] 10.3 Test volatile function behavior (e.g., random())

## 11. Error Handling

- [ ] 11.1 Implement detailed error messages with function name
- [ ] 11.2 Include row index in execution errors
- [ ] 11.3 Proper error propagation through query execution
- [ ] 11.4 Write tests for error scenarios

## 12. Validation

- [ ] 12.1 Run `go test -race` to verify thread safety
- [ ] 12.2 Run `golangci-lint` and fix any issues
- [ ] 12.3 Verify API matches duckdb-go exactly
- [ ] 12.4 Create benchmark comparing to built-in functions
- [ ] 12.5 Document performance characteristics
