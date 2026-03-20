## 1. Scalar Functions

- [x] 1.1 Implement `JSON_CONTAINS(json, value)` scalar function in `internal/executor/expr.go` with helper `evalJSONContains`
- [x] 1.2 Implement `JSON_QUOTE(value)` scalar function in `internal/executor/expr.go` with helper `evalJSONQuote`
- [x] 1.3 Write integration tests for JSON_CONTAINS and JSON_QUOTE in `internal/executor/json_scalar_test.go`

## 2. Aggregate Functions

- [x] 2.1 Register `JSON_GROUP_ARRAY` and `JSON_GROUP_OBJECT` as aggregate function names in the aggregate recognition switch in `internal/executor/expr.go`
- [x] 2.2 Implement `JSON_GROUP_ARRAY(expr)` aggregate in `internal/executor/physical_aggregate.go`
- [x] 2.3 Implement `JSON_GROUP_OBJECT(key, value)` aggregate in `internal/executor/physical_aggregate.go`
- [x] 2.4 Write integration tests for JSON_GROUP_ARRAY and JSON_GROUP_OBJECT in `internal/executor/json_aggregate_test.go`

## 3. Table Function

- [x] 3.1 Create `internal/executor/table_function_json_each.go` implementing `executeJSONEach`
- [x] 3.2 Register `json_each` in the table function dispatch switch in `internal/executor/table_function_csv.go`
- [x] 3.3 Ensure the binder/planner recognizes `json_each` as a table function name
- [x] 3.4 Write integration tests for JSON_EACH in `internal/executor/json_each_test.go`

## 4. Validation

- [x] 4.1 Run `nix develop -c lint` and fix any linting issues
- [x] 4.2 Run `nix develop -c tests` and confirm all tests pass
