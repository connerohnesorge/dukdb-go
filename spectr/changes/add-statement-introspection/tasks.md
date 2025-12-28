## 1. Statement Type Detection

- [ ] 1.1 Define StmtType constants (27 types)
- [ ] 1.2 Implement StatementType() method
- [ ] 1.3 Map parsed AST to statement types
- [ ] 1.4 Write tests for all statement types

## 2. Parameter Metadata

- [ ] 2.1 Implement NumInput() method
- [ ] 2.2 Implement ParamName(index) method
- [ ] 2.3 Implement ParamType(index) method
- [ ] 2.4 Handle positional vs named parameters
- [ ] 2.5 Write tests for parameter metadata

## 3. Column Metadata

- [ ] 3.1 Implement ColumnCount() method
- [ ] 3.2 Implement ColumnName(index) method
- [ ] 3.3 Implement ColumnType(index) method
- [ ] 3.4 Implement ColumnTypeInfo(index) method
- [ ] 3.5 Write tests for column metadata

## 4. Bound Execution

- [ ] 4.1 Add boundParams field to Stmt
- [ ] 4.2 Implement Bind(index, value) method
- [ ] 4.3 Implement ExecBound() method
- [ ] 4.4 Implement QueryBound() method
- [ ] 4.5 Write tests for bound execution

## 5. Deterministic Testing Integration

- [ ] 5.1 Implement ExecBoundContext with clock parameter
- [ ] 5.2 Implement QueryBoundContext with clock parameter
- [ ] 5.3 Use clock.Until() for deadline checking
- [ ] 5.4 Write deterministic tests for bound execution timeout using quartz.Mock
- [ ] 5.5 Verify zero time.Sleep calls in test files

## 6. Validation

- [ ] 6.1 Run `go test -race`
- [ ] 6.2 Run `golangci-lint`
- [ ] 6.3 Verify API matches duckdb-go
- [ ] 6.4 Verify compliance with deterministic-testing spec
