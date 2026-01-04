## ADDED Requirements

### Requirement: CreateScalarUDFStmt Parsing
The parser SHALL parse CREATE FUNCTION for scalar UDFs with full syntax.

#### Scenario: Simple SQL UDF
- GIVEN `CREATE FUNCTION add(a INTEGER, b INTEGER) RETURNS INTEGER AS 'SELECT a + b'`
- THEN CreateScalarUDFStmt{Name:\"add\", Params:[{Name:\"a\",Type:INTEGER},{Name:\"b\",Type:INTEGER}], Returns:INTEGER, Lang:\"sql\", Body:\"SELECT a + b\"}

#### Scenario: Python UDF multi-line
- GIVEN `CREATE FUNCTION py_len(s VARCHAR) RETURNS INTEGER LANGUAGE python AS $$import sys; return len(s)$$`
- THEN Lang:\"python\", Body multi-line

#### Scenario: With attributes
- GIVEN `CREATE OR REPLACE IMMUTABLE FUNCTION safe_div(a DOUBLE, b DOUBLE) RETURNS DOUBLE AS 'SELECT CASE WHEN b=0 THEN NULL ELSE a/b END'`
- THEN OrReplace:true, Volatility:IMMUTABLE

### Requirement: UDF Param Parsing
Params SHALL be name TypeName.

#### Scenario: Multiple params
- GIVEN (id UUID, name VARCHAR(50))
- THEN Params list w/ types

## ADDED Requirements (Execution Tie-in)
### Requirement: Visitor Support
Visitor SHALL have VisitCreateScalarUDFStmt(CreateScalarUDFStmt)