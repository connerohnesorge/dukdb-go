# Change: add-create-scalar-udf-parser-support

## Why
CREATE FUNCTION for scalar UDFs is core DuckDB extensibility feature (1000+ OSS UDFs). Missing parser blocks UDF spec/impl/compatibility. Enables custom funcs like Excel helpers for fidelity calcs.

## What Changes
- Parse `CREATE [OR REPLACE] FUNCTION name(params) RETURNS type [LANGUAGE lang] [IMMUTABLE etc.] AS $$code$$`
- Supports DuckDB UDF attrs for optimizer.

## Impact
- Affected specs: parser, scalar-udf (execution)
- Code: parser.go parseCreateFunction, ast.go CreateFunctionStmt
- Tests: parser_test UDF variants

## Breaking Changes
None.