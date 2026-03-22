# Tasks: Missing List/String Functions

- [ ] 1. Add LIST_APPEND and LIST_PREPEND — Add "LIST_APPEND", "ARRAY_APPEND", "ARRAY_PUSH_BACK" case to evaluateFunctionCall() (expr.go:630) near LIST_CONCAT (line 2590). Append element to end of list. Add "LIST_PREPEND", "ARRAY_PREPEND", "ARRAY_PUSH_FRONT" — note DuckDB signature is (element, list), element first. Add type inference in inferFunctionResultType() (binder/utils.go:347). Validate: `SELECT LIST_APPEND([1,2,3], 4)` returns [1,2,3,4].

- [ ] 2. Add alias functions — Add "LIST_HAS" to existing LIST_CONTAINS case (expr.go:2538). Add "STRING_TO_ARRAY" to existing STRING_SPLIT case (expr.go:1522). Update type inference entries. Validate: `SELECT LIST_HAS([1,2,3], 2)` returns true, `SELECT STRING_TO_ARRAY('a,b,c', ',')` returns array.

- [ ] 3. Add REGEXP_FULL_MATCH — Add case near REGEXP_MATCHES (expr.go:1455). Anchor pattern with ^(?: and )$ for full-string matching. Return TYPE_BOOLEAN from inferFunctionResultType(). Use `&dukdb.Error{}` for errors. Validate: `SELECT REGEXP_FULL_MATCH('hello', 'h.*o')` returns true.

- [ ] 4. Integration tests — Test all functions with NULL inputs, empty lists, edge cases. Verify LIST_PREPEND argument order matches DuckDB (element first). Verify REGEXP_FULL_MATCH rejects partial matches.
