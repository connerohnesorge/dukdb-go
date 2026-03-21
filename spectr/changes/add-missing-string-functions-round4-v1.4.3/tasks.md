# Tasks: Missing String Functions Round 4

- [ ] 1. Add LCASE/UCASE aliases — Add "LCASE" to the LOWER case label at expr.go:1247 and "UCASE" to the UPPER case label at expr.go:1235. Add type inference entries in binder/utils.go. Validate: `SELECT LCASE('HELLO')` returns 'hello'.

- [ ] 2. Add OCTET_LENGTH function — Add case in evaluateFunctionCall() near LENGTH at expr.go:1259. Return int64(len(toString(args[0]))). Add type inference returning TYPE_INTEGER. Validate: `SELECT OCTET_LENGTH('héllo')` returns 6.

- [ ] 3. Add INITCAP function — Add case in evaluateFunctionCall(). Capitalize first letter of each word, lowercase rest. Word boundaries are non-alphanumeric characters. Add type inference returning TYPE_VARCHAR. Validate: `SELECT INITCAP('hello world')` returns 'Hello World'.

- [ ] 4. Add SOUNDEX function — Add case in evaluateFunctionCall(). Implement standard American SOUNDEX algorithm (4-char code). Add type inference returning TYPE_VARCHAR. Validate: `SELECT SOUNDEX('Robert')` returns 'R163'.

- [ ] 5. Add LIKE_ESCAPE function — Add case in evaluateFunctionCall(). Implement LIKE pattern matching with custom escape character. Check for existing LIKE matching helpers in the codebase. Add type inference returning TYPE_BOOLEAN. Validate: `SELECT LIKE_ESCAPE('10%', '10#%', '#')` returns true.

- [ ] 6. Integration tests — Test all functions with NULL propagation, edge cases (empty strings, special characters), and type verification.
