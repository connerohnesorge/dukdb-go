# Tasks: Current Date/Time Functions

- [ ] 1. Parser bare keyword support — In parseIdentExpr() (parser.go:5035), add cases for CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP to the existing keyword switch (line 5039-5083). When not followed by `(`, create a zero-arg FunctionCall. When followed by `(`, fall through to parseFunctionCall at line 5087. Validate: `SELECT CURRENT_DATE` parses without error.

- [ ] 2. Executor function dispatch — In evaluateFunctionCall() (expr.go:661), add cases in the main switch fn.Name block: NOW/CURRENT_TIMESTAMP return time.Now(), CURRENT_DATE/TODAY return time.Now() truncated to date, CURRENT_TIME returns time of day. Validate: `SELECT NOW()` returns non-null timestamp.

- [ ] 3. Integration tests — Test all 5 function names with and without parentheses. Test NULL handling (never NULL). Test type correctness (TIMESTAMP for NOW, DATE for CURRENT_DATE, TIME for CURRENT_TIME). Test in expressions (CURRENT_DATE + INTERVAL '1 day'). Test in INSERT/WHERE clauses.
