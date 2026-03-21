# Tasks: Missing Conversion Functions

- [ ] 1. Add TO_DATE function — Add "TO_DATE" case to evaluateFunctionCall() (expr.go:630). Reuse evalStrptime() (temporal_functions.go:1718) for 2-arg form, then convert result from TIMESTAMP to DATE using timestampToTime() (temporal_functions.go:95) and timeToDate() (temporal_functions.go:461). For 1-arg form, parse ISO date string directly. Add TYPE_DATE return to inferFunctionResultType() (binder/utils.go:347). Use `&dukdb.Error{}` for errors. Validate: `SELECT TO_DATE('2024-01-15', '%Y-%m-%d')` returns correct DATE.

- [ ] 2. Add TO_CHAR function — Add "TO_CHAR" alias to existing STRFTIME case in evaluateFunctionCall() (expr.go near line 1950). Add to inferFunctionResultType() alongside STRFTIME. Validate: `SELECT TO_CHAR(DATE '2024-01-15', '%Y/%m/%d')` returns '2024/01/15'.

- [ ] 3. Add GENERATE_SUBSCRIPTS function — Add "GENERATE_SUBSCRIPTS" case to evaluateFunctionCall(). Takes array and optional dimension, returns 1-based integer indices. Follow list function patterns in expr.go. Add TYPE_INTEGER return to inferFunctionResultType(). Validate: `SELECT GENERATE_SUBSCRIPTS([10, 20, 30], 1)` returns [1, 2, 3].

- [ ] 4. Integration tests — Test TO_DATE with format strings, auto-detection, NULL inputs, invalid dates. Test TO_CHAR with DATE, TIMESTAMP, various formats. Test GENERATE_SUBSCRIPTS with arrays, empty arrays, NULL.
