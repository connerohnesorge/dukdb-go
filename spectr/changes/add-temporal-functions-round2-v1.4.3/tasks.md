# Tasks: Temporal Functions Round 2

- [ ] 1. Add date part specifiers — Add DatePartISODow, DatePartISOYear, DatePartNanosecond constants to temporal_functions.go:19-31. Add 'isodow', 'isoyear', 'weekday', 'weekofyear', 'nanosecond'/'ns' to parseDatePart() at line 35. Add extraction logic in extractPart() at line 946. ISODOW: Monday=1..Sunday=7 (convert Go Weekday). ISOYEAR: use t.ISOWeek(). Validate: `SELECT DATE_PART('isodow', TIMESTAMP '2024-01-01')` returns 1.

- [ ] 2. Add DATEPART alias — Add "DATEPART" to the DATE_PART case label at expr.go:1961. Validate: `SELECT DATEPART('year', TIMESTAMP '2024-06-15')` returns 2024.

- [ ] 3. Add TIME_BUCKET function — Implement TIME_BUCKET(interval, timestamp [, origin]) in evaluateFunctionCall(). Add intervalToMicros() helper. Floor-divide timestamp micros by bucket micros. Handle negative offsets. Default origin: 2000-01-03 (Monday). Add type inference returning TYPE_TIMESTAMP. Validate: `SELECT TIME_BUCKET(INTERVAL '1 hour', TIMESTAMP '2024-01-01 14:37:00')` returns 14:00.

- [ ] 4. Add MAKE_TIMESTAMPTZ function — Implement in evaluateFunctionCall(). Follow MAKE_TIMESTAMP pattern at expr.go:1974. Accept optional 7th timezone argument. Use time.LoadLocation(). Add type inference returning TYPE_TIMESTAMP_TZ. Validate: `SELECT MAKE_TIMESTAMPTZ(2024, 1, 1, 12, 0, 0, 'UTC')`.

- [ ] 5. Add TIMEZONE function — Implement TIMEZONE(tz, timestamp) in evaluateFunctionCall(). Convert timestamp to target timezone via ts.In(loc). Add type inference returning TYPE_TIMESTAMP_TZ. Validate: `SELECT TIMEZONE('UTC', TIMESTAMP '2024-01-01 12:00:00')`.

- [ ] 6. Add EPOCH_NS function — Implement EPOCH_NS(nanoseconds) in evaluateFunctionCall(). Convert int64 nanoseconds to time.Unix(0, ns). Add type inference returning TYPE_TIMESTAMP. Validate: `SELECT EPOCH_NS(1704067200000000000)`.

- [ ] 7. Integration tests — Test all functions with NULL propagation, edge cases, type verification. Test TIME_BUCKET with various intervals. Test MAKE_TIMESTAMPTZ with different timezones.
