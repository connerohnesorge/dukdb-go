# Tasks: Enum Utility Functions

- [ ] 1. Implement ENUM_RANGE — In `internal/executor/expr.go`, add case "ENUM_RANGE" in `evaluateFunctionCall()`. Takes 1 string argument (type name). Look up type via `e.catalog.GetType(typeName, "")`. Verify TypeKind is "ENUM". Return `typeEntry.EnumValues` as `[]any`. In binder utils.go, add "ENUM_RANGE" returning TYPE_ANY. Validate: `CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy'); SELECT ENUM_RANGE('mood')` → ['sad', 'ok', 'happy'].

- [ ] 2. Implement ENUM_FIRST and ENUM_LAST — In `internal/executor/expr.go`, add cases for "ENUM_FIRST" and "ENUM_LAST". Same type lookup as ENUM_RANGE. ENUM_FIRST returns `EnumValues[0]`, ENUM_LAST returns `EnumValues[len-1]`. In binder, return TYPE_VARCHAR. Validate: `SELECT ENUM_FIRST('mood')` → 'sad', `SELECT ENUM_LAST('mood')` → 'happy'.

- [ ] 3. Integration tests — Test all three functions with: valid enum type, non-existent type (error), NULL input (returns NULL), single-value enum, empty string type name. Verify ENUM_RANGE returns correct order matching CREATE TYPE definition.
