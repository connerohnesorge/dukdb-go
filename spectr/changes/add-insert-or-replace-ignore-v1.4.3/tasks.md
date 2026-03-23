# Tasks: INSERT OR REPLACE/IGNORE Syntax

- [ ] 1. Parser change — In parseInsert() at parser.go:1509, after consuming INSERT, check for OR keyword followed by REPLACE or IGNORE. Set a local variable tracking the action. Continue parsing INTO and rest normally. Validate: `INSERT OR IGNORE INTO t VALUES (1)` parses without error.

- [ ] 2. Desugar to OnConflict — At end of parseInsert(), if OR action was detected and no explicit ON CONFLICT exists, create OnConflictClause. OR IGNORE → OnConflictDoNothing. OR REPLACE → OnConflictDoUpdate with UpdateSet for all columns using EXCLUDED references. Validate: Parsed INSERT OR REPLACE generates correct OnConflictClause.

- [ ] 3. Integration tests — Test INSERT OR IGNORE with and without conflicts. Test INSERT OR REPLACE with and without conflicts. Verify existing ON CONFLICT syntax still works. Test with primary key and unique constraints.
