# Tasks: Information Schema Views Implementation

## 1. Infrastructure and Setup

- [ ] 1.1 Create `internal/postgres/catalog/information_schema.go` file
- [ ] 1.2 Implement information_schema schema creation at engine initialization
- [ ] 1.3 Register all 12 views in virtual table registry
- [ ] 1.4 Add information_schema to default search paths

## 2. Core Views Implementation (Part 1)

- [ ] 2.1 Implement information_schema.schemata view
- [ ] 2.2 Implement information_schema.tables view
- [ ] 2.3 Implement information_schema.columns view
- [ ] 2.4 Add tests for 2.1-2.3

## 3. Constraint and Key Views (Part 2)

- [ ] 3.1 Implement information_schema.table_constraints view
- [ ] 3.2 Implement information_schema.key_column_usage view
- [ ] 3.3 Implement information_schema.referential_constraints view
- [ ] 3.4 Implement information_schema.constraint_column_usage view
- [ ] 3.5 Implement information_schema.check_constraints view
- [ ] 3.6 Add tests for 3.1-3.5

## 4. View and Function Views (Part 3)

- [ ] 4.1 Implement information_schema.views view
- [ ] 4.2 Implement information_schema.routines view
- [ ] 4.3 Implement information_schema.parameters view
- [ ] 4.4 Add tests for 4.1-4.3

## 5. Extensibility Views (Part 4)

- [ ] 5.1 Implement information_schema.triggers view (initially empty, extensible)
- [ ] 5.2 Add documentation comment for trigger extensibility
- [ ] 5.3 Add tests for 5.1

## 6. Testing and Validation

- [ ] 6.1 Test all views return correct column schemas
- [ ] 6.2 Test views work with WHERE clauses and filters
- [ ] 6.3 Test views work with JOINs (self-joins, cross-joins)
- [ ] 6.4 Test handling of edge cases (empty database, no constraints)
- [ ] 6.5 Test NULL handling for optional columns
- [ ] 6.6 Cross-validate against DuckDB v1.4.3 information_schema
- [ ] 6.7 Test with various BI/ORM tools if possible
- [ ] 6.8 Benchmark query performance on medium-sized schemas
- [ ] 6.9 Run all existing tests for regressions

## 7. Documentation and Polish

- [ ] 7.1 Add godoc comments to information_schema package
- [ ] 7.2 Document each view's columns and usage
- [ ] 7.3 Add example queries for common scenarios
- [ ] 7.4 Update CLAUDE.md with information_schema documentation

## Dependencies

- **Depends On**: `add-system-functions` (requires metadata infrastructure)
- **Blocks**: `add-postgresql-catalog` (PostgreSQL views should complement information_schema)
- **No other blocking**: Information schema can be independently tested
