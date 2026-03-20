# Tasks: PostgreSQL Catalog Views Implementation

## 1. OID System Implementation

- [ ] 1.1 Design OID allocation strategy (ranges: system, user, types, etc.)
- [ ] 1.2 Implement OID generator in internal/metadata/
- [ ] 1.3 Create stable OID mapping for catalog objects
- [ ] 1.4 Implement OID persistence per connection
- [ ] 1.5 Add OID type support to type system if needed
- [ ] 1.6 Write unit tests for OID generation

## 2. pg_catalog Infrastructure

- [ ] 2.1 Create/extend `internal/postgres/catalog/pg_catalog.go`
- [ ] 2.2 Implement pg_catalog schema creation at engine init
- [ ] 2.3 Register all views in virtual table registry
- [ ] 2.4 Add pg_catalog to default search paths

## 3. Core System Views (Part 1)

- [ ] 3.1 Implement pg_catalog.pg_namespace view
- [ ] 3.2 Implement pg_catalog.pg_class view
- [ ] 3.3 Implement pg_catalog.pg_attribute view
- [ ] 3.4 Add tests for 3.1-3.3 (verify OID joins work)

## 4. Type and Function Views (Part 2)

- [ ] 4.1 Implement pg_catalog.pg_type view
- [ ] 4.2 Implement pg_catalog.pg_proc view
- [ ] 4.3 Add tests for 4.1-4.2

## 5. Index and Constraint Views (Part 3)

- [ ] 5.1 Implement pg_catalog.pg_index view
- [ ] 5.2 Implement pg_catalog.pg_constraint view
- [ ] 5.3 Add tests for 5.1-5.2

## 6. Database and User Views (Part 4)

- [ ] 6.1 Implement pg_catalog.pg_database view
- [ ] 6.2 Implement pg_catalog.pg_user view
- [ ] 6.3 Implement pg_catalog.pg_roles view
- [ ] 6.4 Add tests for 6.1-6.3

## 7. User-Friendly Views (Part 5)

- [ ] 7.1 Implement pg_catalog.pg_tables view
- [ ] 7.2 Implement pg_catalog.pg_views view
- [ ] 7.3 Add tests for 7.1-7.2

## 8. Configuration and Monitoring Views (Part 6)

- [ ] 8.1 Implement pg_catalog.pg_settings view
- [ ] 8.2 Implement pg_catalog.pg_stat_activity view
- [ ] 8.3 Implement pg_catalog.pg_stat_statements view (optional, with provider)
- [ ] 8.4 Add tests for 8.1-8.3

## 9. OID Join Validation

- [ ] 9.1 Test JOINs: pg_class JOIN pg_namespace on relnamespace = oid
- [ ] 9.2 Test JOINs: pg_attribute JOIN pg_class on attrelid = oid
- [ ] 9.3 Test JOINs: pg_attribute JOIN pg_type on atttypid = oid
- [ ] 9.4 Test JOINs: pg_index JOIN pg_class on indexrelid = oid
- [ ] 9.5 Test JOINs: pg_constraint JOIN pg_class on conrelid = oid
- [ ] 9.6 Verify all OID-based relationships work correctly

## 10. Tool Compatibility Testing

- [ ] 10.1 Test with DBeaver (if available)
- [ ] 10.2 Test with pgAdmin (if available)
- [ ] 10.3 Verify schema tree displays correctly
- [ ] 10.4 Test psql-like commands (\dt, \dv, \df)

## 11. Performance and Edge Cases

- [ ] 11.1 Benchmark large schema queries
- [ ] 11.2 Test system schemas visibility (pg_catalog, information_schema)
- [ ] 11.3 Test empty database (no user tables/views)
- [ ] 11.4 Test special characters in identifiers
- [ ] 11.5 Verify NULL handling for optional fields

## 12. Testing and Validation

- [ ] 12.1 Test all views return correct column schemas
- [ ] 12.2 Cross-validate against PostgreSQL v14 pg_catalog
- [ ] 12.3 Test aggregate queries on large catalogs
- [ ] 12.4 Run all existing tests for regressions

## 13. Documentation and Polish

- [ ] 13.1 Add godoc comments to pg_catalog package
- [ ] 13.2 Document each view's columns and usage
- [ ] 13.3 Document OID system and how it works
- [ ] 13.4 Add examples for common pg_catalog queries
- [ ] 13.5 Update CLAUDE.md with pg_catalog documentation

## Dependencies

- **Depends On**: `add-system-functions` (requires metadata infrastructure)
- **Depends On**: `add-information-schema` (pg_catalog complements information_schema)
- **No blocking**: This is the final proposal
