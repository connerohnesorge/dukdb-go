## ADDED Requirements

### Requirement: UNION BY NAME Execution

The engine SHALL execute UNION BY NAME by matching columns by name across both sides, padding missing columns with NULL, and producing a unified result set.

#### Scenario: Overlapping columns with different order

- GIVEN `SELECT a, b FROM t1` returns (1, 2) and `SELECT b, a FROM t2` returns (3, 4)
- WHEN executing `SELECT a, b FROM t1 UNION ALL BY NAME SELECT b, a FROM t2`
- THEN the result contains columns [a, b] with rows (1, 2) and (4, 3)

#### Scenario: Partially overlapping columns with NULL padding

- GIVEN `SELECT a, b FROM t1` returns (1, 2) and `SELECT b, c FROM t2` returns (3, 4)
- WHEN executing `SELECT a, b FROM t1 UNION ALL BY NAME SELECT b, c FROM t2`
- THEN the result has columns [a, b, c]
- AND row from t1 is (1, 2, NULL) — c padded with NULL
- AND row from t2 is (NULL, 3, 4) — a padded with NULL

#### Scenario: No overlapping columns

- GIVEN `SELECT a FROM t1` returns (1) and `SELECT b FROM t2` returns (2)
- WHEN executing `SELECT a FROM t1 UNION ALL BY NAME SELECT b FROM t2`
- THEN the result has columns [a, b]
- AND rows are (1, NULL) and (NULL, 2)

#### Scenario: UNION BY NAME with deduplication

- GIVEN `SELECT a, b FROM t1` returns (1, 2) and `SELECT a, b FROM t2` returns (1, 2)
- WHEN executing `SELECT a, b FROM t1 UNION BY NAME SELECT a, b FROM t2`
- THEN the result contains a single row (1, 2) — duplicates removed

#### Scenario: Type promotion for matching columns

- GIVEN `SELECT 1::INTEGER AS x` and `SELECT 1000000000::BIGINT AS x`
- WHEN executing the UNION BY NAME
- THEN column x has type BIGINT (common supertype)

#### Scenario: Case-insensitive column matching

- GIVEN `SELECT A FROM t1` and `SELECT a FROM t2`
- WHEN executing UNION BY NAME
- THEN columns A and a are matched as the same column
