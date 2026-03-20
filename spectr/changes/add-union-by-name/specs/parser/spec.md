## ADDED Requirements

### Requirement: UNION BY NAME Parsing

The parser SHALL parse `UNION [ALL] BY NAME` as a set operation variant that matches columns by name.

#### Scenario: UNION BY NAME

- WHEN parsing `SELECT a, b FROM t1 UNION BY NAME SELECT b, c FROM t2`
- THEN the parser produces a SelectStmt with SetOp=SetOpUnionByName

#### Scenario: UNION ALL BY NAME

- WHEN parsing `SELECT a FROM t1 UNION ALL BY NAME SELECT b FROM t2`
- THEN the parser produces a SelectStmt with SetOp=SetOpUnionAllByName

#### Scenario: Chained UNION BY NAME

- WHEN parsing `SELECT a FROM t1 UNION BY NAME SELECT b FROM t2 UNION BY NAME SELECT c FROM t3`
- THEN the parser produces a left-associative chain of SetOpUnionByName operations
