# Ddl Dml Extensions Specification

## Requirements

### Requirement: COMMENT ON statement

The COMMENT ON statement SHALL attach a text comment to a database object (TABLE, COLUMN, VIEW, INDEX, SCHEMA). COMMENT ON ... IS NULL MUST remove the comment.

#### Scenario: Set table comment

When `COMMENT ON TABLE employees IS 'Main employee directory'` is executed
Then the table's comment metadata MUST be set to 'Main employee directory'

#### Scenario: Set column comment

When `COMMENT ON COLUMN employees.salary IS 'Annual base salary in USD'` is executed
Then the column's comment metadata MUST be set to 'Annual base salary in USD'

#### Scenario: Drop comment with NULL

When `COMMENT ON TABLE employees IS NULL` is executed
Then the table's comment metadata MUST be removed (set to empty string)

#### Scenario: Error on non-existent object

When `COMMENT ON TABLE nonexistent IS 'hello'` is executed
Then an error MUST be returned indicating the table does not exist

### Requirement: ALTER TABLE ALTER COLUMN TYPE

ALTER TABLE ... ALTER COLUMN ... TYPE MUST change a column's data type and convert existing data.

#### Scenario: Change column type

Given a table `t` with column `c` of type VARCHAR containing values '1', '2', '3'
When `ALTER TABLE t ALTER COLUMN c TYPE INTEGER` is executed
Then column `c` MUST have type INTEGER
And the values MUST be converted to 1, 2, 3

#### Scenario: SET DATA TYPE alternative syntax

When `ALTER TABLE t ALTER COLUMN c SET DATA TYPE VARCHAR` is executed
Then the column type MUST be changed to VARCHAR

#### Scenario: Error on invalid conversion

Given a table `t` with column `c` of type VARCHAR containing 'hello'
When `ALTER TABLE t ALTER COLUMN c TYPE INTEGER` is executed
Then an error MUST be returned indicating the value cannot be converted

### Requirement: DELETE ... USING multi-table delete

DELETE ... USING SHALL support joining additional tables to determine which rows to delete from the target table.

#### Scenario: Basic USING delete

Given tables `orders` and `customers` where some customers are inactive
When `DELETE FROM orders USING customers WHERE orders.customer_id = customers.id AND customers.active = false` is executed
Then only orders belonging to inactive customers MUST be deleted

#### Scenario: Multiple USING tables

When `DELETE FROM t1 USING t2, t3 WHERE t1.a = t2.a AND t2.b = t3.b` is executed
Then rows in t1 matching the join of t2 and t3 MUST be deleted

#### Scenario: USING with RETURNING

When `DELETE FROM orders USING customers WHERE orders.cid = customers.id RETURNING orders.*` is executed
Then the deleted rows MUST be returned in the result set

#### Scenario: USING table does not exist

When `DELETE FROM t1 USING nonexistent WHERE t1.id = nonexistent.id` is executed
Then an error MUST be returned indicating the table does not exist

