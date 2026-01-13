# Transaction Examples

This example demonstrates transaction management in dukdb-go, including basic transactions, savepoints, and isolation levels.

## Features Demonstrated

- Basic ACID transactions (BEGIN, COMMIT, ROLLBACK)
- Transfer operations with consistency
- Savepoints for partial rollbacks
- Isolation level configuration
- Transaction error handling

## Running the Example

```bash
go run main.go
```

## Transaction Patterns

### 1. Basic Transaction
Wrap multiple operations in a single atomic transaction:
```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

### 2. Savepoints
Create named savepoints for partial rollbacks:
```sql
BEGIN;
UPDATE accounts SET balance = balance * 1.05 WHERE id = 1;
SAVEPOINT before_bob_update;
-- If this fails, roll back to savepoint
UPDATE accounts SET balance = balance * 1.05 WHERE id = 2;
COMMIT;
```

### 3. Isolation Levels
Set transaction isolation for consistency:
```sql
SET default_transaction_isolation = 'SERIALIZABLE';
BEGIN;
-- Transaction with serializable isolation
COMMIT;
```

## Supported Isolation Levels

- READ UNCOMMITTED
- READ COMMITTED
- REPEATABLE READ
- SERIALIZABLE

## Error Handling

The example demonstrates proper error handling:
- Rolling back on errors
- Using savepoints for partial recovery
- Checking transaction status before commit
