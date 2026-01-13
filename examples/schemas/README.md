# Schema Examples

This example demonstrates using schemas to organize database objects and manage namespaces in dukdb-go.

## Features Demonstrated

- Creating schemas for organization
- Creating tables in different schemas
- Cross-schema queries
- Schema-based access patterns
- Best practices for schema design

## Running the Example

```bash
go run main.go
```

## Schema Basics

### Creating Schemas
```sql
CREATE SCHEMA sales;
CREATE SCHEMA marketing;
CREATE SCHEMA analytics;
```

### Creating Tables in Schemas
```sql
CREATE TABLE sales.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date DATE,
    total_amount DECIMAL(10,2)
);

CREATE TABLE marketing.campaigns (
    campaign_id INTEGER PRIMARY KEY,
    campaign_name VARCHAR(100),
    start_date DATE,
    budget DECIMAL(10,2)
);
```

### Querying Across Schemas
```sql
-- Query single schema
SELECT * FROM sales.orders;

-- Query multiple schemas
SELECT 
    (SELECT COUNT(*) FROM sales.orders) as order_count,
    (SELECT COUNT(*) FROM marketing.campaigns) as campaign_count;

-- Create views across schemas
CREATE TABLE analytics.metrics AS
SELECT 
    CURRENT_DATE as metric_date,
    (SELECT SUM(total_amount) FROM sales.orders) as revenue;
```

## Schema Use Cases

### Organization
- **sales**: Order management, customers, products
- **marketing**: Campaigns, leads, conversions
- **analytics**: Reports, aggregates, metrics
- **staging**: Temporary tables for ETL processes

### Multi-Tenancy
- Isolate customer data per schema
- Manage permissions at schema level
- Avoid table name conflicts

### Versioning
- Keep multiple versions of tables
- Migrate data between schema versions
- Maintain backward compatibility

## Best Practices

✅ **Do:**
- Use schemas to logically group related tables
- Set appropriate permissions per schema
- Use consistent naming conventions
- Document schema purposes

❌ **Don't:**
- Put everything in the default schema
- Mix unrelated tables in same schema
- Create too many schemas (hard to manage)
