# Change Proposal: Recursive CTEs and Lateral Joins for DuckDB v1.4.3

This directory contains the comprehensive change proposal for implementing Recursive Common Table Expressions (CTEs) and Lateral Joins in dukdb-go to achieve DuckDB v1.4.3 compatibility.

## Directory Structure

```
recursive-cte-lateral-v1.4.3/
├── proposal.md              # High-level overview and motivation
├── design.md               # Detailed technical architecture
├── tasks.jsonc             # Implementation task breakdown
├── specs/
│   ├── recursive-cte/
│   │   └── spec.md        # Recursive CTE requirements
│   └── lateral-join/
│       └── spec.md        # Lateral join requirements
└── README.md              # This file
```

## Key Features

### Recursive CTEs
- Basic recursive query support with `WITH RECURSIVE`
- Hierarchical data traversal (organizational charts, trees)
- Graph algorithms (shortest path, network analysis)
- **USING KEY optimization** for 10-100x performance improvement
- Cycle detection and termination
- Memory-efficient iterative execution

### Lateral Joins
- Row-by-row subquery evaluation with `LATERAL`
- Support for all join types (INNER, LEFT, RIGHT, FULL)
- Multiple lateral joins in single query
- Complex correlations and aggregations
- Performance optimized with batch processing

## Quick Start Examples

### Recursive CTE - Counter
```sql
WITH RECURSIVE counter(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM counter WHERE n < 10
)
SELECT * FROM counter;
```

### Recursive CTE - Shortest Path with USING KEY
```sql
WITH RECURSIVE shortest_path USING KEY(node) AS (
    SELECT 'A' as node, 0 as distance
    UNION ALL
    SELECT e.to_node, sp.distance + e.weight
    FROM shortest_path sp
    JOIN edges e ON sp.node = e.from_node
    WHERE sp.distance + e.weight < COALESCE(
        (SELECT distance FROM shortest_path WHERE node = e.to_node), 999999
    )
)
SELECT * FROM shortest_path;
```

### Lateral Join - Top-N per Group
```sql
SELECT c.customer_name, latest_order.*
FROM customers c
JOIN LATERAL (
    SELECT order_id, order_date, total_amount
    FROM orders o
    WHERE o.customer_id = c.customer_id
    ORDER BY order_date DESC
    LIMIT 1
) latest_order ON true;
```

## Implementation Status

Track implementation progress in `tasks.jsonc`. Each task includes:
- Unique ID and descriptive title
- Implementation status (todo/in_progress/completed)
- Priority level (high/medium/low)
- Estimated effort in hours
- Dependencies on other tasks

## Testing

Comprehensive test suites will be created covering:
- Unit tests for individual components
- Integration tests for end-to-end scenarios
- Performance benchmarks
- DuckDB compatibility validation

## Resources

- [DuckDB Documentation](https://duckdb.org/docs/)
- [PostgreSQL Recursive CTEs](https://www.postgresql.org/docs/current/queries-with.html)
- [SQL:1999 Standard](https://www.wiscorp.com/SQLStandards.html)

## Contact

For questions about this proposal, please:
1. Review the detailed specifications in `specs/`
2. Check the implementation tasks in `tasks.jsonc`
3. Contact the dukdb-go team

## License

This documentation is part of the dukdb-go project and follows the same license terms."}