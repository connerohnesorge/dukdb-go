# Map Type Specification

## Requirements

### Requirement: Map Type Definition and Validation

The system MUST implement a MAP data type that enforces key and value type consistency.

#### Scenario: Create table with MAP column
```sql
-- Given: A database connection
-- When: I execute:
CREATE TABLE user_metrics (
    user_id INTEGER,
    settings MAP(VARCHAR, VARCHAR)
);
-- Then: The table should be created successfully
-- And: The settings column should have type MAP
```

#### Scenario: Reject invalid map type declarations
```sql
-- Given: A database connection
-- When: I execute:
CREATE TABLE invalid_map (
    bad_map MAP  -- Missing type parameters
);
-- Then: Should fail with error "MAP requires key and value types"
```

### Requirement: Map Construction with MAP() Function

The system MUST provide functions to construct MAP values from keys and values.

#### Scenario: Construct map from arrays
```sql
-- Given: Database connection
-- When: I execute:
SELECT MAP(['color', 'size', 'weight'], ['red', 'large', 10.5]) as product_attrs;
-- Then: Should return MAP with three key-value pairs
-- And: Keys should be VARCHAR
-- And: Values should be UNION(VARCHAR, DOUBLE) due to mixed types
```

#### Scenario: Construct empty map
```sql
-- When: I execute:
SELECT MAP([], []) as empty_map;
-- Then: Should return empty MAP (size 0)
```

#### Scenario: Construct map with zip function
```sql
-- Given: Arrays of keys and values
-- When: I execute:
SELECT map(zip(['a','b','c'], [1,2,3])) as my_map;
-- Then: Should return MAP equivalent to MAP(['a','b','c'], [1,2,3])
```

### Requirement: Map Element Access

The system MUST allow access to map elements by key.

#### Scenario: Access map element with bracket notation
```sql
-- Given: A map in table
-- When: I execute:
SELECT settings['theme'] as theme
FROM user_metrics
WHERE user_id = 1;
-- Then: Should return value or NULL if key doesn't exist
```

#### Scenario: Use map as filter condition
```sql
-- Given: Table with MAP column
-- When: I execute:
SELECT *
FROM user_metrics
WHERE settings['enabled'] = 'true';
-- Then: Should filter rows based on map value
```

### Requirement: Map Functions

The system MUST provide utility functions for working with MAPs.

#### Scenario: Use map_keys function
```sql
-- Given: Table with MAP data
-- When: I execute:
SELECT map_keys(settings) as setting_keys
FROM user_metrics;
-- Then: Should return array of all keys in each map
```

#### Scenario: Use map_values function
```sql
-- Given: Table with MAP data
-- When: I execute:
SELECT map_values(settings) as setting_values
FROM user_metrics;
-- Then: Should return array of all values in each map
```

#### Scenario: Use map_extract function
```sql
-- Given: Table with MAP data
-- When: I execute:
SELECT map_extract(settings, 'theme') as theme_value
FROM user_metrics;
-- Then: Should return value for specified key (or NULL)
-- And: Should be equivalent to settings['theme']
```

#### Scenario: Use map_size function
```sql
-- Given: Table with MAP data
-- When: I execute:
SELECT map_size(settings) as num_settings
FROM user_metrics;
-- Then: Should return integer count of key-value pairs
```

### Requirement: Map Key Constraints

The system MUST enforce constraints on map keys, such as uniqueness.

#### Scenario: Reject duplicate keys
```sql
-- Given: Database connection
-- When: I execute:
SELECT MAP(['a', 'a'], [1, 2]) as bad_map;
-- Then: Should fail with error "Duplicate keys in MAP"
```

#### Scenario: Allow different keys per row
```sql
-- Given: Table with MAP column
-- When: I execute:
INSERT INTO user_metrics VALUES
    (1, MAP(['a'], [1])),
    (2, MAP(['b'], [2]));
-- Then: Should succeed
-- And: Different rows can have different keys
```

### Requirement: Map Iteration and Unnesting

The system MUST support unnesting maps into rows.

#### Scenario: Unnest map to rows with UNNEST
```sql
-- Given: Table with MAP column
-- When: I execute:
SELECT user_id, key, value
FROM user_metrics,
     UNNEST(settings);
-- Then: Should expand each key-value pair to separate rows
-- And: Possible with implicit UNNEST
```

#### Scenario: Use with generate_series for key enumeration
```sql
-- Given: Map with numeric keys
-- When: I execute:
SELECT MAP(CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END, i)
FROM generate_series(1, 10) t(i);
-- Then: Should construct maps for each input row
```

### Requirement: Map Operations in Expressions

The system MUST support map operations in complex expressions.

#### Scenario: Concatenate maps
```sql
-- Given: Two maps
-- When: I execute:
SELECT MAP_CAT(
    MAP(['a'], [1]),
    MAP(['b'], [2])
) as combined;
-- Then: Should return MAP with keys from both (if no duplicates)
```

#### Scenario: Map in GROUP BY
```sql
-- Given: Table with MAP column
-- When: I execute:
SELECT settings, COUNT(*) as count
FROM user_metrics
GROUP BY settings;
-- Then: Should group by entire map structure
```

### Requirement: COPY with Maps

The system MUST support importing and exporting MAP data via COPY.

#### Scenario: Export map to CSV
```sql
-- Given: Table with MAP data
-- When: I execute:
COPY user_metrics TO '/tmp/export.csv' (HEADER TRUE);
-- Then: Should export map as serialized format (e.g., {key1=value1,key2=value2})
```

#### Scenario: Import map from CSV/JSON
```sql
-- Given: A file with map data
-- When: I execute:
COPY user_metrics FROM '/tmp/import.csv';
-- Then: Should parse map format correctly
```

### Requirement: Existing Map Type Definition

The system MUST update the existing Map type implementation.

#### Scenario: Enhance types.go Map implementation
- **Given** types.go Map definition
- **When** updated for v1.4.3
- **Then** Value() method is added for parameter binding
- **And** Serialization support is added for database format

### Requirement: Expression Framework Updates

The system MUST update the expression framework to support map operators.

#### Scenario: Add bracket operator support
- **Given** executor operators
- **When** updated
- **Then** MapAccessOperator supports [] notation
- **And** Type checking validates key/value types

