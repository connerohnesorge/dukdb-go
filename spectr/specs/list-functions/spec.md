# List Functions Specification

## Requirements

### Requirement: LIST_ELEMENT SHALL extract element by 1-based index

The LIST_ELEMENT and ARRAY_EXTRACT functions SHALL accept a list and an integer index, returning the element at that position using 1-based indexing.

#### Scenario: Positive index
```
When the user executes "SELECT LIST_ELEMENT([10, 20, 30], 2)"
Then the result is 20
```

#### Scenario: Negative index counts from end
```
When the user executes "SELECT LIST_ELEMENT([10, 20, 30], -1)"
Then the result is 30
```

#### Scenario: Out-of-bounds returns NULL
```
When the user executes "SELECT LIST_ELEMENT([10, 20, 30], 5)"
Then the result is NULL
```

#### Scenario: Index zero returns NULL
```
When the user executes "SELECT LIST_ELEMENT([10, 20, 30], 0)"
Then the result is NULL
Because 1-based indexing has no element at position 0
```

#### Scenario: NULL list returns NULL
```
When the user executes "SELECT LIST_ELEMENT(NULL, 1)"
Then the result is NULL
```

#### Scenario: ARRAY_EXTRACT is alias for LIST_ELEMENT
```
When the user executes "SELECT ARRAY_EXTRACT(['a', 'b', 'c'], 2)"
Then the result is 'b'
```

### Requirement: LIST_AGGREGATE SHALL apply named aggregate to list elements

The LIST_AGGREGATE and ARRAY_AGGREGATE functions SHALL accept a list and an aggregate name string, applying the named aggregate to the list elements.

#### Scenario: Sum aggregate
```
When the user executes "SELECT LIST_AGGREGATE([1, 2, 3, 4], 'sum')"
Then the result is 10
```

#### Scenario: Avg aggregate
```
When the user executes "SELECT LIST_AGGREGATE([10, 20, 30], 'avg')"
Then the result is 20.0
```

#### Scenario: Min aggregate
```
When the user executes "SELECT LIST_AGGREGATE([3, 1, 2], 'min')"
Then the result is 1
```

#### Scenario: Max aggregate
```
When the user executes "SELECT LIST_AGGREGATE([3, 1, 2], 'max')"
Then the result is 3
```

#### Scenario: Count aggregate
```
When the user executes "SELECT LIST_AGGREGATE([1, NULL, 3], 'count')"
Then the result is 2
Because NULL values are excluded from count
```

#### Scenario: String_agg aggregate
```
When the user executes "SELECT LIST_AGGREGATE(['a', 'b', 'c'], 'string_agg', ',')"
Then the result is 'a,b,c'
```

#### Scenario: NULL list returns NULL
```
When the user executes "SELECT LIST_AGGREGATE(NULL, 'sum')"
Then the result is NULL
```

#### Scenario: Empty list returns NULL for most aggregates
```
When the user executes "SELECT LIST_AGGREGATE([], 'sum')"
Then the result is NULL
```

#### Scenario: Unknown aggregate returns error
```
When the user executes "SELECT LIST_AGGREGATE([1,2], 'unknown')"
Then an error is returned indicating unsupported aggregate
```

### Requirement: LIST_REVERSE_SORT SHALL sort list in descending order

The LIST_REVERSE_SORT and ARRAY_REVERSE_SORT functions SHALL sort list elements in descending order with NULLs at the end.

#### Scenario: Descending sort of integers
```
When the user executes "SELECT LIST_REVERSE_SORT([3, 1, 4, 1, 5])"
Then the result is [5, 4, 3, 1, 1]
```

#### Scenario: Descending sort of strings
```
When the user executes "SELECT LIST_REVERSE_SORT(['banana', 'apple', 'cherry'])"
Then the result is ['cherry', 'banana', 'apple']
```

#### Scenario: NULLs sort to end
```
When the user executes "SELECT LIST_REVERSE_SORT([3, NULL, 1])"
Then the result is [3, 1, NULL]
```

#### Scenario: NULL list returns NULL
```
When the user executes "SELECT LIST_REVERSE_SORT(NULL)"
Then the result is NULL
```

### Requirement: ARRAY_TO_STRING SHALL join list elements with separator

The ARRAY_TO_STRING and LIST_TO_STRING functions SHALL join list elements into a string with the specified separator.

#### Scenario: Basic join
```
When the user executes "SELECT ARRAY_TO_STRING([1, 2, 3], ',')"
Then the result is '1,2,3'
```

#### Scenario: NULLs skipped by default
```
When the user executes "SELECT ARRAY_TO_STRING([1, NULL, 3], ',')"
Then the result is '1,3'
```

#### Scenario: NULL replacement string
```
When the user executes "SELECT ARRAY_TO_STRING([1, NULL, 3], ',', 'N/A')"
Then the result is '1,N/A,3'
```

#### Scenario: Empty list
```
When the user executes "SELECT ARRAY_TO_STRING([], ',')"
Then the result is ''
```

#### Scenario: NULL list returns NULL
```
When the user executes "SELECT ARRAY_TO_STRING(NULL, ',')"
Then the result is NULL
```

### Requirement: LIST_ZIP SHALL zip multiple lists into list of structs

The LIST_ZIP function SHALL combine elements from multiple lists at the same position into structs.

#### Scenario: Zip two equal-length lists
```
When the user executes "SELECT LIST_ZIP([1, 2], ['a', 'b'])"
Then the result is a list of two structs: [{f1: 1, f2: 'a'}, {f1: 2, f2: 'b'}]
```

#### Scenario: Zip unequal-length lists pads with NULL
```
When the user executes "SELECT LIST_ZIP([1, 2, 3], ['a'])"
Then the result is [{f1: 1, f2: 'a'}, {f1: 2, f2: NULL}, {f1: 3, f2: NULL}]
```

#### Scenario: Zip three lists
```
When the user executes "SELECT LIST_ZIP([1], ['a'], [true])"
Then the result is [{f1: 1, f2: 'a', f3: true}]
```

#### Scenario: NULL list returns NULL
```
When the user executes "SELECT LIST_ZIP(NULL, [1, 2])"
Then the result is NULL
```

### Requirement: LIST_RESIZE SHALL resize list to target size

The LIST_RESIZE and ARRAY_RESIZE functions SHALL truncate or extend a list to the specified size.

#### Scenario: Extend with NULL padding
```
When the user executes "SELECT LIST_RESIZE([1, 2], 4)"
Then the result is [1, 2, NULL, NULL]
```

#### Scenario: Extend with custom fill value
```
When the user executes "SELECT LIST_RESIZE([1, 2], 5, 0)"
Then the result is [1, 2, 0, 0, 0]
```

#### Scenario: Truncate
```
When the user executes "SELECT LIST_RESIZE([1, 2, 3, 4, 5], 3)"
Then the result is [1, 2, 3]
```

#### Scenario: Same size is no-op
```
When the user executes "SELECT LIST_RESIZE([1, 2, 3], 3)"
Then the result is [1, 2, 3]
```

#### Scenario: Resize to zero
```
When the user executes "SELECT LIST_RESIZE([1, 2, 3], 0)"
Then the result is []
```

#### Scenario: NULL list returns NULL
```
When the user executes "SELECT LIST_RESIZE(NULL, 5)"
Then the result is NULL
```

