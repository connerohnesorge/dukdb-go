# List and String Functions

## ADDED Requirements

### Requirement: LIST_APPEND function

LIST_APPEND SHALL append an element to the end of a list. It MUST accept aliases ARRAY_APPEND and ARRAY_PUSH_BACK.

#### Scenario: Append to list

When `SELECT LIST_APPEND([1, 2, 3], 4)` is executed
Then the result MUST be [1, 2, 3, 4]

#### Scenario: Append to empty list

When `SELECT LIST_APPEND([], 1)` is executed
Then the result MUST be [1]

### Requirement: LIST_PREPEND function

LIST_PREPEND SHALL prepend an element to the start of a list. It MUST accept the element as the first argument and the list as the second argument.

#### Scenario: Prepend to list

When `SELECT LIST_PREPEND(0, [1, 2, 3])` is executed
Then the result MUST be [0, 1, 2, 3]

### Requirement: LIST_HAS alias

LIST_HAS SHALL be an alias for LIST_CONTAINS. It MUST return true if the list contains the given element.

#### Scenario: LIST_HAS equivalence

When `SELECT LIST_HAS([1, 2, 3], 2)` is executed
Then the result MUST be true

### Requirement: STRING_TO_ARRAY alias

STRING_TO_ARRAY SHALL be an alias for STRING_SPLIT. It MUST split a string by a delimiter and return an array.

#### Scenario: STRING_TO_ARRAY equivalence

When `SELECT STRING_TO_ARRAY('a,b,c', ',')` is executed
Then the result MUST be ['a', 'b', 'c']

### Requirement: REGEXP_FULL_MATCH function

REGEXP_FULL_MATCH SHALL return true only if the entire string matches the given pattern. It MUST NOT match partial strings.

#### Scenario: Full match

When `SELECT REGEXP_FULL_MATCH('hello', 'h.*o')` is executed
Then the result MUST be true

#### Scenario: Partial match rejection

When `SELECT REGEXP_FULL_MATCH('hello world', 'hello')` is executed
Then the result MUST be false because 'hello' does not match the full string
