# Enum Utility Functions Specification

## Requirements

### Requirement: ENUM_RANGE function

ENUM_RANGE(type_name) SHALL return all values of the specified enum type as a list of strings in definition order.

#### Scenario: ENUM_RANGE returns all values

Given `CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`
When `SELECT ENUM_RANGE('mood')` is executed
Then the result MUST be ['sad', 'ok', 'happy']

#### Scenario: ENUM_RANGE on non-existent type

When `SELECT ENUM_RANGE('nonexistent')` is executed
Then an error MUST be returned indicating the type is not found

#### Scenario: ENUM_RANGE with NULL

When `SELECT ENUM_RANGE(NULL)` is executed
Then the result MUST be NULL

### Requirement: ENUM_FIRST function

ENUM_FIRST(type_name) SHALL return the first value of the specified enum type.

#### Scenario: ENUM_FIRST returns first value

Given `CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`
When `SELECT ENUM_FIRST('mood')` is executed
Then the result MUST be 'sad'

### Requirement: ENUM_LAST function

ENUM_LAST(type_name) SHALL return the last value of the specified enum type.

#### Scenario: ENUM_LAST returns last value

Given `CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`
When `SELECT ENUM_LAST('mood')` is executed
Then the result MUST be 'happy'

