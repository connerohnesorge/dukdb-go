# Reset Statement Specification

## Requirements

### Requirement: RESET variable

RESET SHALL restore a configuration variable to its default value. It MUST undo the effect of a previous SET statement.

#### Scenario: Reset after SET

Given `SET search_path = 'custom_schema'` has been executed
When `RESET search_path` is executed
Then the search_path MUST be restored to its default value

### Requirement: RESET ALL

RESET ALL SHALL restore all configuration variables to their default values.

#### Scenario: Reset all variables

Given multiple SET statements have been executed
When `RESET ALL` is executed
Then all variables MUST be restored to their default values

