## ADDED Requirements

### Requirement: Replacement Scan Registration

The package SHALL allow registration of replacement scan callbacks.

#### Scenario: Register replacement scan
- GIVEN a ReplacementScanCallback function
- WHEN calling RegisterReplacementScan(conn, callback)
- THEN no error is returned
- AND callback is registered for the connection

#### Scenario: Re-register replacement scan
- GIVEN existing replacement scan on connection
- WHEN calling RegisterReplacementScan with new callback
- THEN new callback replaces the old one

### Requirement: Replacement Scan Invocation

The replacement scan SHALL be called during table resolution.

#### Scenario: Table name triggers callback
- GIVEN callback that matches "*.parquet" pattern
- WHEN executing "SELECT * FROM data.parquet"
- THEN callback receives "data.parquet"

#### Scenario: Callback returns function replacement
- GIVEN callback returning ("read_parquet", ["data.parquet"], nil)
- WHEN table is referenced
- THEN query becomes equivalent to "SELECT * FROM read_parquet('data.parquet')"

#### Scenario: Callback returns empty (no replacement)
- GIVEN callback returning ("", nil, nil)
- WHEN table is referenced
- THEN normal table resolution continues

### Requirement: Replacement Scan Error Handling

The replacement scan SHALL handle errors appropriately.

#### Scenario: Callback returns error
- GIVEN callback returning (_, _, error)
- WHEN table is referenced
- THEN query fails with callback error

#### Scenario: Replaced function not found
- GIVEN callback returning non-existent function name
- WHEN table is referenced
- THEN query fails with "function not found" error

### Requirement: Replacement Scan Parameters

The replacement scan SHALL pass parameters to the replacement function.

#### Scenario: Multiple parameters
- GIVEN callback returning ("my_func", [1, "test", true], nil)
- WHEN replacement occurs
- THEN function is called with all parameters

#### Scenario: No parameters
- GIVEN callback returning ("my_func", nil, nil)
- WHEN replacement occurs
- THEN function is called with no arguments
