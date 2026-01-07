## ADDED Requirements

### Requirement: Checkpoint Threshold Storage

The system SHALL store the checkpoint_threshold setting in the duckdb.settings table for persistence across database restarts.

#### Scenario: Threshold stored in settings table

- GIVEN a database connection
- WHEN executing `PRAGMA checkpoint_threshold = '256MB'`
- THEN the value '256MB' SHALL be stored in `duckdb.settings` table
- AND the setting persists after database close and reopen

#### Scenario: Threshold retrieved from settings

- GIVEN a database with checkpoint_threshold stored in settings
- WHEN the database is opened
- THEN the threshold value SHALL be read from `duckdb.settings`
- AND passed to the CheckpointManager initialization

#### Scenario: Default threshold when not set

- GIVEN a new database or one without checkpoint_threshold in settings
- WHEN no threshold is configured
- THEN a default value of '256MB' SHALL be used
- AND the default can be changed at runtime

### Requirement: Checkpoint Threshold Parser

The system SHALL parse checkpoint_threshold values with standard DuckDB size suffixes.

#### Scenario: Parse byte suffix

- GIVEN threshold value '1024b'
- WHEN parsing the value
- THEN the result SHALL be 1024 bytes

#### Scenario: Parse kilobyte suffix

- GIVEN threshold value '512KB'
- WHEN parsing the value
- THEN the result SHALL be 512 * 1024 = 524288 bytes

#### Scenario: Parse megabyte suffix

- GIVEN threshold value '256MB'
- WHEN parsing the value
- THEN the result SHALL be 256 * 1024 * 1024 = 268435456 bytes

#### Scenario: Parse gigabyte suffix

- GIVEN threshold value '1GB'
- WHEN parsing the value
- THEN the result SHALL be 1 * 1024 * 1024 * 1024 = 1073741824 bytes

#### Scenario: Parse plain number

- GIVEN threshold value '1000000'
- WHEN parsing the value
- THEN the result SHALL be 1000000 bytes (no suffix = bytes)

#### Scenario: Invalid threshold format

- GIVEN threshold value 'invalid'
- WHEN parsing the value
- THEN an error SHALL be returned
- AND the previous threshold value SHALL be preserved

### Requirement: CheckpointManager Threshold Integration

The CheckpointManager SHALL accept and use a configurable threshold for automatic checkpoint triggering.

#### Scenario: CheckpointManager accepts configurable threshold

- GIVEN a CheckpointManager constructor
- WHEN called with thresholdBytes = 268435456 (256MB)
- THEN the manager SHALL use this threshold for auto-checkpoint decisions
- AND not a hardcoded value

#### Scenario: Checkpoint triggered at threshold

- GIVEN checkpoint_threshold set to '1GB'
- AND WAL size grows to exceed 1GB
- WHEN the WAL size is checked after a commit
- THEN an automatic checkpoint SHALL be triggered
- AND WAL SHALL be truncated after successful checkpoint

#### Scenario: Threshold update at runtime

- GIVEN a running database with threshold = '256MB'
- WHEN executing `PRAGMA checkpoint_threshold = '1GB'`
- THEN subsequent checkpoint decisions SHALL use 1GB
- AND the new value SHALL be persisted to settings table

### Requirement: PRAGMA Execution Integration

The PRAGMA checkpoint_threshold execution handler SHALL connect to the configuration system.

#### Scenario: Execute SET checkpoint_threshold

- GIVEN a PRAGMA checkpoint_threshold = '512MB' statement
- WHEN executed by the executor
- THEN the value SHALL be parsed and validated
- AND stored in duckdb.settings
- AND the CheckpointManager SHALL be notified of the new threshold

#### Scenario: Execute GET checkpoint_threshold

- GIVEN a PRAGMA checkpoint_threshold statement (without =)
- WHEN executed by the executor
- THEN the current threshold value SHALL be retrieved from settings
- AND returned as the result
