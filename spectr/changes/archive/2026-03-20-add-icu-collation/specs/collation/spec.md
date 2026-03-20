## ADDED Requirements

### Requirement: Collation Registry

The system MUST provide a collation registry that maps collation names to collator implementations for locale-aware string comparison.

#### Scenario: Registry returns built-in collations

- WHEN the registry is initialized
- THEN it contains at least BINARY, NOCASE, NOACCENT, and NFC collators
- AND each collator implements the Compare interface

#### Scenario: Registry creates locale collator on demand

- WHEN a collation name matching a BCP 47 locale tag is requested (e.g., "de_DE", "en_US", "zh_Hans")
- THEN the registry creates a locale-aware collator using golang.org/x/text/collate
- AND caches it for subsequent requests

#### Scenario: Registry resolves chained collation modifiers

- WHEN a collation name contains dot-separated modifiers (e.g., "de_DE.NOCASE.NOACCENT")
- THEN the registry creates the base locale collator
- AND wraps it with each modifier in order (NOCASE then NOACCENT)
- AND returns a composite collator that applies all transformations

#### Scenario: Registry returns error for unknown collation

- WHEN an unrecognized collation name is requested that is not a valid locale or modifier
- THEN the registry returns an error with a descriptive message
- AND the error includes the invalid collation name

---

### Requirement: ORDER BY with COLLATE Clause

The system MUST support locale-aware sorting via the COLLATE clause in ORDER BY expressions.

#### Scenario: Sort with locale collation

- WHEN executing `SELECT name FROM t ORDER BY name COLLATE de_DE`
- THEN results are sorted according to German locale rules
- AND characters like umlauts (ae, oe, ue) sort in locale-correct positions

#### Scenario: Sort with NOCASE collation

- WHEN executing `SELECT name FROM t ORDER BY name COLLATE NOCASE`
- THEN results are sorted case-insensitively
- AND 'alice', 'Alice', 'ALICE' are treated as equal for sort purposes

#### Scenario: Sort with NOACCENT collation

- WHEN executing `SELECT name FROM t ORDER BY name COLLATE NOACCENT`
- THEN results are sorted without accent distinction
- AND 'cafe' and 'cafe' with accent are treated as equal for sort purposes

#### Scenario: Sort with chained collation

- WHEN executing `SELECT name FROM t ORDER BY name COLLATE de_DE.NOCASE.NOACCENT`
- THEN results are sorted using German locale rules
- AND case differences are ignored
- AND accent differences are ignored

#### Scenario: COLLATE does not affect non-string columns

- WHEN executing `SELECT id FROM t ORDER BY id COLLATE NOCASE`
- AND the column id is of numeric type
- THEN the COLLATE clause is ignored for non-string values
- AND numeric ordering is preserved

#### Scenario: Unknown collation in ORDER BY produces error

- WHEN executing `SELECT name FROM t ORDER BY name COLLATE nonexistent_locale`
- THEN the query returns an error indicating the collation is not recognized

---

### Requirement: COLLATE in CREATE TABLE Column Definitions

The system MUST support specifying a default collation for VARCHAR columns in CREATE TABLE statements.

#### Scenario: Define column with collation

- WHEN executing `CREATE TABLE t (name VARCHAR COLLATE en_US)`
- THEN the column's default collation is stored as "en_US" in the catalog
- AND subsequent ORDER BY on that column without explicit COLLATE uses "en_US"

#### Scenario: Explicit COLLATE overrides column default

- WHEN a column has default collation "en_US"
- AND a query specifies `ORDER BY name COLLATE de_DE`
- THEN the explicit COLLATE "de_DE" is used instead of the column default

#### Scenario: Column collation is visible in metadata

- WHEN a table has a column with COLLATE specified
- THEN `PRAGMA table_info('t')` or information_schema reflects the collation

---

### Requirement: Case-Insensitive Collation (NOCASE)

The system MUST provide a NOCASE collation that performs case-insensitive string comparison using Unicode case folding.

#### Scenario: NOCASE uses Unicode case folding

- WHEN comparing strings under NOCASE collation
- THEN Unicode case folding is applied (not simple ASCII lowering)
- AND the German sharp-s is treated as equivalent to "ss"
- AND Turkish dotted-I edge cases are handled via Unicode rules

#### Scenario: NOCASE as modifier on locale collation

- WHEN using `COLLATE de_DE.NOCASE`
- THEN case folding is applied before German locale comparison
- AND the combined collation handles both locale ordering and case insensitivity

---

### Requirement: Accent-Insensitive Collation (NOACCENT)

The system MUST provide a NOACCENT collation that performs accent-insensitive string comparison using Unicode normalization.

#### Scenario: NOACCENT removes diacritical marks

- WHEN comparing strings under NOACCENT collation
- THEN Unicode NFKD decomposition is applied to split accented characters
- AND combining marks (Unicode category Mn) are stripped
- AND the result is compared after NFC normalization

#### Scenario: NOACCENT as modifier on locale collation

- WHEN using `COLLATE en_US.NOACCENT`
- THEN accent removal is applied before English locale comparison
- AND 'resume' matches 'resume' with accents

---

### Requirement: Locale-Aware UPPER and LOWER Functions

The system MUST support locale-aware case conversion in UPPER() and LOWER() functions when the input column has a collation defined.

#### Scenario: UPPER respects column collation locale

- WHEN executing `SELECT UPPER(name) FROM t` where name has COLLATE tr_TR
- THEN Turkish locale rules apply (dotless-i becomes capital I without dot)

#### Scenario: LOWER respects column collation locale

- WHEN executing `SELECT LOWER(name) FROM t` where name has COLLATE tr_TR
- THEN Turkish locale rules apply (capital I becomes dotless-i)

#### Scenario: UPPER/LOWER without collation uses default behavior

- WHEN executing `SELECT UPPER(name) FROM t` where name has no collation
- THEN standard Go `strings.ToUpper` behavior is used (Unicode default casing)

---

### Requirement: PRAGMA collations Lists Available Collations

The system MUST return all available collations from the registry when `PRAGMA collations` is executed.

#### Scenario: List includes built-in collations

- WHEN executing `PRAGMA collations`
- THEN the result includes at least NOCASE, NOACCENT, NFC, and BINARY
- AND each row has a `collation_name` column

#### Scenario: List includes previously used locale collations

- WHEN a query has used `COLLATE de_DE` causing the locale collator to be created
- AND then `PRAGMA collations` is executed
- THEN the result includes "DE_DE" in the collation list

---

### Requirement: Pure Go Collation Implementation

The system MUST implement all collation functionality using pure Go packages with zero CGO dependencies.

#### Scenario: Collation works without C toolchain

- WHEN the project is built with `CGO_ENABLED=0`
- THEN all collation functionality compiles and works correctly
- AND `golang.org/x/text/collate` is the backing implementation for locale support

#### Scenario: Collation works in constrained environments

- WHEN the project targets WASM or TinyGo
- THEN collation functionality does not use any C FFI or system calls
- AND relies solely on `golang.org/x/text` pure Go packages
