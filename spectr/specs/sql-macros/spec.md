# Sql Macros Specification

## Requirements

### Requirement: Create Scalar Macro

The system MUST support creating scalar macros that return a single value from an expression.

#### Scenario: Create a simple scalar macro
- GIVEN no macro named "add" exists
- WHEN executing `CREATE MACRO add(a, b) AS a + b`
- THEN the macro "add" is stored in the catalog
- AND the macro has parameters [a, b] with no defaults
- AND the macro body is the expression `a + b`
- AND subsequent calls to `add(1, 2)` return `3`

#### Scenario: Create a scalar macro with CASE expression
- GIVEN no macro named "ifelse" exists
- WHEN executing `CREATE MACRO ifelse(a, b, c) AS CASE WHEN a THEN b ELSE c END`
- THEN the macro is stored in the catalog
- AND `SELECT ifelse(true, 'yes', 'no')` returns `'yes'`
- AND `SELECT ifelse(false, 'yes', 'no')` returns `'no'`

#### Scenario: Create a scalar macro with default parameters
- GIVEN no macro named "inc" exists
- WHEN executing `CREATE MACRO inc(a, step := 1) AS a + step`
- THEN the macro is stored with parameter "step" having default value `1`
- AND `SELECT inc(5)` returns `6` (uses default step=1)
- AND `SELECT inc(5, 3)` returns `8` (overrides default)

#### Scenario: Scalar macro used in WHERE clause
- GIVEN table "t" with column "x" containing values [1, 2, 3, 4, 5]
- AND macro `CREATE MACRO gt(a, b) AS a > b` exists
- WHEN executing `SELECT x FROM t WHERE gt(x, 3)`
- THEN the result contains [4, 5]

#### Scenario: Error on duplicate macro without OR REPLACE
- GIVEN macro "add" already exists
- WHEN executing `CREATE MACRO add(a, b) AS a + b`
- THEN an error is returned indicating macro "add" already exists

---

### Requirement: Call Scalar Macro

The system MUST expand scalar macro calls inline during query binding by substituting parameters with argument expressions.

#### Scenario: Expand scalar macro in SELECT
- GIVEN macro `CREATE MACRO double(x) AS x * 2`
- WHEN executing `SELECT double(5)`
- THEN the macro call is expanded to `5 * 2`
- AND the result is `10`

#### Scenario: Expand scalar macro with column references
- GIVEN table "t" with columns [a, b] and row (3, 4)
- AND macro `CREATE MACRO add(x, y) AS x + y`
- WHEN executing `SELECT add(a, b) FROM t`
- THEN the macro expands to `a + b`
- AND the result is `7`

#### Scenario: Error on wrong number of arguments
- GIVEN macro `CREATE MACRO add(a, b) AS a + b`
- WHEN executing `SELECT add(1)`
- THEN an error is returned indicating missing argument for parameter "b"

#### Scenario: Error on too many arguments
- GIVEN macro `CREATE MACRO add(a, b) AS a + b`
- WHEN executing `SELECT add(1, 2, 3)`
- THEN an error is returned indicating too many arguments

#### Scenario: Nested macro expansion
- GIVEN macro `CREATE MACRO double(x) AS x * 2`
- AND macro `CREATE MACRO quadruple(x) AS double(double(x))`
- WHEN executing `SELECT quadruple(3)`
- THEN the result is `12`

#### Scenario: Expansion depth limit
- GIVEN a macro that would cause infinite recursion
- WHEN macro expansion exceeds 32 levels
- THEN an error is returned indicating maximum macro expansion depth exceeded

---

### Requirement: Create Table Macro

The system MUST support creating table macros that return a table from a SELECT query.

#### Scenario: Create a simple table macro
- GIVEN no macro named "my_range" exists
- WHEN executing `CREATE MACRO my_range(n) AS TABLE SELECT * FROM range(n)`
- THEN the macro "my_range" is stored in the catalog as a table macro
- AND subsequent calls to `SELECT * FROM my_range(5)` return 5 rows

#### Scenario: Create a table macro with filtering
- GIVEN table "employees" with columns [name, dept, salary]
- AND no macro named "dept_employees" exists
- WHEN executing `CREATE MACRO dept_employees(d) AS TABLE SELECT * FROM employees WHERE dept = d`
- THEN the macro is stored as a table macro
- AND `SELECT * FROM dept_employees('Engineering')` returns only engineering employees

#### Scenario: Create a table macro with default parameters
- GIVEN no macro named "top_n" exists
- WHEN executing `CREATE MACRO top_n(tbl, n := 10) AS TABLE SELECT * FROM tbl LIMIT n`
- THEN parameter "n" has default value `10`
- AND the macro can be called with one or two arguments

---

### Requirement: Call Table Macro

The system MUST expand table macro calls in the FROM clause by substituting the call with the macro's query as a subquery.

#### Scenario: Expand table macro in FROM clause
- GIVEN macro `CREATE MACRO my_range(n) AS TABLE SELECT i FROM range(n) t(i)`
- WHEN executing `SELECT * FROM my_range(3)`
- THEN the table macro expands to a subquery `(SELECT i FROM range(3) t(i))`
- AND returns rows [0, 1, 2]

#### Scenario: Table macro with JOIN
- GIVEN macro `CREATE MACRO my_range(n) AS TABLE SELECT i FROM range(n) t(i)`
- WHEN executing `SELECT a.i, b.i FROM my_range(3) a JOIN my_range(2) b ON true`
- THEN both macro calls expand independently
- AND the cross join produces 6 rows

#### Scenario: Error calling scalar macro as table
- GIVEN scalar macro `CREATE MACRO add(a, b) AS a + b`
- WHEN executing `SELECT * FROM add(1, 2)`
- THEN an error is returned indicating "add" is not a table macro

---

### Requirement: Drop Macro

The system MUST support dropping macro definitions from the catalog.

#### Scenario: Drop an existing scalar macro
- GIVEN macro "add" exists
- WHEN executing `DROP MACRO add`
- THEN the macro "add" is removed from the catalog
- AND subsequent calls to `SELECT add(1, 2)` produce an error

#### Scenario: Drop macro with IF EXISTS on nonexistent macro
- GIVEN no macro named "nonexistent" exists
- WHEN executing `DROP MACRO IF EXISTS nonexistent`
- THEN no error is returned
- AND the operation is a no-op

#### Scenario: Drop nonexistent macro without IF EXISTS
- GIVEN no macro named "nonexistent" exists
- WHEN executing `DROP MACRO nonexistent`
- THEN an error is returned indicating macro "nonexistent" does not exist

#### Scenario: Drop a table macro
- GIVEN table macro "my_range" exists
- WHEN executing `DROP MACRO TABLE my_range`
- THEN the table macro is removed from the catalog

#### Scenario: Drop table macro with IF EXISTS
- GIVEN no table macro named "nonexistent" exists
- WHEN executing `DROP MACRO TABLE IF EXISTS nonexistent`
- THEN no error is returned

---

### Requirement: Create Or Replace Macro

The system MUST support replacing an existing macro definition without first dropping it.

#### Scenario: Replace an existing scalar macro
- GIVEN macro `CREATE MACRO add(a, b) AS a + b` exists
- WHEN executing `CREATE OR REPLACE MACRO add(a, b) AS a + b + 1`
- THEN the macro "add" is updated with the new body
- AND `SELECT add(1, 2)` returns `4` (not `3`)

#### Scenario: Create or replace when macro does not exist
- GIVEN no macro named "mul" exists
- WHEN executing `CREATE OR REPLACE MACRO mul(a, b) AS a * b`
- THEN the macro "mul" is created
- AND `SELECT mul(3, 4)` returns `12`

#### Scenario: Replace scalar macro with different parameters
- GIVEN macro `CREATE MACRO f(a) AS a + 1` exists
- WHEN executing `CREATE OR REPLACE MACRO f(a, b) AS a + b`
- THEN the macro "f" is replaced with new parameter list
- AND `SELECT f(1, 2)` returns `3`
- AND `SELECT f(1)` returns an error (missing argument for "b")

---

### Requirement: Macro with Default Parameters

The system MUST support default parameter values using `:=` or `DEFAULT` syntax.

#### Scenario: Default with `:=` syntax
- WHEN executing `CREATE MACRO greet(name, greeting := 'Hello') AS greeting || ' ' || name`
- THEN `SELECT greet('World')` returns `'Hello World'`
- AND `SELECT greet('World', 'Hi')` returns `'Hi World'`

#### Scenario: Default with DEFAULT keyword
- WHEN executing `CREATE MACRO inc(a, step DEFAULT 1) AS a + step`
- THEN `SELECT inc(5)` returns `6`
- AND `SELECT inc(5, 10)` returns `15`

#### Scenario: Multiple defaults
- WHEN executing `CREATE MACRO calc(a, b := 1, c := 0) AS a * b + c`
- THEN `SELECT calc(5)` returns `5` (b=1, c=0)
- AND `SELECT calc(5, 2)` returns `10` (c=0)
- AND `SELECT calc(5, 2, 3)` returns `13`

#### Scenario: Error when required parameter follows default
- WHEN executing `CREATE MACRO bad(a := 1, b) AS a + b`
- THEN an error is returned indicating required parameters must precede default parameters

