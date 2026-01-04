## ADDED Requirements

### Requirement: Excel Table Function Parsing
The parser SHALL parse read_excel and read_excel_auto table functions in FROM clause with full DuckDB options for visual fidelity verification.

#### Scenario: Basic read_excel parsing
- GIVEN `SELECT * FROM read_excel('data.xlsx')`
- WHEN parsing
- THEN TableRef.TableFunction.Name == \"read_excel\"
- AND Args == [Literal 'data.xlsx']

#### Scenario: read_excel_auto with options
- GIVEN `SELECT * FROM read_excel_auto('data.xlsx', sheet='Sheet1', range='A1:C10')`
- WHEN parsing
- THEN NamedArgs contains \"sheet\"='Sheet1', \"range\"='A1:C10'

### Requirement: TableFunctionRef Excel Extensions
The TableFunctionRef SHALL handle Excel-specific named arguments without parsing errors, storing unknown options for executor handling.

#### Scenario: Unknown Excel option
- GIVEN `read_excel(..., unknown_opt='val')`
- WHEN parsing TableFunctionRef
- THEN NamedArgs[\"unknown_opt\"] == Literal 'val' (no parse error)

#### Scenario: Basic read_excel
- GIVEN `SELECT * FROM read_excel('data.xlsx')`
- THEN TableRef.TableFunction.Name == \"read_excel\"
- AND Args[0] == Literal string 'data.xlsx'

#### Scenario: read_excel_auto with sheet/range
- GIVEN `SELECT * FROM read_excel_auto('data.xlsx', sheet='Sheet1', range='A1:C10', header=true)`
- THEN TableFunction.NamedArgs[\"sheet\"] == Literal 'Sheet1'
- AND NamedArgs[\"range\"] == Literal 'A1:C10'
- AND NamedArgs[\"header\"] == Literal true

#### Scenario: All Excel options
- GIVEN full opts (header_row=0, skip_rows=1, dtype=map, na_values=list etc.)
- THEN all NamedArgs populated as Expr (Literal/Map/LIst)

