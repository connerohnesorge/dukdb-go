## Context
DuckDB's read_excel/read_excel_auto table functions parse XLSX using OpenXML standards for visual fidelity (sheet names, cell ranges, merged cells, formulas render as data). Parser must match arg syntax exactly. Later impl uses pure-Go XLSX lib (github.com/xuri/excelize?) validated vs OpenXML-SDK C# golden outputs.

## Goals
- Parse all Excel TF args w/o error (no validation)
- Support fidelity testing: generate XLSX -> LibreOffice pdf -> look_at inspect renderables (tables/charts/formulas)
- Non-goals: XLS exec impl, write_excel (separate change)

## Decisions
- **TableFunctionRef extension**: NamedArgs map[string]Expr for options (sheet='1', range='A1:B10', header=true, dtype=map[string]string etc.)
  - Args: positional path only
  - e.g. read_excel('data.xlsx', sheet='Sheet1', range='A1:Z100', header_row=1)
- **Lexer**: Treat options as ident=expr (no new tokens)
- **Parser flow**: parseTableFunction dispatches if Name=='read_excel' or 'read_excel_auto', logs warnings for unknown opts
- **Fidelity ref**: Match OpenXML SDK (github.com/dotnet/OpenXML-SDK) WorkbookPart.WorksheetPart parsing (sheets, dims, shared-strings, merges)
  - Test: Create XLSX w/OpenXML SDK -> DuckDB read_excel -> pure-Go impl -> diff rendered pdf (LibreOffice)

## Alternatives Considered
- Custom ExcelTableFunctionRef AST: Overkill, generic TableFunctionRef suffices
- Strict arg validation in parser: Defer to binder/executor

## Impl Notes (Parser Only)
1. In parseTableFunction(name):
   if name in [\"read_excel\", \"read_excel_auto\"]:
     NamedArgs[\"sheet\"] = str default '1'
     NamedArgs[\"range\"] = str default nil (whole sheet)
     NamedArgs[\"header\"] = bool default true
     ... (full list from DuckDB docs)
   Normalize case-insensitive keys
2. Expr parsing: string lits unescaped, bool/number as Literal
3. TableExtractor: Treat as external table ref
4. Errors: Unknown arg -> parse warning, store as-is

## Risks/Trade-offs
- Arg explosion (20+ opts): Use map, defer validation
- Multi-line code? No, TF args simple exprs
- Fidelity: Post-parser XLSX impl must match OpenXML row/col/dim parsing

## Open Questions
- Sheet as int or str? Both (str preferred)
- Range A1 vs 'A1:B10'? Both normalized

## Migration
N/A (new feature)