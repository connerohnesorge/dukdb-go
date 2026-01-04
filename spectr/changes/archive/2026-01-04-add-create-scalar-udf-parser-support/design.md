## Context
DuckDB UDFs extend SQL w/ custom scalar funcs (Python/JS/SQL). Parser must match syntax exactly. Ref OpenXML VBA for lang/code body (multi-line strings).

## Goals
- Parse scalar UDF full syntax (table/agg later)
- Param types inferred/bound later
- Code body as string (exec via lang dispatcher)

## Decisions
- AST: CreateScalarUDFStmt (name ColName, params []UDFParam, returns LogicalType, lang string default 'sql', body string, volatility VolatilityType)
- Volatility: IMMUTABLE/STABLE/VOLATILE default VOLATILE
- Body: single 'str' or $$multi$$
- Lexer: new keywords, $$ as tokenSTRING_DOLLAR
- Parse flow: CREATE FUNCTION name ( param1 type1, ... ) RETURNS type [LANGUAGE lang] [IMMUTABLE] AS body

Ref OpenXML SDK: VBA modules as code bodies; map UDF body to VBA-like exec for Excel fidelity funcs (e.g. custom render checks).

## Impl Notes
1. lexer.go: add FUNCTION,RETURNS,LANGUAGE,IMMUTABLE etc. keywords
2. parser.go: in parseStatement if tok==FUNCTION: parseCreateUDF()
   - parseIdent name
   - expect '(', parseParams: loop ident Type until ')'
   - expect RETURNS Type
   - optional LANGUAGE ident
   - optional attr IMMUTABLE etc.
   - expect AS body: if $$, multi-line until $$ else 'str'
3. ast.go: structs + String() Accept(visitor)
4. binder: resolve types, register UDF
5. Fidelity: UDF for Excel val validation vs OpenXML cell.ValueRendered/Formulas

## Risks
- Multi-lang body: defer exec
- Type resolution: binder phase

## Migration
N/A