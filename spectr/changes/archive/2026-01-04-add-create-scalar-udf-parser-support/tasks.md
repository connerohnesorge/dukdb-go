## 1. Lexer Updates
- [ ] Add tokens: FUNCTION, RETURNS, LANGUAGE, IMMUTABLE/STABLE/VOLATILE, STRICT, LEAKPROOF, PARALLEL SAFE/UNSAFE

## 2. Parser
- [ ] Implement parseCreateFunction after CREATE FUNCTION
- [ ] Parse param list: ident TypeName [, ...]
- [ ] RETURNS TypeName
- [ ] Optional LANGUAGE ident, AS 'string' or $$multi-line$$
- [ ] Attr list (IMMUTABLE etc.)

## 3. AST
- [ ] CreateFunctionStmt{Name, Params[]FuncParam{Name string, Type dukdb.Type}, Returns dukdb.Type, Lang string, Body string, Attrs[]string}
- [ ] Update Visitor.VisitCreateFunctionStmt

## 4. Tests
- [ ] Simple UDF, multi-param, $$body$$, all attrs
- [ ] Param counting, TableExtractor no tables

## 5. Validate
- [ ] spectr validate
- [ ] Archive