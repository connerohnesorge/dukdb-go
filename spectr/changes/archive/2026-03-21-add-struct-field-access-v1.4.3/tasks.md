# Tasks: Struct Dot Notation Field Access

- [ ] 1. Add BoundFieldAccess expression — Add `BoundFieldAccess` struct to binder/expressions.go after BoundColumnRef (line 18). Fields: Struct (BoundExpr), Field (string), ResType (dukdb.Type). Implement boundExprNode() and ResultType(). Validate: Compiles without error.

- [ ] 2. Modify binder to resolve struct fields — In bindColumnRef() (binder/bind_expr.go:113-181), when table lookup fails, check if ref.Table matches a struct-typed column name. If so, create BoundFieldAccess wrapping a BoundColumnRef for the struct column. Table.column resolution takes priority over struct.field. Validate: `SELECT s.name FROM t` where s is a struct column resolves correctly.

- [ ] 3. Evaluate BoundFieldAccess in executor — Add case for *binder.BoundFieldAccess in expression evaluation (executor/expr.go). Extract field from map[string]any using field name. Follow STRUCT_EXTRACT pattern (expr.go:2233-2255). Support case-insensitive field lookup. NULL struct returns NULL. Validate: `SELECT s.name FROM t` returns correct field value.

- [ ] 4. Integration tests — Test struct field access with SELECT, WHERE, ORDER BY. Test mixed table.column and struct.field in same query. Test ambiguity resolution (table name priority). Test NULL propagation. Test non-existent fields return NULL. Test with nested structs if supported.
