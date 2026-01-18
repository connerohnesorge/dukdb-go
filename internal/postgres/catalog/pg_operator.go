package catalog

// pg_operator columns - PostgreSQL operator catalog
// Reference: https://www.postgresql.org/docs/current/catalog-pg-operator.html
var pgOperatorColumns = []string{
	"oid",          // Row identifier
	"oprname",      // Name of the operator
	"oprnamespace", // OID of namespace containing this operator
	"oprowner",     // Owner of the operator
	"oprkind",      // b = infix, l = prefix, (obsolete: r = postfix)
	"oprcanmerge",  // Operator can be used for merge join
	"oprcanhash",   // Operator can be used for hash join
	"oprleft",      // Type of left operand (0 for prefix operator)
	"oprright",     // Type of right operand
	"oprresult",    // Type of result
	"oprcom",       // OID of commutator operator, or 0
	"oprnegate",    // OID of negator operator, or 0
	"oprcode",      // OID of function implementing this operator
	"oprrest",      // OID of restriction selectivity estimator function
	"oprjoin",      // OID of join selectivity estimator function
}

// builtinOperators contains commonly used PostgreSQL operators.
var builtinOperators = []struct {
	oid         int64
	oprname     string
	oprkind     string
	oprleft     int64 // OID of left type
	oprright    int64 // OID of right type
	oprresult   int64 // OID of result type
	oprcanmerge bool
	oprcanhash  bool
}{
	// Integer comparison operators
	{1, "=", "b", 23, 23, 16, true, true},    // int4 = int4 -> bool
	{2, "<>", "b", 23, 23, 16, false, false}, // int4 <> int4 -> bool
	{3, "<", "b", 23, 23, 16, false, false},  // int4 < int4 -> bool
	{4, "<=", "b", 23, 23, 16, false, false}, // int4 <= int4 -> bool
	{5, ">", "b", 23, 23, 16, false, false},  // int4 > int4 -> bool
	{6, ">=", "b", 23, 23, 16, false, false}, // int4 >= int4 -> bool

	// Integer arithmetic operators
	{7, "+", "b", 23, 23, 23, false, false},  // int4 + int4 -> int4
	{8, "-", "b", 23, 23, 23, false, false},  // int4 - int4 -> int4
	{9, "*", "b", 23, 23, 23, false, false},  // int4 * int4 -> int4
	{10, "/", "b", 23, 23, 23, false, false}, // int4 / int4 -> int4
	{11, "%", "b", 23, 23, 23, false, false}, // int4 % int4 -> int4

	// Bigint comparison operators
	{20, "=", "b", 20, 20, 16, true, true},    // int8 = int8 -> bool
	{21, "<>", "b", 20, 20, 16, false, false}, // int8 <> int8 -> bool
	{22, "<", "b", 20, 20, 16, false, false},  // int8 < int8 -> bool
	{23, "<=", "b", 20, 20, 16, false, false}, // int8 <= int8 -> bool
	{24, ">", "b", 20, 20, 16, false, false},  // int8 > int8 -> bool
	{25, ">=", "b", 20, 20, 16, false, false}, // int8 >= int8 -> bool

	// Bigint arithmetic operators
	{26, "+", "b", 20, 20, 20, false, false}, // int8 + int8 -> int8
	{27, "-", "b", 20, 20, 20, false, false}, // int8 - int8 -> int8
	{28, "*", "b", 20, 20, 20, false, false}, // int8 * int8 -> int8
	{29, "/", "b", 20, 20, 20, false, false}, // int8 / int8 -> int8
	{30, "%", "b", 20, 20, 20, false, false}, // int8 % int8 -> int8

	// Float comparison operators
	{40, "=", "b", 701, 701, 16, true, true},    // float8 = float8 -> bool
	{41, "<>", "b", 701, 701, 16, false, false}, // float8 <> float8 -> bool
	{42, "<", "b", 701, 701, 16, false, false},  // float8 < float8 -> bool
	{43, "<=", "b", 701, 701, 16, false, false}, // float8 <= float8 -> bool
	{44, ">", "b", 701, 701, 16, false, false},  // float8 > float8 -> bool
	{45, ">=", "b", 701, 701, 16, false, false}, // float8 >= float8 -> bool

	// Float arithmetic operators
	{46, "+", "b", 701, 701, 701, false, false}, // float8 + float8 -> float8
	{47, "-", "b", 701, 701, 701, false, false}, // float8 - float8 -> float8
	{48, "*", "b", 701, 701, 701, false, false}, // float8 * float8 -> float8
	{49, "/", "b", 701, 701, 701, false, false}, // float8 / float8 -> float8

	// Text comparison operators
	{60, "=", "b", 25, 25, 16, true, true},    // text = text -> bool
	{61, "<>", "b", 25, 25, 16, false, false}, // text <> text -> bool
	{62, "<", "b", 25, 25, 16, false, false},  // text < text -> bool
	{63, "<=", "b", 25, 25, 16, false, false}, // text <= text -> bool
	{64, ">", "b", 25, 25, 16, false, false},  // text > text -> bool
	{65, ">=", "b", 25, 25, 16, false, false}, // text >= text -> bool

	// Text concatenation operator
	{66, "||", "b", 25, 25, 25, false, false}, // text || text -> text

	// Pattern matching operators
	{70, "~~", "b", 25, 25, 16, false, false},   // text ~~ text -> bool (LIKE)
	{71, "!~~", "b", 25, 25, 16, false, false},  // text !~~ text -> bool (NOT LIKE)
	{72, "~~*", "b", 25, 25, 16, false, false},  // text ~~* text -> bool (ILIKE)
	{73, "!~~*", "b", 25, 25, 16, false, false}, // text !~~* text -> bool (NOT ILIKE)
	{74, "~", "b", 25, 25, 16, false, false},    // text ~ text -> bool (regex match)
	{75, "!~", "b", 25, 25, 16, false, false},   // text !~ text -> bool (regex not match)
	{76, "~*", "b", 25, 25, 16, false, false},   // text ~* text -> bool (case-insensitive regex)
	{
		77,
		"!~*",
		"b",
		25,
		25,
		16,
		false,
		false,
	}, // text !~* text -> bool (case-insensitive regex not match)

	// Boolean operators
	{80, "=", "b", 16, 16, 16, true, true},     // bool = bool -> bool
	{81, "<>", "b", 16, 16, 16, false, false},  // bool <> bool -> bool
	{82, "AND", "b", 16, 16, 16, false, false}, // bool AND bool -> bool
	{83, "OR", "b", 16, 16, 16, false, false},  // bool OR bool -> bool
	{84, "NOT", "l", 0, 16, 16, false, false},  // NOT bool -> bool (prefix operator)

	// Unary minus operators
	{90, "-", "l", 0, 23, 23, false, false},   // -int4 -> int4
	{91, "-", "l", 0, 20, 20, false, false},   // -int8 -> int8
	{92, "-", "l", 0, 701, 701, false, false}, // -float8 -> float8

	// Date/time comparison operators
	{100, "=", "b", 1082, 1082, 16, true, true},    // date = date -> bool
	{101, "<>", "b", 1082, 1082, 16, false, false}, // date <> date -> bool
	{102, "<", "b", 1082, 1082, 16, false, false},  // date < date -> bool
	{103, "<=", "b", 1082, 1082, 16, false, false}, // date <= date -> bool
	{104, ">", "b", 1082, 1082, 16, false, false},  // date > date -> bool
	{105, ">=", "b", 1082, 1082, 16, false, false}, // date >= date -> bool

	// Timestamp comparison operators
	{110, "=", "b", 1114, 1114, 16, true, true},    // timestamp = timestamp -> bool
	{111, "<>", "b", 1114, 1114, 16, false, false}, // timestamp <> timestamp -> bool
	{112, "<", "b", 1114, 1114, 16, false, false},  // timestamp < timestamp -> bool
	{113, "<=", "b", 1114, 1114, 16, false, false}, // timestamp <= timestamp -> bool
	{114, ">", "b", 1114, 1114, 16, false, false},  // timestamp > timestamp -> bool
	{115, ">=", "b", 1114, 1114, 16, false, false}, // timestamp >= timestamp -> bool

	// JSON operators
	{120, "->", "b", 114, 23, 114, false, false},   // json -> int -> json (array element)
	{121, "->", "b", 114, 25, 114, false, false},   // json -> text -> json (object field)
	{122, "->>", "b", 114, 23, 25, false, false},   // json ->> int -> text (array element as text)
	{123, "->>", "b", 114, 25, 25, false, false},   // json ->> text -> text (object field as text)
	{124, "#>", "b", 114, 1009, 114, false, false}, // json #> text[] -> json (path)
	{125, "#>>", "b", 114, 1009, 25, false, false}, // json #>> text[] -> text (path as text)

	// JSONB operators
	{130, "->", "b", 3802, 23, 3802, false, false},   // jsonb -> int -> jsonb
	{131, "->", "b", 3802, 25, 3802, false, false},   // jsonb -> text -> jsonb
	{132, "->>", "b", 3802, 23, 25, false, false},    // jsonb ->> int -> text
	{133, "->>", "b", 3802, 25, 25, false, false},    // jsonb ->> text -> text
	{134, "@>", "b", 3802, 3802, 16, false, false},   // jsonb @> jsonb -> bool (contains)
	{135, "<@", "b", 3802, 3802, 16, false, false},   // jsonb <@ jsonb -> bool (contained by)
	{136, "?", "b", 3802, 25, 16, false, false},      // jsonb ? text -> bool (key exists)
	{137, "?|", "b", 3802, 1009, 16, false, false},   // jsonb ?| text[] -> bool (any key exists)
	{138, "?&", "b", 3802, 1009, 16, false, false},   // jsonb ?& text[] -> bool (all keys exist)
	{139, "||", "b", 3802, 3802, 3802, false, false}, // jsonb || jsonb -> jsonb (concat)
}

// queryPgOperator returns data for pg_catalog.pg_operator.
func (pg *PgCatalog) queryPgOperator(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgOperatorColumns,
		Rows:    make([]map[string]any, 0),
	}

	for _, op := range builtinOperators {
		row := map[string]any{
			"oid":          op.oid,
			"oprname":      op.oprname,
			"oprnamespace": pgCatalogNamespaceOID,
			"oprowner":     int64(10), // Superuser
			"oprkind":      op.oprkind,
			"oprcanmerge":  op.oprcanmerge,
			"oprcanhash":   op.oprcanhash,
			"oprleft":      op.oprleft,
			"oprright":     op.oprright,
			"oprresult":    op.oprresult,
			"oprcom":       int64(0), // Commutator OID (0 for simplicity)
			"oprnegate":    int64(0), // Negator OID (0 for simplicity)
			"oprcode":      int64(0), // Function OID (0 for simplicity)
			"oprrest":      int64(0), // Restriction selectivity function
			"oprjoin":      int64(0), // Join selectivity function
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}
