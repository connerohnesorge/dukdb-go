package functions

// Common DuckDB function name constants for string functions.
const (
	funcLength = "length"
)

// Argument count constants for string functions.
const (
	argsMaxThree = 3
	argsMaxFour  = 4
)

// registerStringFuncs registers string function aliases.
func (r *FunctionAliasRegistry) registerStringFuncs() {
	aliases := []*FunctionAlias{
		{PostgreSQLName: "concat", DuckDBName: "concat", Category: DirectAlias,
			MinArgs: 1, MaxArgs: -1, Description: "Concatenates strings"},
		{PostgreSQLName: "concat_ws", DuckDBName: "concat_ws", Category: DirectAlias,
			MinArgs: 2, MaxArgs: -1, Description: "Concatenates strings with separator"},
		{PostgreSQLName: "length", DuckDBName: funcLength, Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns string length"},
		{PostgreSQLName: "char_length", DuckDBName: funcLength, Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns character length"},
		{PostgreSQLName: "character_length", DuckDBName: funcLength, Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns character length"},
		{PostgreSQLName: "octet_length", DuckDBName: "octet_length", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns byte length"},
		{PostgreSQLName: "bit_length", DuckDBName: "bit_length", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns bit length"},
		{PostgreSQLName: "lower", DuckDBName: "lower", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Converts to lowercase"},
		{PostgreSQLName: "upper", DuckDBName: "upper", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Converts to uppercase"},
		{PostgreSQLName: "initcap", DuckDBName: "initcap", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Capitalizes first letter of each word"},
		{PostgreSQLName: "trim", DuckDBName: "trim", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 2, Description: "Removes whitespace from string"},
		{PostgreSQLName: "ltrim", DuckDBName: "ltrim", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 2, Description: "Removes leading whitespace"},
		{PostgreSQLName: "rtrim", DuckDBName: "rtrim", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 2, Description: "Removes trailing whitespace"},
		{PostgreSQLName: "btrim", DuckDBName: "trim", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 2, Description: "Removes leading and trailing characters"},
		{PostgreSQLName: "lpad", DuckDBName: "lpad", Category: DirectAlias,
			MinArgs: 2, MaxArgs: argsMaxThree, Description: "Left-pads string"},
		{PostgreSQLName: "rpad", DuckDBName: "rpad", Category: DirectAlias,
			MinArgs: 2, MaxArgs: argsMaxThree, Description: "Right-pads string"},
		{PostgreSQLName: "substring", DuckDBName: "substring", Category: DirectAlias,
			MinArgs: 2, MaxArgs: argsMaxThree, Description: "Extracts substring"},
		{PostgreSQLName: "substr", DuckDBName: "substr", Category: DirectAlias,
			MinArgs: 2, MaxArgs: argsMaxThree, Description: "Extracts substring"},
		{PostgreSQLName: "replace", DuckDBName: "replace", Category: DirectAlias,
			MinArgs: argsMaxThree, MaxArgs: argsMaxThree, Description: "Replaces substring"},
		{PostgreSQLName: "translate", DuckDBName: "translate", Category: DirectAlias,
			MinArgs: argsMaxThree, MaxArgs: argsMaxThree, Description: "Replaces characters"},
		{PostgreSQLName: "reverse", DuckDBName: "reverse", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Reverses string"},
		{PostgreSQLName: "repeat", DuckDBName: "repeat", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Repeats string"},
		{PostgreSQLName: "position", DuckDBName: "position", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Finds position of substring"},
		{PostgreSQLName: "strpos", DuckDBName: "strpos", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Finds position of substring"},
		{PostgreSQLName: "left", DuckDBName: "left", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Returns leftmost characters"},
		{PostgreSQLName: "right", DuckDBName: "right", Category: DirectAlias,
			MinArgs: 2, MaxArgs: 2, Description: "Returns rightmost characters"},
		{PostgreSQLName: "split_part", DuckDBName: "split_part", Category: DirectAlias,
			MinArgs: argsMaxThree, MaxArgs: argsMaxThree, Description: "Splits string and returns part"},
		{PostgreSQLName: "string_to_array", DuckDBName: "string_split", Category: DirectAlias,
			MinArgs: 2, MaxArgs: argsMaxThree, Description: "Splits string into array"},
		{PostgreSQLName: "regexp_replace", DuckDBName: "regexp_replace", Category: DirectAlias,
			MinArgs: argsMaxThree, MaxArgs: argsMaxFour, Description: "Regular expression replace"},
		{PostgreSQLName: "regexp_matches", DuckDBName: "regexp_matches", Category: DirectAlias,
			MinArgs: 2, MaxArgs: argsMaxThree, Description: "Regular expression match"},
		{PostgreSQLName: "overlay", DuckDBName: "overlay", Category: DirectAlias,
			MinArgs: argsMaxThree, MaxArgs: argsMaxFour, Description: "Replaces substring"},
		{PostgreSQLName: "md5", DuckDBName: "md5", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Computes MD5 hash"},
		{PostgreSQLName: "ascii", DuckDBName: "ascii", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns ASCII code of first character"},
		{PostgreSQLName: "chr", DuckDBName: "chr", Category: DirectAlias,
			MinArgs: 1, MaxArgs: 1, Description: "Returns character from ASCII code"},
		{PostgreSQLName: "format", DuckDBName: "format", Category: DirectAlias,
			MinArgs: 1, MaxArgs: -1, Description: "Formats string with printf-style"},
	}

	for _, alias := range aliases {
		r.Register(alias)
	}
}
