package functions

import (
	"testing"
)

// Test constants.
const (
	testSchemaMain = "main"
)

// TestFunctionAliasIntegration tests the complete function alias workflow.
func TestFunctionAliasIntegration(t *testing.T) {
	registry := NewRegistry()

	// Test cases for common PostgreSQL functions and their DuckDB equivalents
	tests := []struct {
		name         string
		pgFuncName   string
		wantDuckDB   string
		wantCategory AliasCategory
		wantMinArgs  int
		wantMaxArgs  int
	}{
		// Date/Time functions
		{"now_to_current_timestamp", "now", "current_timestamp", DirectAlias, 0, 0},
		{"current_timestamp_passthrough", "current_timestamp", "current_timestamp", DirectAlias, 0, 0},
		{"current_date_passthrough", "current_date", "current_date", DirectAlias, 0, 0},
		{"localtime_to_current_time", "localtime", "current_time", DirectAlias, 0, 0},
		{"clock_timestamp", "clock_timestamp", "current_timestamp", DirectAlias, 0, 0},

		// String functions
		{"concat_passthrough", "concat", "concat", DirectAlias, 1, -1},
		{"concat_ws_passthrough", "concat_ws", "concat_ws", DirectAlias, 2, -1},
		{"length_passthrough", "length", "length", DirectAlias, 1, 1},
		{"char_length_to_length", "char_length", "length", DirectAlias, 1, 1},
		{"upper_passthrough", "upper", "upper", DirectAlias, 1, 1},
		{"lower_passthrough", "lower", "lower", DirectAlias, 1, 1},
		{"trim_passthrough", "trim", "trim", DirectAlias, 1, 2},
		{"substring_passthrough", "substring", "substring", DirectAlias, 2, 3},
		{"replace_passthrough", "replace", "replace", DirectAlias, 3, 3},
		{"reverse_passthrough", "reverse", "reverse", DirectAlias, 1, 1},
		{"repeat_passthrough", "repeat", "repeat", DirectAlias, 2, 2},
		{"md5_passthrough", "md5", "md5", DirectAlias, 1, 1},

		// Math functions
		{"abs_passthrough", "abs", "abs", DirectAlias, 1, 1},
		{"ceil_passthrough", "ceil", "ceil", DirectAlias, 1, 1},
		{"ceiling_to_ceil", "ceiling", "ceil", DirectAlias, 1, 1},
		{"floor_passthrough", "floor", "floor", DirectAlias, 1, 1},
		{"round_passthrough", "round", "round", DirectAlias, 1, 2},
		{"power_to_pow", "power", "pow", DirectAlias, 2, 2},
		{"sqrt_passthrough", "sqrt", "sqrt", DirectAlias, 1, 1},
		{"random_passthrough", "random", "random", DirectAlias, 0, 0},

		// Aggregate functions
		{"array_agg_to_list", "array_agg", "list", DirectAlias, 1, 1},
		{"string_agg_passthrough", "string_agg", "string_agg", DirectAlias, 2, 2},
		{"count_passthrough", "count", "count", DirectAlias, 0, 1},
		{"sum_passthrough", "sum", "sum", DirectAlias, 1, 1},
		{"avg_passthrough", "avg", "avg", DirectAlias, 1, 1},
		{"min_passthrough", "min", "min", DirectAlias, 1, 1},
		{"max_passthrough", "max", "max", DirectAlias, 1, 1},

		// Conditional functions
		{"coalesce_passthrough", "coalesce", "coalesce", DirectAlias, 1, -1},
		{"nullif_passthrough", "nullif", "nullif", DirectAlias, 2, 2},
		{"greatest_passthrough", "greatest", "greatest", DirectAlias, 1, -1},
		{"least_passthrough", "least", "least", DirectAlias, 1, -1},

		// Transformed functions
		{"generate_series_to_range", "generate_series", "range", TransformedAlias, 2, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alias := registry.Resolve(tt.pgFuncName)
			if alias == nil {
				t.Fatalf("Resolve(%q) returned nil", tt.pgFuncName)
			}

			if alias.DuckDBName != tt.wantDuckDB {
				t.Errorf("DuckDBName = %q, want %q", alias.DuckDBName, tt.wantDuckDB)
			}

			if alias.Category != tt.wantCategory {
				t.Errorf("Category = %d, want %d", alias.Category, tt.wantCategory)
			}

			if alias.MinArgs != tt.wantMinArgs {
				t.Errorf("MinArgs = %d, want %d", alias.MinArgs, tt.wantMinArgs)
			}

			if alias.MaxArgs != tt.wantMaxArgs {
				t.Errorf("MaxArgs = %d, want %d", alias.MaxArgs, tt.wantMaxArgs)
			}
		})
	}
}

// TestCaseInsensitiveResolution verifies function names are case-insensitive.
func TestCaseInsensitiveResolution(t *testing.T) {
	registry := NewRegistry()

	functions := []string{
		"now", "NOW", "Now", "NoW",
		"concat", "CONCAT", "Concat",
		"array_agg", "ARRAY_AGG", "Array_Agg",
		"generate_series", "GENERATE_SERIES",
	}

	for _, fn := range functions {
		t.Run(fn, func(t *testing.T) {
			alias := registry.Resolve(fn)
			if alias == nil {
				t.Errorf("Resolve(%q) returned nil", fn)
			}
		})
	}
}

// TestPgCatalogPrefixResolution verifies pg_catalog.function_name resolution.
func TestPgCatalogPrefixResolution(t *testing.T) {
	registry := NewRegistry()

	tests := []struct {
		qualifiedName string
		wantDuckDB    string
	}{
		{"pg_catalog.now", "current_timestamp"},
		{"pg_catalog.concat", "concat"},
		{"pg_catalog.array_agg", "list"},
		{"pg_catalog.length", "length"},
		{"PG_CATALOG.NOW", "current_timestamp"}, // Case insensitive
		{"Pg_Catalog.Concat", "concat"},
	}

	for _, tt := range tests {
		t.Run(tt.qualifiedName, func(t *testing.T) {
			alias := registry.Resolve(tt.qualifiedName)
			if alias == nil {
				t.Fatalf("Resolve(%q) returned nil", tt.qualifiedName)
			}
			if alias.DuckDBName != tt.wantDuckDB {
				t.Errorf("DuckDBName = %q, want %q", alias.DuckDBName, tt.wantDuckDB)
			}
		})
	}
}

// TestGetDuckDBNameFallback verifies unknown functions return original name.
func TestGetDuckDBNameFallback(t *testing.T) {
	registry := NewRegistry()

	unknownFunctions := []string{
		"my_custom_function",
		"nonexistent_function",
		"some_other_func",
	}

	for _, fn := range unknownFunctions {
		t.Run(fn, func(t *testing.T) {
			result := registry.GetDuckDBName(fn)
			if result != fn {
				t.Errorf("GetDuckDBName(%q) = %q, want %q (original)", fn, result, fn)
			}
		})
	}
}

// TestTransformerExecution verifies argument transformers work correctly.
func TestTransformerExecution(t *testing.T) {
	registry := NewRegistry()

	// Test generate_series transformer
	alias := registry.Resolve("generate_series")
	if alias == nil {
		t.Fatal("generate_series alias not found")
	}

	if alias.Transformer == nil {
		t.Fatal("generate_series has no transformer")
	}

	tests := []struct {
		name     string
		args     []string
		wantFunc string
		wantArgs []string
		wantErr  bool
	}{
		{
			name:     "simple_integers",
			args:     []string{"1", "5"},
			wantFunc: "range",
			wantArgs: []string{"1", "6"}, // +1 for exclusive end
			wantErr:  false,
		},
		{
			name:     "with_step",
			args:     []string{"1", "10", "2"},
			wantFunc: "range",
			wantArgs: []string{"1", "11", "2"},
			wantErr:  false,
		},
		{
			name:     "variable_arguments",
			args:     []string{"start_var", "end_var"},
			wantFunc: "range",
			wantArgs: []string{"start_var", "(end_var + 1)"},
			wantErr:  false,
		},
		{
			name:    "too_few_args",
			args:    []string{"1"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			funcName, args, err := alias.Transformer("generate_series", tt.args)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if funcName != tt.wantFunc {
				t.Errorf("funcName = %q, want %q", funcName, tt.wantFunc)
			}

			if len(args) != len(tt.wantArgs) {
				t.Fatalf("len(args) = %d, want %d", len(args), len(tt.wantArgs))
			}

			for i, arg := range args {
				if arg != tt.wantArgs[i] {
					t.Errorf("args[%d] = %q, want %q", i, arg, tt.wantArgs[i])
				}
			}
		})
	}
}

// TestCategoryFiltering verifies category-based filtering works.
func TestCategoryFiltering(t *testing.T) {
	registry := NewRegistry()

	direct := registry.DirectAliases()
	transformed := registry.TransformedAliases()
	system := registry.SystemFunctions()
	all := registry.AllAliases()

	// Verify counts make sense
	if len(direct) == 0 {
		t.Error("DirectAliases() returned empty slice")
	}

	if len(transformed) == 0 {
		t.Error("TransformedAliases() returned empty slice")
	}

	// System functions may or may not exist depending on implementation
	// Just log the count
	t.Logf("Found %d direct, %d transformed, %d system aliases",
		len(direct), len(transformed), len(system))

	// All should be sum of categories
	totalCategorized := len(direct) + len(transformed) + len(system)
	if len(all) != totalCategorized {
		t.Errorf("AllAliases() count (%d) != sum of categories (%d)",
			len(all), totalCategorized)
	}

	// Verify categories are correct
	for _, alias := range direct {
		if alias.Category != DirectAlias {
			t.Errorf("DirectAliases contains %q with category %d",
				alias.PostgreSQLName, alias.Category)
		}
	}

	for _, alias := range transformed {
		if alias.Category != TransformedAlias {
			t.Errorf("TransformedAliases contains %q with category %d",
				alias.PostgreSQLName, alias.Category)
		}
	}

	for _, alias := range system {
		if alias.Category != SystemFunction {
			t.Errorf("SystemFunctions contains %q with category %d",
				alias.PostgreSQLName, alias.Category)
		}
	}
}

// TestSpecialFunctionsIntegration tests special functions work with the registry.
func TestSpecialFunctionsIntegration(t *testing.T) {
	// Test pg_typeof
	pgTypeof := &PgTypeof{}
	typeName := pgTypeof.EvaluateFromDuckDBType("INTEGER")
	if typeName != "integer" {
		t.Errorf("pg_typeof(INTEGER) = %q, want integer", typeName)
	}

	// Test current_schema
	currentSchema := NewCurrentSchema()
	schema := currentSchema.Evaluate()
	if schema != testSchemaMain {
		t.Errorf("current_schema() = %q, want %s", schema, testSchemaMain)
	}

	// Test current_user
	currentUser := NewCurrentUser("")
	user := currentUser.Evaluate()
	if user != "dukdb" {
		t.Errorf("current_user = %q, want dukdb", user)
	}

	// Test settings
	settings := NewSettings()
	version, ok := settings.Get("server_version")
	if !ok || version == "" {
		t.Error("server_version setting not found or empty")
	}

	// Test pg_size_pretty
	sizePretty := &PgSizePretty{}
	pretty := sizePretty.Evaluate(1073741824) // 1 GB
	if pretty != "1 GB" {
		t.Errorf("pg_size_pretty(1GB) = %q, want '1 GB'", pretty)
	}
}

// TestMinimumFunctionCoverage ensures we have minimum required function coverage.
func TestMinimumFunctionCoverage(t *testing.T) {
	registry := NewRegistry()

	// These are functions commonly used by ORMs and tools
	requiredFunctions := []string{
		// Date/time
		"now", "current_timestamp", "current_date", "current_time",

		// String
		"concat", "concat_ws", "length", "lower", "upper", "trim",
		"substring", "replace", "position",

		// Math
		"abs", "ceil", "floor", "round", "power", "sqrt",

		// Aggregate
		"count", "sum", "avg", "min", "max", "array_agg", "string_agg",

		// Conditional
		"coalesce", "nullif", "greatest", "least",

		// Type
		"cast",

		// Series
		"generate_series",
	}

	var missing []string
	for _, fn := range requiredFunctions {
		if !registry.Has(fn) {
			missing = append(missing, fn)
		}
	}

	if len(missing) > 0 {
		t.Errorf("Missing required functions: %v", missing)
	}

	t.Logf("Total aliases registered: %d", len(registry.AllAliases()))
}

// TestWindowFunctionsIntegration verifies window functions are registered.
func TestWindowFunctionsIntegration(t *testing.T) {
	registry := NewRegistry()

	windowFunctions := []string{
		"row_number", "rank", "dense_rank", "percent_rank", "cume_dist",
		"ntile", "lag", "lead", "first_value", "last_value", "nth_value",
	}

	for _, fn := range windowFunctions {
		t.Run(fn, func(t *testing.T) {
			if !registry.Has(fn) {
				t.Errorf("Window function %q not registered", fn)
			}
		})
	}
}

// TestJSONFunctionsIntegration verifies JSON functions are registered.
func TestJSONFunctionsIntegration(t *testing.T) {
	registry := NewRegistry()

	jsonFunctions := []string{
		"json_array_length", "json_typeof", "json_extract_path",
		"json_extract_path_text",
	}

	for _, fn := range jsonFunctions {
		t.Run(fn, func(t *testing.T) {
			if !registry.Has(fn) {
				t.Skipf("JSON function %q not registered (may be optional)", fn)
			}
		})
	}
}

// TestGlobalRegistryAccess verifies global registry functions work.
func TestGlobalRegistryAccess(t *testing.T) {
	// GetDefaultRegistry
	reg := GetDefaultRegistry()
	if reg == nil {
		t.Fatal("GetDefaultRegistry() returned nil")
	}

	// ResolveFunction
	alias := ResolveFunction("now")
	if alias == nil {
		t.Error("ResolveFunction(now) returned nil")
	}

	// GetDuckDBFunctionName
	duckName := GetDuckDBFunctionName("array_agg")
	if duckName != "list" {
		t.Errorf("GetDuckDBFunctionName(array_agg) = %q, want list", duckName)
	}

	// Unknown function returns original
	unknownName := GetDuckDBFunctionName("my_unknown_func")
	if unknownName != "my_unknown_func" {
		t.Errorf("GetDuckDBFunctionName(unknown) = %q, want original", unknownName)
	}
}

// TestTransformedFunctionValidation verifies all transformed functions have transformers.
func TestTransformedFunctionValidation(t *testing.T) {
	registry := NewRegistry()
	transformed := registry.TransformedAliases()

	for _, alias := range transformed {
		t.Run(alias.PostgreSQLName, func(t *testing.T) {
			if alias.Transformer == nil {
				t.Errorf("TransformedAlias %q has no Transformer function", alias.PostgreSQLName)
			}
		})
	}
}

// TestSystemFunctionRegistration verifies system functions are properly registered.
func TestSystemFunctionRegistration(t *testing.T) {
	registry := NewRegistry()

	systemFuncs := []struct {
		name     string
		wantType AliasCategory
	}{
		{"pg_typeof", SystemFunction},
		{"pg_backend_pid", SystemFunction},
		{"current_schema", SystemFunction},
		{"current_schemas", SystemFunction},
		{"has_table_privilege", SystemFunction},
		{"has_schema_privilege", SystemFunction},
		{"has_database_privilege", SystemFunction},
		{"pg_is_in_recovery", SystemFunction},
		{"pg_is_wal_replay_paused", SystemFunction},
		{"current_setting", SystemFunction},
		{"set_config", SystemFunction},
	}

	for _, sf := range systemFuncs {
		t.Run(sf.name, func(t *testing.T) {
			alias := registry.Resolve(sf.name)
			if alias == nil {
				t.Fatalf("System function %q not registered", sf.name)
			}
			if alias.Category != sf.wantType {
				t.Errorf("Category = %d, want %d", alias.Category, sf.wantType)
			}
		})
	}
}

// TestFunctionArgumentBounds verifies argument count validation logic.
func TestFunctionArgumentBounds(t *testing.T) {
	registry := NewRegistry()

	tests := []struct {
		funcName    string
		wantMinArgs int
		wantMaxArgs int // -1 means unlimited
	}{
		{"now", 0, 0},
		{"concat", 1, -1},
		{"concat_ws", 2, -1},
		{"substring", 2, 3},
		{"replace", 3, 3},
		{"coalesce", 1, -1},
		{"nullif", 2, 2},
		{"generate_series", 2, 3},
	}

	for _, tt := range tests {
		t.Run(tt.funcName, func(t *testing.T) {
			alias := registry.Resolve(tt.funcName)
			if alias == nil {
				t.Fatalf("Function %q not found", tt.funcName)
			}

			if alias.MinArgs != tt.wantMinArgs {
				t.Errorf("MinArgs = %d, want %d", alias.MinArgs, tt.wantMinArgs)
			}

			if alias.MaxArgs != tt.wantMaxArgs {
				t.Errorf("MaxArgs = %d, want %d", alias.MaxArgs, tt.wantMaxArgs)
			}
		})
	}
}

// TestSpecificNameMappings verifies important PostgreSQL to DuckDB name mappings.
func TestSpecificNameMappings(t *testing.T) {
	registry := NewRegistry()

	// These are critical mappings where PostgreSQL and DuckDB differ
	mappings := []struct {
		pgName   string
		duckName string
	}{
		// Name differs significantly
		{"array_agg", "list"},
		{"power", "pow"},
		{"ceiling", "ceil"},
		{"truncate", "trunc"},
		{"char_length", "length"},
		{"character_length", "length"},
		{"variance", "var_samp"},
		{"stddev", "stddev_samp"},
		{"every", "bool_and"},
		{"gen_random_uuid", "uuid"},
		{"uuid_generate_v4", "uuid"},
		{"string_to_array", "string_split"},
		{"json_typeof", "json_type"},
		{"jsonb_typeof", "json_type"},
		{"percentile_cont", "quantile_cont"},
		{"percentile_disc", "quantile_disc"},
		{"generate_series", "range"},

		// Same name (passthrough)
		{"now", "current_timestamp"},
		{"concat", "concat"},
		{"lower", "lower"},
		{"upper", "upper"},
	}

	for _, m := range mappings {
		t.Run(m.pgName, func(t *testing.T) {
			alias := registry.Resolve(m.pgName)
			if alias == nil {
				t.Fatalf("Alias not found for %s", m.pgName)
			}
			if alias.DuckDBName != m.duckName {
				t.Errorf("DuckDBName = %q, want %q", alias.DuckDBName, m.duckName)
			}
		})
	}
}

// TestAliasDescriptions verifies all aliases have descriptions.
func TestAliasDescriptions(t *testing.T) {
	registry := NewRegistry()
	all := registry.AllAliases()

	emptyDescriptions := 0
	for _, alias := range all {
		if alias.Description == "" {
			emptyDescriptions++
			t.Logf("Alias %q has empty description", alias.PostgreSQLName)
		}
	}

	// Allow some empty descriptions but warn if too many
	maxEmptyAllowed := len(all) / 10 // 10% threshold
	if emptyDescriptions > maxEmptyAllowed {
		t.Errorf("Too many aliases with empty descriptions: %d/%d", emptyDescriptions, len(all))
	}
}

// TestMinimumAliasCount verifies we meet minimum function coverage.
func TestMinimumAliasCount(t *testing.T) {
	registry := NewRegistry()

	// Requirement: at least 50 function aliases registered
	minRequired := 50
	total := len(registry.AllAliases())

	if total < minRequired {
		t.Errorf("Expected at least %d aliases, got %d", minRequired, total)
	}

	t.Logf("Total function aliases registered: %d", total)

	// Log category breakdown
	direct := len(registry.DirectAliases())
	transformed := len(registry.TransformedAliases())
	system := len(registry.SystemFunctions())

	t.Logf("Breakdown - Direct: %d, Transformed: %d, System: %d",
		direct, transformed, system)
}

// TestAllCategoriesHaveAliases verifies each category has at least one alias.
func TestAllCategoriesHaveAliases(t *testing.T) {
	registry := NewRegistry()

	categories := []struct {
		name  string
		slice []*FunctionAlias
	}{
		{"DirectAlias", registry.DirectAliases()},
		{"TransformedAlias", registry.TransformedAliases()},
		{"SystemFunction", registry.SystemFunctions()},
	}

	for _, cat := range categories {
		t.Run(cat.name, func(t *testing.T) {
			if len(cat.slice) == 0 {
				t.Errorf("Category %s has no aliases", cat.name)
			}
		})
	}
}

// TestNoDuplicateRegistrations verifies no duplicate function names.
func TestNoDuplicateRegistrations(t *testing.T) {
	registry := NewRegistry()
	all := registry.AllAliases()

	seen := make(map[string]bool)
	for _, alias := range all {
		key := alias.PostgreSQLName
		if seen[key] {
			// This shouldn't happen with map-based registry, but verify
			t.Errorf("Duplicate registration for %q", key)
		}
		seen[key] = true
	}
}

// TestORMCompatibilityFunctions verifies functions commonly used by ORMs.
func TestORMCompatibilityFunctions(t *testing.T) {
	registry := NewRegistry()

	// Functions commonly used by Prisma, TypeORM, GORM, SQLAlchemy, etc.
	ormFunctions := []string{
		// Basic queries
		"count", "sum", "avg", "min", "max",

		// String manipulation
		"lower", "upper", "length", "concat", "substring",

		// Date/time
		"now", "current_timestamp", "current_date",

		// Conditionals
		"coalesce", "nullif",

		// Type casting
		"cast",

		// UUID generation
		"gen_random_uuid",

		// Array aggregation
		"array_agg",

		// Sequence operations
		"nextval", "currval",
	}

	var missing []string
	for _, fn := range ormFunctions {
		if !registry.Has(fn) {
			missing = append(missing, fn)
		}
	}

	if len(missing) > 0 {
		t.Errorf("Missing ORM-critical functions: %v", missing)
	}
}

// TestPsqlCompatibilityFunctions verifies functions used by psql tool.
func TestPsqlCompatibilityFunctions(t *testing.T) {
	registry := NewRegistry()

	// Functions psql uses for introspection
	psqlFunctions := []string{
		"pg_typeof",
		"current_schema",
		"current_schemas",
		"version",
		"current_user",
		"current_database",
	}

	for _, fn := range psqlFunctions {
		t.Run(fn, func(t *testing.T) {
			if !registry.Has(fn) {
				t.Errorf("psql-required function %q not registered", fn)
			}
		})
	}
}

// TestSpecialFunctionTypeMapping verifies pg_typeof type mappings.
func TestSpecialFunctionTypeMapping(t *testing.T) {
	pgTypeof := &PgTypeof{}

	// Test comprehensive type mappings based on actual implementation
	// in internal/postgres/types/mapper.go and oids.go
	typeMappings := []struct {
		duckDBType string
		pgType     string
	}{
		// Integer types
		{"INTEGER", "integer"},
		{"INT", "integer"},
		{"INT4", "integer"},
		{"BIGINT", "bigint"},
		{"INT8", "bigint"},
		{"SMALLINT", "smallint"},
		{"INT2", "smallint"},
		{"TINYINT", "smallint"}, // PostgreSQL doesn't have 1-byte int
		{"HUGEINT", "numeric"},  // Use numeric for huge integers

		// Floating point
		{"DOUBLE", "double precision"},
		{"FLOAT8", "double precision"},
		{"REAL", "real"},
		{"FLOAT", "real"},
		{"FLOAT4", "real"},

		// Numeric
		{"DECIMAL", "numeric"},
		{"NUMERIC", "numeric"},

		// String types
		{"VARCHAR", "text"},
		{"STRING", "text"},
		{"TEXT", "text"},
		{"CHAR", "character"},
		{"BPCHAR", "character"},

		// Boolean
		{"BOOLEAN", "boolean"},
		{"BOOL", "boolean"},

		// Date/Time
		{"DATE", "date"},
		{"TIME", "time without time zone"},
		{"TIMESTAMP", "timestamp without time zone"},
		{"TIMESTAMPTZ", "timestamp with time zone"},
		{"INTERVAL", "interval"},

		// Other types
		{"UUID", "uuid"},
		{"BLOB", "bytea"},
		{"BYTEA", "bytea"},
		{"JSON", "json"},

		// LIST maps to text[] in current implementation
		{"LIST", "text[]"},
	}

	for _, tm := range typeMappings {
		t.Run(tm.duckDBType, func(t *testing.T) {
			result := pgTypeof.EvaluateFromDuckDBType(tm.duckDBType)
			if result != tm.pgType {
				t.Errorf("EvaluateFromDuckDBType(%q) = %q, want %q",
					tm.duckDBType, result, tm.pgType)
			}
		})
	}
}

// TestPgSizePrettyFormatting verifies size formatting works correctly.
func TestPgSizePrettyFormatting(t *testing.T) {
	sizePretty := &PgSizePretty{}

	tests := []struct {
		size int64
		want string
	}{
		{0, "0 bytes"},
		{1, "1 bytes"},
		{100, "100 bytes"},
		{1023, "1023 bytes"},
		{1024, "1 kB"},
		{1536, "1.5 kB"},
		{10240, "10 kB"},
		{1048576, "1 MB"},
		{1572864, "1.5 MB"},
		{1073741824, "1 GB"},
		{1610612736, "1.5 GB"},
		{1099511627776, "1 TB"},
		{1649267441664, "1.5 TB"},
		{1125899906842624, "1 PB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			result := sizePretty.Evaluate(tt.size)
			if result != tt.want {
				t.Errorf("Evaluate(%d) = %q, want %q", tt.size, result, tt.want)
			}
		})
	}
}

// TestSettingsDefaultValues verifies default settings are present.
func TestSettingsDefaultValues(t *testing.T) {
	settings := NewSettings()

	expectedSettings := []string{
		"server_version",
		"server_version_num",
		"standard_conforming_strings",
		"client_encoding",
		"DateStyle",
		"TimeZone",
		"max_identifier_length",
	}

	for _, name := range expectedSettings {
		t.Run(name, func(t *testing.T) {
			val, ok := settings.Get(name)
			if !ok {
				t.Errorf("Setting %q not found", name)
			}
			if val == "" {
				t.Errorf("Setting %q has empty value", name)
			}
		})
	}
}

// TestCurrentSchemasOutput verifies current_schemas function output.
func TestCurrentSchemasOutput(t *testing.T) {
	schemas := NewCurrentSchemas()

	// With implicit schemas
	withImplicit := schemas.EvaluateWithImplicit()
	if len(withImplicit) == 0 {
		t.Error("EvaluateWithImplicit() returned empty")
	}

	// Should contain main and pg_catalog
	foundMain := false
	foundPgCatalog := false
	for _, s := range withImplicit {
		if s == testSchemaMain {
			foundMain = true
		}
		if s == "pg_catalog" {
			foundPgCatalog = true
		}
	}

	if !foundMain {
		t.Error("EvaluateWithImplicit() should contain 'main' schema")
	}
	if !foundPgCatalog {
		t.Error("EvaluateWithImplicit() should contain 'pg_catalog'")
	}

	// Without implicit schemas
	withoutImplicit := schemas.EvaluateWithoutImplicit()
	for _, s := range withoutImplicit {
		if s == "pg_catalog" || s == "pg_temp" {
			t.Errorf("EvaluateWithoutImplicit() should not contain %q", s)
		}
	}
}

// TestPrivilegeFunctionsReturnTrue verifies privilege functions return true.
func TestPrivilegeFunctionsReturnTrue(t *testing.T) {
	// These functions should always return true (no privilege restrictions)

	hasTable := &HasTablePrivilege{}
	if !hasTable.Evaluate("user", "table", "SELECT") {
		t.Error("HasTablePrivilege should return true")
	}

	hasSchema := &HasSchemaPrivilege{}
	if !hasSchema.Evaluate("user", "schema", "USAGE") {
		t.Error("HasSchemaPrivilege should return true")
	}

	hasDB := &HasDatabasePrivilege{}
	if !hasDB.Evaluate("user", "db", "CONNECT") {
		t.Error("HasDatabasePrivilege should return true")
	}

	hasCol := &HasColumnPrivilege{}
	if !hasCol.Evaluate("user", "table", "col", "SELECT") {
		t.Error("HasColumnPrivilege should return true")
	}

	hasSeq := &HasSequencePrivilege{}
	if !hasSeq.Evaluate("user", "seq", "USAGE") {
		t.Error("HasSequencePrivilege should return true")
	}

	hasFunc := &HasFunctionPrivilege{}
	if !hasFunc.Evaluate("user", "func", "EXECUTE") {
		t.Error("HasFunctionPrivilege should return true")
	}

	hasRole := &PgHasRole{}
	if !hasRole.Evaluate("user", "role", "MEMBER") {
		t.Error("PgHasRole should return true")
	}
}
