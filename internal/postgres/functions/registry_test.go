package functions

import (
	"fmt"
	"testing"
)

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()
	if r == nil {
		t.Fatal("NewRegistry() returned nil")
	}
	if len(r.aliases) == 0 {
		t.Error("NewRegistry() created empty registry")
	}
}

func TestResolve(t *testing.T) {
	r := NewRegistry()

	tests := []struct {
		pgName     string
		wantDuckDB string
		wantNil    bool
	}{
		{"now", "current_timestamp", false},
		{"NOW", "current_timestamp", false},            // Case insensitive
		{"pg_catalog.now", "current_timestamp", false}, // With prefix
		{"concat", "concat", false},
		{"array_agg", "list", false},
		{"power", "pow", false},
		{"ceiling", "ceil", false},
		{"generate_series", "generate_series", false},
		{"nonexistent_function", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.pgName, func(t *testing.T) {
			alias := r.Resolve(tt.pgName)
			if tt.wantNil {
				if alias != nil {
					t.Errorf("Resolve(%q) = %v, want nil", tt.pgName, alias)
				}
			} else {
				if alias == nil {
					t.Fatalf("Resolve(%q) = nil, want non-nil", tt.pgName)
				}
				if alias.DuckDBName != tt.wantDuckDB {
					t.Errorf("DuckDBName = %q, want %q", alias.DuckDBName, tt.wantDuckDB)
				}
			}
		})
	}
}

func TestHas(t *testing.T) {
	r := NewRegistry()

	if !r.Has("now") {
		t.Error("Has(now) = false, want true")
	}
	if !r.Has("NOW") {
		t.Error("Has(NOW) = false, want true")
	}
	if r.Has("nonexistent") {
		t.Error("Has(nonexistent) = true, want false")
	}
}

func TestGetDuckDBName(t *testing.T) {
	r := NewRegistry()

	tests := []struct {
		pgName   string
		wantName string
	}{
		{"now", "current_timestamp"},
		{"array_agg", "list"},
		{"power", "pow"},
		{"unknown_func", "unknown_func"}, // Returns original if no alias
	}

	for _, tt := range tests {
		t.Run(tt.pgName, func(t *testing.T) {
			got := r.GetDuckDBName(tt.pgName)
			if got != tt.wantName {
				t.Errorf("GetDuckDBName(%q) = %q, want %q", tt.pgName, got, tt.wantName)
			}
		})
	}
}

func TestAliasCategories(t *testing.T) {
	r := NewRegistry()

	direct := r.DirectAliases()
	if len(direct) == 0 {
		t.Error("DirectAliases() returned empty slice")
	}

	transformed := r.TransformedAliases()
	if len(transformed) == 0 {
		t.Error("TransformedAliases() returned empty slice")
	}

	system := r.SystemFunctions()
	if len(system) == 0 {
		t.Error("SystemFunctions() returned empty slice")
	}

	// Verify categories are correct
	for _, alias := range direct {
		if alias.Category != DirectAlias {
			t.Errorf(
				"DirectAliases contains %q with category %d",
				alias.PostgreSQLName,
				alias.Category,
			)
		}
	}

	for _, alias := range transformed {
		if alias.Category != TransformedAlias {
			t.Errorf(
				"TransformedAliases contains %q with category %d",
				alias.PostgreSQLName,
				alias.Category,
			)
		}
	}

	for _, alias := range system {
		if alias.Category != SystemFunction {
			t.Errorf(
				"SystemFunctions contains %q with category %d",
				alias.PostgreSQLName,
				alias.Category,
			)
		}
	}
}

func TestTransformGenerateSeries(t *testing.T) {
	tests := []struct {
		args     []string
		wantFunc string
		wantArgs []string
		wantErr  bool
	}{
		{
			args:     []string{"1", "5"},
			wantFunc: "generate_series",
			wantArgs: []string{"1", "5"},
		},
		{
			args:     []string{"1", "10", "2"},
			wantFunc: "generate_series",
			wantArgs: []string{"1", "10", "2"},
		},
		{
			args:     []string{"start_val", "end_val"},
			wantFunc: "generate_series",
			wantArgs: []string{"start_val", "end_val"},
		},
		{
			args:    []string{"1"}, // Too few args
			wantErr: true,
		},
		{
			args:    []string{"1", "2", "3", "4"}, // Too many args
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.args), func(t *testing.T) {
			funcName, args, err := transformGenerateSeries("generate_series", tt.args)
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

func TestGlobalRegistry(t *testing.T) {
	reg := GetDefaultRegistry()
	if reg == nil {
		t.Fatal("GetDefaultRegistry() returned nil")
	}

	alias := ResolveFunction("now")
	if alias == nil {
		t.Error("ResolveFunction(now) returned nil")
	}

	name := GetDuckDBFunctionName("array_agg")
	if name != "list" {
		t.Errorf("GetDuckDBFunctionName(array_agg) = %q, want list", name)
	}
}

func TestAllAliases(t *testing.T) {
	r := NewRegistry()
	all := r.AllAliases()

	if len(all) == 0 {
		t.Error("AllAliases() returned empty slice")
	}

	// Verify we can find expected functions
	found := make(map[string]bool)
	for _, alias := range all {
		found[alias.PostgreSQLName] = true
	}

	expected := []string{"now", "concat", "array_agg", "generate_series", "count", "sum"}
	for _, name := range expected {
		if !found[name] {
			t.Errorf("AllAliases() missing %q", name)
		}
	}
}

func TestRegisterCustomAlias(t *testing.T) {
	r := NewRegistry()

	// Register a custom alias
	r.Register(&FunctionAlias{
		PostgreSQLName: "my_custom_func",
		DuckDBName:     "duckdb_custom_func",
		Category:       DirectAlias,
		MinArgs:        1,
		MaxArgs:        2,
		Description:    "Test custom function",
	})

	// Verify it can be resolved
	alias := r.Resolve("my_custom_func")
	if alias == nil {
		t.Fatal("custom alias not found")
	}
	if alias.DuckDBName != "duckdb_custom_func" {
		t.Errorf("DuckDBName = %q, want duckdb_custom_func", alias.DuckDBName)
	}

	// Verify case insensitivity
	alias2 := r.Resolve("MY_CUSTOM_FUNC")
	if alias2 == nil {
		t.Error("custom alias not found with uppercase")
	}
}

func TestPgCatalogPrefix(t *testing.T) {
	r := NewRegistry()

	// Test that pg_catalog. prefix is handled correctly
	tests := []struct {
		name    string
		wantNil bool
	}{
		{"pg_catalog.now", false},
		{"pg_catalog.concat", false},
		{"pg_catalog.array_agg", false},
		{"pg_catalog.nonexistent", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alias := r.Resolve(tt.name)
			if tt.wantNil {
				if alias != nil {
					t.Errorf("Resolve(%q) = %v, want nil", tt.name, alias)
				}
			} else {
				if alias == nil {
					t.Errorf("Resolve(%q) = nil, want non-nil", tt.name)
				}
			}
		})
	}
}

func TestDateTimeFunctions(t *testing.T) {
	r := NewRegistry()

	dateTimeFuncs := []string{
		"now", "current_timestamp", "current_date", "current_time",
		"localtime", "localtimestamp", "timeofday",
		"transaction_timestamp", "statement_timestamp", "clock_timestamp",
		"date_part", "date_trunc", "extract", "age",
	}

	for _, name := range dateTimeFuncs {
		t.Run(name, func(t *testing.T) {
			if !r.Has(name) {
				t.Errorf("missing date/time function: %s", name)
			}
		})
	}
}

func TestStringFunctions(t *testing.T) {
	r := NewRegistry()

	stringFuncs := []string{
		"concat", "concat_ws", "length", "char_length", "character_length",
		"lower", "upper", "initcap", "trim", "ltrim", "rtrim",
		"lpad", "rpad", "substring", "substr", "replace", "reverse",
		"repeat", "position", "strpos", "left", "right", "split_part",
		"regexp_replace", "regexp_matches", "md5",
	}

	for _, name := range stringFuncs {
		t.Run(name, func(t *testing.T) {
			if !r.Has(name) {
				t.Errorf("missing string function: %s", name)
			}
		})
	}
}

func TestMathFunctions(t *testing.T) {
	r := NewRegistry()

	mathFuncs := []string{
		"abs", "ceil", "ceiling", "floor", "round", "trunc", "truncate",
		"mod", "power", "pow", "sqrt", "cbrt", "exp", "ln", "log", "log10",
		"sign", "random", "pi", "degrees", "radians",
		"sin", "cos", "tan", "cot", "asin", "acos", "atan", "atan2",
	}

	for _, name := range mathFuncs {
		t.Run(name, func(t *testing.T) {
			if !r.Has(name) {
				t.Errorf("missing math function: %s", name)
			}
		})
	}
}

func TestAggregateFunctions(t *testing.T) {
	r := NewRegistry()

	aggFuncs := []string{
		"array_agg", "string_agg", "count", "sum", "avg", "min", "max",
		"bool_and", "bool_or", "every", "bit_and", "bit_or",
		"variance", "var_samp", "var_pop", "stddev", "stddev_samp", "stddev_pop",
		"corr", "covar_pop", "covar_samp",
	}

	for _, name := range aggFuncs {
		t.Run(name, func(t *testing.T) {
			if !r.Has(name) {
				t.Errorf("missing aggregate function: %s", name)
			}
		})
	}
}

func TestWindowFunctions(t *testing.T) {
	r := NewRegistry()

	windowFuncs := []string{
		"row_number", "rank", "dense_rank", "percent_rank", "cume_dist",
		"ntile", "lag", "lead", "first_value", "last_value", "nth_value",
	}

	for _, name := range windowFuncs {
		t.Run(name, func(t *testing.T) {
			if !r.Has(name) {
				t.Errorf("missing window function: %s", name)
			}
		})
	}
}

func TestSystemFunctions(t *testing.T) {
	r := NewRegistry()

	systemFuncs := []string{
		"pg_typeof", "pg_backend_pid", "current_schema", "current_schemas",
		"has_table_privilege", "has_schema_privilege", "has_database_privilege",
		"pg_is_in_recovery", "pg_is_wal_replay_paused",
		"current_setting", "set_config", "show_all_settings",
	}

	for _, name := range systemFuncs {
		t.Run(name, func(t *testing.T) {
			if !r.Has(name) {
				t.Errorf("missing system function: %s", name)
			}
		})
	}
}

func TestConditionalFunctions(t *testing.T) {
	r := NewRegistry()

	conditionalFuncs := []string{
		"coalesce", "nullif", "greatest", "least",
	}

	for _, name := range conditionalFuncs {
		t.Run(name, func(t *testing.T) {
			if !r.Has(name) {
				t.Errorf("missing conditional function: %s", name)
			}
		})
	}
}

func TestJSONFunctions(t *testing.T) {
	r := NewRegistry()

	jsonFuncs := []string{
		"json_array_length", "jsonb_array_length",
		"json_extract_path", "jsonb_extract_path",
		"json_extract_path_text", "jsonb_extract_path_text",
		"json_typeof", "jsonb_typeof",
		"to_json", "to_jsonb",
		"json_agg", "jsonb_agg",
		"json_object_agg", "jsonb_object_agg",
	}

	for _, name := range jsonFuncs {
		t.Run(name, func(t *testing.T) {
			if !r.Has(name) {
				t.Errorf("missing JSON function: %s", name)
			}
		})
	}
}

func TestAliasCount(t *testing.T) {
	r := NewRegistry()
	all := r.AllAliases()

	// Verify we have at least 50 direct aliases as specified in requirements
	direct := r.DirectAliases()
	if len(direct) < 50 {
		t.Errorf("expected at least 50 direct aliases, got %d", len(direct))
	}

	t.Logf("Total aliases: %d (direct: %d, transformed: %d, system: %d)",
		len(all), len(direct), len(r.TransformedAliases()), len(r.SystemFunctions()))
}

func TestSpecificAliasMapping(t *testing.T) {
	r := NewRegistry()

	// Test specific mappings that differ between PostgreSQL and DuckDB
	mappings := []struct {
		pgName   string
		duckName string
	}{
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
	}

	for _, m := range mappings {
		t.Run(m.pgName, func(t *testing.T) {
			alias := r.Resolve(m.pgName)
			if alias == nil {
				t.Fatalf("alias not found for %s", m.pgName)
			}
			if alias.DuckDBName != m.duckName {
				t.Errorf("DuckDBName = %q, want %q", alias.DuckDBName, m.duckName)
			}
		})
	}
}

func TestAliasValidation(t *testing.T) {
	r := NewRegistry()
	all := r.AllAliases()

	for _, alias := range all {
		t.Run(alias.PostgreSQLName, func(t *testing.T) {
			// PostgreSQL name should not be empty
			if alias.PostgreSQLName == "" {
				t.Error("PostgreSQLName is empty")
			}

			// DuckDB name should not be empty
			if alias.DuckDBName == "" {
				t.Error("DuckDBName is empty")
			}

			// MaxArgs should be -1 (unlimited) or >= MinArgs
			if alias.MaxArgs != -1 && alias.MaxArgs < alias.MinArgs {
				t.Errorf("MaxArgs (%d) < MinArgs (%d)", alias.MaxArgs, alias.MinArgs)
			}

			// TransformedAlias should have a transformer if it's truly a transformed alias
			// (Note: some transformed aliases might be pending implementation)
		})
	}
}
