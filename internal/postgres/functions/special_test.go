package functions

import (
	"testing"
)

// Test constants.
const (
	testSchemaPgCatalog = "pg_catalog"
	testSchemaPgTemp    = "pg_temp"
	testDefaultUser     = "dukdb"
)

func TestPgTypeof(t *testing.T) {
	f := &PgTypeof{}

	if f.Name() != "pg_typeof" {
		t.Errorf("Name() = %q, want pg_typeof", f.Name())
	}

	// Test with known DuckDB types
	tests := []struct {
		duckDBType string
		wantName   string
	}{
		{"INTEGER", "integer"},
		{"VARCHAR", "text"},
		{"BOOLEAN", "boolean"},
		{"TIMESTAMP", "timestamp without time zone"},
		{"BIGINT", "bigint"},
		{"DOUBLE", "double precision"},
		{"DATE", "date"},
	}

	for _, tt := range tests {
		t.Run(tt.duckDBType, func(t *testing.T) {
			name := f.EvaluateFromDuckDBType(tt.duckDBType)
			if name != tt.wantName {
				t.Errorf("EvaluateFromDuckDBType(%q) = %q, want %q", tt.duckDBType, name, tt.wantName)
			}
		})
	}
}

func TestCurrentSchema(t *testing.T) {
	f := NewCurrentSchema()

	result := f.Evaluate()
	if result != "main" {
		t.Errorf("Evaluate() = %q, want main", result)
	}

	// Test with custom schema
	f.DefaultSchema = "custom"
	if f.Evaluate() != "custom" {
		t.Errorf("Evaluate() = %q, want custom", f.Evaluate())
	}
}

func TestCurrentSchemas(t *testing.T) {
	f := NewCurrentSchemas()

	// With implicit schemas
	withImplicit := f.EvaluateWithImplicit()
	if len(withImplicit) < 2 {
		t.Errorf("EvaluateWithImplicit() returned %d schemas, want at least 2", len(withImplicit))
	}

	// Verify pg_catalog is included
	foundPgCatalog := false
	for _, s := range withImplicit {
		if s == testSchemaPgCatalog {
			foundPgCatalog = true

			break
		}
	}
	if !foundPgCatalog {
		t.Error("EvaluateWithImplicit() should include pg_catalog")
	}

	// Without implicit schemas
	withoutImplicit := f.EvaluateWithoutImplicit()
	for _, s := range withoutImplicit {
		if s == testSchemaPgCatalog || s == testSchemaPgTemp {
			t.Errorf("EvaluateWithoutImplicit() included implicit schema %q", s)
		}
	}
}

func TestCurrentUser(t *testing.T) {
	f := NewCurrentUser("")
	if f.Evaluate() != testDefaultUser {
		t.Errorf("Evaluate() = %q, want %s (default)", f.Evaluate(), testDefaultUser)
	}

	f2 := NewCurrentUser("testuser")
	if f2.Evaluate() != "testuser" {
		t.Errorf("Evaluate() = %q, want testuser", f2.Evaluate())
	}
}

func TestCurrentDatabase(t *testing.T) {
	f := NewCurrentDatabase("")
	if f.Evaluate() != testDefaultUser {
		t.Errorf("Evaluate() = %q, want %s (default)", f.Evaluate(), testDefaultUser)
	}

	f2 := NewCurrentDatabase("mydb")
	if f2.Evaluate() != "mydb" {
		t.Errorf("Evaluate() = %q, want mydb", f2.Evaluate())
	}
}

func TestHasTablePrivilege(t *testing.T) {
	f := &HasTablePrivilege{}

	// All calls should return true
	if !f.Evaluate("user", "table", "SELECT") {
		t.Error("HasTablePrivilege.Evaluate() = false, want true")
	}

	if !f.Evaluate("", "", "") {
		t.Error("HasTablePrivilege.Evaluate() with empty args = false, want true")
	}

	if !f.EvaluateWithOID("user", 12345, "INSERT") {
		t.Error("HasTablePrivilege.EvaluateWithOID() = false, want true")
	}
}

func TestHasSchemaPrivilege(t *testing.T) {
	f := &HasSchemaPrivilege{}
	if !f.Evaluate("user", "schema", "USAGE") {
		t.Error("HasSchemaPrivilege.Evaluate() = false, want true")
	}
}

func TestHasDatabasePrivilege(t *testing.T) {
	f := &HasDatabasePrivilege{}
	if !f.Evaluate("user", "db", "CONNECT") {
		t.Error("HasDatabasePrivilege.Evaluate() = false, want true")
	}
}

func TestHasColumnPrivilege(t *testing.T) {
	f := &HasColumnPrivilege{}
	if !f.Evaluate("user", "table", "column", "SELECT") {
		t.Error("HasColumnPrivilege.Evaluate() = false, want true")
	}
}

func TestHasSequencePrivilege(t *testing.T) {
	f := &HasSequencePrivilege{}
	if !f.Evaluate("user", "seq", "USAGE") {
		t.Error("HasSequencePrivilege.Evaluate() = false, want true")
	}
}

func TestHasFunctionPrivilege(t *testing.T) {
	f := &HasFunctionPrivilege{}
	if !f.Evaluate("user", "func", "EXECUTE") {
		t.Error("HasFunctionPrivilege.Evaluate() = false, want true")
	}
}

func TestHasAnyColumnPrivilege(t *testing.T) {
	f := &HasAnyColumnPrivilege{}
	if !f.Evaluate("user", "table", "SELECT") {
		t.Error("HasAnyColumnPrivilege.Evaluate() = false, want true")
	}
}

func TestPgHasRole(t *testing.T) {
	f := &PgHasRole{}
	if !f.Evaluate("user", "role", "MEMBER") {
		t.Error("PgHasRole.Evaluate() = false, want true")
	}
}

func TestPgBackendPid(t *testing.T) {
	f1 := NewPgBackendPid()
	f2 := NewPgBackendPid()

	pid1 := f1.Evaluate()
	pid2 := f2.Evaluate()

	if pid1 == 0 {
		t.Error("Evaluate() returned 0")
	}

	// Different instances should have different IDs
	if pid1 == pid2 {
		t.Error("Different PgBackendPid instances returned same ID")
	}
}

func TestPgCancelBackend(t *testing.T) {
	f := &PgCancelBackend{}
	if f.Evaluate(12345) {
		t.Error("PgCancelBackend.Evaluate() = true, want false")
	}
}

func TestPgTerminateBackend(t *testing.T) {
	f := &PgTerminateBackend{}
	if f.Evaluate(12345) {
		t.Error("PgTerminateBackend.Evaluate() = true, want false")
	}
}

func TestInetClientAddr(t *testing.T) {
	f := &InetClientAddr{}
	if f.Evaluate() != nil {
		t.Error("InetClientAddr.Evaluate() with empty addr should return nil")
	}

	f.ClientAddr = "127.0.0.1"
	result := f.Evaluate()
	if result == nil || *result != "127.0.0.1" {
		t.Errorf("InetClientAddr.Evaluate() = %v, want 127.0.0.1", result)
	}
}

func TestInetClientPort(t *testing.T) {
	f := &InetClientPort{}
	if f.Evaluate() != nil {
		t.Error("InetClientPort.Evaluate() with zero port should return nil")
	}

	f.ClientPort = 5432
	result := f.Evaluate()
	if result == nil || *result != 5432 {
		t.Errorf("InetClientPort.Evaluate() = %v, want 5432", result)
	}
}

func TestInetServerAddr(t *testing.T) {
	f := &InetServerAddr{}
	if f.Evaluate() != nil {
		t.Error("InetServerAddr.Evaluate() with empty addr should return nil")
	}

	f.ServerAddr = "0.0.0.0"
	result := f.Evaluate()
	if result == nil || *result != "0.0.0.0" {
		t.Errorf("InetServerAddr.Evaluate() = %v, want 0.0.0.0", result)
	}
}

func TestInetServerPort(t *testing.T) {
	f := &InetServerPort{}
	if f.Evaluate() != nil {
		t.Error("InetServerPort.Evaluate() with zero port should return nil")
	}

	f.ServerPort = 5432
	result := f.Evaluate()
	if result == nil || *result != 5432 {
		t.Errorf("InetServerPort.Evaluate() = %v, want 5432", result)
	}
}

func TestSettings(t *testing.T) {
	s := NewSettings()

	// Test Get for existing setting
	val, ok := s.Get("server_version")
	if !ok {
		t.Error("Get(server_version) returned not ok")
	}
	if val == "" {
		t.Error("Get(server_version) returned empty string")
	}

	// Test Get for non-existent setting
	_, ok = s.Get("nonexistent_setting")
	if ok {
		t.Error("Get(nonexistent_setting) returned ok, want not ok")
	}

	// Test Set and Get
	s.Set("test_setting", "test_value")
	val, ok = s.Get("test_setting")
	if !ok || val != "test_value" {
		t.Errorf("Get(test_setting) = %q, %v, want test_value, true", val, ok)
	}

	// Test overwriting existing setting
	s.Set("test_setting", "new_value")
	val, ok = s.Get("test_setting")
	if !ok || val != "new_value" {
		t.Errorf("Get(test_setting) after overwrite = %q, %v, want new_value, true", val, ok)
	}
}

func TestCurrentSetting(t *testing.T) {
	settings := NewSettings()
	f := NewCurrentSetting(settings)

	// Test existing setting with strict mode
	val, err := f.EvaluateStrict("server_version")
	if err != nil {
		t.Errorf("EvaluateStrict(server_version) error: %v", err)
	}
	if val == "" {
		t.Error("EvaluateStrict(server_version) returned empty string")
	}

	// Test missing setting with optional mode
	val = f.EvaluateOptional("nonexistent")
	if val != "" {
		t.Errorf("EvaluateOptional(nonexistent) = %q, want empty", val)
	}

	// Test missing setting with strict mode
	_, err = f.EvaluateStrict("nonexistent")
	if err == nil {
		t.Error("EvaluateStrict(nonexistent) should return error")
	}
}

func TestCurrentSettingWithNilSettings(t *testing.T) {
	f := NewCurrentSetting(nil)

	// Should use default settings
	val, err := f.EvaluateStrict("server_version")
	if err != nil {
		t.Errorf("EvaluateStrict with nil settings error: %v", err)
	}
	if val == "" {
		t.Error("EvaluateStrict with nil settings returned empty string")
	}
}

func TestSetConfig(t *testing.T) {
	settings := NewSettings()
	f := NewSetConfig(settings)

	result := f.Evaluate("my_setting", "my_value", false)
	if result != "my_value" {
		t.Errorf("Evaluate() = %q, want my_value", result)
	}

	// Verify setting was stored
	val, ok := settings.Get("my_setting")
	if !ok || val != "my_value" {
		t.Errorf("Setting not stored correctly: %q, %v", val, ok)
	}

	// Test with isLocal=true (should behave the same in our implementation)
	result = f.Evaluate("local_setting", "local_value", true)
	if result != "local_value" {
		t.Errorf("Evaluate() with isLocal=true = %q, want local_value", result)
	}
}

func TestSetConfigWithNilSettings(t *testing.T) {
	f := NewSetConfig(nil)

	// Should use default settings
	result := f.Evaluate("test", "value", false)
	if result != "value" {
		t.Errorf("SetConfig.Evaluate with nil settings = %q, want value", result)
	}
}

func TestGetDefaultSettings(t *testing.T) {
	s := GetDefaultSettings()
	if s == nil {
		t.Error("GetDefaultSettings() returned nil")
	}

	// Verify it returns the same instance
	s2 := GetDefaultSettings()
	if s != s2 {
		t.Error("GetDefaultSettings() returned different instances")
	}
}

func TestPgSizePretty(t *testing.T) {
	f := &PgSizePretty{}

	tests := []struct {
		size int64
		want string
	}{
		{0, "0 bytes"},
		{100, "100 bytes"},
		{1023, "1023 bytes"},
		{1024, "1 kB"},
		{1536, "1.5 kB"},
		{2048, "2 kB"},
		{1048576, "1 MB"},
		{1572864, "1.5 MB"},
		{1073741824, "1 GB"},
		{1610612736, "1.5 GB"},
		{1099511627776, "1 TB"},
		{1125899906842624, "1 PB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			result := f.Evaluate(tt.size)
			if result != tt.want {
				t.Errorf("Evaluate(%d) = %q, want %q", tt.size, result, tt.want)
			}
		})
	}
}

func TestPgDatabaseSize(t *testing.T) {
	f := &PgDatabaseSize{}

	if f.Evaluate("test") != 0 {
		t.Error("PgDatabaseSize.Evaluate() != 0")
	}

	if f.EvaluateByOID(12345) != 0 {
		t.Error("PgDatabaseSize.EvaluateByOID() != 0")
	}
}

func TestPgTableSize(t *testing.T) {
	f := &PgTableSize{}

	if f.Evaluate("test") != 0 {
		t.Error("PgTableSize.Evaluate() != 0")
	}

	if f.EvaluateByOID(12345) != 0 {
		t.Error("PgTableSize.EvaluateByOID() != 0")
	}
}

func TestPgRelationSize(t *testing.T) {
	f := &PgRelationSize{}

	if f.Evaluate("test") != 0 {
		t.Error("PgRelationSize.Evaluate() != 0")
	}

	if f.EvaluateByOID(12345) != 0 {
		t.Error("PgRelationSize.EvaluateByOID() != 0")
	}
}

func TestPgTotalRelationSize(t *testing.T) {
	f := &PgTotalRelationSize{}

	if f.Evaluate("test") != 0 {
		t.Error("PgTotalRelationSize.Evaluate() != 0")
	}

	if f.EvaluateByOID(12345) != 0 {
		t.Error("PgTotalRelationSize.EvaluateByOID() != 0")
	}
}

func TestPgIndexesSize(t *testing.T) {
	f := &PgIndexesSize{}

	if f.Evaluate("test") != 0 {
		t.Error("PgIndexesSize.Evaluate() != 0")
	}

	if f.EvaluateByOID(12345) != 0 {
		t.Error("PgIndexesSize.EvaluateByOID() != 0")
	}
}

func TestPgTablespaceSize(t *testing.T) {
	f := &PgTablespaceSize{}

	if f.Evaluate("test") != 0 {
		t.Error("PgTablespaceSize.Evaluate() != 0")
	}

	if f.EvaluateByOID(12345) != 0 {
		t.Error("PgTablespaceSize.EvaluateByOID() != 0")
	}
}
