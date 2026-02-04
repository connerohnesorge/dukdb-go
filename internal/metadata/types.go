package metadata

import (
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
)

const (
	DefaultDatabaseName = "memory"
	DefaultSchemaName   = "main"
)

// TableMetadata describes a table in the catalog.
type TableMetadata struct {
	DatabaseName  string
	SchemaName    string
	TableName     string
	TableType     string
	RowCount      int64
	EstimatedSize int64
	ColumnCount   int
	HasPrimaryKey bool
}

// ColumnMetadata describes a column in a table.
type ColumnMetadata struct {
	DatabaseName  string
	SchemaName    string
	TableName     string
	ColumnName    string
	ColumnIndex   int
	DataType      string
	IsNullable    bool
	ColumnDefault string
}

// ConstraintMetadata describes a table constraint.
type ConstraintMetadata struct {
	DatabaseName     string
	SchemaName       string
	TableName        string
	ConstraintName   string
	ConstraintType   string
	ConstraintColumn string
}

// ViewMetadata describes a view in the catalog.
type ViewMetadata struct {
	DatabaseName   string
	SchemaName     string
	ViewName       string
	ViewDefinition string
}

// IndexMetadata describes an index in the catalog.
type IndexMetadata struct {
	DatabaseName string
	SchemaName   string
	TableName    string
	IndexName    string
	IsUnique     bool
	IsPrimary    bool
	IndexColumns string
}

// SequenceMetadata describes a sequence in the catalog.
type SequenceMetadata struct {
	DatabaseName string
	SchemaName   string
	SequenceName string
	StartValue   int64
	IncrementBy  int64
	MinValue     int64
	MaxValue     int64
	IsCycle      bool
	CurrentValue int64
}

// FunctionMetadata describes a function exposed to SQL.
type FunctionMetadata struct {
	DatabaseName string
	SchemaName   string
	FunctionName string
	FunctionType string
	Parameters   string
	ReturnType   string
	Description  string
}

// SettingMetadata describes a configuration setting.
type SettingMetadata struct {
	Name        string
	Value       string
	Description string
	InputType   string
	Scope       string
}

// DatabaseMetadata describes a database in the catalog.
type DatabaseMetadata struct {
	DatabaseName string
	DatabaseOID  int64
	DatabaseSize int64
	DatabaseType string
}

// DependencyMetadata describes object dependencies.
type DependencyMetadata struct {
	DatabaseName   string
	SchemaName     string
	ObjectName     string
	ObjectType     string
	DependencyName string
	DependencyType string
}

// OptimizerMetadata describes optimizer settings.
type OptimizerMetadata struct {
	Name        string
	Description string
	Value       string
}

// KeywordMetadata describes a SQL keyword.
type KeywordMetadata struct {
	Keyword  string
	Category string
	Reserved bool
}

// ExtensionMetadata describes an extension entry.
type ExtensionMetadata struct {
	ExtensionName string
	Loaded        bool
	Installed     bool
	Description   string
}

// MemoryUsageMetadata describes memory usage statistics.
type MemoryUsageMetadata struct {
	MemoryUsage  int64
	MaxMemory    int64
	SystemMemory int64
}

// TempDirectoryMetadata describes the temp directory configuration.
type TempDirectoryMetadata struct {
	TempDirectory string
}

// ColumnNames returns the column names for a slice of column definitions.
func ColumnNames(defs []*catalog.ColumnDef) []string {
	names := make([]string, len(defs))
	for i, def := range defs {
		names[i] = def.Name
	}
	return names
}

// FormatTypeInfo returns a SQL type string for type info.
func FormatTypeInfo(info dukdb.TypeInfo) string {
	if info == nil {
		return ""
	}
	return info.SQLType()
}

// JoinColumns joins column names for display.
func JoinColumns(cols []string) string {
	return strings.Join(cols, ",")
}
