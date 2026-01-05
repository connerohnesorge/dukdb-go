// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file defines catalog entry types for DuckDB format
// serialization, matching DuckDB's catalog structures.
package duckdb

// CatalogEntry is the interface implemented by all catalog entry types.
// It provides common accessors for catalog metadata operations.
type CatalogEntry interface {
	// Type returns the CatalogType identifying this entry kind.
	Type() CatalogType

	// GetName returns the name of the catalog entry.
	GetName() string

	// GetSchema returns the schema name of the catalog entry.
	GetSchema() string
}

// OnCreateConflict specifies the conflict resolution strategy when creating
// catalog entries.
type OnCreateConflict uint8

// OnCreateConflict constants.
const (
	// OnCreateConflictError raises an error if the object already exists.
	OnCreateConflictError OnCreateConflict = 0

	// OnCreateConflictIgnore ignores the create statement if the object exists.
	OnCreateConflictIgnore OnCreateConflict = 1

	// OnCreateConflictReplace replaces the existing object with the new one.
	OnCreateConflictReplace OnCreateConflict = 2
)

// onCreateConflictNames maps OnCreateConflict values to their string names.
var onCreateConflictNames = map[OnCreateConflict]string{
	OnCreateConflictError:   "ERROR",
	OnCreateConflictIgnore:  "IGNORE",
	OnCreateConflictReplace: "REPLACE",
}

// String returns the string representation of an OnCreateConflict.
func (c OnCreateConflict) String() string {
	if name, ok := onCreateConflictNames[c]; ok {
		return name
	}

	return strUnknown
}

// DependencyType specifies the type of dependency relationship between
// catalog objects.
type DependencyType uint8

// DependencyType constants.
const (
	// DependencyTypeRegular indicates a regular dependency (e.g., view depends on table).
	DependencyTypeRegular DependencyType = 0

	// DependencyTypeAutomatic indicates an automatically managed dependency.
	DependencyTypeAutomatic DependencyType = 1

	// DependencyTypeOwnership indicates an ownership dependency (e.g., index owns table).
	DependencyTypeOwnership DependencyType = 2
)

// dependencyTypeNames maps DependencyType values to their string names.
var dependencyTypeNames = map[DependencyType]string{
	DependencyTypeRegular:   "REGULAR",
	DependencyTypeAutomatic: "AUTOMATIC",
	DependencyTypeOwnership: "OWNERSHIP",
}

// String returns the string representation of a DependencyType.
func (d DependencyType) String() string {
	if name, ok := dependencyTypeNames[d]; ok {
		return name
	}

	return strUnknown
}

// DependencyEntry represents a dependency relationship between catalog objects.
type DependencyEntry struct {
	// Catalog is the catalog name of the dependent object.
	Catalog string

	// Schema is the schema name of the dependent object.
	Schema string

	// Name is the name of the dependent object.
	Name string

	// Type is the CatalogType of the dependent object.
	Type CatalogType

	// DependencyType is the type of dependency relationship.
	DependencyType DependencyType
}

// CreateInfo contains the common fields for all catalog entry types.
// These fields are shared across Schema, Table, View, Index, Sequence, and Type entries.
type CreateInfo struct {
	// Catalog is the catalog name (database name).
	Catalog string

	// Schema is the schema name (namespace).
	Schema string

	// Temporary indicates if this is a temporary object (session-scoped).
	Temporary bool

	// Internal indicates if this is an internal/system object.
	Internal bool

	// OnConflict specifies the conflict resolution strategy.
	OnConflict OnCreateConflict

	// SQL is the original SQL statement that created this object.
	SQL string

	// Comment is a user-provided comment/description.
	Comment string

	// Tags are user-defined key-value metadata.
	Tags map[string]string

	// Dependencies lists objects this entry depends on.
	Dependencies []DependencyEntry
}

// SchemaCatalogEntry represents a schema (namespace) definition in the catalog.
type SchemaCatalogEntry struct {
	// CreateInfo contains common catalog entry fields.
	CreateInfo

	// Name is the schema name.
	Name string
}

// Type returns CatalogSchemaEntry.
func (s *SchemaCatalogEntry) Type() CatalogType {
	return CatalogSchemaEntry
}

// GetName returns the schema name.
func (s *SchemaCatalogEntry) GetName() string {
	return s.Name
}

// GetSchema returns the parent schema name (usually empty for schema entries).
func (s *SchemaCatalogEntry) GetSchema() string {
	return s.CreateInfo.Schema
}

// NewSchemaCatalogEntry creates a new SchemaCatalogEntry with the given name.
func NewSchemaCatalogEntry(name string) *SchemaCatalogEntry {
	return &SchemaCatalogEntry{
		CreateInfo: CreateInfo{
			Tags: make(map[string]string),
		},
		Name: name,
	}
}

// ColumnDefinition represents a column definition in a table.
type ColumnDefinition struct {
	// Name is the column name.
	Name string

	// Type is the logical type ID of the column.
	Type LogicalTypeID

	// TypeModifiers contains type-specific information (precision, scale, child types, etc.).
	TypeModifiers TypeModifiers

	// Nullable indicates whether the column allows NULL values.
	Nullable bool

	// HasDefault indicates whether a default value is defined.
	HasDefault bool

	// DefaultValue is the default value expression (as SQL string).
	DefaultValue string

	// Generated indicates if this is a generated column.
	Generated bool

	// GeneratedExpression is the expression for generated columns.
	GeneratedExpression string

	// CompressionType is the compression hint for this column.
	CompressionType CompressionType
}

// TypeModifiers contains type-specific parameters for complex types.
// Not all fields are used for every type - only the relevant fields are populated.
type TypeModifiers struct {
	// Width is the precision/width for DECIMAL, CHAR, etc.
	Width uint8

	// Scale is the scale for DECIMAL type.
	Scale uint8

	// Length is the length for fixed-size types like CHAR, ARRAY.
	Length uint32

	// ChildType is the element type for LIST, ARRAY types.
	ChildType *TypeModifiers

	// ChildTypeID is the LogicalTypeID of the child type.
	ChildTypeID LogicalTypeID

	// KeyType is the key type for MAP.
	KeyType *TypeModifiers

	// KeyTypeID is the LogicalTypeID of the key type.
	KeyTypeID LogicalTypeID

	// ValueType is the value type for MAP.
	ValueType *TypeModifiers

	// ValueTypeID is the LogicalTypeID of the value type.
	ValueTypeID LogicalTypeID

	// StructFields is the list of fields for STRUCT type.
	StructFields []StructField

	// EnumValues is the list of values for ENUM type.
	EnumValues []string

	// UnionMembers is the list of members for UNION type.
	UnionMembers []UnionMember

	// Collation is the collation name for string types.
	Collation string
}

// StructField represents a field in a STRUCT type.
type StructField struct {
	// Name is the field name.
	Name string

	// Type is the LogicalTypeID of the field.
	Type LogicalTypeID

	// TypeModifiers contains nested type information.
	TypeModifiers *TypeModifiers
}

// UnionMember represents a member in a UNION type.
type UnionMember struct {
	// Tag is the tag name for this union member.
	Tag string

	// Type is the LogicalTypeID of this member.
	Type LogicalTypeID

	// TypeModifiers contains nested type information.
	TypeModifiers *TypeModifiers
}

// NewColumnDefinition creates a new ColumnDefinition with the given name and type.
func NewColumnDefinition(name string, typeID LogicalTypeID) *ColumnDefinition {
	return &ColumnDefinition{
		Name:            name,
		Type:            typeID,
		Nullable:        true, // Columns are nullable by default
		CompressionType: CompressionAuto,
	}
}

// ConstraintType specifies the type of table constraint.
type ConstraintType uint8

// ConstraintType constants.
const (
	// ConstraintTypePrimaryKey is a primary key constraint.
	ConstraintTypePrimaryKey ConstraintType = 0

	// ConstraintTypeForeignKey is a foreign key constraint.
	ConstraintTypeForeignKey ConstraintType = 1

	// ConstraintTypeUnique is a unique constraint.
	ConstraintTypeUnique ConstraintType = 2

	// ConstraintTypeCheck is a check constraint.
	ConstraintTypeCheck ConstraintType = 3

	// ConstraintTypeNotNull is a not null constraint.
	ConstraintTypeNotNull ConstraintType = 4
)

// constraintTypeNames maps ConstraintType values to their string names.
var constraintTypeNames = map[ConstraintType]string{
	ConstraintTypePrimaryKey: "PRIMARY_KEY",
	ConstraintTypeForeignKey: "FOREIGN_KEY",
	ConstraintTypeUnique:     "UNIQUE",
	ConstraintTypeCheck:      "CHECK",
	ConstraintTypeNotNull:    "NOT_NULL",
}

// String returns the string representation of a ConstraintType.
func (c ConstraintType) String() string {
	if name, ok := constraintTypeNames[c]; ok {
		return name
	}

	return strUnknown
}

// ForeignKeyAction specifies the action to take on foreign key violations.
type ForeignKeyAction uint8

// ForeignKeyAction constants.
const (
	// ForeignKeyActionNoAction takes no action on violation.
	ForeignKeyActionNoAction ForeignKeyAction = 0

	// ForeignKeyActionRestrict restricts the operation on violation.
	ForeignKeyActionRestrict ForeignKeyAction = 1

	// ForeignKeyActionCascade cascades the operation to related rows.
	ForeignKeyActionCascade ForeignKeyAction = 2

	// ForeignKeyActionSetNull sets the foreign key columns to NULL.
	ForeignKeyActionSetNull ForeignKeyAction = 3

	// ForeignKeyActionSetDefault sets the foreign key columns to their default values.
	ForeignKeyActionSetDefault ForeignKeyAction = 4
)

// foreignKeyActionNames maps ForeignKeyAction values to their string names.
var foreignKeyActionNames = map[ForeignKeyAction]string{
	ForeignKeyActionNoAction:   "NO_ACTION",
	ForeignKeyActionRestrict:   "RESTRICT",
	ForeignKeyActionCascade:    "CASCADE",
	ForeignKeyActionSetNull:    "SET_NULL",
	ForeignKeyActionSetDefault: "SET_DEFAULT",
}

// String returns the string representation of a ForeignKeyAction.
func (f ForeignKeyAction) String() string {
	if name, ok := foreignKeyActionNames[f]; ok {
		return name
	}

	return strUnknown
}

// Constraint represents a table constraint.
type Constraint struct {
	// Type is the constraint type.
	Type ConstraintType

	// Name is the constraint name (optional).
	Name string

	// ColumnIndices is the list of column indices involved in the constraint.
	// For PRIMARY KEY, UNIQUE: the columns forming the key.
	// For NOT NULL: single column index.
	// For CHECK: empty (expression references columns by name).
	ColumnIndices []uint64

	// Expression is the check constraint expression (for CHECK constraints).
	Expression string

	// ForeignKey contains foreign key specific information.
	ForeignKey *ForeignKeyInfo
}

// ForeignKeyInfo contains information specific to foreign key constraints.
type ForeignKeyInfo struct {
	// ReferencedSchema is the schema of the referenced table.
	ReferencedSchema string

	// ReferencedTable is the name of the referenced table.
	ReferencedTable string

	// ReferencedColumns is the list of referenced column names.
	ReferencedColumns []string

	// OnDelete specifies the action on delete.
	OnDelete ForeignKeyAction

	// OnUpdate specifies the action on update.
	OnUpdate ForeignKeyAction
}

// TableCatalogEntry represents a table definition in the catalog.
// Note: Row groups are stored separately via MetadataManager, not in this entry.
type TableCatalogEntry struct {
	// CreateInfo contains common catalog entry fields.
	CreateInfo

	// Name is the table name.
	Name string

	// Columns is the list of column definitions.
	Columns []ColumnDefinition

	// Constraints is the list of table constraints.
	Constraints []Constraint
}

// Type returns CatalogTableEntry.
func (t *TableCatalogEntry) Type() CatalogType {
	return CatalogTableEntry
}

// GetName returns the table name.
func (t *TableCatalogEntry) GetName() string {
	return t.Name
}

// GetSchema returns the schema name.
func (t *TableCatalogEntry) GetSchema() string {
	return t.CreateInfo.Schema
}

// ColumnCount returns the number of columns in the table.
func (t *TableCatalogEntry) ColumnCount() int {
	return len(t.Columns)
}

// GetColumn returns the column definition at the given index.
func (t *TableCatalogEntry) GetColumn(idx int) *ColumnDefinition {
	if idx < 0 || idx >= len(t.Columns) {
		return nil
	}
	return &t.Columns[idx]
}

// GetColumnByName returns the column definition with the given name.
// Returns nil if not found.
func (t *TableCatalogEntry) GetColumnByName(name string) *ColumnDefinition {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

// NewTableCatalogEntry creates a new TableCatalogEntry with the given name.
func NewTableCatalogEntry(name string) *TableCatalogEntry {
	return &TableCatalogEntry{
		CreateInfo: CreateInfo{
			Tags: make(map[string]string),
		},
		Name:        name,
		Columns:     make([]ColumnDefinition, 0),
		Constraints: make([]Constraint, 0),
	}
}

// AddColumn adds a column to the table.
func (t *TableCatalogEntry) AddColumn(col ColumnDefinition) {
	t.Columns = append(t.Columns, col)
}

// AddConstraint adds a constraint to the table.
func (t *TableCatalogEntry) AddConstraint(constraint Constraint) {
	t.Constraints = append(t.Constraints, constraint)
}

// ViewCatalogEntry represents a view definition in the catalog.
type ViewCatalogEntry struct {
	// CreateInfo contains common catalog entry fields.
	CreateInfo

	// Name is the view name.
	Name string

	// Query is the SELECT query that defines the view.
	Query string

	// Aliases is the list of column aliases (may be empty if using query column names).
	Aliases []string

	// Types is the list of column types for the view.
	Types []LogicalTypeID

	// TypeModifiers contains type-specific information for each column.
	TypeModifiers []TypeModifiers
}

// Type returns CatalogViewEntry.
func (v *ViewCatalogEntry) Type() CatalogType {
	return CatalogViewEntry
}

// GetName returns the view name.
func (v *ViewCatalogEntry) GetName() string {
	return v.Name
}

// GetSchema returns the schema name.
func (v *ViewCatalogEntry) GetSchema() string {
	return v.CreateInfo.Schema
}

// NewViewCatalogEntry creates a new ViewCatalogEntry with the given name and query.
func NewViewCatalogEntry(name, query string) *ViewCatalogEntry {
	return &ViewCatalogEntry{
		CreateInfo: CreateInfo{
			Tags: make(map[string]string),
		},
		Name:          name,
		Query:         query,
		Aliases:       make([]string, 0),
		Types:         make([]LogicalTypeID, 0),
		TypeModifiers: make([]TypeModifiers, 0),
	}
}

// IndexType specifies the type of index.
type IndexType uint8

// IndexType constants.
const (
	// IndexTypeInvalid is an invalid index type.
	IndexTypeInvalid IndexType = 0

	// IndexTypeART is an Adaptive Radix Tree index.
	IndexTypeART IndexType = 1

	// IndexTypeHash is a hash index.
	IndexTypeHash IndexType = 2
)

// indexTypeNames maps IndexType values to their string names.
var indexTypeNames = map[IndexType]string{
	IndexTypeInvalid: strInvalid,
	IndexTypeART:     "ART",
	IndexTypeHash:    "HASH",
}

// String returns the string representation of an IndexType.
func (i IndexType) String() string {
	if name, ok := indexTypeNames[i]; ok {
		return name
	}

	return strUnknown
}

// IndexConstraintType specifies the constraint type for an index.
type IndexConstraintType uint8

// IndexConstraintType constants.
const (
	// IndexConstraintNone indicates no constraint.
	IndexConstraintNone IndexConstraintType = 0

	// IndexConstraintUnique indicates a unique constraint.
	IndexConstraintUnique IndexConstraintType = 1

	// IndexConstraintPrimary indicates a primary key constraint.
	IndexConstraintPrimary IndexConstraintType = 2

	// IndexConstraintForeign indicates a foreign key constraint.
	IndexConstraintForeign IndexConstraintType = 3
)

// indexConstraintTypeNames maps IndexConstraintType values to their string names.
var indexConstraintTypeNames = map[IndexConstraintType]string{
	IndexConstraintNone:    "NONE",
	IndexConstraintUnique:  "UNIQUE",
	IndexConstraintPrimary: "PRIMARY",
	IndexConstraintForeign: "FOREIGN",
}

// String returns the string representation of an IndexConstraintType.
func (i IndexConstraintType) String() string {
	if name, ok := indexConstraintTypeNames[i]; ok {
		return name
	}

	return strUnknown
}

// IndexCatalogEntry represents an index definition in the catalog.
type IndexCatalogEntry struct {
	// CreateInfo contains common catalog entry fields.
	CreateInfo

	// Name is the index name.
	Name string

	// TableName is the name of the indexed table.
	TableName string

	// IndexType is the type of index (ART, Hash, etc.).
	IndexType IndexType

	// Constraint is the constraint type for this index.
	Constraint IndexConstraintType

	// ColumnIDs is the list of column indices in the indexed table.
	ColumnIDs []uint64

	// Expressions is the list of index expressions (for expression indexes).
	// If empty, the index is on the columns specified by ColumnIDs.
	Expressions []string
}

// Type returns CatalogIndexEntry.
func (i *IndexCatalogEntry) Type() CatalogType {
	return CatalogIndexEntry
}

// GetName returns the index name.
func (i *IndexCatalogEntry) GetName() string {
	return i.Name
}

// GetSchema returns the schema name.
func (i *IndexCatalogEntry) GetSchema() string {
	return i.CreateInfo.Schema
}

// IsUnique returns true if this is a unique index.
func (i *IndexCatalogEntry) IsUnique() bool {
	return i.Constraint == IndexConstraintUnique || i.Constraint == IndexConstraintPrimary
}

// IsPrimary returns true if this is a primary key index.
func (i *IndexCatalogEntry) IsPrimary() bool {
	return i.Constraint == IndexConstraintPrimary
}

// NewIndexCatalogEntry creates a new IndexCatalogEntry with the given name and table.
func NewIndexCatalogEntry(name, tableName string) *IndexCatalogEntry {
	return &IndexCatalogEntry{
		CreateInfo: CreateInfo{
			Tags: make(map[string]string),
		},
		Name:        name,
		TableName:   tableName,
		IndexType:   IndexTypeART,
		Constraint:  IndexConstraintNone,
		ColumnIDs:   make([]uint64, 0),
		Expressions: make([]string, 0),
	}
}

// SequenceUsage specifies how a sequence is used.
type SequenceUsage uint8

// SequenceUsage constants.
const (
	// SequenceUsageNone indicates no usage tracking.
	SequenceUsageNone SequenceUsage = 0

	// SequenceUsageOwned indicates the sequence is owned by a column.
	SequenceUsageOwned SequenceUsage = 1
)

// sequenceUsageNames maps SequenceUsage values to their string names.
var sequenceUsageNames = map[SequenceUsage]string{
	SequenceUsageNone:  "NONE",
	SequenceUsageOwned: "OWNED",
}

// String returns the string representation of a SequenceUsage.
func (s SequenceUsage) String() string {
	if name, ok := sequenceUsageNames[s]; ok {
		return name
	}

	return strUnknown
}

// SequenceCatalogEntry represents a sequence definition in the catalog.
type SequenceCatalogEntry struct {
	// CreateInfo contains common catalog entry fields.
	CreateInfo

	// Name is the sequence name.
	Name string

	// Usage specifies how the sequence is used.
	Usage SequenceUsage

	// StartWith is the initial/restart value.
	StartWith int64

	// Increment is the increment step (can be negative).
	Increment int64

	// MinValue is the minimum allowed value.
	MinValue int64

	// MaxValue is the maximum allowed value.
	MaxValue int64

	// Cycle indicates whether the sequence wraps around at limits.
	Cycle bool

	// Counter is the current sequence value.
	Counter int64
}

// Type returns CatalogSequenceEntry.
func (s *SequenceCatalogEntry) Type() CatalogType {
	return CatalogSequenceEntry
}

// GetName returns the sequence name.
func (s *SequenceCatalogEntry) GetName() string {
	return s.Name
}

// GetSchema returns the schema name.
func (s *SequenceCatalogEntry) GetSchema() string {
	return s.CreateInfo.Schema
}

// NewSequenceCatalogEntry creates a new SequenceCatalogEntry with default values.
func NewSequenceCatalogEntry(name string) *SequenceCatalogEntry {
	return &SequenceCatalogEntry{
		CreateInfo: CreateInfo{
			Tags: make(map[string]string),
		},
		Name:      name,
		Usage:     SequenceUsageNone,
		StartWith: 1,
		Increment: 1,
		MinValue:  1,
		MaxValue:  9223372036854775807, // Max int64
		Cycle:     false,
		Counter:   1,
	}
}

// TypeCatalogEntry represents a custom type or enum definition in the catalog.
type TypeCatalogEntry struct {
	// CreateInfo contains common catalog entry fields.
	CreateInfo

	// Name is the type name.
	Name string

	// TypeID is the underlying logical type.
	TypeID LogicalTypeID

	// TypeModifiers contains type-specific information.
	TypeModifiers TypeModifiers
}

// Type returns CatalogTypeEntry.
func (t *TypeCatalogEntry) Type() CatalogType {
	return CatalogTypeEntry
}

// GetName returns the type name.
func (t *TypeCatalogEntry) GetName() string {
	return t.Name
}

// GetSchema returns the schema name.
func (t *TypeCatalogEntry) GetSchema() string {
	return t.CreateInfo.Schema
}

// IsEnum returns true if this is an enum type.
func (t *TypeCatalogEntry) IsEnum() bool {
	return t.TypeID == TypeEnum
}

// NewTypeCatalogEntry creates a new TypeCatalogEntry with the given name.
func NewTypeCatalogEntry(name string, typeID LogicalTypeID) *TypeCatalogEntry {
	return &TypeCatalogEntry{
		CreateInfo: CreateInfo{
			Tags: make(map[string]string),
		},
		Name:   name,
		TypeID: typeID,
	}
}

// NewEnumTypeCatalogEntry creates a new TypeCatalogEntry for an enum type.
func NewEnumTypeCatalogEntry(name string, values []string) *TypeCatalogEntry {
	entry := &TypeCatalogEntry{
		CreateInfo: CreateInfo{
			Tags: make(map[string]string),
		},
		Name:   name,
		TypeID: TypeEnum,
		TypeModifiers: TypeModifiers{
			EnumValues: values,
		},
	}
	return entry
}

// Property IDs for ViewCatalogEntry.
const (
	PropViewQuery    = 210 // View query
	PropViewAliases  = 211 // Column aliases
	PropViewTypes    = 212 // Column types
	PropViewTypeMods = 213 // Column type modifiers
)

// Property IDs for IndexCatalogEntry.
const (
	PropIndexName       = 220 // Index name
	PropIndexTable      = 221 // Indexed table
	PropIndexType       = 222 // Index type
	PropIndexConstraint = 223 // Index constraint type
	PropIndexColumnIDs  = 224 // Column IDs
	PropIndexExprs      = 225 // Index expressions
)

// Property IDs for SequenceCatalogEntry.
const (
	PropSeqName      = 230 // Sequence name
	PropSeqUsage     = 231 // Usage type
	PropSeqStartWith = 232 // Start value
	PropSeqIncrement = 233 // Increment step
	PropSeqMinValue  = 234 // Minimum value
	PropSeqMaxValue  = 235 // Maximum value
	PropSeqCycle     = 236 // Cycle flag
	PropSeqCounter   = 237 // Current counter
)

// Property IDs for TypeCatalogEntry.
const (
	PropTypeName     = 240 // Type name
	PropTypeID       = 241 // Logical type ID
	PropTypeModifier = 242 // Type modifiers
)

// Property IDs for ColumnDefinition.
const (
	PropColName        = 250 // Column name
	PropColType        = 251 // Column type
	PropColTypeMod     = 252 // Type modifiers
	PropColNullable    = 253 // Nullable flag
	PropColHasDefault  = 254 // Has default flag
	PropColDefault     = 255 // Default value
	PropColGenerated   = 256 // Generated flag
	PropColGeneratedEx = 257 // Generated expression
	PropColCompression = 258 // Compression type
)

// Property IDs for Constraint.
const (
	PropConstraintType    = 260 // Constraint type
	PropConstraintName    = 261 // Constraint name
	PropConstraintCols    = 262 // Column indices
	PropConstraintExpr    = 263 // Check expression
	PropConstraintFK      = 264 // Foreign key info
	PropFKSchema          = 265 // FK referenced schema
	PropFKTable           = 266 // FK referenced table
	PropFKColumns         = 267 // FK referenced columns
	PropFKOnDelete        = 268 // FK on delete action
	PropFKOnUpdate        = 269 // FK on update action
)
