// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file provides bidirectional conversion between
// DuckDB catalog structures and dukdb-go catalog structures.
package duckdb

import (
	"fmt"
	"math"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
)

// DuckDBCatalog is a container for all DuckDB catalog entries.
// It represents the complete catalog state for serialization/deserialization.
type DuckDBCatalog struct {
	// Schemas contains all schema definitions.
	Schemas []*SchemaCatalogEntry

	// Tables contains all table definitions.
	Tables []*TableCatalogEntry

	// Views contains all view definitions.
	Views []*ViewCatalogEntry

	// Indexes contains all index definitions.
	Indexes []*IndexCatalogEntry

	// Sequences contains all sequence definitions.
	Sequences []*SequenceCatalogEntry

	// Types contains all custom type definitions.
	Types []*TypeCatalogEntry
}

// NewDuckDBCatalog creates an empty DuckDBCatalog.
func NewDuckDBCatalog() *DuckDBCatalog {
	return &DuckDBCatalog{
		Schemas:   make([]*SchemaCatalogEntry, 0),
		Tables:    make([]*TableCatalogEntry, 0),
		Views:     make([]*ViewCatalogEntry, 0),
		Indexes:   make([]*IndexCatalogEntry, 0),
		Sequences: make([]*SequenceCatalogEntry, 0),
		Types:     make([]*TypeCatalogEntry, 0),
	}
}

// -----------------------------------------------------------------------------
// dukdb-go to DuckDB conversion functions (for writing .duckdb files)
// -----------------------------------------------------------------------------

// ConvertCatalogToDuckDB converts a dukdb-go Catalog to a DuckDBCatalog.
func ConvertCatalogToDuckDB(cat *catalog.Catalog) (*DuckDBCatalog, error) {
	if cat == nil {
		return nil, fmt.Errorf("cannot convert nil catalog")
	}

	result := NewDuckDBCatalog()

	// Convert all schemas
	for _, schema := range cat.ListSchemas() {
		// Convert schema entry
		schemaEntry := NewSchemaCatalogEntry(schema.Name())
		result.Schemas = append(result.Schemas, schemaEntry)

		// Convert tables in this schema
		for _, table := range schema.ListTables() {
			tableEntry, err := ConvertTableToDuckDB(table)
			if err != nil {
				return nil, fmt.Errorf("failed to convert table %s.%s: %w",
					schema.Name(), table.Name, err)
			}
			tableEntry.CreateInfo.Schema = schema.Name()
			result.Tables = append(result.Tables, tableEntry)
		}

		// Convert views in this schema
		for _, view := range schema.ListViews() {
			viewEntry, err := ConvertViewToDuckDB(view)
			if err != nil {
				return nil, fmt.Errorf("failed to convert view %s.%s: %w",
					schema.Name(), view.Name, err)
			}
			viewEntry.CreateInfo.Schema = schema.Name()
			result.Views = append(result.Views, viewEntry)
		}

		// Convert indexes in this schema
		for _, index := range schema.ListIndexes() {
			indexEntry, err := ConvertIndexToDuckDB(index)
			if err != nil {
				return nil, fmt.Errorf("failed to convert index %s.%s: %w",
					schema.Name(), index.Name, err)
			}
			indexEntry.CreateInfo.Schema = schema.Name()
			result.Indexes = append(result.Indexes, indexEntry)
		}

		// Convert sequences in this schema
		for _, seq := range schema.ListSequences() {
			seqEntry, err := ConvertSequenceToDuckDB(seq)
			if err != nil {
				return nil, fmt.Errorf("failed to convert sequence %s.%s: %w",
					schema.Name(), seq.Name, err)
			}
			seqEntry.CreateInfo.Schema = schema.Name()
			result.Sequences = append(result.Sequences, seqEntry)
		}
	}

	return result, nil
}

// ConvertTableToDuckDB converts a dukdb-go TableDef to a TableCatalogEntry.
func ConvertTableToDuckDB(t *catalog.TableDef) (*TableCatalogEntry, error) {
	if t == nil {
		return nil, fmt.Errorf("cannot convert nil table")
	}

	entry := NewTableCatalogEntry(t.Name)
	entry.CreateInfo.Schema = t.Schema

	// Convert columns
	for _, col := range t.Columns {
		colDef, err := ConvertColumnToDuckDB(col)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s: %w", col.Name, err)
		}
		entry.AddColumn(*colDef)
	}

	// Convert primary key constraint
	if len(t.PrimaryKey) > 0 {
		pkConstraint := Constraint{
			Type:          ConstraintTypePrimaryKey,
			ColumnIndices: make([]uint64, len(t.PrimaryKey)),
		}
		for i, idx := range t.PrimaryKey {
			pkConstraint.ColumnIndices[i] = uint64(idx)
		}
		entry.AddConstraint(pkConstraint)
	}

	return entry, nil
}

// ConvertViewToDuckDB converts a dukdb-go ViewDef to a ViewCatalogEntry.
func ConvertViewToDuckDB(v *catalog.ViewDef) (*ViewCatalogEntry, error) {
	if v == nil {
		return nil, fmt.Errorf("cannot convert nil view")
	}

	entry := NewViewCatalogEntry(v.Name, v.Query)
	entry.CreateInfo.Schema = v.Schema

	// Store table dependencies in the Dependencies field
	for _, dep := range v.TableDependencies {
		entry.CreateInfo.Dependencies = append(entry.CreateInfo.Dependencies, DependencyEntry{
			Schema: v.Schema,
			Name:   dep,
			Type:   CatalogTableEntry,
		})
	}

	return entry, nil
}

// ConvertIndexToDuckDB converts a dukdb-go IndexDef to an IndexCatalogEntry.
func ConvertIndexToDuckDB(idx *catalog.IndexDef) (*IndexCatalogEntry, error) {
	if idx == nil {
		return nil, fmt.Errorf("cannot convert nil index")
	}

	entry := NewIndexCatalogEntry(idx.Name, idx.Table)
	entry.CreateInfo.Schema = idx.Schema

	// Set index type - dukdb-go uses hash indexes by default
	entry.IndexType = IndexTypeHash

	// Set constraint type
	if idx.IsPrimary {
		entry.Constraint = IndexConstraintPrimary
	} else if idx.IsUnique {
		entry.Constraint = IndexConstraintUnique
	} else {
		entry.Constraint = IndexConstraintNone
	}

	// Store column names as expressions (we don't have column indices here)
	// The actual column indices would need to be resolved against the table
	entry.Expressions = make([]string, len(idx.Columns))
	copy(entry.Expressions, idx.Columns)

	return entry, nil
}

// ConvertSequenceToDuckDB converts a dukdb-go SequenceDef to a SequenceCatalogEntry.
func ConvertSequenceToDuckDB(seq *catalog.SequenceDef) (*SequenceCatalogEntry, error) {
	if seq == nil {
		return nil, fmt.Errorf("cannot convert nil sequence")
	}

	entry := NewSequenceCatalogEntry(seq.Name)
	entry.CreateInfo.Schema = seq.Schema
	entry.StartWith = seq.StartWith
	entry.Increment = seq.IncrementBy
	entry.MinValue = seq.MinValue
	entry.MaxValue = seq.MaxValue
	entry.Cycle = seq.IsCycle
	entry.Counter = seq.GetCurrentVal()

	return entry, nil
}

// ConvertColumnToDuckDB converts a dukdb-go ColumnDef to a ColumnDefinition.
func ConvertColumnToDuckDB(col *catalog.ColumnDef) (*ColumnDefinition, error) {
	if col == nil {
		return nil, fmt.Errorf("cannot convert nil column")
	}

	typeID, typeMods, err := ConvertTypeToDuckDB(col.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to convert type: %w", err)
	}

	def := NewColumnDefinition(col.Name, typeID)
	def.TypeModifiers = *typeMods
	def.Nullable = col.Nullable
	def.HasDefault = col.HasDefault

	// Convert default value to string representation if present
	if col.HasDefault && col.DefaultValue != nil {
		def.DefaultValue = fmt.Sprintf("%v", col.DefaultValue)
	}

	return def, nil
}

// ConvertTypeToDuckDB converts a dukdb.Type to a DuckDB LogicalTypeID and TypeModifiers.
func ConvertTypeToDuckDB(typ dukdb.Type) (LogicalTypeID, *TypeModifiers, error) {
	mods := &TypeModifiers{}

	switch typ {
	// Boolean
	case dukdb.TYPE_BOOLEAN:
		return TypeBoolean, mods, nil

	// Integer types
	case dukdb.TYPE_TINYINT:
		return TypeTinyInt, mods, nil
	case dukdb.TYPE_SMALLINT:
		return TypeSmallInt, mods, nil
	case dukdb.TYPE_INTEGER:
		return TypeInteger, mods, nil
	case dukdb.TYPE_BIGINT:
		return TypeBigInt, mods, nil
	case dukdb.TYPE_HUGEINT:
		return TypeHugeInt, mods, nil

	// Unsigned integer types
	case dukdb.TYPE_UTINYINT:
		return TypeUTinyInt, mods, nil
	case dukdb.TYPE_USMALLINT:
		return TypeUSmallInt, mods, nil
	case dukdb.TYPE_UINTEGER:
		return TypeUInteger, mods, nil
	case dukdb.TYPE_UBIGINT:
		return TypeUBigInt, mods, nil
	case dukdb.TYPE_UHUGEINT:
		return TypeUHugeInt, mods, nil

	// Floating point types
	case dukdb.TYPE_FLOAT:
		return TypeFloat, mods, nil
	case dukdb.TYPE_DOUBLE:
		return TypeDouble, mods, nil

	// Decimal type
	case dukdb.TYPE_DECIMAL:
		// Default precision/scale - actual values would come from TypeInfo
		mods.Width = 18
		mods.Scale = 3
		return TypeDecimal, mods, nil

	// String types
	case dukdb.TYPE_VARCHAR:
		return TypeVarchar, mods, nil
	case dukdb.TYPE_BLOB:
		return TypeBlob, mods, nil
	case dukdb.TYPE_BIT:
		return TypeBit, mods, nil
	case dukdb.TYPE_JSON:
		// JSON is stored as VARCHAR with a tag in DuckDB
		return TypeVarchar, mods, nil

	// Date/Time types
	case dukdb.TYPE_DATE:
		return TypeDate, mods, nil
	case dukdb.TYPE_TIME:
		return TypeTime, mods, nil
	case dukdb.TYPE_TIME_TZ:
		return TypeTimeTZ, mods, nil
	case dukdb.TYPE_TIMESTAMP:
		return TypeTimestamp, mods, nil
	case dukdb.TYPE_TIMESTAMP_S:
		return TypeTimestampS, mods, nil
	case dukdb.TYPE_TIMESTAMP_MS:
		return TypeTimestampMS, mods, nil
	case dukdb.TYPE_TIMESTAMP_NS:
		return TypeTimestampNS, mods, nil
	case dukdb.TYPE_TIMESTAMP_TZ:
		return TypeTimestampTZ, mods, nil
	case dukdb.TYPE_INTERVAL:
		return TypeInterval, mods, nil

	// UUID
	case dukdb.TYPE_UUID:
		return TypeUUID, mods, nil

	// Nested types
	case dukdb.TYPE_LIST:
		return TypeList, mods, nil
	case dukdb.TYPE_STRUCT:
		return TypeStruct, mods, nil
	case dukdb.TYPE_MAP:
		return TypeMap, mods, nil
	case dukdb.TYPE_ARRAY:
		return TypeArray, mods, nil
	case dukdb.TYPE_UNION:
		return TypeUnion, mods, nil

	// Enum
	case dukdb.TYPE_ENUM:
		return TypeEnum, mods, nil

	// Other types
	case dukdb.TYPE_ANY:
		return TypeAny, mods, nil
	case dukdb.TYPE_SQLNULL:
		return TypeSQLNull, mods, nil
	case dukdb.TYPE_GEOMETRY:
		return TypeGeometry, mods, nil
	case dukdb.TYPE_LAMBDA:
		return TypeLambda, mods, nil
	case dukdb.TYPE_VARIANT:
		return TypeVariant, mods, nil
	case dukdb.TYPE_BIGNUM:
		return TypeBigNum, mods, nil

	default:
		return TypeInvalid, mods, fmt.Errorf("unsupported type: %s", typ.String())
	}
}

// -----------------------------------------------------------------------------
// DuckDB to dukdb-go conversion functions (for reading .duckdb files)
// -----------------------------------------------------------------------------

// ConvertCatalogFromDuckDB converts a DuckDBCatalog to a dukdb-go Catalog.
func ConvertCatalogFromDuckDB(dcat *DuckDBCatalog) (*catalog.Catalog, error) {
	if dcat == nil {
		return nil, fmt.Errorf("cannot convert nil DuckDB catalog")
	}

	cat := catalog.NewCatalog()

	// Create schemas first (skip "main" as it's created by default)
	for _, schemaEntry := range dcat.Schemas {
		if strings.ToLower(schemaEntry.Name) != "main" {
			_, err := cat.CreateSchemaIfNotExists(schemaEntry.Name, true)
			if err != nil {
				return nil, fmt.Errorf("failed to create schema %s: %w", schemaEntry.Name, err)
			}
		}
	}

	// Convert tables
	for _, tableEntry := range dcat.Tables {
		tableDef, err := ConvertTableFromDuckDB(tableEntry)
		if err != nil {
			return nil, fmt.Errorf("failed to convert table %s.%s: %w",
				tableEntry.GetSchema(), tableEntry.Name, err)
		}

		schemaName := tableEntry.GetSchema()
		if schemaName == "" {
			schemaName = "main"
		}

		// Ensure schema exists
		_, err = cat.CreateSchemaIfNotExists(schemaName, true)
		if err != nil {
			return nil, fmt.Errorf("failed to create schema %s: %w", schemaName, err)
		}

		err = cat.CreateTableInSchema(schemaName, tableDef)
		if err != nil {
			return nil, fmt.Errorf("failed to create table %s.%s: %w",
				schemaName, tableDef.Name, err)
		}
	}

	// Convert views
	for _, viewEntry := range dcat.Views {
		viewDef, err := ConvertViewFromDuckDB(viewEntry)
		if err != nil {
			return nil, fmt.Errorf("failed to convert view %s.%s: %w",
				viewEntry.GetSchema(), viewEntry.Name, err)
		}

		schemaName := viewEntry.GetSchema()
		if schemaName == "" {
			schemaName = "main"
		}

		err = cat.CreateViewInSchema(schemaName, viewDef)
		if err != nil {
			return nil, fmt.Errorf("failed to create view %s.%s: %w",
				schemaName, viewDef.Name, err)
		}
	}

	// Convert indexes
	for _, indexEntry := range dcat.Indexes {
		indexDef, err := ConvertIndexFromDuckDB(indexEntry)
		if err != nil {
			return nil, fmt.Errorf("failed to convert index %s.%s: %w",
				indexEntry.GetSchema(), indexEntry.Name, err)
		}

		schemaName := indexEntry.GetSchema()
		if schemaName == "" {
			schemaName = "main"
		}

		err = cat.CreateIndexInSchema(schemaName, indexDef)
		if err != nil {
			return nil, fmt.Errorf("failed to create index %s.%s: %w",
				schemaName, indexDef.Name, err)
		}
	}

	// Convert sequences
	for _, seqEntry := range dcat.Sequences {
		seqDef, err := ConvertSequenceFromDuckDB(seqEntry)
		if err != nil {
			return nil, fmt.Errorf("failed to convert sequence %s.%s: %w",
				seqEntry.GetSchema(), seqEntry.Name, err)
		}

		schemaName := seqEntry.GetSchema()
		if schemaName == "" {
			schemaName = "main"
		}

		err = cat.CreateSequenceInSchema(schemaName, seqDef)
		if err != nil {
			return nil, fmt.Errorf("failed to create sequence %s.%s: %w",
				schemaName, seqDef.Name, err)
		}
	}

	return cat, nil
}

// ConvertTableFromDuckDB converts a TableCatalogEntry to a dukdb-go TableDef.
func ConvertTableFromDuckDB(t *TableCatalogEntry) (*catalog.TableDef, error) {
	if t == nil {
		return nil, fmt.Errorf("cannot convert nil table entry")
	}

	// Convert columns
	columns := make([]*catalog.ColumnDef, len(t.Columns))
	for i := range t.Columns {
		col, err := ConvertColumnFromDuckDB(&t.Columns[i])
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s: %w", t.Columns[i].Name, err)
		}
		columns[i] = col
	}

	tableDef := catalog.NewTableDef(t.Name, columns)
	tableDef.Schema = t.GetSchema()

	// Extract primary key from constraints
	for _, constraint := range t.Constraints {
		if constraint.Type == ConstraintTypePrimaryKey {
			pkIndices := make([]int, len(constraint.ColumnIndices))
			for i, idx := range constraint.ColumnIndices {
				pkIndices[i] = int(idx)
			}
			tableDef.PrimaryKey = pkIndices
			break
		}
	}

	return tableDef, nil
}

// ConvertViewFromDuckDB converts a ViewCatalogEntry to a dukdb-go ViewDef.
func ConvertViewFromDuckDB(v *ViewCatalogEntry) (*catalog.ViewDef, error) {
	if v == nil {
		return nil, fmt.Errorf("cannot convert nil view entry")
	}

	// Extract table dependencies from Dependencies field
	var tableDeps []string
	for _, dep := range v.CreateInfo.Dependencies {
		if dep.Type == CatalogTableEntry {
			tableDeps = append(tableDeps, dep.Name)
		}
	}

	if len(tableDeps) > 0 {
		return catalog.NewViewDefWithDependencies(v.Name, v.GetSchema(), v.Query, tableDeps), nil
	}

	return catalog.NewViewDef(v.Name, v.GetSchema(), v.Query), nil
}

// ConvertIndexFromDuckDB converts an IndexCatalogEntry to a dukdb-go IndexDef.
func ConvertIndexFromDuckDB(idx *IndexCatalogEntry) (*catalog.IndexDef, error) {
	if idx == nil {
		return nil, fmt.Errorf("cannot convert nil index entry")
	}

	// Use expressions as column names (they were stored this way during conversion)
	columns := make([]string, len(idx.Expressions))
	copy(columns, idx.Expressions)

	isUnique := idx.Constraint == IndexConstraintUnique || idx.Constraint == IndexConstraintPrimary
	indexDef := catalog.NewIndexDef(idx.Name, idx.GetSchema(), idx.TableName, columns, isUnique)
	indexDef.IsPrimary = idx.Constraint == IndexConstraintPrimary

	return indexDef, nil
}

// ConvertSequenceFromDuckDB converts a SequenceCatalogEntry to a dukdb-go SequenceDef.
func ConvertSequenceFromDuckDB(seq *SequenceCatalogEntry) (*catalog.SequenceDef, error) {
	if seq == nil {
		return nil, fmt.Errorf("cannot convert nil sequence entry")
	}

	seqDef := catalog.NewSequenceDef(seq.Name, seq.GetSchema())
	seqDef.StartWith = seq.StartWith
	seqDef.IncrementBy = seq.Increment
	seqDef.MinValue = seq.MinValue
	seqDef.MaxValue = seq.MaxValue
	seqDef.IsCycle = seq.Cycle
	seqDef.SetCurrentVal(seq.Counter)

	return seqDef, nil
}

// ConvertColumnFromDuckDB converts a ColumnDefinition to a dukdb-go ColumnDef.
func ConvertColumnFromDuckDB(col *ColumnDefinition) (*catalog.ColumnDef, error) {
	if col == nil {
		return nil, fmt.Errorf("cannot convert nil column definition")
	}

	typ, err := ConvertTypeFromDuckDB(col.Type, &col.TypeModifiers)
	if err != nil {
		return nil, fmt.Errorf("failed to convert type: %w", err)
	}

	colDef := catalog.NewColumnDef(col.Name, typ)
	colDef.Nullable = col.Nullable
	colDef.HasDefault = col.HasDefault

	// Parse default value if present
	if col.HasDefault && col.DefaultValue != "" {
		colDef.DefaultValue = col.DefaultValue
	}

	return colDef, nil
}

// ConvertTypeFromDuckDB converts a DuckDB LogicalTypeID and TypeModifiers to a dukdb.Type.
func ConvertTypeFromDuckDB(typeID LogicalTypeID, mods *TypeModifiers) (dukdb.Type, error) {
	switch typeID {
	// Special types
	case TypeInvalid:
		return dukdb.TYPE_INVALID, nil
	case TypeSQLNull:
		return dukdb.TYPE_SQLNULL, nil
	case TypeAny:
		return dukdb.TYPE_ANY, nil

	// Boolean
	case TypeBoolean:
		return dukdb.TYPE_BOOLEAN, nil

	// Integer types
	case TypeTinyInt:
		return dukdb.TYPE_TINYINT, nil
	case TypeSmallInt:
		return dukdb.TYPE_SMALLINT, nil
	case TypeInteger:
		return dukdb.TYPE_INTEGER, nil
	case TypeBigInt:
		return dukdb.TYPE_BIGINT, nil
	case TypeHugeInt:
		return dukdb.TYPE_HUGEINT, nil

	// Unsigned integer types
	case TypeUTinyInt:
		return dukdb.TYPE_UTINYINT, nil
	case TypeUSmallInt:
		return dukdb.TYPE_USMALLINT, nil
	case TypeUInteger:
		return dukdb.TYPE_UINTEGER, nil
	case TypeUBigInt:
		return dukdb.TYPE_UBIGINT, nil
	case TypeUHugeInt:
		return dukdb.TYPE_UHUGEINT, nil

	// Floating point types
	case TypeFloat:
		return dukdb.TYPE_FLOAT, nil
	case TypeDouble:
		return dukdb.TYPE_DOUBLE, nil

	// Decimal type
	case TypeDecimal:
		return dukdb.TYPE_DECIMAL, nil

	// String types
	case TypeChar:
		// CHAR is stored as VARCHAR in dukdb-go
		return dukdb.TYPE_VARCHAR, nil
	case TypeVarchar:
		return dukdb.TYPE_VARCHAR, nil
	case TypeBlob:
		return dukdb.TYPE_BLOB, nil
	case TypeBit:
		return dukdb.TYPE_BIT, nil

	// Date/Time types
	case TypeDate:
		return dukdb.TYPE_DATE, nil
	case TypeTime:
		return dukdb.TYPE_TIME, nil
	case TypeTimeNS:
		// TIME_NS maps to TIME in dukdb-go (with loss of precision)
		return dukdb.TYPE_TIME, nil
	case TypeTimeTZ:
		return dukdb.TYPE_TIME_TZ, nil
	case TypeTimestamp:
		return dukdb.TYPE_TIMESTAMP, nil
	case TypeTimestampS:
		return dukdb.TYPE_TIMESTAMP_S, nil
	case TypeTimestampMS:
		return dukdb.TYPE_TIMESTAMP_MS, nil
	case TypeTimestampNS:
		return dukdb.TYPE_TIMESTAMP_NS, nil
	case TypeTimestampTZ:
		return dukdb.TYPE_TIMESTAMP_TZ, nil
	case TypeInterval:
		return dukdb.TYPE_INTERVAL, nil

	// UUID
	case TypeUUID:
		return dukdb.TYPE_UUID, nil

	// Nested types
	case TypeList:
		return dukdb.TYPE_LIST, nil
	case TypeStruct:
		return dukdb.TYPE_STRUCT, nil
	case TypeMap:
		return dukdb.TYPE_MAP, nil
	case TypeArray:
		return dukdb.TYPE_ARRAY, nil
	case TypeUnion:
		return dukdb.TYPE_UNION, nil

	// Enum
	case TypeEnum:
		return dukdb.TYPE_ENUM, nil

	// Other types
	case TypeGeometry:
		return dukdb.TYPE_GEOMETRY, nil
	case TypeLambda:
		return dukdb.TYPE_LAMBDA, nil
	case TypeVariant:
		return dukdb.TYPE_VARIANT, nil
	case TypeBigNum:
		return dukdb.TYPE_BIGNUM, nil

	default:
		return dukdb.TYPE_INVALID, fmt.Errorf("unsupported DuckDB type: %s", typeID.String())
	}
}

// -----------------------------------------------------------------------------
// Helper functions for type conversion with TypeInfo support
// -----------------------------------------------------------------------------

// ConvertTypeToDuckDBWithInfo converts a dukdb.Type along with TypeInfo to DuckDB types.
// This version handles complex types with additional metadata (e.g., DECIMAL precision/scale).
func ConvertTypeToDuckDBWithInfo(
	typ dukdb.Type,
	info dukdb.TypeInfo,
) (LogicalTypeID, *TypeModifiers, error) {
	typeID, mods, err := ConvertTypeToDuckDB(typ)
	if err != nil {
		return typeID, mods, err
	}

	// If we have TypeInfo, extract additional information
	if info != nil {
		details := info.Details()
		if details != nil {
			switch d := details.(type) {
			case *dukdb.DecimalDetails:
				mods.Width = d.Width
				mods.Scale = d.Scale
			case *dukdb.ListDetails:
				// Get child type
				childInfo := d.Child
				if childInfo != nil {
					childTypeID, childMods, err := ConvertTypeToDuckDBWithInfo(
						childInfo.InternalType(), childInfo)
					if err == nil {
						mods.ChildTypeID = childTypeID
						mods.ChildType = childMods
					}
				}
			case *dukdb.StructDetails:
				// Convert struct entries
				for _, entry := range d.Entries {
					fieldInfo := entry.Info()
					if fieldInfo == nil {
						continue
					}
					fieldTypeID, fieldMods, err := ConvertTypeToDuckDBWithInfo(
						fieldInfo.InternalType(), fieldInfo)
					if err != nil {
						continue
					}
					mods.StructFields = append(mods.StructFields, StructField{
						Name:          entry.Name(),
						Type:          fieldTypeID,
						TypeModifiers: fieldMods,
					})
				}
			case *dukdb.MapDetails:
				// Get key and value types
				keyInfo := d.Key
				valueInfo := d.Value
				if keyInfo != nil {
					keyTypeID, keyMods, err := ConvertTypeToDuckDBWithInfo(
						keyInfo.InternalType(), keyInfo)
					if err == nil {
						mods.KeyTypeID = keyTypeID
						mods.KeyType = keyMods
					}
				}
				if valueInfo != nil {
					valueTypeID, valueMods, err := ConvertTypeToDuckDBWithInfo(
						valueInfo.InternalType(), valueInfo)
					if err == nil {
						mods.ValueTypeID = valueTypeID
						mods.ValueType = valueMods
					}
				}
			case *dukdb.EnumDetails:
				mods.EnumValues = d.Values
			}
		}
	}

	return typeID, mods, nil
}

// ConvertTypeFromDuckDBWithInfo converts a DuckDB LogicalTypeID and TypeModifiers to
// dukdb.Type and dukdb.TypeInfo for full type information preservation.
func ConvertTypeFromDuckDBWithInfo(
	typeID LogicalTypeID,
	mods *TypeModifiers,
) (dukdb.Type, dukdb.TypeInfo, error) {
	typ, err := ConvertTypeFromDuckDB(typeID, mods)
	if err != nil {
		return typ, nil, err
	}

	// Create TypeInfo based on the type
	var info dukdb.TypeInfo

	switch typeID {
	case TypeDecimal:
		if mods != nil {
			info, _ = dukdb.NewDecimalInfo(mods.Width, mods.Scale)
		}
	case TypeList:
		if mods != nil && mods.ChildType != nil {
			childType, childInfo, _ := ConvertTypeFromDuckDBWithInfo(
				mods.ChildTypeID,
				mods.ChildType,
			)
			if childInfo == nil {
				childInfo, _ = dukdb.NewTypeInfo(childType)
			}
			if childInfo != nil {
				info, _ = dukdb.NewListInfo(childInfo)
			}
		}
	case TypeStruct:
		if mods != nil && len(mods.StructFields) > 0 {
			entries := make([]dukdb.StructEntry, 0, len(mods.StructFields))
			for _, field := range mods.StructFields {
				fieldType, fieldInfo, _ := ConvertTypeFromDuckDBWithInfo(
					field.Type,
					field.TypeModifiers,
				)
				if fieldInfo == nil {
					fieldInfo, _ = dukdb.NewTypeInfo(fieldType)
				}
				if fieldInfo != nil {
					entry, err := dukdb.NewStructEntry(fieldInfo, field.Name)
					if err == nil {
						entries = append(entries, entry)
					}
				}
			}
			if len(entries) > 0 {
				info, _ = dukdb.NewStructInfo(entries[0], entries[1:]...)
			}
		}
	case TypeMap:
		if mods != nil {
			var keyInfo, valueInfo dukdb.TypeInfo
			if mods.KeyType != nil {
				keyType, ki, _ := ConvertTypeFromDuckDBWithInfo(mods.KeyTypeID, mods.KeyType)
				if ki == nil {
					ki, _ = dukdb.NewTypeInfo(keyType)
				}
				keyInfo = ki
			}
			if mods.ValueType != nil {
				valueType, vi, _ := ConvertTypeFromDuckDBWithInfo(mods.ValueTypeID, mods.ValueType)
				if vi == nil {
					vi, _ = dukdb.NewTypeInfo(valueType)
				}
				valueInfo = vi
			}
			if keyInfo != nil && valueInfo != nil {
				info, _ = dukdb.NewMapInfo(keyInfo, valueInfo)
			}
		}
	case TypeEnum:
		if mods != nil && len(mods.EnumValues) > 0 {
			if len(mods.EnumValues) > 0 {
				info, _ = dukdb.NewEnumInfo(mods.EnumValues[0], mods.EnumValues[1:]...)
			}
		}
	case TypeArray:
		if mods != nil && mods.ChildType != nil && mods.Length > 0 {
			childType, childInfo, _ := ConvertTypeFromDuckDBWithInfo(
				mods.ChildTypeID,
				mods.ChildType,
			)
			if childInfo == nil {
				childInfo, _ = dukdb.NewTypeInfo(childType)
			}
			if childInfo != nil {
				info, _ = dukdb.NewArrayInfo(childInfo, uint64(mods.Length))
			}
		}
	default:
		// For simple types, create basic TypeInfo
		info, _ = dukdb.NewTypeInfo(typ)
	}

	return typ, info, nil
}

// ValidateTypeConversion checks if a type can be round-tripped between dukdb-go and DuckDB.
func ValidateTypeConversion(typ dukdb.Type) error {
	typeID, mods, err := ConvertTypeToDuckDB(typ)
	if err != nil {
		return fmt.Errorf("cannot convert to DuckDB: %w", err)
	}

	backType, err := ConvertTypeFromDuckDB(typeID, mods)
	if err != nil {
		return fmt.Errorf("cannot convert back from DuckDB: %w", err)
	}

	// Some types may convert to different but equivalent types
	// (e.g., CHAR -> VARCHAR, TIME_NS -> TIME)
	switch typ {
	case dukdb.TYPE_JSON:
		// JSON is stored as VARCHAR in DuckDB
		if backType != dukdb.TYPE_VARCHAR {
			return fmt.Errorf("type mismatch: %s -> %s", typ, backType)
		}
	default:
		if backType != typ {
			return fmt.Errorf("type mismatch: %s -> %s", typ, backType)
		}
	}

	return nil
}

// GetSequenceMinMaxDefaults returns the default min/max values for sequences.
// These match DuckDB's default sequence bounds.
func GetSequenceMinMaxDefaults() (minValue, maxValue int64) {
	return math.MinInt64, math.MaxInt64
}
