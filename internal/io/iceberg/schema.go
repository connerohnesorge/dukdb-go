// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements Iceberg to DuckDB schema mapping.
package iceberg

import (
	"fmt"

	"github.com/apache/iceberg-go"

	dukdb "github.com/dukdb/dukdb-go"
)

// SchemaMapper provides methods for mapping Iceberg schemas to DuckDB types.
type SchemaMapper struct{}

// NewSchemaMapper creates a new SchemaMapper.
func NewSchemaMapper() *SchemaMapper {
	return &SchemaMapper{}
}

// MapSchema maps an Iceberg schema to DuckDB column names and types.
func (m *SchemaMapper) MapSchema(schema *iceberg.Schema) ([]string, []dukdb.Type, error) {
	if schema == nil {
		return nil, nil, fmt.Errorf("%w: schema is nil", ErrInvalidMetadata)
	}

	fields := schema.Fields()
	names := make([]string, len(fields))
	types := make([]dukdb.Type, len(fields))

	for i, field := range fields {
		names[i] = field.Name
		t, err := m.MapType(field.Type)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to map field %q: %w", field.Name, err)
		}
		types[i] = t
	}

	return names, types, nil
}

// MapType maps a single Iceberg type to a DuckDB type.
func (m *SchemaMapper) MapType(icebergType iceberg.Type) (dukdb.Type, error) {
	if icebergType == nil {
		return dukdb.TYPE_INVALID, fmt.Errorf("%w: type is nil", ErrUnsupportedType)
	}

	switch icebergType.(type) {
	// Primitive types
	case iceberg.BooleanType:
		return dukdb.TYPE_BOOLEAN, nil

	case iceberg.Int32Type:
		return dukdb.TYPE_INTEGER, nil

	case iceberg.Int64Type:
		return dukdb.TYPE_BIGINT, nil

	case iceberg.Float32Type:
		return dukdb.TYPE_FLOAT, nil

	case iceberg.Float64Type:
		return dukdb.TYPE_DOUBLE, nil

	case iceberg.StringType:
		return dukdb.TYPE_VARCHAR, nil

	case iceberg.BinaryType:
		return dukdb.TYPE_BLOB, nil

	case iceberg.DateType:
		return dukdb.TYPE_DATE, nil

	case iceberg.TimeType:
		return dukdb.TYPE_TIME, nil

	case iceberg.TimestampType:
		return dukdb.TYPE_TIMESTAMP, nil

	case iceberg.TimestampTzType:
		return dukdb.TYPE_TIMESTAMP_TZ, nil

	case iceberg.UUIDType:
		return dukdb.TYPE_UUID, nil

	case iceberg.FixedType:
		// Fixed-length binary maps to BLOB
		return dukdb.TYPE_BLOB, nil

	case iceberg.DecimalType:
		// Decimal maps to DECIMAL
		return dukdb.TYPE_DECIMAL, nil

	// Nested types
	case *iceberg.StructType:
		return dukdb.TYPE_STRUCT, nil

	case *iceberg.ListType:
		return dukdb.TYPE_LIST, nil

	case *iceberg.MapType:
		return dukdb.TYPE_MAP, nil

	default:
		return dukdb.TYPE_INVALID, fmt.Errorf("%w: %s", ErrUnsupportedType, icebergType.Type())
	}
}

// ColumnInfo contains information about a column in an Iceberg schema.
type ColumnInfo struct {
	// ID is the Iceberg field ID.
	ID int
	// Name is the column name.
	Name string
	// Type is the DuckDB type.
	Type dukdb.Type
	// IcebergType is the original Iceberg type.
	IcebergType iceberg.Type
	// Required indicates if the column is required (not nullable).
	Required bool
	// Doc is the optional documentation string.
	Doc string
}

// MapSchemaToColumnInfo maps an Iceberg schema to detailed column information.
func (m *SchemaMapper) MapSchemaToColumnInfo(schema *iceberg.Schema) ([]ColumnInfo, error) {
	if schema == nil {
		return nil, fmt.Errorf("%w: schema is nil", ErrInvalidMetadata)
	}

	fields := schema.Fields()
	columns := make([]ColumnInfo, len(fields))

	for i, field := range fields {
		t, err := m.MapType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to map field %q: %w", field.Name, err)
		}

		columns[i] = ColumnInfo{
			ID:          field.ID,
			Name:        field.Name,
			Type:        t,
			IcebergType: field.Type,
			Required:    field.Required,
			Doc:         field.Doc,
		}
	}

	return columns, nil
}

// FindColumnByID finds a column in the schema by its Iceberg field ID.
func (m *SchemaMapper) FindColumnByID(schema *iceberg.Schema, fieldID int) (*ColumnInfo, error) {
	columns, err := m.MapSchemaToColumnInfo(schema)
	if err != nil {
		return nil, err
	}

	for _, col := range columns {
		if col.ID == fieldID {
			return &col, nil
		}
	}

	return nil, fmt.Errorf("%w: column ID %d", ErrSchemaNotFound, fieldID)
}

// FindColumnByName finds a column in the schema by its name.
func (m *SchemaMapper) FindColumnByName(schema *iceberg.Schema, name string) (*ColumnInfo, error) {
	columns, err := m.MapSchemaToColumnInfo(schema)
	if err != nil {
		return nil, err
	}

	for _, col := range columns {
		if col.Name == name {
			return &col, nil
		}
	}

	return nil, fmt.Errorf("%w: column name %q", ErrSchemaNotFound, name)
}

// ProjectSchema creates a projected schema containing only the specified columns.
// The columns can be specified by name.
func (m *SchemaMapper) ProjectSchema(
	schema *iceberg.Schema,
	columnNames []string,
) ([]ColumnInfo, error) {
	allColumns, err := m.MapSchemaToColumnInfo(schema)
	if err != nil {
		return nil, err
	}

	// Create a map for fast lookup
	columnMap := make(map[string]ColumnInfo)
	for _, col := range allColumns {
		columnMap[col.Name] = col
	}

	// Build projected schema
	projected := make([]ColumnInfo, 0, len(columnNames))
	for _, name := range columnNames {
		col, ok := columnMap[name]
		if !ok {
			return nil, fmt.Errorf("%w: column name %q", ErrSchemaNotFound, name)
		}
		projected = append(projected, col)
	}

	return projected, nil
}

// SchemaEvolutionChecker provides methods to check schema evolution compatibility.
type SchemaEvolutionChecker struct {
	mapper *SchemaMapper
}

// NewSchemaEvolutionChecker creates a new SchemaEvolutionChecker.
func NewSchemaEvolutionChecker() *SchemaEvolutionChecker {
	return &SchemaEvolutionChecker{mapper: NewSchemaMapper()}
}

// GetAddedColumns returns columns that were added between oldSchema and newSchema.
func (c *SchemaEvolutionChecker) GetAddedColumns(
	oldSchema, newSchema *iceberg.Schema,
) ([]ColumnInfo, error) {
	oldColumns, err := c.mapper.MapSchemaToColumnInfo(oldSchema)
	if err != nil {
		return nil, err
	}

	newColumns, err := c.mapper.MapSchemaToColumnInfo(newSchema)
	if err != nil {
		return nil, err
	}

	// Create a set of old column IDs
	oldIDs := make(map[int]bool)
	for _, col := range oldColumns {
		oldIDs[col.ID] = true
	}

	// Find columns in new schema that are not in old schema
	added := make([]ColumnInfo, 0)
	for _, col := range newColumns {
		if !oldIDs[col.ID] {
			added = append(added, col)
		}
	}

	return added, nil
}

// GetDroppedColumns returns columns that were dropped between oldSchema and newSchema.
func (c *SchemaEvolutionChecker) GetDroppedColumns(
	oldSchema, newSchema *iceberg.Schema,
) ([]ColumnInfo, error) {
	oldColumns, err := c.mapper.MapSchemaToColumnInfo(oldSchema)
	if err != nil {
		return nil, err
	}

	newColumns, err := c.mapper.MapSchemaToColumnInfo(newSchema)
	if err != nil {
		return nil, err
	}

	// Create a set of new column IDs
	newIDs := make(map[int]bool)
	for _, col := range newColumns {
		newIDs[col.ID] = true
	}

	// Find columns in old schema that are not in new schema
	dropped := make([]ColumnInfo, 0)
	for _, col := range oldColumns {
		if !newIDs[col.ID] {
			dropped = append(dropped, col)
		}
	}

	return dropped, nil
}

// GetRenamedColumns returns columns that were renamed between oldSchema and newSchema.
// Returns a map from old name to new name.
func (c *SchemaEvolutionChecker) GetRenamedColumns(
	oldSchema, newSchema *iceberg.Schema,
) (map[string]string, error) {
	oldColumns, err := c.mapper.MapSchemaToColumnInfo(oldSchema)
	if err != nil {
		return nil, err
	}

	newColumns, err := c.mapper.MapSchemaToColumnInfo(newSchema)
	if err != nil {
		return nil, err
	}

	// Create maps from ID to column for both schemas
	oldByID := make(map[int]ColumnInfo)
	for _, col := range oldColumns {
		oldByID[col.ID] = col
	}

	newByID := make(map[int]ColumnInfo)
	for _, col := range newColumns {
		newByID[col.ID] = col
	}

	// Find columns with the same ID but different names
	renamed := make(map[string]string)
	for id, oldCol := range oldByID {
		if newCol, ok := newByID[id]; ok {
			if oldCol.Name != newCol.Name {
				renamed[oldCol.Name] = newCol.Name
			}
		}
	}

	return renamed, nil
}

// IsCompatible checks if newSchema is compatible with oldSchema for reading.
// A schema is compatible if all columns in oldSchema exist in newSchema with compatible types.
func (c *SchemaEvolutionChecker) IsCompatible(oldSchema, newSchema *iceberg.Schema) bool {
	oldColumns, err := c.mapper.MapSchemaToColumnInfo(oldSchema)
	if err != nil {
		return false
	}

	newColumns, err := c.mapper.MapSchemaToColumnInfo(newSchema)
	if err != nil {
		return false
	}

	// Create a map of new columns by ID
	newByID := make(map[int]ColumnInfo)
	for _, col := range newColumns {
		newByID[col.ID] = col
	}

	// Check that all old columns exist in new schema with compatible types
	for _, oldCol := range oldColumns {
		newCol, ok := newByID[oldCol.ID]
		if !ok {
			// Column was dropped - this is OK for reading (will be NULL)
			continue
		}

		// Check type compatibility
		if oldCol.Type != newCol.Type {
			// Types don't match - not compatible
			// Note: This is a simplified check; real type compatibility is more nuanced
			return false
		}
	}

	return true
}
