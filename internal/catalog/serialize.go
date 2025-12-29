// Package catalog provides schema metadata management for the native Go DuckDB implementation.
package catalog

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/persistence"
)

// Export exports the catalog to a JSON-serializable structure
func (c *Catalog) Export() *persistence.CatalogJSON {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data := &persistence.CatalogJSON{
		Version: 1,
		Schemas: make(map[string]*persistence.SchemaJSON),
	}

	for name, schema := range c.schemas {
		schemaData := &persistence.SchemaJSON{
			Name:   name,
			Tables: make(map[string]*persistence.TableJSON),
		}
		schema.mu.RLock()
		for tableName, tableDef := range schema.tables {
			schemaData.Tables[tableName] = exportTableDef(tableDef)
		}
		schema.mu.RUnlock()
		data.Schemas[name] = schemaData
	}

	return data
}

// Import imports the catalog from a serialized structure
func (c *Catalog) Import(data *persistence.CatalogJSON) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for schemaName, schemaData := range data.Schemas {
		schema, ok := c.schemas[schemaName]
		if !ok {
			schema = NewSchema(schemaName)
			c.schemas[schemaName] = schema
		}

		schema.mu.Lock()
		for _, tableData := range schemaData.Tables {
			tableDef := importTableDef(tableData)
			schema.tables[tableDef.Name] = tableDef
		}
		schema.mu.Unlock()
	}

	return nil
}

// exportTableDef converts a TableDef to a serializable TableJSON
func exportTableDef(t *TableDef) *persistence.TableJSON {
	data := &persistence.TableJSON{
		Name:       t.Name,
		Schema:     t.Schema,
		Columns:    make([]persistence.ColumnJSON, len(t.Columns)),
		PrimaryKey: make([]int, len(t.PrimaryKey)),
	}

	copy(data.PrimaryKey, t.PrimaryKey)

	for i, col := range t.Columns {
		data.Columns[i] = exportColumnDef(col)
	}

	return data
}

// importTableDef converts a TableJSON to a TableDef
func importTableDef(data *persistence.TableJSON) *TableDef {
	columns := make([]*ColumnDef, len(data.Columns))
	for i, colData := range data.Columns {
		columns[i] = importColumnDef(&colData)
	}

	t := NewTableDef(data.Name, columns)
	t.Schema = data.Schema
	t.PrimaryKey = make([]int, len(data.PrimaryKey))
	copy(t.PrimaryKey, data.PrimaryKey)

	return t
}

// exportColumnDef converts a ColumnDef to a serializable ColumnJSON
func exportColumnDef(c *ColumnDef) persistence.ColumnJSON {
	data := persistence.ColumnJSON{
		Name:         c.Name,
		Type:         int(c.Type),
		Nullable:     c.Nullable,
		HasDefault:   c.HasDefault,
		DefaultValue: c.DefaultValue,
	}

	// Export type info for complex types
	if c.TypeInfo != nil {
		data.TypeInfo = exportTypeInfo(c.TypeInfo)
	}

	return data
}

// importColumnDef converts a ColumnJSON to a ColumnDef
func importColumnDef(data *persistence.ColumnJSON) *ColumnDef {
	col := &ColumnDef{
		Name:         data.Name,
		Type:         dukdb.Type(data.Type),
		Nullable:     data.Nullable,
		HasDefault:   data.HasDefault,
		DefaultValue: data.DefaultValue,
	}

	// Import type info for complex types
	if data.TypeInfo != nil {
		col.TypeInfo = importTypeInfo(dukdb.Type(data.Type), data.TypeInfo)
	}

	return col
}

// exportTypeInfo converts a TypeInfo to a serializable TypeJSON
func exportTypeInfo(ti dukdb.TypeInfo) *persistence.TypeJSON {
	if ti == nil {
		return nil
	}

	data := &persistence.TypeJSON{}
	details := ti.Details()

	switch d := details.(type) {
	case *dukdb.DecimalDetails:
		data.Precision = int(d.Width)
		data.Scale = int(d.Scale)

	case *dukdb.EnumDetails:
		data.EnumValues = make([]string, len(d.Values))
		copy(data.EnumValues, d.Values)

	case *dukdb.ListDetails:
		if d.Child != nil {
			childCol := persistence.ColumnJSON{
				Type: int(d.Child.InternalType()),
			}
			if d.Child.Details() != nil {
				childCol.TypeInfo = exportTypeInfo(d.Child)
			}
			data.ElementType = &childCol
		}

	case *dukdb.ArrayDetails:
		data.ArraySize = int(d.Size)
		if d.Child != nil {
			childCol := persistence.ColumnJSON{
				Type: int(d.Child.InternalType()),
			}
			if d.Child.Details() != nil {
				childCol.TypeInfo = exportTypeInfo(d.Child)
			}
			data.ElementType = &childCol
		}

	case *dukdb.MapDetails:
		if d.Key != nil {
			keyCol := persistence.ColumnJSON{
				Type: int(d.Key.InternalType()),
			}
			if d.Key.Details() != nil {
				keyCol.TypeInfo = exportTypeInfo(d.Key)
			}
			data.KeyType = &keyCol
		}
		if d.Value != nil {
			valCol := persistence.ColumnJSON{
				Type: int(d.Value.InternalType()),
			}
			if d.Value.Details() != nil {
				valCol.TypeInfo = exportTypeInfo(d.Value)
			}
			data.ValueType = &valCol
		}

	case *dukdb.StructDetails:
		data.Fields = make([]persistence.ColumnJSON, len(d.Entries))
		for i, entry := range d.Entries {
			data.Fields[i] = persistence.ColumnJSON{
				Name: entry.Name(),
				Type: int(entry.Info().InternalType()),
			}
			if entry.Info().Details() != nil {
				data.Fields[i].TypeInfo = exportTypeInfo(entry.Info())
			}
		}
	}

	return data
}

// importTypeInfo converts a TypeJSON to a TypeInfo
func importTypeInfo(typ dukdb.Type, data *persistence.TypeJSON) dukdb.TypeInfo {
	if data == nil {
		// For primitive types, create basic type info
		info, _ := dukdb.NewTypeInfo(typ)
		return info
	}

	switch typ {
	case dukdb.TYPE_DECIMAL:
		info, _ := dukdb.NewDecimalInfo(uint8(data.Precision), uint8(data.Scale))
		return info

	case dukdb.TYPE_ENUM:
		if len(data.EnumValues) > 0 {
			info, _ := dukdb.NewEnumInfo(data.EnumValues[0], data.EnumValues[1:]...)
			return info
		}

	case dukdb.TYPE_LIST:
		if data.ElementType != nil {
			childInfo := importTypeInfo(dukdb.Type(data.ElementType.Type), data.ElementType.TypeInfo)
			if childInfo == nil {
				childInfo, _ = dukdb.NewTypeInfo(dukdb.Type(data.ElementType.Type))
			}
			if childInfo != nil {
				info, _ := dukdb.NewListInfo(childInfo)
				return info
			}
		}

	case dukdb.TYPE_ARRAY:
		if data.ElementType != nil {
			childInfo := importTypeInfo(dukdb.Type(data.ElementType.Type), data.ElementType.TypeInfo)
			if childInfo == nil {
				childInfo, _ = dukdb.NewTypeInfo(dukdb.Type(data.ElementType.Type))
			}
			if childInfo != nil {
				info, _ := dukdb.NewArrayInfo(childInfo, uint64(data.ArraySize))
				return info
			}
		}

	case dukdb.TYPE_MAP:
		if data.KeyType != nil && data.ValueType != nil {
			keyInfo := importTypeInfo(dukdb.Type(data.KeyType.Type), data.KeyType.TypeInfo)
			if keyInfo == nil {
				keyInfo, _ = dukdb.NewTypeInfo(dukdb.Type(data.KeyType.Type))
			}
			valInfo := importTypeInfo(dukdb.Type(data.ValueType.Type), data.ValueType.TypeInfo)
			if valInfo == nil {
				valInfo, _ = dukdb.NewTypeInfo(dukdb.Type(data.ValueType.Type))
			}
			if keyInfo != nil && valInfo != nil {
				info, _ := dukdb.NewMapInfo(keyInfo, valInfo)
				return info
			}
		}

	case dukdb.TYPE_STRUCT:
		if len(data.Fields) > 0 {
			entries := make([]dukdb.StructEntry, len(data.Fields))
			for i, field := range data.Fields {
				fieldInfo := importTypeInfo(dukdb.Type(field.Type), field.TypeInfo)
				if fieldInfo == nil {
					fieldInfo, _ = dukdb.NewTypeInfo(dukdb.Type(field.Type))
				}
				if fieldInfo != nil {
					entry, _ := dukdb.NewStructEntry(fieldInfo, field.Name)
					entries[i] = entry
				}
			}
			if len(entries) > 0 && entries[0] != nil {
				info, _ := dukdb.NewStructInfo(entries[0], entries[1:]...)
				return info
			}
		}
	}

	// Default: create basic type info for primitive types
	info, _ := dukdb.NewTypeInfo(typ)
	return info
}
