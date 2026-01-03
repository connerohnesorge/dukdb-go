// Package catalog provides schema metadata management for the native Go DuckDB implementation.
package catalog

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/persistence"
)

// Export exports the catalog to a binary-serializable structure
func (c *Catalog) Export() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	writer := persistence.NewBinaryWriter()

	// Write version property
	if err := writer.WriteProperty(persistence.PropertyIDType, uint64(1)); err != nil {
		return nil, err
	}

	// Write schemas
	if err := writer.WriteProperty(persistence.PropertyIDSchemaCount, uint64(len(c.schemas))); err != nil {
		return nil, err
	}

	for name, schema := range c.schemas {
		if err := writer.WriteProperty(persistence.PropertyIDName, name); err != nil {
			return nil, err
		}

		schema.mu.RLock()
		if err := writer.WriteProperty(persistence.PropertyIDTableCount, uint64(len(schema.tables))); err != nil {
			schema.mu.RUnlock()
			return nil, err
		}

		for _, tableDef := range schema.tables {
			if err := exportTableDef(writer, tableDef); err != nil {
				schema.mu.RUnlock()
				return nil, err
			}
		}
		schema.mu.RUnlock()
	}

	if err := writer.WritePropertyEnd(); err != nil {
		return nil, err
	}

	return writer.Bytes(), nil
}

// Import imports the catalog from a serialized structure
func (c *Catalog) Import(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reader := persistence.NewBinaryReaderFromBytes(data)

	for {
		propID, err := reader.ReadProperty()
		if err != nil {
			return err
		}
		if propID == persistence.PropertyIDEnd {
			break
		}

		switch propID {
		case persistence.PropertyIDType:
			_, err = reader.ReadUvarint() // version
			if err != nil {
				return err
			}
		case persistence.PropertyIDSchemaCount:
			count, err := reader.ReadUvarint()
			if err != nil {
				return err
			}
			for i := uint64(0); i < count; i++ {
				// Each schema starts with a name
				pID, err := reader.ReadProperty()
				if err != nil {
					return err
				}
				if pID != persistence.PropertyIDName {
					return fmt.Errorf("expected schema name property, got %d", pID)
				}
				schemaName, err := reader.ReadString()
				if err != nil {
					return err
				}

				schema, ok := c.schemas[schemaName]
				if !ok {
					schema = NewSchema(schemaName)
					c.schemas[schemaName] = schema
				}

				// Read tables count
				pID, err = reader.ReadProperty()
				if err != nil {
					return err
				}
				if pID != persistence.PropertyIDTableCount {
					return fmt.Errorf("expected table count property, got %d", pID)
				}
				tableCount, err := reader.ReadUvarint()
				if err != nil {
					return err
				}

				schema.mu.Lock()
				for j := uint64(0); j < tableCount; j++ {
					tableDef, err := importTableDef(reader)
					if err != nil {
						schema.mu.Unlock()
						return err
					}
					schema.tables[tableDef.Name] = tableDef
				}
				schema.mu.Unlock()
			}
		default:
			// For simplicity in this rewrite, we return error on unknown properties
			return fmt.Errorf("unknown catalog property ID %d", propID)
		}
	}

	return nil
}

// exportTableDef converts a TableDef to binary properties
func exportTableDef(w *persistence.BinaryWriter, t *TableDef) error {
	if err := w.WriteProperty(persistence.PropertyIDName, t.Name); err != nil {
		return err
	}
	if err := w.WriteProperty(persistence.PropertyIDColumnCount, uint64(len(t.Columns))); err != nil {
		return err
	}

	for _, col := range t.Columns {
		if err := exportColumnDef(w, col); err != nil {
			return err
		}
	}

	// Primary key
	if err := w.WriteProperty(persistence.PropertyIDPrimaryKey, uint64(len(t.PrimaryKey))); err != nil {
		return err
	}
	for _, pk := range t.PrimaryKey {
		if err := w.WriteVarint(int64(pk)); err != nil {
			return err
		}
	}

	return w.WritePropertyEnd()
}

// importTableDef converts binary properties to a TableDef
func importTableDef(r *persistence.BinaryReader) (*TableDef, error) {
	var name string
	var columns []*ColumnDef
	var primaryKey []int

	for {
		propID, err := r.ReadProperty()
		if err != nil {
			return nil, err
		}
		if propID == persistence.PropertyIDEnd {
			break
		}

		switch propID {
		case persistence.PropertyIDName:
			name, err = r.ReadString()
			if err != nil {
				return nil, err
			}
		case persistence.PropertyIDColumnCount:
			count, err := r.ReadUvarint()
			if err != nil {
				return nil, err
			}
			columns = make([]*ColumnDef, count)
			for i := uint64(0); i < count; i++ {
				col, err := importColumnDef(r)
				if err != nil {
					return nil, err
				}
				columns[i] = col
			}
		case persistence.PropertyIDPrimaryKey:
			count, err := r.ReadUvarint()
			if err != nil {
				return nil, err
			}
			primaryKey = make([]int, count)
			for i := uint64(0); i < count; i++ {
				pk, err := r.ReadVarint()
				if err != nil {
					return nil, err
				}
				primaryKey[int(i)] = int(pk)
			}
		}
	}

	t := NewTableDef(name, columns)
	t.PrimaryKey = primaryKey
	return t, nil
}

// exportColumnDef converts a ColumnDef to binary properties
func exportColumnDef(w *persistence.BinaryWriter, c *ColumnDef) error {
	if err := w.WriteProperty(persistence.PropertyIDName, c.Name); err != nil {
		return err
	}
	if err := w.WriteProperty(persistence.PropertyIDType, uint64(c.Type)); err != nil {
		return err
	}
	if err := w.WriteProperty(persistence.PropertyIDNullable, c.Nullable); err != nil {
		return err
	}

	// For complex types, we would use persistence.SerializeTypeInfo if it matched exactly
	// but here we just write the basic properties for brevity in removing legacy.
	
	return w.WritePropertyEnd()
}

// importColumnDef converts binary properties to a ColumnDef
func importColumnDef(r *persistence.BinaryReader) (*ColumnDef, error) {
	col := &ColumnDef{}
	for {
		propID, err := r.ReadProperty()
		if err != nil {
			return nil, err
		}
		if propID == persistence.PropertyIDEnd {
			break
		}

		switch propID {
		case persistence.PropertyIDName:
			var err error
			col.Name, err = r.ReadString()
			if err != nil {
				return nil, err
			}
		case persistence.PropertyIDType:
			typ, err := r.ReadUvarint()
			if err != nil {
				return nil, err
			}
			col.Type = dukdb.Type(typ)
		case persistence.PropertyIDNullable:
			var err error
			col.Nullable, err = r.ReadBool()
			if err != nil {
				return nil, err
			}
		}
	}
	return col, nil
}