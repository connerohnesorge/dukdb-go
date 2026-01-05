package duckdb

// This file implements serialization for catalog entry types (Schema, Table, View, etc.).
// These are the top-level catalog entries stored in DuckDB metadata blocks.
//
// Serialization Format:
// Each catalog entry is serialized with the following structure:
//   1. Catalog type byte (CatalogType)
//   2. CreateInfo fields (common to all entries)
//   3. Entry-specific fields with property IDs for forward/backward compatibility
//   4. PropEnd marker (0) to indicate end of entry
//
// The property-based serialization allows newer versions to skip unknown properties
// and older versions to ignore new properties, enabling format evolution.

// Serialize writes a SchemaCatalogEntry to the binary writer.
// Schema entries represent database namespaces that contain tables, views, etc.
// Format: CatalogType + CreateInfo + PropSchemaName + name + PropEnd
func (s *SchemaCatalogEntry) Serialize(w *BinaryWriter) error {
	// Write the catalog type identifier
	w.WriteUint8(uint8(CatalogSchemaEntry))
	// Write common CreateInfo fields (catalog, schema, temp, internal, etc.)
	if err := s.CreateInfo.Serialize(w); err != nil {
		return err
	}
	// Write schema name with property ID
	w.WritePropertyID(PropSchemaName)
	w.WriteString(s.Name)
	// Write end marker
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// Serialize writes a TableCatalogEntry to the binary writer.
// Table entries contain column definitions and constraints.
// Row group data is stored separately via MetadataManager.
// Format: CatalogType + CreateInfo + name + columns + constraints + PropEnd
func (t *TableCatalogEntry) Serialize(w *BinaryWriter) error {
	// Write the catalog type identifier
	w.WriteUint8(uint8(CatalogTableEntry))
	// Write common CreateInfo fields
	if err := t.CreateInfo.Serialize(w); err != nil {
		return err
	}
	// Write table name
	w.WritePropertyID(PropTableName)
	w.WriteString(t.Name)
	// Write column definitions
	w.WritePropertyID(PropTableColumns)
	w.WriteUint32(uint32(len(t.Columns)))
	for i := range t.Columns {
		if err := t.Columns[i].Serialize(w); err != nil {
			return err
		}
	}
	// Write table constraints (PK, FK, UNIQUE, CHECK, NOT NULL)
	w.WritePropertyID(PropTableConstraints)
	w.WriteUint32(uint32(len(t.Constraints)))
	for i := range t.Constraints {
		if err := t.Constraints[i].Serialize(w); err != nil {
			return err
		}
	}
	// Write end marker
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// Serialize writes a ViewCatalogEntry to the binary writer.
// View entries store the query definition and column metadata.
// Format: CatalogType + CreateInfo + name + query + aliases + types + type_mods + PropEnd
func (v *ViewCatalogEntry) Serialize(w *BinaryWriter) error {
	// Write the catalog type identifier
	w.WriteUint8(uint8(CatalogViewEntry))
	// Write common CreateInfo fields
	if err := v.CreateInfo.Serialize(w); err != nil {
		return err
	}
	// Write view name (reusing table name property for consistency)
	w.WritePropertyID(PropTableName)
	w.WriteString(v.Name)
	// Write the SELECT query that defines the view
	w.WritePropertyID(PropViewQuery)
	w.WriteString(v.Query)
	// Write column aliases (optional, may use query column names)
	w.WritePropertyID(PropViewAliases)
	w.WriteUint32(uint32(len(v.Aliases)))
	for _, alias := range v.Aliases {
		w.WriteString(alias)
	}
	// Write column types
	w.WritePropertyID(PropViewTypes)
	w.WriteUint32(uint32(len(v.Types)))
	for _, tp := range v.Types {
		w.WriteUint8(uint8(tp))
	}
	// Write type modifiers for complex types
	w.WritePropertyID(PropViewTypeMods)
	w.WriteUint32(uint32(len(v.TypeModifiers)))
	for i := range v.TypeModifiers {
		if err := v.TypeModifiers[i].Serialize(w); err != nil {
			return err
		}
	}
	// Write end marker
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// Serialize writes an IndexCatalogEntry to the binary writer.
// Index entries store metadata about table indexes (ART, Hash).
// Format: CatalogType + CreateInfo + name + table + type + constraint + cols + exprs + PropEnd
func (i *IndexCatalogEntry) Serialize(w *BinaryWriter) error {
	// Write the catalog type identifier
	w.WriteUint8(uint8(CatalogIndexEntry))
	// Write common CreateInfo fields
	if err := i.CreateInfo.Serialize(w); err != nil {
		return err
	}
	// Write index name
	w.WritePropertyID(PropIndexName)
	w.WriteString(i.Name)
	// Write the table this index is on
	w.WritePropertyID(PropIndexTable)
	w.WriteString(i.TableName)
	// Write index type (ART, Hash)
	w.WritePropertyID(PropIndexType)
	w.WriteUint8(uint8(i.IndexType))
	// Write constraint type (None, Unique, Primary, Foreign)
	w.WritePropertyID(PropIndexConstraint)
	w.WriteUint8(uint8(i.Constraint))
	// Write column IDs that form the index key
	w.WritePropertyID(PropIndexColumnIDs)
	w.WriteUint32(uint32(len(i.ColumnIDs)))
	for _, id := range i.ColumnIDs {
		w.WriteUint64(id)
	}
	// Write expressions for expression indexes (e.g., lower(column))
	w.WritePropertyID(PropIndexExprs)
	w.WriteUint32(uint32(len(i.Expressions)))
	for _, expr := range i.Expressions {
		w.WriteString(expr)
	}
	// Write end marker
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// Serialize writes a SequenceCatalogEntry to the binary writer.
// Sequence entries store auto-increment generator state.
// Format: CatalogType + CreateInfo + name + usage + start + inc + min + max + cycle + counter + PropEnd
func (s *SequenceCatalogEntry) Serialize(w *BinaryWriter) error {
	// Write the catalog type identifier
	w.WriteUint8(uint8(CatalogSequenceEntry))
	// Write common CreateInfo fields
	if err := s.CreateInfo.Serialize(w); err != nil {
		return err
	}
	// Write sequence name
	w.WritePropertyID(PropSeqName)
	w.WriteString(s.Name)
	// Write usage type (None, Owned by column)
	w.WritePropertyID(PropSeqUsage)
	w.WriteUint8(uint8(s.Usage))
	// Write sequence parameters
	w.WritePropertyID(PropSeqStartWith)
	w.WriteInt64(s.StartWith)
	w.WritePropertyID(PropSeqIncrement)
	w.WriteInt64(s.Increment)
	w.WritePropertyID(PropSeqMinValue)
	w.WriteInt64(s.MinValue)
	w.WritePropertyID(PropSeqMaxValue)
	w.WriteInt64(s.MaxValue)
	w.WritePropertyID(PropSeqCycle)
	w.WriteBool(s.Cycle)
	// Write current counter value (persisted state)
	w.WritePropertyID(PropSeqCounter)
	w.WriteInt64(s.Counter)
	// Write end marker
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// Serialize writes a TypeCatalogEntry to the binary writer.
// Type entries represent custom types, including enums.
// Format: CatalogType + CreateInfo + name + typeID + modifiers + PropEnd
func (t *TypeCatalogEntry) Serialize(w *BinaryWriter) error {
	// Write the catalog type identifier
	w.WriteUint8(uint8(CatalogTypeEntry))
	// Write common CreateInfo fields
	if err := t.CreateInfo.Serialize(w); err != nil {
		return err
	}
	// Write type name
	w.WritePropertyID(PropTypeName)
	w.WriteString(t.Name)
	// Write underlying logical type ID
	w.WritePropertyID(PropTypeID)
	w.WriteUint8(uint8(t.TypeID))
	// Write type modifiers (enum values, struct fields, etc.)
	w.WritePropertyID(PropTypeModifier)
	if err := t.TypeModifiers.Serialize(w); err != nil {
		return err
	}
	// Write end marker
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// SerializeCatalogEntry serializes any CatalogEntry implementation.
// This is a helper function that dispatches to the appropriate Serialize method
// based on the entry type. It provides a unified interface for serializing
// heterogeneous catalog entries.
//
// Returns an error if serialization fails or if the entry type is unknown.
// For unknown entry types, CatalogInvalid is written as the type byte.
func SerializeCatalogEntry(w *BinaryWriter, entry CatalogEntry) error {
	switch e := entry.(type) {
	case *SchemaCatalogEntry:
		return e.Serialize(w)
	case *TableCatalogEntry:
		return e.Serialize(w)
	case *ViewCatalogEntry:
		return e.Serialize(w)
	case *IndexCatalogEntry:
		return e.Serialize(w)
	case *SequenceCatalogEntry:
		return e.Serialize(w)
	case *TypeCatalogEntry:
		return e.Serialize(w)
	default:
		// Unknown entry type - write invalid marker
		w.WriteUint8(uint8(CatalogInvalid))

		return w.Err()
	}
}
