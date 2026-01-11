// Package duckdb implements DuckDB-compatible storage format utilities.
package duckdb

// SerializeCreateSchemaInfo serializes a SchemaCatalogEntry using BinarySerializer.
// This produces DuckDB-compatible binary format for schema entries.
//
// Format (matches DuckDB's CreateInfo::Serialize):
//   Property 100: type (CatalogType enum as varint, NOT a string!)
//   Property 101: catalog name (string)
//   Property 102: schema name (string)
//   Property 103: temporary (bool)
//   Property 104: internal (bool)
//   Property 105: on_conflict (OnCreateConflict enum)
//   Property 106: sql (string, optional)
//   Property 107: comment (Value, optional)
//   Property 108: tags (map<string,string>, optional)
//   MESSAGE_TERMINATOR (0xFFFF)
func SerializeCreateSchemaInfo(s *BinarySerializer, schema *SchemaCatalogEntry) {
	// Property 100: type (CatalogType enum, NOT a string!)
	s.WriteProperty(PropCreateType, "type", uint8(CatalogSchemaEntry))

	// Property 101: catalog name (WritePropertyWithDefault - skip if empty)
	// Native DuckDB does NOT write this field for schema entries when empty
	if schema.Catalog != "" {
		s.WriteProperty(PropCreateCatalog, "catalog", schema.Catalog)
	}

	// Property 102: schema name (WritePropertyWithDefault - skip if empty)
	// Native DuckDB also uses WritePropertyWithDefault here
	if schema.Name != "" {
		s.WriteProperty(PropCreateSchema, "schema", schema.Name)
	}

	// Property 103: temporary (WritePropertyWithDefault - only write if true)
	// Don't write if false (default)

	// Property 104: internal (WritePropertyWithDefault - only write if true)
	// Don't write if false (default)

	// Property 105: on_conflict
	s.WriteProperty(PropCreateOnConflict, "on_conflict", uint8(schema.OnConflict))

	// Property 106: sql (WritePropertyWithDefault - skip if empty)
	// Not used for schema entries

	// Property 107: comment (WritePropertyWithDefault - skip if empty)
	if schema.Comment != "" {
		s.WriteProperty(PropCreateComment, "comment", schema.Comment)
	}

	// Property 108: tags (WritePropertyWithDefault - skip if empty)
	if len(schema.Tags) > 0 {
		s.OnPropertyBegin(PropCreateTags, "tags")
		s.OnListBegin(uint64(len(schema.Tags)))
		for key, val := range schema.Tags {
			s.OnObjectBegin()
			s.WriteString(key)
			s.WriteString(val)
			s.OnObjectEnd()
		}
	}

	// Object terminator
	s.OnObjectEnd()
}

// SerializeCreateInfoBase serializes the base CreateInfo fields common to all catalog entries.
// This is a helper function for serializing the common properties shared across different
// catalog entry types (schema, table, view, index, sequence, type).
//
// entryType is the type string like "schema", "table", "view", etc.
// entryName is the name of the catalog entry (e.g., schema name, table name).
//
// Format:
//   Property 100: type (entry type string)
//   Property 101: catalog name (string)
//   Property 102: schema name (string)
//   Property 103: on_conflict (OnCreateConflict enum as uint8)
//   Property 104: comment (string, optional - skip if empty)
//   Property 105: tags (map<string,string>, optional - skip if empty)
func SerializeCreateInfoBase(s *BinarySerializer, info *CreateInfo, entryType, entryName string) {
	// Property 100: type
	s.WriteProperty(PropCreateType, "type", entryType)

	// Property 101: catalog name
	s.WriteProperty(PropCreateCatalog, "catalog", info.Catalog)

	// Property 102: schema name
	// Note: For schema entries specifically, this is the NAME of the schema being created.
	// For other entry types (table, view, index), this is the parent schema name.
	// The caller should pass the appropriate value based on the entry type.
	s.WriteProperty(PropCreateSchema, "schema", info.Schema)

	// Property 103: on_conflict
	s.WriteProperty(PropCreateOnConflict, "on_conflict", uint8(info.OnConflict))

	// Property 104: comment (optional)
	if info.Comment != "" {
		s.WriteProperty(PropCreateComment, "comment", info.Comment)
	}

	// Property 105: tags (optional)
	if len(info.Tags) > 0 {
		s.OnPropertyBegin(PropCreateTags, "tags")
		s.OnListBegin(uint64(len(info.Tags)))
		for key, val := range info.Tags {
			s.OnObjectBegin()
			s.WriteString(key)
			s.WriteString(val)
			s.OnObjectEnd()
		}
	}
}

// SerializeLogicalType serializes a LogicalTypeID and its modifiers using BinarySerializer.
// This produces DuckDB-compatible binary format for column types.
//
// Format:
//   Property 100: type_id (LogicalTypeId enum as uint8)
//   Property 101: type_info (ExtraTypeInfo, optional, nullable pointer)
//
// Note: physical_type is NOT serialized - it's derived from type_id at runtime.
// IMPORTANT: LogicalType does NOT have its own terminator when serialized as a nested
// object (e.g., inside ColumnDefinition). Only top-level objects or list elements have terminators.
func SerializeLogicalType(s *BinarySerializer, typeID LogicalTypeID, modifiers *TypeModifiers) {
	// Property 100: type_id (LogicalTypeId enum as uint8)
	s.WriteProperty(PropLogicalTypeID, "type_id", uint8(typeID))

	// Property 101: type_info - optional, for types that need extra info
	// Only write if we have meaningful type info to serialize
	needsTypeInfo := modifiers != nil && (
		(typeID == TypeDecimal && (modifiers.Width != 0 || modifiers.Scale != 0)) ||
		((typeID == TypeVarchar || typeID == TypeChar) && modifiers.Collation != "") ||
		(typeID == TypeList && modifiers.ChildType != nil) ||
		(typeID == TypeStruct && len(modifiers.StructFields) > 0))

	if needsTypeInfo {
		s.OnPropertyBegin(PropLogicalTypeInfo, "type_info")
		s.WriteBool(true) // Nullable: present

		// Write the ExtraTypeInfo object - this DOES need a terminator because it's
		// a nullable pointer (unique_ptr) which has its own object scope
		switch {
		case typeID == TypeDecimal:
			// DecimalTypeInfo
			s.WriteProperty(100, "type", uint8(2)) // ExtraTypeInfoDecimal = 2
			s.WriteProperty(200, "width", modifiers.Width)
			s.WriteProperty(201, "scale", modifiers.Scale)
			s.OnObjectEnd()

		case (typeID == TypeVarchar || typeID == TypeChar) && modifiers.Collation != "":
			// StringTypeInfo
			s.WriteProperty(100, "type", uint8(3)) // ExtraTypeInfoString = 3
			s.WriteProperty(200, "collation", modifiers.Collation)
			s.OnObjectEnd()

		case typeID == TypeList && modifiers.ChildType != nil:
			// ListTypeInfo
			s.WriteProperty(100, "type", uint8(4)) // ExtraTypeInfoList = 4
			s.OnPropertyBegin(200, "child_type")
			SerializeLogicalType(s, modifiers.ChildTypeID, modifiers.ChildType)
			s.OnObjectEnd()

		case typeID == TypeStruct && len(modifiers.StructFields) > 0:
			// StructTypeInfo
			s.WriteProperty(100, "type", uint8(5)) // ExtraTypeInfoStruct = 5
			s.OnPropertyBegin(200, "child_types")
			s.OnListBegin(uint64(len(modifiers.StructFields)))
			for _, field := range modifiers.StructFields {
				s.OnObjectBegin()
				s.WriteProperty(0, "first", field.Name) // pair.first = name
				s.OnPropertyBegin(1, "second")          // pair.second = LogicalType
				SerializeLogicalType(s, field.Type, field.TypeModifiers)
				s.OnObjectEnd() // Close pair
			}
			s.OnObjectEnd()
		}
	}

	// LogicalType DOES need a terminator when serialized as a property.
	// DuckDB's BinarySerializer writes a terminator after every object, including nested ones.
	s.OnObjectEnd()
}

// SerializeColumnDefinition serializes a ColumnDefinition using BinarySerializer.
// This produces DuckDB-compatible binary format for column definitions.
//
// Format:
//   Property 100: name (string)
//   Property 101: type (LogicalType - nested object)
//   Property 102: expression (optional, for generated columns)
//   Property 103: category (ColumnCategory enum)
//   Property 104: compression_type (optional)
//   Property 105: comment (string, optional)
//   Property 106: tags (map<string,string>, optional)
//   MESSAGE_TERMINATOR (0xFFFF)
func SerializeColumnDefinition(s *BinarySerializer, col *ColumnDefinition) {
	// Property 100: column name
	s.WriteProperty(PropColumnDefName, "name", col.Name)

	// Property 101: type (nested LogicalType object)
	// Note: SerializeLogicalType writes its own terminator, so we don't use WriteObject
	s.OnPropertyBegin(PropColumnDefType, "type")
	SerializeLogicalType(s, col.Type, &col.TypeModifiers)

	// Property 103: category
	category := ColumnCategoryStandard
	if col.Generated {
		category = ColumnCategoryGenerated
	}
	s.WriteProperty(PropColumnDefCategory, "category", category)

	// Property 102: expression (optional, for generated columns)
	if col.Generated && col.GeneratedExpression != "" {
		s.WriteProperty(PropColumnDefExpression, "expression", col.GeneratedExpression)
	}

	// Property 104: compression_type (REQUIRED - DuckDB uses WriteProperty, not WritePropertyWithDefault)
	s.WriteProperty(PropColumnDefCompression, "compression_type", uint8(col.CompressionType))

	// Property 105: comment (optional)
	// Note: ColumnDefinition doesn't have Comment field currently
	// Skip for now - can be added later if needed

	// Property 106: tags (optional)
	// Note: ColumnDefinition doesn't have Tags field currently
	// Skip for now - can be added later if needed

	// Object terminator
	s.OnObjectEnd()
}

// SerializeCreateTableInfo serializes a TableCatalogEntry using BinarySerializer.
// This produces DuckDB-compatible binary format for table entries.
//
// Format (matches DuckDB's CreateInfo::Serialize + CreateTableInfo::Serialize):
//   Property 100: type (CatalogType enum = 1 for TABLE_ENTRY)
//   Property 101: catalog name (string)
//   Property 102: schema name (string)
//   Property 103: temporary (bool)
//   Property 104: internal (bool)
//   Property 105: on_conflict (OnCreateConflict enum)
//   Property 106: sql (string, optional)
//   Property 107: comment (Value, optional)
//   Property 108: tags (map, optional)
//   Property 200: table name (string)
//   Property 201: columns (ColumnList object)
//   Property 202: constraints (list<Constraint>)
//   Property 203: query (optional, for CREATE TABLE AS)
//   MESSAGE_TERMINATOR (0xFFFF)
func SerializeCreateTableInfo(s *BinarySerializer, table *TableCatalogEntry) {
	// Property 100: type (CatalogType enum, NOT a string!)
	s.WriteProperty(PropCreateType, "type", uint8(CatalogTableEntry))

	// Property 101: catalog name (REQUIRED for tables - DuckDB always writes this)
	// Native DuckDB CLI writes "cli" as the catalog name when using the CLI.
	// Using the same 3-character name ensures byte alignment matches native DuckDB files.
	catalogName := table.Catalog
	if catalogName == "" {
		catalogName = "cli" // Match native DuckDB CLI catalog name
	}
	s.WriteProperty(PropCreateCatalog, "catalog", catalogName)

	// Property 102: schema name (REQUIRED for tables - default to "main")
	schemaName := table.Schema
	if schemaName == "" {
		schemaName = "main"
	}
	s.WriteProperty(PropCreateSchema, "schema", schemaName)

	// Property 103: temporary (WritePropertyWithDefault - only write if true)
	if table.Temporary {
		s.WriteProperty(PropCreateTemporary, "temporary", table.Temporary)
	}

	// Property 104: internal (WritePropertyWithDefault - only write if true)
	// Don't write if false (default)

	// Property 105: on_conflict
	s.WriteProperty(PropCreateOnConflict, "on_conflict", uint8(table.OnConflict))

	// Property 106: sql (WritePropertyWithDefault - skip if empty)
	// Not used for table entries

	// Property 107: comment (WritePropertyWithDefault - skip if empty)
	if table.Comment != "" {
		s.WriteProperty(PropCreateComment, "comment", table.Comment)
	}

	// Property 108: tags (WritePropertyWithDefault - skip if empty)
	if len(table.Tags) > 0 {
		s.OnPropertyBegin(PropCreateTags, "tags")
		s.OnListBegin(uint64(len(table.Tags)))
		for key, val := range table.Tags {
			s.OnObjectBegin()
			s.WriteString(key)
			s.WriteString(val)
			s.OnObjectEnd()
		}
	}

	// Property 200: table name (WritePropertyWithDefault - only write if non-empty)
	if table.Name != "" {
		s.WriteProperty(PropTableInfoName, "table", table.Name)
	}

	// Property 201: columns (ColumnList object)
	s.OnPropertyBegin(PropTableInfoColumns, "columns")
	s.OnObjectBegin() // Begin ColumnList object
	// Field 100 within ColumnList: physical_columns vector
	s.OnPropertyBegin(100, "physical_columns")
	s.OnListBegin(uint64(len(table.Columns)))
	for i := range table.Columns {
		// SerializeColumnDefinition handles its own object markers and terminator
		SerializeColumnDefinition(s, &table.Columns[i])
	}
	// Lists don't have terminators - just count + elements
	// ColumnList object terminator ends both the list and the ColumnList
	s.OnObjectEnd() // End ColumnList object

	// Property 202: constraints (list) - WritePropertyWithDefault, skip if empty
	// Native DuckDB uses WritePropertyWithDefault which skips empty lists
	if len(table.Constraints) > 0 {
		s.OnPropertyBegin(PropTableInfoConstraints, "constraints")
		s.OnListBegin(uint64(len(table.Constraints)))
		// Serialize each constraint
		for i := range table.Constraints {
			SerializeConstraint(s, &table.Constraints[i])
		}
	}

	// Property 203: query (optional, for CREATE TABLE AS)
	// Not implemented for now as most tables are DDL-created

	// NOTE: table_statistics (property 8) is NOT written here.
	// DuckDB does not expect this field in the catalog serialization for CreateTableInfo.
	// The error "expected end of object, but found field id: 8" confirms this.

	// Object terminator for CreateTableInfo
	// This ends property 100's object data. Properties 101, 102, 103 are
	// SIBLING properties at the entry level, written by serializeTableDataPointer().
	s.OnObjectEnd()
}

// SerializeConstraint serializes a Constraint using BinarySerializer.
// This produces DuckDB-compatible binary format for table constraints.
//
// Format:
//   Property 100: type (ConstraintType enum)
//   Property 101: name (string, optional)
//   Property 102: column indices (list<uint64>)
//   Property 103: expression (string, for CHECK constraints, optional)
//   Property 104: foreign key info (for FK constraints, optional)
//   MESSAGE_TERMINATOR (0xFFFF)
func SerializeConstraint(s *BinarySerializer, c *Constraint) {
	// Property 100: type
	s.WriteProperty(100, "type", uint8(c.Type))

	// Property 101: name (optional)
	if c.Name != "" {
		s.WriteProperty(101, "name", c.Name)
	}

	// Property 102: column indices (list)
	s.OnPropertyBegin(102, "columns")
	s.OnListBegin(uint64(len(c.ColumnIndices)))
	for _, idx := range c.ColumnIndices {
		s.WriteUint64(idx)
	}

	// Property 103: expression (for CHECK constraints, optional)
	if c.Type == ConstraintTypeCheck && c.Expression != "" {
		s.WriteProperty(103, "expression", c.Expression)
	}

	// Property 104: foreign key info (for FK constraints, optional)
	if c.Type == ConstraintTypeForeignKey && c.ForeignKey != nil {
		s.WriteObject(104, "foreign_key", func() {
			// Property 100: referenced schema
			s.WriteProperty(100, "schema", c.ForeignKey.ReferencedSchema)

			// Property 101: referenced table
			s.WriteProperty(101, "table", c.ForeignKey.ReferencedTable)

			// Property 102: referenced columns (list)
			s.OnPropertyBegin(102, "columns")
			s.OnListBegin(uint64(len(c.ForeignKey.ReferencedColumns)))
			for _, col := range c.ForeignKey.ReferencedColumns {
				s.WriteString(col)
			}

			// Property 103: on_delete action
			s.WriteProperty(103, "on_delete", uint8(c.ForeignKey.OnDelete))

			// Property 104: on_update action
			s.WriteProperty(104, "on_update", uint8(c.ForeignKey.OnUpdate))
		})
	}

	// Object terminator
	s.OnObjectEnd()
}

// SerializeCreateViewInfo serializes a ViewCatalogEntry using BinarySerializer.
// This produces DuckDB-compatible binary format for view entries.
//
// Format:
//   Property 100: type = "view"
//   Property 101: catalog name (string)
//   Property 102: schema name (string)
//   Property 103: on_conflict (OnCreateConflict enum)
//   Property 104: view name (string)
//   Property 200: query (string)
//   MESSAGE_TERMINATOR (0xFFFF)
func SerializeCreateViewInfo(s *BinarySerializer, view *ViewCatalogEntry) {
	// Property 100: type = "view"
	s.WriteProperty(PropCreateType, "type", "view")

	// Property 101: catalog name
	s.WriteProperty(PropCreateCatalog, "catalog", view.Catalog)

	// Property 102: schema name
	s.WriteProperty(PropCreateSchema, "schema", view.Schema)

	// Property 103: on_conflict
	s.WriteProperty(PropCreateOnConflict, "on_conflict", uint8(view.OnConflict))

	// Property 104: view name
	s.WriteProperty(104, "view_name", view.Name)

	// Property 200: query
	s.WriteProperty(200, "query", view.Query)

	// Object terminator
	s.OnObjectEnd()
}

// SerializeCreateIndexInfo serializes an IndexCatalogEntry using BinarySerializer (stub).
// This is a placeholder for future implementation.
func SerializeCreateIndexInfo(s *BinarySerializer, index *IndexCatalogEntry) {
	// Stub implementation - write minimal data
	s.WriteProperty(PropCreateType, "type", "index")
	s.WriteProperty(PropCreateCatalog, "catalog", index.Catalog)
	s.WriteProperty(PropCreateSchema, "schema", index.Schema)
	s.WriteProperty(104, "index_name", index.Name)
	s.OnObjectEnd()
}

// SerializeCreateSequenceInfo serializes a SequenceCatalogEntry using BinarySerializer (stub).
// This is a placeholder for future implementation.
func SerializeCreateSequenceInfo(s *BinarySerializer, seq *SequenceCatalogEntry) {
	// Stub implementation - write minimal data
	s.WriteProperty(PropCreateType, "type", "sequence")
	s.WriteProperty(PropCreateCatalog, "catalog", seq.Catalog)
	s.WriteProperty(PropCreateSchema, "schema", seq.Schema)
	s.WriteProperty(104, "sequence_name", seq.Name)
	s.OnObjectEnd()
}

// SerializeCreateTypeInfo serializes a TypeCatalogEntry using BinarySerializer (stub).
// This is a placeholder for future implementation.
func SerializeCreateTypeInfo(s *BinarySerializer, typ *TypeCatalogEntry) {
	// Stub implementation - write minimal data
	s.WriteProperty(PropCreateType, "type", "type")
	s.WriteProperty(PropCreateCatalog, "catalog", typ.Catalog)
	s.WriteProperty(PropCreateSchema, "schema", typ.Schema)
	s.WriteProperty(104, "type_name", typ.Name)
	s.OnObjectEnd()
}

// TableStorageInfo contains the storage metadata for a table entry.
// This is used when serializing table entries to write the correct
// table_pointer and total_rows fields.
type TableStorageInfo struct {
	// TablePointer points to the table storage metadata block.
	// For empty tables (no row groups), use InvalidBlockID.
	TablePointer MetaBlockPointer

	// TotalRows is the sum of all row group tuple counts.
	// For empty tables, use 0.
	TotalRows uint64
}

// SerializeCatalogEntryBinary serializes a single catalog entry using BinarySerializer.
// This is the top-level serialization that wraps entry-specific data with property 99 (catalog_type)
// and property 100 (entry data).
//
// Format (matches DuckDB's CheckpointWriter::WriteEntry + Write*):
//   Property 99: catalog_type (CatalogType enum as uint8)
//   Property 100: [nullable bool] + entry-specific data (CreateInfo fields + terminator)
//   For TABLE entries only:
//     Property 101: table_pointer (MetaBlockPointer object)
//     Property 102: total_rows (uint64)
//     Property 103: index_pointers (empty list for forward compatibility)
//
// The entry data (property 100) contains the CreateInfo serialization specific to each
// catalog entry type (schema, table, view, index, sequence, type).
//
// For TABLE entries, tableStorageInfo parameter should contain the storage metadata with
// the correct BlockIndex for this table's storage location. For multi-table databases,
// each table gets its own sub-blocks:
// - Table 0: BlockIndex=1 (sub-blocks 1-3 for 3-column table)
// - Table 1: BlockIndex=4 (sub-blocks 4-6 for 3-column table)
//
// For other entry types, tableStorageInfo can be nil.
func SerializeCatalogEntryBinary(s *BinarySerializer, entry CatalogEntry, tableStorageInfo *TableStorageInfo) {
	// Property 99: catalog_type
	s.WriteProperty(PropCatalogType, "catalog_type", uint8(entry.Type()))

	// Property 100: entry data (nullable pointer format)
	// In DuckDB's WriteProperty for pointers:
	// 1. Write field ID
	// 2. Write nullable bool (true = present)
	// 3. Serialize the object directly (its Serialize() writes fields + terminator)
	s.OnPropertyBegin(PropCatalogEntryData, "entry")
	s.WriteBool(true) // Entry is present (not null)

	// Serialize the entry-specific data
	// NOTE: Do NOT call OnObjectBegin() here! The entry serializers write
	// their fields directly and end with OnObjectEnd() which writes the terminator.
	switch e := entry.(type) {
	case *SchemaCatalogEntry:
		SerializeCreateSchemaInfo(s, e)

	case *TableCatalogEntry:
		SerializeCreateTableInfo(s, e)
		// For TABLE entries, DuckDB expects additional fields after CreateTableInfo:
		// Property 101: table_pointer (MetaBlockPointer)
		// Property 102: total_rows (uint64)
		// Property 103: index_pointers (empty list for forward compatibility)
		// These are written by TableDataWriter::FinalizeTable in DuckDB.
		//
		// Use the provided storage info which should have the correct BlockIndex
		// for this table's storage location.
		// Offset=8 to skip the 8-byte next_ptr header at the start of each sub-block
		tablePointer := MetaBlockPointer{BlockID: 0, BlockIndex: 1, Offset: 8}
		totalRows := uint64(0)
		if tableStorageInfo != nil {
			tablePointer = tableStorageInfo.TablePointer
			totalRows = tableStorageInfo.TotalRows
		}
		serializeTableDataPointer(s, tablePointer, totalRows)

	case *ViewCatalogEntry:
		SerializeCreateViewInfo(s, e)

	case *IndexCatalogEntry:
		SerializeCreateIndexInfo(s, e)

	case *SequenceCatalogEntry:
		SerializeCreateSequenceInfo(s, e)

	case *TypeCatalogEntry:
		SerializeCreateTypeInfo(s, e)
	}

	// Note: Entry serializers already call OnObjectEnd() which writes the terminator
}

// serializeTableDataPointer writes the table data pointer fields (101-103) for a table entry.
// These fields are written after CreateTableInfo by DuckDB's TableDataWriter::FinalizeTable.
//
// Format:
//   Property 101: table_pointer (MetaBlockPointer object)
//     - Property 100: block_pointer (uint64, default: 0, but INVALID_INDEX is not default!)
//     - Property 101: offset (uint32, default: 0)
//     - MESSAGE_TERMINATOR (0xFFFF)
//   Property 102: total_rows (uint64)
//   Property 103: index_pointers (empty list of BlockPointer for forward compatibility)
//
// Parameters:
//   - tablePointer: The MetaBlockPointer to the table storage metadata block.
//     For empty tables (no row groups), use InvalidBlockID.
//   - totalRows: The total number of rows in the table (sum of all row group tuple counts).
//     For empty tables, use 0.
func serializeTableDataPointer(s *BinarySerializer, tablePointer MetaBlockPointer, totalRows uint64) {
	// Property 101: table_pointer (MetaBlockPointer)
	// Native DuckDB uses 0x100000000000000 (2^56) as the block_pointer for empty tables.
	// This encodes to block_id=0, block_index=1, pointing to table storage in sub-block 1.
	// For multi-table databases, ALL tables share the same table_pointer (InvalidIndex).
	// DuckDB always tries to load from this pointer, so valid data must exist there.
	s.OnPropertyBegin(101, "table_pointer")

	// Native DuckDB uses 0x100000000000000 (2^56) as INVALID_INDEX for MetaBlockPointer
	// This encodes as: block_id = 0, block_index = 1 (since 2^56 >> 56 = 1)
	// So it points to block 0, sub-block 1 where the table storage metadata lives.
	// Encode the MetaBlockPointer with BlockIndex in the high byte
	// DuckDB encodes: BlockID (bits 0-55) + BlockIndex (bits 56-63)
	blockPointer := tablePointer.BlockID | (uint64(tablePointer.BlockIndex) << 56)
	if tablePointer.BlockID == InvalidBlockID {
		// For empty tables (no row groups), use the standard InvalidIndex
		// which points to sub-block 1 of block 0 where empty table storage lives
		blockPointer = 0x100000000000000 // InvalidIndex: block 0, sub-block 1
	}

	s.WriteProperty(100, "block_pointer", blockPointer)
	// Offset is relative to the start of the sub-block.
	// DuckDB uses offset=8 to skip the 8-byte next_ptr header at the start of each sub-block,
	// so the TableStatistics data starts at offset 8 within the sub-block.
	s.WriteProperty(101, "offset", uint32(tablePointer.Offset))
	s.OnObjectEnd() // MetaBlockPointer terminator

	// Property 102: total_rows (uint64)
	// For empty tables, write 0 rows
	// DuckDB uses WriteProperty (not WithDefault), so this is always written
	s.WriteProperty(102, "total_rows", totalRows)

	// Property 103: index_pointers (empty list of BlockPointer for forward compatibility)
	// DuckDB writes this as: serializer.WriteProperty(103, "index_pointers", compat_block_pointers);
	// where compat_block_pointers is an empty vector<BlockPointer>
	s.OnPropertyBegin(103, "index_pointers")
	s.OnListBegin(0) // Empty list

	// NOTE: No terminator here! Properties 101-103 are siblings of property 100,
	// and the entry object terminator is written by WriteBinaryFormat after this returns.
}
