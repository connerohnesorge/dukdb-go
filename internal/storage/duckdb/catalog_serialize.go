package duckdb

// This file implements serialization (write) methods for all catalog entry types.
// Serialization uses property IDs for forward/backward compatibility, matching
// DuckDB's serialization format.

// Property IDs for TypeModifiers serialization.
const (
	PropTypeModWidth        = 270 // Width (precision)
	PropTypeModScale        = 271 // Scale
	PropTypeModLength       = 272 // Length
	PropTypeModChildType    = 273 // Child type (LIST, ARRAY)
	PropTypeModChildTypeID  = 274 // Child type ID
	PropTypeModKeyType      = 275 // Key type (MAP)
	PropTypeModKeyTypeID    = 276 // Key type ID
	PropTypeModValueType    = 277 // Value type (MAP)
	PropTypeModValueTypeID  = 278 // Value type ID
	PropTypeModStructFields = 279 // Struct fields
	PropTypeModEnumValues   = 280 // Enum values
	PropTypeModUnionMembers = 281 // Union members
	PropTypeModCollation    = 282 // Collation
)

// Property IDs for DependencyEntry serialization.
const (
	PropDepCatalog = 290 // Dependency catalog
	PropDepSchema  = 291 // Dependency schema
	PropDepName    = 292 // Dependency name
	PropDepType    = 293 // Dependency catalog type
	PropDepDepType = 294 // Dependency type (regular/automatic/ownership)
)

// Property IDs for names.
const (
	PropSchemaName = 300 // Schema name
	PropTableName  = 301 // Table name
)

// PropEnd marks the end of property-based serialization for an entry.
const PropEnd uint32 = 0

// Serialize writes a DependencyEntry to the binary writer.
func (d *DependencyEntry) Serialize(w *BinaryWriter) error {
	w.WritePropertyID(PropDepCatalog)
	w.WriteString(d.Catalog)
	w.WritePropertyID(PropDepSchema)
	w.WriteString(d.Schema)
	w.WritePropertyID(PropDepName)
	w.WriteString(d.Name)
	w.WritePropertyID(PropDepType)
	w.WriteUint8(uint8(d.Type))
	w.WritePropertyID(PropDepDepType)
	w.WriteUint8(uint8(d.DependencyType))
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// serializeTypeModBasic writes basic type modifier fields.
func (tm *TypeModifiers) serializeTypeModBasic(w *BinaryWriter) {
	if tm.Width != 0 {
		w.WritePropertyID(PropTypeModWidth)
		w.WriteUint8(tm.Width)
	}
	if tm.Scale != 0 {
		w.WritePropertyID(PropTypeModScale)
		w.WriteUint8(tm.Scale)
	}
	if tm.Length != 0 {
		w.WritePropertyID(PropTypeModLength)
		w.WriteUint32(tm.Length)
	}
	if tm.Collation != "" {
		w.WritePropertyID(PropTypeModCollation)
		w.WriteString(tm.Collation)
	}
}

// serializeChildType writes child type for LIST/ARRAY to the writer.
func (tm *TypeModifiers) serializeChildType(w *BinaryWriter) error {
	if tm.ChildTypeID == TypeInvalid {
		return nil
	}
	w.WritePropertyID(PropTypeModChildTypeID)
	w.WriteUint8(uint8(tm.ChildTypeID))
	if tm.ChildType == nil {
		return nil
	}
	w.WritePropertyID(PropTypeModChildType)

	return tm.ChildType.Serialize(w)
}

// serializeKeyType writes key type for MAP to the writer.
func (tm *TypeModifiers) serializeKeyType(w *BinaryWriter) error {
	if tm.KeyTypeID == TypeInvalid {
		return nil
	}
	w.WritePropertyID(PropTypeModKeyTypeID)
	w.WriteUint8(uint8(tm.KeyTypeID))
	if tm.KeyType == nil {
		return nil
	}
	w.WritePropertyID(PropTypeModKeyType)

	return tm.KeyType.Serialize(w)
}

// serializeValueType writes value type for MAP to the writer.
func (tm *TypeModifiers) serializeValueType(w *BinaryWriter) error {
	if tm.ValueTypeID == TypeInvalid {
		return nil
	}
	w.WritePropertyID(PropTypeModValueTypeID)
	w.WriteUint8(uint8(tm.ValueTypeID))
	if tm.ValueType == nil {
		return nil
	}
	w.WritePropertyID(PropTypeModValueType)

	return tm.ValueType.Serialize(w)
}

// serializeTypeModNested writes nested type modifier fields for LIST, MAP, etc.
func (tm *TypeModifiers) serializeTypeModNested(w *BinaryWriter) error {
	if err := tm.serializeChildType(w); err != nil {
		return err
	}
	if err := tm.serializeKeyType(w); err != nil {
		return err
	}

	return tm.serializeValueType(w)
}

// serializeTypeModComplex writes complex type modifier fields.
func (tm *TypeModifiers) serializeTypeModComplex(w *BinaryWriter) error {
	if len(tm.StructFields) > 0 {
		w.WritePropertyID(PropTypeModStructFields)
		w.WriteUint32(uint32(len(tm.StructFields)))
		for i := range tm.StructFields {
			if err := tm.StructFields[i].Serialize(w); err != nil {
				return err
			}
		}
	}
	if len(tm.EnumValues) > 0 {
		w.WritePropertyID(PropTypeModEnumValues)
		w.WriteUint32(uint32(len(tm.EnumValues)))
		for _, val := range tm.EnumValues {
			w.WriteString(val)
		}
	}
	if len(tm.UnionMembers) > 0 {
		w.WritePropertyID(PropTypeModUnionMembers)
		w.WriteUint8(uint8(len(tm.UnionMembers)))
		for i := range tm.UnionMembers {
			if err := tm.UnionMembers[i].Serialize(w); err != nil {
				return err
			}
		}
	}

	return nil
}

// Serialize writes a TypeModifiers to the binary writer.
func (tm *TypeModifiers) Serialize(w *BinaryWriter) error {
	tm.serializeTypeModBasic(w)
	if err := tm.serializeTypeModNested(w); err != nil {
		return err
	}
	if err := tm.serializeTypeModComplex(w); err != nil {
		return err
	}
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// Serialize writes a StructField to the binary writer.
func (sf *StructField) Serialize(w *BinaryWriter) error {
	w.WriteString(sf.Name)
	w.WriteUint8(uint8(sf.Type))
	hasModifiers := sf.TypeModifiers != nil
	w.WriteBool(hasModifiers)
	if hasModifiers {
		if err := sf.TypeModifiers.Serialize(w); err != nil {
			return err
		}
	}

	return w.Err()
}

// Serialize writes a UnionMember to the binary writer.
func (um *UnionMember) Serialize(w *BinaryWriter) error {
	w.WriteString(um.Tag)
	w.WriteUint8(uint8(um.Type))
	hasModifiers := um.TypeModifiers != nil
	w.WriteBool(hasModifiers)
	if hasModifiers {
		if err := um.TypeModifiers.Serialize(w); err != nil {
			return err
		}
	}

	return w.Err()
}

// Serialize writes a ColumnDefinition to the binary writer.
// For generated columns, this serializes the expression using PropColGeneratedEx
// (equivalent to PropColumnDefExpression) and the kind using PropColGeneratedKind
// (equivalent to ColumnCategoryGenerated).
func (cd *ColumnDefinition) Serialize(w *BinaryWriter) error {
	w.WritePropertyID(PropColName)
	w.WriteString(cd.Name)
	w.WritePropertyID(PropColType)
	w.WriteUint8(uint8(cd.Type))
	w.WritePropertyID(PropColTypeMod)
	if err := cd.TypeModifiers.Serialize(w); err != nil {
		return err
	}
	w.WritePropertyID(PropColNullable)
	w.WriteBool(cd.Nullable)
	w.WritePropertyID(PropColHasDefault)
	w.WriteBool(cd.HasDefault)
	if cd.HasDefault {
		w.WritePropertyID(PropColDefault)
		w.WriteString(cd.DefaultValue)
	}
	w.WritePropertyID(PropColGenerated)
	w.WriteBool(cd.Generated)
	if cd.Generated {
		// PropColGeneratedEx corresponds to PropColumnDefExpression in the BinarySerializer format.
		w.WritePropertyID(PropColGeneratedEx)
		w.WriteString(cd.GeneratedExpression)
		// PropColGeneratedKind encodes the ColumnCategoryGenerated kind (STORED=0, VIRTUAL=1).
		w.WritePropertyID(PropColGeneratedKind)
		w.WriteUint8(cd.GeneratedKind)
	}
	w.WritePropertyID(PropColCompression)
	w.WriteUint8(uint8(cd.CompressionType))
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// Serialize writes a ForeignKeyInfo to the binary writer.
func (fk *ForeignKeyInfo) Serialize(w *BinaryWriter) error {
	w.WritePropertyID(PropFKSchema)
	w.WriteString(fk.ReferencedSchema)
	w.WritePropertyID(PropFKTable)
	w.WriteString(fk.ReferencedTable)
	w.WritePropertyID(PropFKColumns)
	w.WriteUint32(uint32(len(fk.ReferencedColumns)))
	for _, col := range fk.ReferencedColumns {
		w.WriteString(col)
	}
	w.WritePropertyID(PropFKOnDelete)
	w.WriteUint8(uint8(fk.OnDelete))
	w.WritePropertyID(PropFKOnUpdate)
	w.WriteUint8(uint8(fk.OnUpdate))
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// Serialize writes a Constraint to the binary writer.
func (c *Constraint) Serialize(w *BinaryWriter) error {
	w.WritePropertyID(PropConstraintType)
	w.WriteUint8(uint8(c.Type))
	w.WritePropertyID(PropConstraintName)
	w.WriteString(c.Name)
	w.WritePropertyID(PropConstraintCols)
	w.WriteUint32(uint32(len(c.ColumnIndices)))
	for _, colIdx := range c.ColumnIndices {
		w.WriteUint64(colIdx)
	}
	if c.Type == ConstraintTypeCheck && c.Expression != "" {
		w.WritePropertyID(PropConstraintExpr)
		w.WriteString(c.Expression)
	}
	if c.Type == ConstraintTypeForeignKey && c.ForeignKey != nil {
		w.WritePropertyID(PropConstraintFK)
		if err := c.ForeignKey.Serialize(w); err != nil {
			return err
		}
	}
	w.WritePropertyID(PropEnd)

	return w.Err()
}

// Serialize writes the base CreateInfo fields to the binary writer.
func (ci *CreateInfo) Serialize(w *BinaryWriter) error {
	w.WritePropertyID(PropCatalog)
	w.WriteString(ci.Catalog)
	w.WritePropertyID(PropSchema)
	w.WriteString(ci.Schema)
	w.WritePropertyID(PropTemporary)
	w.WriteBool(ci.Temporary)
	w.WritePropertyID(PropInternal)
	w.WriteBool(ci.Internal)
	w.WritePropertyID(PropOnConflict)
	w.WriteUint8(uint8(ci.OnConflict))
	w.WritePropertyID(PropSQL)
	w.WriteString(ci.SQL)
	w.WritePropertyID(PropComment)
	w.WriteString(ci.Comment)
	w.WritePropertyID(PropTags)
	w.WriteUint32(uint32(len(ci.Tags)))
	for k, v := range ci.Tags {
		w.WriteString(k)
		w.WriteString(v)
	}
	w.WritePropertyID(PropDependencies)
	w.WriteUint32(uint32(len(ci.Dependencies)))
	for i := range ci.Dependencies {
		if err := ci.Dependencies[i].Serialize(w); err != nil {
			return err
		}
	}

	return w.Err()
}
