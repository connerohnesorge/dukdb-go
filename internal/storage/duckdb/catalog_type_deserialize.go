package duckdb

// This file implements deserialization for type-related catalog structures.
// These include TypeModifiers, StructField, UnionMember, ColumnDefinition,
// ForeignKeyInfo, Constraint, and DependencyEntry.

// Deserialize reads a DependencyEntry from the binary reader.
func (d *DependencyEntry) Deserialize(r *BinaryReader) error {
	for {
		propID := r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
		if propID == PropEnd {
			break
		}
		switch propID {
		case PropDepCatalog:
			d.Catalog = r.ReadString()
		case PropDepSchema:
			d.Schema = r.ReadString()
		case PropDepName:
			d.Name = r.ReadString()
		case PropDepType:
			d.Type = CatalogType(r.ReadUint8())
		case PropDepDepType:
			d.DependencyType = DependencyType(r.ReadUint8())
		default:
			if err := skipPropertyValue(r, propID); err != nil {
				return err
			}
		}
	}

	return r.Err()
}

// Deserialize reads a TypeModifiers from the binary reader.
func (tm *TypeModifiers) Deserialize(r *BinaryReader) error {
	for {
		propID := r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
		if propID == PropEnd {
			break
		}
		if err := tm.deserializeProperty(r, propID); err != nil {
			return err
		}
	}

	return r.Err()
}

// deserializeProperty handles a single property in TypeModifiers deserialization.
func (tm *TypeModifiers) deserializeProperty(r *BinaryReader, propID uint32) error {
	switch propID {
	case PropTypeModWidth:
		tm.Width = r.ReadUint8()
	case PropTypeModScale:
		tm.Scale = r.ReadUint8()
	case PropTypeModLength:
		tm.Length = r.ReadUint32()
	case PropTypeModCollation:
		tm.Collation = r.ReadString()
	case PropTypeModChildTypeID:
		tm.ChildTypeID = LogicalTypeID(r.ReadUint8())
	case PropTypeModChildType:
		tm.ChildType = &TypeModifiers{}

		return tm.ChildType.Deserialize(r)
	case PropTypeModKeyTypeID:
		tm.KeyTypeID = LogicalTypeID(r.ReadUint8())
	case PropTypeModKeyType:
		tm.KeyType = &TypeModifiers{}

		return tm.KeyType.Deserialize(r)
	case PropTypeModValueTypeID:
		tm.ValueTypeID = LogicalTypeID(r.ReadUint8())
	case PropTypeModValueType:
		tm.ValueType = &TypeModifiers{}

		return tm.ValueType.Deserialize(r)
	case PropTypeModStructFields:
		count := r.ReadUint32()
		tm.StructFields = make([]StructField, count)
		for i := uint32(0); i < count; i++ {
			if err := tm.StructFields[i].Deserialize(r); err != nil {
				return err
			}
		}
	case PropTypeModEnumValues:
		count := r.ReadUint32()
		tm.EnumValues = make([]string, count)
		for i := uint32(0); i < count; i++ {
			tm.EnumValues[i] = r.ReadString()
		}
	case PropTypeModUnionMembers:
		count := r.ReadUint8()
		tm.UnionMembers = make([]UnionMember, count)
		for i := uint8(0); i < count; i++ {
			if err := tm.UnionMembers[i].Deserialize(r); err != nil {
				return err
			}
		}
	default:
		return skipPropertyValue(r, propID)
	}

	return nil
}

// Deserialize reads a StructField from the binary reader.
func (sf *StructField) Deserialize(r *BinaryReader) error {
	sf.Name = r.ReadString()
	sf.Type = LogicalTypeID(r.ReadUint8())
	hasModifiers := r.ReadBool()
	if hasModifiers {
		sf.TypeModifiers = &TypeModifiers{}
		if err := sf.TypeModifiers.Deserialize(r); err != nil {
			return err
		}
	}

	return r.Err()
}

// Deserialize reads a UnionMember from the binary reader.
func (um *UnionMember) Deserialize(r *BinaryReader) error {
	um.Tag = r.ReadString()
	um.Type = LogicalTypeID(r.ReadUint8())
	hasModifiers := r.ReadBool()
	if hasModifiers {
		um.TypeModifiers = &TypeModifiers{}
		if err := um.TypeModifiers.Deserialize(r); err != nil {
			return err
		}
	}

	return r.Err()
}

// Deserialize reads a ColumnDefinition from the binary reader.
func (cd *ColumnDefinition) Deserialize(r *BinaryReader) error {
	for {
		propID := r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
		if propID == PropEnd {
			break
		}
		switch propID {
		case PropColName:
			cd.Name = r.ReadString()
		case PropColType:
			cd.Type = LogicalTypeID(r.ReadUint8())
		case PropColTypeMod:
			if err := cd.TypeModifiers.Deserialize(r); err != nil {
				return err
			}
		case PropColNullable:
			cd.Nullable = r.ReadBool()
		case PropColHasDefault:
			cd.HasDefault = r.ReadBool()
		case PropColDefault:
			cd.DefaultValue = r.ReadString()
		case PropColGenerated:
			cd.Generated = r.ReadBool()
		case PropColGeneratedEx:
			cd.GeneratedExpression = r.ReadString()
		case PropColCompression:
			cd.CompressionType = CompressionType(r.ReadUint8())
		default:
			if err := skipPropertyValue(r, propID); err != nil {
				return err
			}
		}
	}

	return r.Err()
}

// Deserialize reads a ForeignKeyInfo from the binary reader.
func (fk *ForeignKeyInfo) Deserialize(r *BinaryReader) error {
	for {
		propID := r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
		if propID == PropEnd {
			break
		}
		switch propID {
		case PropFKSchema:
			fk.ReferencedSchema = r.ReadString()
		case PropFKTable:
			fk.ReferencedTable = r.ReadString()
		case PropFKColumns:
			count := r.ReadUint32()
			fk.ReferencedColumns = make([]string, count)
			for i := uint32(0); i < count; i++ {
				fk.ReferencedColumns[i] = r.ReadString()
			}
		case PropFKOnDelete:
			fk.OnDelete = ForeignKeyAction(r.ReadUint8())
		case PropFKOnUpdate:
			fk.OnUpdate = ForeignKeyAction(r.ReadUint8())
		default:
			if err := skipPropertyValue(r, propID); err != nil {
				return err
			}
		}
	}

	return r.Err()
}

// Deserialize reads a Constraint from the binary reader.
func (c *Constraint) Deserialize(r *BinaryReader) error {
	for {
		propID := r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
		if propID == PropEnd {
			break
		}
		switch propID {
		case PropConstraintType:
			c.Type = ConstraintType(r.ReadUint8())
		case PropConstraintName:
			c.Name = r.ReadString()
		case PropConstraintCols:
			count := r.ReadUint32()
			c.ColumnIndices = make([]uint64, count)
			for i := uint32(0); i < count; i++ {
				c.ColumnIndices[i] = r.ReadUint64()
			}
		case PropConstraintExpr:
			c.Expression = r.ReadString()
		case PropConstraintFK:
			c.ForeignKey = &ForeignKeyInfo{}
			if err := c.ForeignKey.Deserialize(r); err != nil {
				return err
			}
		default:
			if err := skipPropertyValue(r, propID); err != nil {
				return err
			}
		}
	}

	return r.Err()
}

// deserializeCreateInfoWithRemainingProp deserializes CreateInfo and returns
// the first property ID that doesn't belong to CreateInfo.
func deserializeCreateInfoWithRemainingProp(r *BinaryReader, ci *CreateInfo) (uint32, error) {
	ci.Tags = make(map[string]string)
	ci.Dependencies = nil

	for {
		propID := r.ReadUint32()
		if r.Err() != nil {
			return 0, r.Err()
		}
		err := deserializeCreateInfoProperty(r, ci, propID)
		if err == nil {
			continue
		}
		if _, ok := err.(*unknownPropError); ok {
			return propID, r.Err()
		}

		return 0, err
	}
}

// unknownPropError indicates an unknown property was encountered.
type unknownPropError struct{}

func (*unknownPropError) Error() string {
	return "unknown property"
}

// deserializeCreateInfoProperty handles a single property in CreateInfo deserialization.
// Returns unknownPropError if the property is not recognized.
func deserializeCreateInfoProperty(r *BinaryReader, ci *CreateInfo, propID uint32) error {
	switch propID {
	case PropEnd:
		return &unknownPropError{}
	case PropCatalog:
		ci.Catalog = r.ReadString()
	case PropSchema:
		ci.Schema = r.ReadString()
	case PropTemporary:
		ci.Temporary = r.ReadBool()
	case PropInternal:
		ci.Internal = r.ReadBool()
	case PropOnConflict:
		ci.OnConflict = OnCreateConflict(r.ReadUint8())
	case PropSQL:
		ci.SQL = r.ReadString()
	case PropComment:
		ci.Comment = r.ReadString()
	case PropTags:
		count := r.ReadUint32()
		for i := uint32(0); i < count; i++ {
			key := r.ReadString()
			value := r.ReadString()
			ci.Tags[key] = value
		}
	case PropDependencies:
		count := r.ReadUint32()
		ci.Dependencies = make([]DependencyEntry, count)
		for i := uint32(0); i < count; i++ {
			if err := ci.Dependencies[i].Deserialize(r); err != nil {
				return err
			}
		}
	default:
		return &unknownPropError{}
	}

	return nil
}
