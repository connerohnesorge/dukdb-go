package duckdb

import "fmt"

// This file implements the skipPropertyValue function for forward compatibility
// during deserialization of DuckDB catalog metadata.

// skipPropertyValue skips a property value based on the property ID.
// This allows forward compatibility by skipping unknown properties from newer format versions.
func skipPropertyValue(r *BinaryReader, propID uint32) error {
	switch propID {
	// String properties
	case PropCatalog, PropSchema, PropSQL, PropComment, PropSchemaName, PropTableName,
		PropViewQuery, PropColName, PropColDefault, PropColGeneratedEx, PropConstraintName,
		PropConstraintExpr, PropFKSchema, PropFKTable, PropIndexName, PropIndexTable,
		PropSeqName, PropTypeName, PropDepCatalog, PropDepSchema, PropDepName, PropTypeModCollation:
		_ = r.ReadString()
	// Boolean properties
	case PropTemporary,
		PropInternal,
		PropColNullable,
		PropColHasDefault,
		PropColGenerated,
		PropSeqCycle:
		_ = r.ReadBool()
	// Uint8 properties
	case PropOnConflict, PropColType, PropColCompression, PropConstraintType, PropFKOnDelete,
		PropFKOnUpdate, PropIndexType, PropIndexConstraint, PropSeqUsage, PropDepType, PropDepDepType,
		PropTypeID, PropTypeModWidth, PropTypeModScale, PropTypeModChildTypeID, PropTypeModKeyTypeID,
		PropTypeModValueTypeID:
		_ = r.ReadUint8()
	// Uint32 properties
	case PropTypeModLength:
		_ = r.ReadUint32()
	// Int64 properties
	case PropSeqStartWith, PropSeqIncrement, PropSeqMinValue, PropSeqMaxValue, PropSeqCounter:
		_ = r.ReadInt64()
	// Complex array properties handled by helper functions
	case PropTags:
		skipTagsProperty(r)
	case PropDependencies:
		return skipDependenciesProperty(r)
	case PropTableColumns:
		return skipTableColumnsProperty(r)
	case PropTableConstraints:
		return skipTableConstraintsProperty(r)
	case PropViewAliases, PropIndexExprs, PropFKColumns:
		skipStringArrayProperty(r)
	case PropViewTypes:
		skipUint8ArrayProperty(r)
	case PropViewTypeMods, PropTypeModStructFields:
		return skipTypeModifiersArrayProperty(r)
	case PropIndexColumnIDs, PropConstraintCols:
		skipUint64ArrayProperty(r)
	case PropConstraintFK:
		return skipForeignKeyProperty(r)
	case PropColTypeMod,
		PropTypeModifier,
		PropTypeModChildType,
		PropTypeModKeyType,
		PropTypeModValueType:
		return skipTypeModifiersProperty(r)
	case PropTypeModEnumValues:
		skipStringArrayProperty(r)
	case PropTypeModUnionMembers:
		return skipUnionMembersProperty(r)
	default:
		return fmt.Errorf("unknown property ID: %d", propID)
	}

	return r.Err()
}

// Helper functions for skipping complex property values during deserialization.

func skipTagsProperty(r *BinaryReader) {
	count := r.ReadUint32()
	for i := uint32(0); i < count; i++ {
		_ = r.ReadString()
		_ = r.ReadString()
	}
}

func skipDependenciesProperty(r *BinaryReader) error {
	count := r.ReadUint32()
	for i := uint32(0); i < count; i++ {
		var dep DependencyEntry
		if err := dep.Deserialize(r); err != nil {
			return err
		}
	}

	return nil
}

func skipTableColumnsProperty(r *BinaryReader) error {
	count := r.ReadUint32()
	for i := uint32(0); i < count; i++ {
		var col ColumnDefinition
		if err := col.Deserialize(r); err != nil {
			return err
		}
	}

	return nil
}

func skipTableConstraintsProperty(r *BinaryReader) error {
	count := r.ReadUint32()
	for i := uint32(0); i < count; i++ {
		var c Constraint
		if err := c.Deserialize(r); err != nil {
			return err
		}
	}

	return nil
}

func skipStringArrayProperty(r *BinaryReader) {
	count := r.ReadUint32()
	for i := uint32(0); i < count; i++ {
		_ = r.ReadString()
	}
}

func skipUint8ArrayProperty(r *BinaryReader) {
	count := r.ReadUint32()
	for i := uint32(0); i < count; i++ {
		_ = r.ReadUint8()
	}
}

func skipUint64ArrayProperty(r *BinaryReader) {
	count := r.ReadUint32()
	for i := uint32(0); i < count; i++ {
		_ = r.ReadUint64()
	}
}

func skipTypeModifiersArrayProperty(r *BinaryReader) error {
	count := r.ReadUint32()
	for i := uint32(0); i < count; i++ {
		var tm TypeModifiers
		if err := tm.Deserialize(r); err != nil {
			return err
		}
	}

	return nil
}

func skipForeignKeyProperty(r *BinaryReader) error {
	var fk ForeignKeyInfo

	return fk.Deserialize(r)
}

func skipTypeModifiersProperty(r *BinaryReader) error {
	var tm TypeModifiers

	return tm.Deserialize(r)
}

func skipUnionMembersProperty(r *BinaryReader) error {
	count := r.ReadUint8()
	for i := uint8(0); i < count; i++ {
		var um UnionMember
		if err := um.Deserialize(r); err != nil {
			return err
		}
	}

	return nil
}
