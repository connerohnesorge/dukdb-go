package duckdb

import (
	"errors"
	"fmt"
)

// This file implements deserialization for catalog entry types (Schema, Table, View, etc.).
// These are the top-level catalog entries stored in DuckDB metadata blocks.

// Deserialize reads a SchemaCatalogEntry from the binary reader.
// Note: The catalog type byte has already been read by DeserializeCatalogEntry.
func (s *SchemaCatalogEntry) Deserialize(r *BinaryReader) error {
	// Read CreateInfo fields, getting the first non-CreateInfo property
	propID, err := deserializeCreateInfoWithRemainingProp(r, &s.CreateInfo)
	if err != nil {
		return err
	}

	// Process remaining properties
	for propID != PropEnd {
		switch propID {
		case PropSchemaName:
			s.Name = r.ReadString()
		default:
			if err := skipPropertyValue(r, propID); err != nil {
				return err
			}
		}

		propID = r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
	}

	return r.Err()
}

// Deserialize reads a TableCatalogEntry from the binary reader.
// Note: The catalog type byte has already been read by DeserializeCatalogEntry.
func (t *TableCatalogEntry) Deserialize(r *BinaryReader) error {
	// Initialize slices
	t.Columns = nil
	t.Constraints = nil

	// Read CreateInfo fields, getting the first non-CreateInfo property
	propID, err := deserializeCreateInfoWithRemainingProp(r, &t.CreateInfo)
	if err != nil {
		return err
	}

	// Process remaining properties
	for propID != PropEnd {
		switch propID {
		case PropTableName:
			t.Name = r.ReadString()
		case PropTableColumns:
			count := r.ReadUint32()
			t.Columns = make([]ColumnDefinition, count)
			for i := uint32(0); i < count; i++ {
				if err := t.Columns[i].Deserialize(r); err != nil {
					return err
				}
			}
		case PropTableConstraints:
			count := r.ReadUint32()
			t.Constraints = make([]Constraint, count)
			for i := uint32(0); i < count; i++ {
				if err := t.Constraints[i].Deserialize(r); err != nil {
					return err
				}
			}
		default:
			if err := skipPropertyValue(r, propID); err != nil {
				return err
			}
		}

		propID = r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
	}

	return r.Err()
}

// Deserialize reads a ViewCatalogEntry from the binary reader.
// Note: The catalog type byte has already been read by DeserializeCatalogEntry.
func (v *ViewCatalogEntry) Deserialize(r *BinaryReader) error {
	// Initialize slices
	v.Aliases = nil
	v.Types = nil
	v.TypeModifiers = nil

	// Read CreateInfo fields, getting the first non-CreateInfo property
	propID, err := deserializeCreateInfoWithRemainingProp(r, &v.CreateInfo)
	if err != nil {
		return err
	}

	// Process remaining properties
	for propID != PropEnd {
		switch propID {
		case PropTableName:
			v.Name = r.ReadString()
		case PropViewQuery:
			v.Query = r.ReadString()
		case PropViewAliases:
			count := r.ReadUint32()
			v.Aliases = make([]string, count)
			for i := uint32(0); i < count; i++ {
				v.Aliases[i] = r.ReadString()
			}
		case PropViewTypes:
			count := r.ReadUint32()
			v.Types = make([]LogicalTypeID, count)
			for i := uint32(0); i < count; i++ {
				v.Types[i] = LogicalTypeID(r.ReadUint8())
			}
		case PropViewTypeMods:
			count := r.ReadUint32()
			v.TypeModifiers = make([]TypeModifiers, count)
			for i := uint32(0); i < count; i++ {
				if err := v.TypeModifiers[i].Deserialize(r); err != nil {
					return err
				}
			}
		default:
			if err := skipPropertyValue(r, propID); err != nil {
				return err
			}
		}

		propID = r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
	}

	return r.Err()
}

// Deserialize reads an IndexCatalogEntry from the binary reader.
// Note: The catalog type byte has already been read by DeserializeCatalogEntry.
func (i *IndexCatalogEntry) Deserialize(r *BinaryReader) error {
	// Initialize slices
	i.ColumnIDs = nil
	i.Expressions = nil

	// Read CreateInfo fields, getting the first non-CreateInfo property
	propID, err := deserializeCreateInfoWithRemainingProp(r, &i.CreateInfo)
	if err != nil {
		return err
	}

	// Process remaining properties
	for propID != PropEnd {
		switch propID {
		case PropIndexName:
			i.Name = r.ReadString()
		case PropIndexTable:
			i.TableName = r.ReadString()
		case PropIndexType:
			i.IndexType = IndexType(r.ReadUint8())
		case PropIndexConstraint:
			i.Constraint = IndexConstraintType(r.ReadUint8())
		case PropIndexColumnIDs:
			count := r.ReadUint32()
			i.ColumnIDs = make([]uint64, count)
			for j := uint32(0); j < count; j++ {
				i.ColumnIDs[j] = r.ReadUint64()
			}
		case PropIndexExprs:
			count := r.ReadUint32()
			i.Expressions = make([]string, count)
			for j := uint32(0); j < count; j++ {
				i.Expressions[j] = r.ReadString()
			}
		default:
			if err := skipPropertyValue(r, propID); err != nil {
				return err
			}
		}

		propID = r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
	}

	return r.Err()
}

// Deserialize reads a SequenceCatalogEntry from the binary reader.
// Note: The catalog type byte has already been read by DeserializeCatalogEntry.
func (s *SequenceCatalogEntry) Deserialize(r *BinaryReader) error {
	// Read CreateInfo fields, getting the first non-CreateInfo property
	propID, err := deserializeCreateInfoWithRemainingProp(r, &s.CreateInfo)
	if err != nil {
		return err
	}

	// Process remaining properties
	for propID != PropEnd {
		switch propID {
		case PropSeqName:
			s.Name = r.ReadString()
		case PropSeqUsage:
			s.Usage = SequenceUsage(r.ReadUint8())
		case PropSeqStartWith:
			s.StartWith = r.ReadInt64()
		case PropSeqIncrement:
			s.Increment = r.ReadInt64()
		case PropSeqMinValue:
			s.MinValue = r.ReadInt64()
		case PropSeqMaxValue:
			s.MaxValue = r.ReadInt64()
		case PropSeqCycle:
			s.Cycle = r.ReadBool()
		case PropSeqCounter:
			s.Counter = r.ReadInt64()
		default:
			if err := skipPropertyValue(r, propID); err != nil {
				return err
			}
		}

		propID = r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
	}

	return r.Err()
}

// Deserialize reads a TypeCatalogEntry from the binary reader.
// Note: The catalog type byte has already been read by DeserializeCatalogEntry.
func (t *TypeCatalogEntry) Deserialize(r *BinaryReader) error {
	// Read CreateInfo fields, getting the first non-CreateInfo property
	propID, err := deserializeCreateInfoWithRemainingProp(r, &t.CreateInfo)
	if err != nil {
		return err
	}

	// Process remaining properties
	for propID != PropEnd {
		switch propID {
		case PropTypeName:
			t.Name = r.ReadString()
		case PropTypeID:
			t.TypeID = LogicalTypeID(r.ReadUint8())
		case PropTypeModifier:
			if err := t.TypeModifiers.Deserialize(r); err != nil {
				return err
			}
		default:
			if err := skipPropertyValue(r, propID); err != nil {
				return err
			}
		}

		propID = r.ReadUint32()
		if r.Err() != nil {
			return r.Err()
		}
	}

	return r.Err()
}

// DeserializeCatalogEntry reads a catalog entry from the binary reader.
// It first reads the catalog type byte, then dispatches to the appropriate
// deserializer based on the type.
//
// Returns the deserialized CatalogEntry and any error encountered.
// For unknown or invalid catalog types, returns nil and an error.
func DeserializeCatalogEntry(r *BinaryReader) (CatalogEntry, error) {
	// Read the catalog type byte
	catalogType := CatalogType(r.ReadUint8())
	if r.Err() != nil {
		return nil, r.Err()
	}

	var entry CatalogEntry

	switch catalogType {
	case CatalogSchemaEntry:
		schema := &SchemaCatalogEntry{}
		if err := schema.Deserialize(r); err != nil {
			return nil, err
		}
		entry = schema

	case CatalogTableEntry:
		table := &TableCatalogEntry{}
		if err := table.Deserialize(r); err != nil {
			return nil, err
		}
		entry = table

	case CatalogViewEntry:
		view := &ViewCatalogEntry{}
		if err := view.Deserialize(r); err != nil {
			return nil, err
		}
		entry = view

	case CatalogIndexEntry:
		index := &IndexCatalogEntry{}
		if err := index.Deserialize(r); err != nil {
			return nil, err
		}
		entry = index

	case CatalogSequenceEntry:
		seq := &SequenceCatalogEntry{}
		if err := seq.Deserialize(r); err != nil {
			return nil, err
		}
		entry = seq

	case CatalogTypeEntry:
		typeEntry := &TypeCatalogEntry{}
		if err := typeEntry.Deserialize(r); err != nil {
			return nil, err
		}
		entry = typeEntry

	case CatalogInvalid:
		return nil, errors.New("invalid catalog entry type")

	case CatalogPreparedStatement, CatalogCollationEntry, CatalogDatabaseEntry,
		CatalogTableFunctionEntry, CatalogScalarFunctionEntry, CatalogAggregateFunctionEntry,
		CatalogPragmaFunctionEntry, CatalogCopyFunctionEntry, CatalogMacroEntry,
		CatalogTableMacroEntry, CatalogDeletedEntry, CatalogRenamedEntry,
		CatalogSecretEntry, CatalogSecretTypeEntry, CatalogSecretFunctionEntry,
		CatalogDependencyEntry:
		return nil, fmt.Errorf("unsupported catalog entry type: %d (%s)", catalogType, catalogType.String())
	}

	return entry, nil
}
