package wal

// EntryHandler defines the interface for handling WAL entries during recovery.
// Implementations of this interface handle catalog operations by updating
// the catalog state based on WAL entry data.
type EntryHandler interface {
	// HandleCreateTable handles a CREATE TABLE entry.
	HandleCreateTable(entry *CreateTableEntry) error

	// HandleDropTable handles a DROP TABLE entry.
	HandleDropTable(entry *DropTableEntry) error

	// HandleCreateView handles a CREATE VIEW entry.
	HandleCreateView(entry *CreateViewEntry) error

	// HandleDropView handles a DROP VIEW entry.
	HandleDropView(entry *DropViewEntry) error

	// HandleCreateIndex handles a CREATE INDEX entry.
	HandleCreateIndex(entry *CreateIndexEntry) error

	// HandleDropIndex handles a DROP INDEX entry.
	HandleDropIndex(entry *DropIndexEntry) error

	// HandleCreateSequence handles a CREATE SEQUENCE entry.
	HandleCreateSequence(entry *CreateSequenceEntry) error

	// HandleDropSequence handles a DROP SEQUENCE entry.
	HandleDropSequence(entry *DropSequenceEntry) error

	// HandleCreateSchema handles a CREATE SCHEMA entry.
	HandleCreateSchema(entry *CreateSchemaEntry) error

	// HandleDropSchema handles a DROP SCHEMA entry.
	HandleDropSchema(entry *DropSchemaEntry) error

	// HandleAlterTable handles an ALTER TABLE entry.
	HandleAlterTable(entry *AlterTableEntry) error
}
