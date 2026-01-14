package functions

// HasTablePrivilege checks if user has privilege on table.
// In dukdb-go, we don't have a privilege system, so always return true.
type HasTablePrivilege struct{}

// Evaluate always returns true since dukdb-go doesn't implement privileges.
func (*HasTablePrivilege) Evaluate(user, table, privilege string) bool {
	_, _, _ = user, table, privilege // unused

	return true
}

// EvaluateWithOID is the variant that takes table OID.
func (*HasTablePrivilege) EvaluateWithOID(user string, tableOID uint32, privilege string) bool {
	_, _, _ = user, tableOID, privilege // unused

	return true
}

// HasSchemaPrivilege checks if user has privilege on schema.
type HasSchemaPrivilege struct{}

// Evaluate always returns true.
func (*HasSchemaPrivilege) Evaluate(user, schema, privilege string) bool {
	_, _, _ = user, schema, privilege // unused

	return true
}

// HasDatabasePrivilege checks if user has privilege on database.
type HasDatabasePrivilege struct{}

// Evaluate always returns true.
func (*HasDatabasePrivilege) Evaluate(user, database, privilege string) bool {
	_, _, _ = user, database, privilege // unused

	return true
}

// HasColumnPrivilege checks if user has privilege on column.
type HasColumnPrivilege struct{}

// Evaluate always returns true.
func (*HasColumnPrivilege) Evaluate(user, table, column, privilege string) bool {
	_, _, _, _ = user, table, column, privilege // unused

	return true
}

// HasSequencePrivilege checks if user has privilege on sequence.
type HasSequencePrivilege struct{}

// Evaluate always returns true.
func (*HasSequencePrivilege) Evaluate(user, sequence, privilege string) bool {
	_, _, _ = user, sequence, privilege // unused

	return true
}

// HasFunctionPrivilege checks if user has privilege on function.
type HasFunctionPrivilege struct{}

// Evaluate always returns true.
func (*HasFunctionPrivilege) Evaluate(user, function, privilege string) bool {
	_, _, _ = user, function, privilege // unused

	return true
}

// HasAnyColumnPrivilege checks if user has any column privilege.
type HasAnyColumnPrivilege struct{}

// Evaluate always returns true.
func (*HasAnyColumnPrivilege) Evaluate(user, table, privilege string) bool {
	_, _, _ = user, table, privilege // unused

	return true
}

// PgHasRole checks if user has role.
type PgHasRole struct{}

// Evaluate always returns true.
func (*PgHasRole) Evaluate(user, role, privilege string) bool {
	_, _, _ = user, role, privilege // unused

	return true
}
