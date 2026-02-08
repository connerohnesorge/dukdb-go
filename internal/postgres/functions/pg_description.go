package functions

// ObjDescription returns the comment for an object.
// Comments are not stored, so this returns NULL.
type ObjDescription struct{}

// Evaluate returns the object description (NULL when not available).
func (*ObjDescription) Evaluate(oid uint32, catalog string) *string {
	_, _ = oid, catalog

	return nil
}

// ColDescription returns the comment for a column.
// Comments are not stored, so this returns NULL.
type ColDescription struct{}

// Evaluate returns the column description (NULL when not available).
func (*ColDescription) Evaluate(oid uint32, column int) *string {
	_, _ = oid, column

	return nil
}

// ShobjDescription returns the comment for a shared object.
// Comments are not stored, so this returns NULL.
type ShobjDescription struct{}

// Evaluate returns the shared object description (NULL when not available).
func (*ShobjDescription) Evaluate(oid uint32, catalog string) *string {
	_, _ = oid, catalog

	return nil
}
