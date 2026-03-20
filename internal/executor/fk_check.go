package executor

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// checkForeignKeys validates that all FK constraints on tableDef are satisfied for the given row values.
// columnNames maps value indices to column names.
func (e *Executor) checkForeignKeys(tableDef *catalog.TableDef, values []any, columnNames []string) error {
	for _, c := range tableDef.Constraints {
		fk, ok := c.(*catalog.ForeignKeyConstraintDef)
		if !ok {
			continue
		}
		if err := e.checkParentKeyExists(fk, values, columnNames); err != nil {
			return err
		}
	}

	return nil
}

// extractFKValues extracts FK column values from row values and returns them along with whether all are null.
func extractFKValues(fk *catalog.ForeignKeyConstraintDef, values []any, columnNames []string) ([]any, bool) {
	fkValues := make([]any, len(fk.Columns))
	allNull := true

	for i, fkCol := range fk.Columns {
		for j, colName := range columnNames {
			if strings.EqualFold(colName, fkCol) {
				if j < len(values) {
					fkValues[i] = values[j]
					if values[j] != nil {
						allNull = false
					}
				}

				break
			}
		}
	}

	return fkValues, allNull
}

// resolveRefColumnIndices finds column indices in parent table for the referenced columns.
func resolveRefColumnIndices(parentDef *catalog.TableDef, fk *catalog.ForeignKeyConstraintDef) ([]int, error) {
	refIndices := make([]int, len(fk.RefColumns))

	for i, refCol := range fk.RefColumns {
		refIndices[i] = -1
		for j, col := range parentDef.Columns {
			if strings.EqualFold(col.Name, refCol) {
				refIndices[i] = j

				break
			}
		}

		if refIndices[i] == -1 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeConstraint,
				Msg:  fmt.Sprintf("foreign key references column %q not found in table %q", refCol, fk.RefTable),
			}
		}
	}

	return refIndices, nil
}

// checkParentKeyExists verifies that the FK column values in a child row exist in the parent table.
func (e *Executor) checkParentKeyExists(fk *catalog.ForeignKeyConstraintDef, values []any, columnNames []string) error {
	fkValues, allNull := extractFKValues(fk, values, columnNames)

	// NULL FK values are allowed (skip validation)
	if allNull {
		return nil
	}

	// Get parent table from storage
	parentTable, ok := e.storage.GetTable(fk.RefTable)
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeConstraint,
			Msg:  fmt.Sprintf("foreign key references table %q which does not exist in storage", fk.RefTable),
		}
	}

	// Get parent table def for column indices
	parentDef, exists := e.catalog.GetTable(fk.RefTable)
	if !exists {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeConstraint,
			Msg:  fmt.Sprintf("foreign key references table %q which does not exist in catalog", fk.RefTable),
		}
	}

	refIndices, err := resolveRefColumnIndices(parentDef, fk)
	if err != nil {
		return err
	}

	// Scan parent table for matching key
	if !parentKeyExists(parentTable, refIndices, fkValues) {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeConstraint,
			Msg:  fmt.Sprintf("foreign key violation: key (%s) not found in table %q", formatFKValues(fk.Columns, fkValues), fk.RefTable),
		}
	}

	return nil
}

// parentKeyExists scans parent table for a row matching the given values at the given column indices.
func parentKeyExists(table *storage.Table, colIndices []int, values []any) bool {
	scanner := table.Scan()

	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		for i := range chunk.Count() {
			match := true
			for j, colIdx := range colIndices {
				cellVal := chunk.GetValue(i, colIdx)
				if !fkValuesEqual(cellVal, values[j]) {
					match = false

					break
				}
			}

			if match {
				return true
			}
		}
	}

	return false
}

// findChildForeignKeys finds all FK constraints across all tables that reference the given table.
func (e *Executor) findChildForeignKeys(parentTableName string) []*childFKRef {
	var refs []*childFKRef

	tables := e.catalog.ListTables()
	for _, tableDef := range tables {
		for _, c := range tableDef.Constraints {
			fk, ok := c.(*catalog.ForeignKeyConstraintDef)
			if !ok {
				continue
			}

			if strings.EqualFold(fk.RefTable, parentTableName) {
				refs = append(refs, &childFKRef{
					ChildTable: tableDef,
					FK:         fk,
				})
			}
		}
	}

	return refs
}

type childFKRef struct {
	ChildTable *catalog.TableDef
	FK         *catalog.ForeignKeyConstraintDef
}

// buildChildSearchParams builds the column indices and search values for scanning a child table.
func buildChildSearchParams(ref *childFKRef, keyValues []any, keyColumns []string) ([]int, []any) {
	childColIndices := make([]int, len(ref.FK.Columns))
	searchValues := make([]any, len(ref.FK.Columns))

	for i, refCol := range ref.FK.RefColumns {
		for j, keyCol := range keyColumns {
			if strings.EqualFold(keyCol, refCol) {
				searchValues[i] = keyValues[j]

				break
			}
		}

		childColIndices[i] = -1
		for j, col := range ref.ChildTable.Columns {
			if strings.EqualFold(col.Name, ref.FK.Columns[i]) {
				childColIndices[i] = j

				break
			}
		}
	}

	return childColIndices, searchValues
}

// scanChildForMatch scans a child table looking for rows matching the given column indices and values.
func scanChildForMatch(childTable *storage.Table, childColIndices []int, searchValues []any) bool {
	scanner := childTable.Scan()

	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		for i := range chunk.Count() {
			match := true
			allNull := true

			for j, colIdx := range childColIndices {
				if colIdx < 0 {
					match = false

					break
				}

				cellVal := chunk.GetValue(i, colIdx)
				if cellVal != nil {
					allNull = false
				}

				if !fkValuesEqual(cellVal, searchValues[j]) {
					match = false

					break
				}
			}

			if match && !allNull {
				return true
			}
		}
	}

	return false
}

// checkNoChildReferences verifies no child rows reference the given key values being deleted/updated.
// parentDef is the table definition for the parent table (used for context, the name is used for lookup).
func (e *Executor) checkNoChildReferences(parentTableName string, _ *catalog.TableDef, keyValues []any, keyColumns []string) error {
	childRefs := e.findChildForeignKeys(parentTableName)

	for _, ref := range childRefs {
		childTable, ok := e.storage.GetTable(ref.ChildTable.Name)
		if !ok {
			continue
		}

		childColIndices, searchValues := buildChildSearchParams(ref, keyValues, keyColumns)

		if scanChildForMatch(childTable, childColIndices, searchValues) {
			return &dukdb.Error{
				Type: dukdb.ErrorTypeConstraint,
				Msg:  fmt.Sprintf("foreign key violation: key (%s) is still referenced by table %q", formatFKValues(keyColumns, keyValues), ref.ChildTable.Name),
			}
		}
	}

	return nil
}

// fkValuesEqual compares two values for equality (handling type coercion for numbers).
func fkValuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	// Convert to comparable form
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// formatFKValues formats FK column names and values for error messages.
func formatFKValues(columns []string, values []any) string {
	parts := make([]string, len(columns))

	for i := range columns {
		if i < len(values) {
			parts[i] = fmt.Sprintf("%s=%v", columns[i], values[i])
		} else {
			parts[i] = fmt.Sprintf("%s=?", columns[i])
		}
	}

	return strings.Join(parts, ", ")
}
