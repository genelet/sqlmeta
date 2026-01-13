package xmeta

// diff.go implements the schema comparison logic.

import (
	"google.golang.org/protobuf/proto"
)

// DiffDatabase compares two MetaDatabase states and returns the changes needed
// to transform 'current' into 'desired'.
func DiffDatabase(current, desired *MetaDatabase) []SchemaChange {
	var changes []SchemaChange

	// Build maps for efficient lookup
	currentTables := tablesByName(current.GetTables())
	desiredTables := tablesByName(desired.GetTables())

	// Find tables to drop (in current but not in desired)
	for name, currTable := range currentTables {
		if _, exists := desiredTables[name]; !exists {
			// Drop all constraints first (will be ordered by SortChanges)
			for _, elem := range currTable.Elements {
				if tc := elem.GetTableConstraintElement(); tc != nil {
					changes = append(changes, DropConstraint{
						TableName:      currTable.Name,
						ConstraintName: tc.Name,
						IsForeignKey:   tc.Spec.GetReferenceItem() != nil,
					})
				}
			}
			changes = append(changes, DropTable{TableName: currTable.Name})
		}
	}

	// Find tables to add (in desired but not in current)
	for name, desTable := range desiredTables {
		if _, exists := currentTables[name]; !exists {
			changes = append(changes, AddTable{Table: desTable})
		}
	}

	// Find tables that exist in both and diff them
	for name, desTable := range desiredTables {
		if currTable, exists := currentTables[name]; exists {
			tableChanges := diffTable(currTable, desTable)
			changes = append(changes, tableChanges...)
		}
	}

	SortChanges(changes)
	return changes
}

// diffTable compares two tables and returns the changes.
func diffTable(current, desired *MetaTable) []SchemaChange {
	var changes []SchemaChange

	// Compare table-level options and comments
	if current.Comment != desired.Comment || !mapsEqual(current.Options, desired.Options) {
		changes = append(changes, AlterTableOptions{
			TableName:  desired.Name,
			OldComment: current.Comment,
			NewComment: desired.Comment,
			OldOptions: current.Options,
			NewOptions: desired.Options,
		})
	}

	// Extract columns and constraints from elements
	currentCols := columnsFromElements(current.Elements)
	desiredCols := columnsFromElements(desired.Elements)
	currentConstraints := constraintsFromElements(current.Elements)
	desiredConstraints := constraintsFromElements(desired.Elements)

	// Diff columns
	colChanges := diffColumns(desired.Name, currentCols, desiredCols)
	changes = append(changes, colChanges...)

	// Diff constraints
	constraintChanges := diffConstraints(desired.Name, currentConstraints, desiredConstraints)
	changes = append(changes, constraintChanges...)

	return changes
}

// diffColumns compares column lists and returns changes.
func diffColumns(tableName *ObjectName, current, desired map[string]*ColumnDef) []SchemaChange {
	var changes []SchemaChange

	// Find columns to drop
	for name := range current {
		if _, exists := desired[name]; !exists {
			changes = append(changes, DropColumn{
				TableName:  tableName,
				ColumnName: name,
			})
		}
	}

	// Find columns to add
	for name, desCol := range desired {
		if _, exists := current[name]; !exists {
			changes = append(changes, AddColumn{
				TableName: tableName,
				Column:    desCol,
			})
		}
	}

	// Find columns to alter
	for name, desCol := range desired {
		if currCol, exists := current[name]; exists {
			if !columnsEqual(currCol, desCol) {
				changes = append(changes, AlterColumn{
					TableName: tableName,
					OldColumn: currCol,
					NewColumn: desCol,
				})
			}
		}
	}

	return changes
}

// diffConstraints compares constraint lists and returns changes.
func diffConstraints(tableName *ObjectName, current, desired map[string]*TableConstraint) []SchemaChange {
	var changes []SchemaChange

	// Find constraints to drop
	for name, currCon := range current {
		if _, exists := desired[name]; !exists {
			changes = append(changes, DropConstraint{
				TableName:      tableName,
				ConstraintName: name,
				IsForeignKey:   currCon.Spec.GetReferenceItem() != nil,
			})
		}
	}

	// Find constraints to add
	for name, desCon := range desired {
		if _, exists := current[name]; !exists {
			changes = append(changes, AddConstraint{
				TableName:  tableName,
				Constraint: desCon,
			})
		}
	}

	// Note: Constraint modifications are complex. For v1, we drop and re-add.
	// A future version could detect in-place modifications.

	return changes
}

// =============================================================================
// Helper Functions
// =============================================================================

// tablesByName creates a map of tables keyed by their simple name.
func tablesByName(tables []*MetaTable) map[string]*MetaTable {
	m := make(map[string]*MetaTable)
	for _, t := range tables {
		name := tableName(t.Name)
		m[name] = t
	}
	return m
}

// tableName extracts the simple table name from an ObjectName.
func tableName(on *ObjectName) string {
	if on == nil || len(on.Idents) == 0 {
		return ""
	}
	return on.Idents[len(on.Idents)-1]
}

// columnsFromElements extracts columns from TableElements into a map.
func columnsFromElements(elems []*TableElement) map[string]*ColumnDef {
	m := make(map[string]*ColumnDef)
	for _, elem := range elems {
		if col := elem.GetColumnDefElement(); col != nil {
			m[col.Name] = col
		}
	}
	return m
}

// constraintsFromElements extracts named constraints from TableElements.
func constraintsFromElements(elems []*TableElement) map[string]*TableConstraint {
	m := make(map[string]*TableConstraint)
	for _, elem := range elems {
		if tc := elem.GetTableConstraintElement(); tc != nil && tc.Name != "" {
			m[tc.Name] = tc
		}
	}
	return m
}

// columnsEqual compares two ColumnDefs for equality.
func columnsEqual(a, b *ColumnDef) bool {
	if a.Name != b.Name {
		return false
	}
	if a.Comment != b.Comment {
		return false
	}
	// Compare DataType using proto.Equal for deep comparison
	if !proto.Equal(a.DataType, b.DataType) {
		return false
	}
	// Compare Default (both are Any, use proto.Equal)
	if !proto.Equal(a.Default, b.Default) {
		return false
	}
	// For v1, skip detailed constraint comparison within column
	// Future: compare Constraints slice
	return true
}

// mapsEqual compares two string maps.
func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
