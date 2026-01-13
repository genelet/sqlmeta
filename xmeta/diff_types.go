package xmeta

// diff_types.go defines the types representing schema changes.
// These are used as the output of the Diff engine.

// SchemaChange is the common interface for all schema change types.
type SchemaChange interface {
	// IsDestructive returns true if the change can cause data loss.
	IsDestructive() bool
	// Priority returns the execution order priority (lower = earlier).
	Priority() int
}

// =============================================================================
// Table-level Changes
// =============================================================================

// AddTable represents adding a new table.
type AddTable struct {
	Table *MetaTable
}

func (c AddTable) IsDestructive() bool { return false }
func (c AddTable) Priority() int       { return 40 } // After drops, before columns

// DropTable represents dropping an existing table.
type DropTable struct {
	TableName *ObjectName
}

func (c DropTable) IsDestructive() bool { return true }
func (c DropTable) Priority() int       { return 30 } // After drop columns

// AlterTableOptions represents changing table-level options (comment, engine, etc).
type AlterTableOptions struct {
	TableName  *ObjectName
	OldOptions map[string]string
	NewOptions map[string]string
	OldComment string
	NewComment string
}

func (c AlterTableOptions) IsDestructive() bool { return false }
func (c AlterTableOptions) Priority() int       { return 70 } // Last

// =============================================================================
// Column-level Changes
// =============================================================================

// AddColumn represents adding a new column to a table.
type AddColumn struct {
	TableName *ObjectName
	Column    *ColumnDef
}

func (c AddColumn) IsDestructive() bool { return false }
func (c AddColumn) Priority() int       { return 50 } // After add table

// DropColumn represents dropping an existing column.
type DropColumn struct {
	TableName  *ObjectName
	ColumnName string
}

func (c DropColumn) IsDestructive() bool { return true }
func (c DropColumn) Priority() int       { return 20 } // After drop constraints

// AlterColumn represents changing a column's definition.
type AlterColumn struct {
	TableName *ObjectName
	OldColumn *ColumnDef
	NewColumn *ColumnDef
}

// IsDestructive: true if type is being narrowed or changed incompatibly.
// For now, any type change is considered potentially destructive.
func (c AlterColumn) IsDestructive() bool {
	// Conservative: any type change is destructive
	// A smarter implementation would check if it's a widening change
	return true
}
func (c AlterColumn) Priority() int { return 70 }

// =============================================================================
// Constraint-level Changes
// =============================================================================

// AddConstraint represents adding a constraint to a table.
type AddConstraint struct {
	TableName  *ObjectName
	Constraint *TableConstraint
}

func (c AddConstraint) IsDestructive() bool { return false }
func (c AddConstraint) Priority() int       { return 60 } // After add columns

// DropConstraint represents dropping a constraint.
type DropConstraint struct {
	TableName      *ObjectName
	ConstraintName string
	IsForeignKey   bool // FKs must be dropped before referencing table is dropped
}

func (c DropConstraint) IsDestructive() bool { return false } // Dropping constraint doesn't lose data
func (c DropConstraint) Priority() int {
	if c.IsForeignKey {
		return 5 // FK constraints dropped first
	}
	return 10
}

// =============================================================================
// Utility: Sort Changes
// =============================================================================

// SortChanges sorts schema changes by priority for safe execution order.
func SortChanges(changes []SchemaChange) {
	// Simple bubble sort for clarity; could use sort.Slice
	for i := 0; i < len(changes); i++ {
		for j := i + 1; j < len(changes); j++ {
			if changes[i].Priority() > changes[j].Priority() {
				changes[i], changes[j] = changes[j], changes[i]
			}
		}
	}
}
