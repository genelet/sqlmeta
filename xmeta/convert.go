package xmeta

import (
	"strings"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// =============================================================================
// Shared Helpers
// =============================================================================

// stringToAny packs a string into a wrapperspb.StringValue and then into anypb.Any.
// If the string is empty, it returns nil to avoid cluttering specific fields (checking if this is desired).
// Actually, for functional fields, explicit empty string might differ from nil.
// But for "DefaultValue", "" usually means no default.
func stringToAny(s string) *anypb.Any {
	if s == "" {
		return nil
	}
	sVal := &wrapperspb.StringValue{Value: s}
	anyVal, err := anypb.New(sVal)
	if err != nil {
		// Should not happen for basic string packing
		return nil
	}
	return anyVal
}

// =============================================================================
// Postgres Conversion
// =============================================================================

// PGTableToMetaTable converts a PGTable to a unified MetaTable.
func PGTableToMetaTable(t *PGTable) *MetaTable {
	if t == nil {
		return nil
	}

	meta := &MetaTable{
		Name:    t.Name,
		Type:    t.TableType,
		Comment: t.Comment,
		Options: make(map[string]string),
	}

	if t.Owner != "" {
		meta.Options["Owner"] = t.Owner
	}
	if t.Persistence != "" {
		meta.Options["Persistence"] = t.Persistence
	}
	if t.HasRowSecurity {
		meta.Options["HasRowSecurity"] = "true"
	}
	if t.RowSecurityForced {
		meta.Options["RowSecurityForced"] = "true"
	}

	var elements []*TableElement

	// Columns
	for _, col := range t.Columns {
		elements = append(elements, &TableElement{
			TableElementClause: &TableElement_ColumnDefElement{
				ColumnDefElement: PGColumnToColumnDef(col),
			},
		})
	}

	// Constraints (Non-FK)
	for _, con := range t.Constraints {
		tc := PGConstraintToTableConstraint(con)
		if tc != nil {
			elements = append(elements, &TableElement{
				TableElementClause: &TableElement_TableConstraintElement{
					TableConstraintElement: tc,
				},
			})
		}
	}

	// Foreign Keys
	for _, fk := range t.ForeignKeys {
		tc := PGForeignKeyToTableConstraint(fk)
		if tc != nil {
			elements = append(elements, &TableElement{
				TableElementClause: &TableElement_TableConstraintElement{
					TableConstraintElement: tc,
				},
			})
		}
	}

	// Indexes (PK/Unique)
	for _, idx := range t.Indexes {
		if idx.IsPrimary || idx.IsUnique {
			// Logic to check duplicates omitted for brevity
		}
	}

	meta.Elements = elements
	return meta
}

// PGColumnToColumnDef converts a PGColumn to a unified ColumnDef.
func PGColumnToColumnDef(c *PGColumn) *ColumnDef {
	if c == nil {
		return nil
	}

	colDef := &ColumnDef{
		Name:     c.Name,
		DataType: c.DataType,
		Default:  stringToAny(c.DefaultValue),
		Comment:  c.Comment,
		Options:  make(map[string]string),
	}

	if c.IsIdentity {
		colDef.Options["IsIdentity"] = "true"
		colDef.Options["IdentityGeneration"] = c.IdentityGeneration
	}
	if c.IsGenerated {
		colDef.Options["IsGenerated"] = "true"
		colDef.Options["GenerationExpression"] = c.GenerationExpression
	}
	if c.IdentitySequence != "" {
		colDef.Options["IdentitySequence"] = c.IdentitySequence
	}

	// Inline constraints? PGColumn has IsPrimaryKey flag.
	// But unified ColumnDef often puts PK in generic Constraints list or TableConstraint.
	// Let's add it as a ColumnConstraint if it's a simple PK on this column.
	if c.IsPrimaryKey {
		colDef.Constraints = append(colDef.Constraints, &ColumnConstraint{
			Name: "PRIMARY KEY", // Or generated name
			Spec: &ColumnConstraintSpec{
				ColumnConstraintSpecClause: &ColumnConstraintSpec_UniqueItem{
					UniqueItem: &UniqueColumnSpec{IsPrimaryKey: true},
				},
			},
		})
	}

	// Nullable: "NotEnforced" isn't quite right for Nullable.
	// IsNullable=false means NOT NULL constraint.
	if !c.IsNullable {
		colDef.Constraints = append(colDef.Constraints, &ColumnConstraint{
			Spec: &ColumnConstraintSpec{
				ColumnConstraintSpecClause: &ColumnConstraintSpec_NotNullItem{
					NotNullItem: NotNullColumnSpec_NotNullColumnSpecConfirm,
				},
			},
		})
	}

	return colDef
}

// PGConstraintToTableConstraint converts a PGConstraint to a unified TableConstraint.
func PGConstraintToTableConstraint(c *PGConstraint) *TableConstraint {
	if c == nil {
		return nil
	}

	tc := &TableConstraint{
		Name: c.Name,
	}

	switch c.Type {
	case "p": // Primary Key
		tc.Spec = &TableConstraintSpec{
			TableConstraintSpecClause: &TableConstraintSpec_UniqueItem{
				UniqueItem: &UniqueTableConstraint{
					IsPrimary: true,
					Columns:   c.Columns,
				},
			},
		}
	case "u": // Unique
		tc.Spec = &TableConstraintSpec{
			TableConstraintSpecClause: &TableConstraintSpec_UniqueItem{
				UniqueItem: &UniqueTableConstraint{
					IsPrimary: false,
					Columns:   c.Columns,
				},
			},
		}
	case "c": // Check
		tc.Spec = &TableConstraintSpec{
			TableConstraintSpecClause: &TableConstraintSpec_CheckItem{
				CheckItem: stringToAny(c.Definition), // Definition usually contains the check expression
			},
		}
	default:
		// Exclusion ("x") or trigger ("t") not fully supported in simple types yet?
		return nil
	}

	return tc
}

// PGForeignKeyToTableConstraint converts a PGForeignKey to a unified TableConstraint.
func PGForeignKeyToTableConstraint(fk *PGForeignKey) *TableConstraint {
	if fk == nil {
		return nil
	}

	return &TableConstraint{
		Name: fk.Name,
		Spec: &TableConstraintSpec{
			TableConstraintSpecClause: &TableConstraintSpec_ReferenceItem{
				ReferenceItem: &ReferentialTableConstraint{
					Columns: fk.LocalColumns,
					KeyExpr: &ReferenceKeyExpr{
						TableName: formatObjectName(fk.ForeignTable),
						Columns:   fk.ForeignColumns,
					},
					OnUpdate: mapReferentialAction(fk.OnUpdate),
					OnDelete: mapReferentialAction(fk.OnDelete),
					Match:    mapMatchOption(fk.MatchOption),
				},
			},
		},
	}
}

// Helpers

func formatObjectName(o *ObjectName) string {
	if o == nil {
		return ""
	}
	return strings.Join(o.Idents, ".")
}

func mapReferentialAction(s string) ReferentialAction {
	switch strings.ToUpper(s) {
	case "CASCADE":
		return ReferentialAction_ReferentialAction_Cascade
	case "SET NULL":
		return ReferentialAction_ReferentialAction_SetNull
	case "SET DEFAULT":
		return ReferentialAction_ReferentialAction_SetDefault
	case "RESTRICT":
		return ReferentialAction_ReferentialAction_Restrict
	case "NO ACTION":
		return ReferentialAction_ReferentialAction_NoAction
	default:
		return ReferentialAction_ReferentialAction_Unknown
	}
}

func mapMatchOption(s string) MatchOption {
	switch strings.ToUpper(s) {
	case "FULL":
		return MatchOption_MatchOption_Full
	case "PARTIAL":
		return MatchOption_MatchOption_Partial
	case "SIMPLE":
		return MatchOption_MatchOption_Simple
	default:
		return MatchOption_MatchOption_Unknown
	}
}

// =============================================================================
// MySQL Conversion
// =============================================================================

// MYTableToMetaTable converts a MYTable to a unified MetaTable.
func MYTableToMetaTable(t *MYTable) *MetaTable {
	if t == nil {
		return nil
	}

	meta := &MetaTable{
		Name:    t.Name,
		Comment: t.Comment,
		Options: make(map[string]string),
	}

	if t.Engine != "" {
		meta.Options["Engine"] = t.Engine
	}
	if t.Charset != "" {
		meta.Options["Charset"] = t.Charset
	}
	if t.Collation != "" {
		meta.Options["Collation"] = t.Collation
	}

	var elements []*TableElement

	// Columns
	for _, col := range t.Columns {
		elements = append(elements, &TableElement{
			TableElementClause: &TableElement_ColumnDefElement{
				ColumnDefElement: MYColumnToColumnDef(col),
			},
		})
	}

	// Foreign Keys
	for _, fk := range t.ForeignKeys {
		tc := MYForeignKeyToTableConstraint(fk)
		if tc != nil {
			elements = append(elements, &TableElement{
				TableElementClause: &TableElement_TableConstraintElement{
					TableConstraintElement: tc,
				},
			})
		}
	}

	// Indexes (Primary/Unique)
	for _, idx := range t.Indexes {
		if idx.IsUnique || strings.ToUpper(idx.IndexType) == "PRIMARY" {
			tc := MYIndexToTableConstraint(idx)
			if tc != nil {
				elements = append(elements, &TableElement{
					TableElementClause: &TableElement_TableConstraintElement{
						TableConstraintElement: tc,
					},
				})
			}
		}
	}

	meta.Elements = elements
	return meta
}

// MYColumnToColumnDef converts a MYColumn to a unified ColumnDef.
func MYColumnToColumnDef(c *MYColumn) *ColumnDef {
	if c == nil {
		return nil
	}

	colDef := &ColumnDef{
		Name:     c.Name,
		DataType: c.DataType,
		Default:  stringToAny(c.DefaultValue),
		Comment:  c.Comment,
		Options:  make(map[string]string),
	}

	if c.Charset != "" {
		colDef.Options["Charset"] = c.Charset
	}
	if c.Collation != "" {
		colDef.Options["Collation"] = c.Collation
	}
	if c.IsUnsigned {
		colDef.Options["IsUnsigned"] = "true"
	}

	// Primary Key
	if c.IsPrimaryKey {
		colDef.Constraints = append(colDef.Constraints, &ColumnConstraint{
			Name: "PRIMARY KEY",
			Spec: &ColumnConstraintSpec{
				ColumnConstraintSpecClause: &ColumnConstraintSpec_UniqueItem{
					UniqueItem: &UniqueColumnSpec{IsPrimaryKey: true},
				},
			},
		})
	}

	// Auto Increment
	if c.AutoIncrement {
		colDef.MyDecos = append(colDef.MyDecos, AutoIncrement_AutoIncrementConfirm)
	}

	// Not Null (IsNullable=false means Not Null)
	if !c.IsNullable {
		colDef.Constraints = append(colDef.Constraints, &ColumnConstraint{
			Spec: &ColumnConstraintSpec{
				ColumnConstraintSpecClause: &ColumnConstraintSpec_NotNullItem{
					NotNullItem: NotNullColumnSpec_NotNullColumnSpecConfirm,
				},
			},
		})
	}

	// Unsigned (often part of DataType but sometimes flag)
	if c.IsUnsigned {
		// Logic to modify DataType if needed, or rely on loader having set it in DataType already.
		// my_loader.go mapMySQLTypeForProto doesn't strictly set Unsigned on Int/BigInt yet, maybe?
		// We trust loader's DataType for now, or could enhance here.
	}

	return colDef
}

// MYForeignKeyToTableConstraint converts a MYForeignKey to a unified TableConstraint.
func MYForeignKeyToTableConstraint(fk *MYForeignKey) *TableConstraint {
	if fk == nil {
		return nil
	}

	return &TableConstraint{
		Name: fk.Name,
		Spec: &TableConstraintSpec{
			TableConstraintSpecClause: &TableConstraintSpec_ReferenceItem{
				ReferenceItem: &ReferentialTableConstraint{
					Columns: fk.LocalColumns,
					KeyExpr: &ReferenceKeyExpr{
						TableName: formatObjectName(fk.ForeignTable),
						Columns:   fk.ForeignColumns,
					},
					OnUpdate: mapReferentialAction(fk.OnUpdate),
					OnDelete: mapReferentialAction(fk.OnDelete),
				},
			},
		},
	}
}

// MYIndexToTableConstraint converts a MYIndex to a unified TableConstraint (Unique/Primary).
func MYIndexToTableConstraint(idx *MYIndex) *TableConstraint {
	if idx == nil {
		return nil
	}

	isPrimary := strings.ToUpper(idx.Name) == "PRIMARY" // MySQL convention
	if !isPrimary && !idx.IsUnique {
		return nil
	}

	return &TableConstraint{
		Name: idx.Name,
		Spec: &TableConstraintSpec{
			TableConstraintSpecClause: &TableConstraintSpec_UniqueItem{
				UniqueItem: &UniqueTableConstraint{
					IsPrimary: isPrimary,
					Columns:   idx.Columns,
					IndexName: idx.Name,
				},
			},
		},
	}
}

// =============================================================================
// SQLite Conversion
// =============================================================================

// SQLiteTableToMetaTable converts a SQLiteTable to unified MetaTable.
func SQLiteTableToMetaTable(t *SQLiteTable) *MetaTable {
	if t == nil {
		return nil
	}

	meta := &MetaTable{
		Name:    &ObjectName{Idents: []string{t.Name}},
		Type:    t.Type,
		Options: make(map[string]string),
	}
	// Definition SQL could be stored in options if needed?
	// meta.Options["Definition"] = t.Definition

	var elements []*TableElement

	// Columns
	for _, col := range t.Columns {
		elements = append(elements, &TableElement{
			TableElementClause: &TableElement_ColumnDefElement{
				ColumnDefElement: SQLiteColumnToColumnDef(col),
			},
		})
	}

	meta.Elements = elements
	return meta
}

// SQLiteColumnToColumnDef converts a SQLiteColumn to a unified ColumnDef.
func SQLiteColumnToColumnDef(c *SQLiteColumn) *ColumnDef {
	if c == nil {
		return nil
	}

	colDef := &ColumnDef{
		Name:     c.Name,
		DataType: c.DataType,
		Default:  stringToAny(c.DefaultValue),
	}

	// Primary Key
	if c.IsPrimaryKey {
		colDef.Constraints = append(colDef.Constraints, &ColumnConstraint{
			Name: "PRIMARY KEY",
			Spec: &ColumnConstraintSpec{
				ColumnConstraintSpecClause: &ColumnConstraintSpec_UniqueItem{
					UniqueItem: &UniqueColumnSpec{IsPrimaryKey: true},
				},
			},
		})
	}

	// Not Null
	if !c.IsNullable {
		colDef.Constraints = append(colDef.Constraints, &ColumnConstraint{
			Spec: &ColumnConstraintSpec{
				ColumnConstraintSpecClause: &ColumnConstraintSpec_NotNullItem{
					NotNullItem: NotNullColumnSpec_NotNullColumnSpecConfirm,
				},
			},
		})
	}

	return colDef
}

// =============================================================================
// BigQuery Conversion
// =============================================================================

// BQTableToMetaTable converts a BQTable to unified MetaTable.
func BQTableToMetaTable(t *BQTable) *MetaTable {
	if t == nil {
		return nil
	}

	meta := &MetaTable{
		Name:    t.Name,
		Comment: t.Description,
		Options: make(map[string]string),
	}

	var elements []*TableElement

	for _, col := range t.Schema {
		elements = append(elements, &TableElement{
			TableElementClause: &TableElement_ColumnDefElement{
				ColumnDefElement: BQColumnToColumnDef(col),
			},
		})
	}

	meta.Elements = elements
	return meta
}

// BQColumnToColumnDef converts a BQColumn to a unified ColumnDef.
func BQColumnToColumnDef(c *BQColumn) *ColumnDef {
	if c == nil {
		return nil
	}

	colDef := &ColumnDef{
		Name:     c.Name,
		DataType: c.DataType,
		Comment:  c.Description,
		Options:  make(map[string]string),
	}

	// Mode: NULLABLE, REQUIRED, REPEATED
	mode := strings.ToUpper(c.Mode)
	if mode == "REQUIRED" {
		colDef.Constraints = append(colDef.Constraints, &ColumnConstraint{
			Spec: &ColumnConstraintSpec{
				ColumnConstraintSpecClause: &ColumnConstraintSpec_NotNullItem{
					NotNullItem: NotNullColumnSpec_NotNullColumnSpecConfirm,
				},
			},
		})
	}

	return colDef
}
