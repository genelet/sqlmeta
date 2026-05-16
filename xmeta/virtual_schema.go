package xmeta

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
)

// ApplySchemaRelationshipOverrides returns a cloned MetaDatabase with manual
// app-level PK/FK relationships applied as a virtual schema. The database is
// not modified. Invalid overrides are skipped and returned as warnings.
func ApplySchemaRelationshipOverrides(meta *MetaDatabase, overrides *SchemaRelationshipOverrides) (*MetaDatabase, []string, error) {
	virtual, diagnostics, err := ApplySchemaRelationshipOverridesWithDiagnostics(meta, overrides)
	return virtual, DiagnosticMessages(diagnostics), err
}

// ApplySchemaRelationshipOverridesWithDiagnostics is ApplySchemaRelationshipOverrides
// with stable diagnostic codes retained beside each warning message.
func ApplySchemaRelationshipOverridesWithDiagnostics(meta *MetaDatabase, overrides *SchemaRelationshipOverrides) (*MetaDatabase, []Diagnostic, error) {
	if meta == nil {
		return nil, nil, fmt.Errorf("metadata database is nil")
	}
	virtual := proto.Clone(meta).(*MetaDatabase)
	if overrides == nil {
		return virtual, nil, nil
	}

	index := newAppTableIndex(virtual.GetTables())
	effectivePKs := map[string][]string{}
	for _, table := range virtual.GetTables() {
		if pks := primaryKeyColumns(table); len(pks) > 0 {
			effectivePKs[objectNameKey(table.GetName())] = pks
		}
	}

	var diagnostics []Diagnostic
	for _, pk := range overrides.GetPrimaryKeys() {
		override := resolveManualPrimaryKeyOverride(index, pk)
		diagnostics = append(diagnostics, override.diagnostics...)
		if !override.ok {
			continue
		}
		removeMetaPrimaryKey(index.byKey[override.key])
		addMetaPrimaryKey(index.byKey[override.key], defaultOverrideName(pk.GetName(), "manual_pk"), override.columns)
		effectivePKs[override.key] = override.columns
	}

	for _, fk := range overrides.GetForeignKeys() {
		override := resolveManualForeignKeyOverride(index, effectivePKs, fk, "virtual schema")
		diagnostics = append(diagnostics, override.diagnostics...)
		if !override.ok {
			continue
		}
		removeMetaForeignKey(index.byKey[override.childKey], override.childCols)
		addMetaForeignKey(index.byKey[override.childKey], defaultOverrideName(fk.GetName(), "manual_fk"), override.childCols[0], index.byKey[override.parentKey].GetName(), override.parentCols[0])
	}

	return virtual, uniqueDiagnostics(diagnostics), nil
}

func missingColumns(table *MetaTable, columns []string) []string {
	var missing []string
	for _, column := range columns {
		if !tableHasColumn(table, column) {
			missing = append(missing, column)
		}
	}
	return missing
}

func removeMetaPrimaryKey(table *MetaTable) {
	if table == nil {
		return
	}
	for _, elem := range table.GetElements() {
		if col := elem.GetColumnDefElement(); col != nil {
			filtered := col.Constraints[:0]
			for _, constraint := range col.GetConstraints() {
				if constraint.GetSpec().GetUniqueItem().GetIsPrimaryKey() {
					continue
				}
				filtered = append(filtered, constraint)
			}
			col.Constraints = filtered
			continue
		}
		constraint := elem.GetTableConstraintElement()
		if constraint.GetSpec().GetUniqueItem().GetIsPrimary() {
			elem.TableElementClause = nil
		}
	}
	table.Elements = compactMetaElements(table.GetElements())
}

func addMetaPrimaryKey(table *MetaTable, name string, columns []string) {
	if table == nil {
		return
	}
	table.Elements = append(table.Elements, &TableElement{
		TableElementClause: &TableElement_TableConstraintElement{
			TableConstraintElement: &TableConstraint{
				Name: name,
				Spec: &TableConstraintSpec{
					TableConstraintSpecClause: &TableConstraintSpec_UniqueItem{
						UniqueItem: &UniqueTableConstraint{IsPrimary: true, Columns: columns},
					},
				},
			},
		},
	})
}

func removeMetaForeignKey(table *MetaTable, columns []string) {
	if table == nil {
		return
	}
	key := overrideColumnKey(columns)
	for _, elem := range table.GetElements() {
		if col := elem.GetColumnDefElement(); col != nil {
			filtered := col.Constraints[:0]
			for _, constraint := range col.GetConstraints() {
				if constraint.GetSpec().GetReferenceItem() != nil && overrideColumnKey([]string{col.GetName()}) == key {
					continue
				}
				filtered = append(filtered, constraint)
			}
			col.Constraints = filtered
			continue
		}
		constraint := elem.GetTableConstraintElement()
		ref := constraint.GetSpec().GetReferenceItem()
		if ref != nil && overrideColumnKey(ref.GetColumns()) == key {
			elem.TableElementClause = nil
		}
	}
	table.Elements = compactMetaElements(table.GetElements())
}

func addMetaForeignKey(table *MetaTable, name, childColumn string, parentTable *ObjectName, parentColumn string) {
	if table == nil {
		return
	}
	table.Elements = append(table.Elements, &TableElement{
		TableElementClause: &TableElement_TableConstraintElement{
			TableConstraintElement: &TableConstraint{
				Name: name,
				Spec: &TableConstraintSpec{
					TableConstraintSpecClause: &TableConstraintSpec_ReferenceItem{
						ReferenceItem: &ReferentialTableConstraint{
							Columns: []string{childColumn},
							KeyExpr: &ReferenceKeyExpr{
								TableName:       objectNameKey(parentTable),
								TableObjectName: cloneObjectName(parentTable),
								Columns:         []string{parentColumn},
							},
						},
					},
				},
			},
		},
	})
}

func compactMetaElements(elements []*TableElement) []*TableElement {
	out := elements[:0]
	for _, elem := range elements {
		if elem.GetColumnDefElement() != nil || elem.GetTableConstraintElement() != nil {
			out = append(out, elem)
		}
	}
	return out
}

func overrideColumnKey(columns []string) string {
	cleaned := cleanColumns(columns)
	for i := range cleaned {
		cleaned[i] = strings.ToLower(cleaned[i])
	}
	return strings.Join(cleaned, ",")
}

func defaultOverrideName(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
