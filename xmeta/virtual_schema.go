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

	var warnings []string
	for _, pk := range overrides.GetPrimaryKeys() {
		key, warning := index.resolve(pk.GetTableName())
		if warning != "" {
			warnings = append(warnings, fmt.Sprintf("manual primary key %s target %q: %s", pk.GetName(), objectNameKey(pk.GetTableName()), warning))
		}
		if key == "" {
			continue
		}
		cols := cleanColumns(pk.GetColumns())
		if len(cols) == 0 {
			warnings = append(warnings, fmt.Sprintf("manual primary key %s on %s has no columns", pk.GetName(), key))
			continue
		}
		if missing := missingColumns(index.byKey[key], cols); len(missing) > 0 {
			for _, col := range missing {
				warnings = append(warnings, fmt.Sprintf("manual primary key %s on %s references missing column %s", pk.GetName(), key, col))
			}
			continue
		}
		removeMetaPrimaryKey(index.byKey[key])
		addMetaPrimaryKey(index.byKey[key], defaultOverrideName(pk.GetName(), "manual_pk"), cols)
		effectivePKs[key] = cols
	}

	for _, fk := range overrides.GetForeignKeys() {
		childKey, childWarning := index.resolve(fk.GetChildTable())
		parentKey, parentWarning := index.resolve(fk.GetParentTable())
		if childWarning != "" {
			warnings = append(warnings, fmt.Sprintf("manual foreign key %s child table %q: %s", fk.GetName(), objectNameKey(fk.GetChildTable()), childWarning))
		}
		if parentWarning != "" {
			warnings = append(warnings, fmt.Sprintf("manual foreign key %s parent table %q: %s", fk.GetName(), objectNameKey(fk.GetParentTable()), parentWarning))
		}
		if childKey == "" || parentKey == "" {
			continue
		}
		childCols := cleanColumns(fk.GetChildColumns())
		parentCols := cleanColumns(fk.GetParentColumns())
		if len(parentCols) == 0 {
			parentCols = effectivePKs[parentKey]
		}
		if len(childCols) == 0 {
			warnings = append(warnings, fmt.Sprintf("manual foreign key %s on %s has no child columns", fk.GetName(), childKey))
			continue
		}
		if len(parentCols) == 0 {
			warnings = append(warnings, fmt.Sprintf("manual foreign key %s on %s has no parent columns and %s has no primary key", fk.GetName(), childKey, parentKey))
			continue
		}
		if len(childCols) != 1 || len(parentCols) != 1 {
			warnings = append(warnings, fmt.Sprintf("manual foreign key %s on %s has composite columns; skipped virtual schema edge", fk.GetName(), childKey))
			continue
		}
		if missing := missingColumns(index.byKey[childKey], childCols); len(missing) > 0 {
			for _, col := range missing {
				warnings = append(warnings, fmt.Sprintf("manual foreign key %s on %s references missing child column %s", fk.GetName(), childKey, col))
			}
			continue
		}
		if missing := missingColumns(index.byKey[parentKey], parentCols); len(missing) > 0 {
			for _, col := range missing {
				warnings = append(warnings, fmt.Sprintf("manual foreign key %s on %s references missing parent column %s.%s", fk.GetName(), childKey, parentKey, col))
			}
			continue
		}
		removeMetaForeignKey(index.byKey[childKey], childCols)
		addMetaForeignKey(index.byKey[childKey], defaultOverrideName(fk.GetName(), "manual_fk"), childCols[0], index.byKey[parentKey].GetName(), parentCols[0])
	}

	return virtual, uniqueAppStrings(warnings), nil
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
