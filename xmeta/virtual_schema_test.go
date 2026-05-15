package xmeta

import (
	"strings"
	"testing"
)

func TestApplySchemaRelationshipOverridesBuildsVirtualSchema(t *testing.T) {
	meta := &MetaDatabase{
		Name: "appdb",
		Tables: []*MetaTable{
			testAppTable("users", "id", "public_id"),
			testAppTable("orders", "id", "user_public_id"),
			testAppTable("audit_log", "id"),
		},
	}
	addTestPrimaryKey(meta.Tables[0], "users_pk", []string{"id"})
	addTestPrimaryKey(meta.Tables[1], "orders_pk", []string{"id"})
	addTestFK(meta.Tables[1], "orders_old_user_fk", []string{"user_public_id"}, "audit_log", []string{"id"})

	virtual, warnings, err := ApplySchemaRelationshipOverrides(meta, &SchemaRelationshipOverrides{
		PrimaryKeys: []*ManualPrimaryKey{{
			Name:      "users_public_id_pk",
			TableName: ObjectNameFromString("users"),
			Columns:   []string{"public_id"},
		}},
		ForeignKeys: []*ManualForeignKey{{
			Name:         "orders_user_public_id_fk",
			ChildTable:   ObjectNameFromString("orders"),
			ChildColumns: []string{"user_public_id"},
			ParentTable:  ObjectNameFromString("users"),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v", warnings)
	}
	if got := strings.Join(primaryKeyColumns(virtual.Tables[0]), ","); got != "public_id" {
		t.Fatalf("virtual users PK = %s, want public_id", got)
	}
	if got := strings.Join(primaryKeyColumns(meta.Tables[0]), ","); got != "id" {
		t.Fatalf("physical metadata was mutated, PK = %s", got)
	}
	graph := buildFKGraph(virtual.GetTables(), newAppTableIndex(virtual.GetTables()), nil)
	if len(graph.children["users"]) != 1 {
		t.Fatalf("virtual FK graph children = %#v", graph.children)
	}
	edge := graph.children["users"][0]
	if edge.childKey != "orders" || edge.localColumn != "user_public_id" || edge.refColumn != "public_id" {
		t.Fatalf("unexpected virtual edge: %#v", edge)
	}
}

func TestApplySchemaRelationshipOverridesSkipsInvalidOverrides(t *testing.T) {
	meta := &MetaDatabase{
		Name: "appdb",
		Tables: []*MetaTable{
			testAppTable("users", "id"),
			testAppTable("orders", "id", "user_id"),
		},
	}
	addTestPrimaryKey(meta.Tables[0], "users_pk", []string{"id"})

	virtual, warnings, err := ApplySchemaRelationshipOverrides(meta, &SchemaRelationshipOverrides{
		PrimaryKeys: []*ManualPrimaryKey{{
			Name:      "bad_pk",
			TableName: ObjectNameFromString("users"),
			Columns:   []string{"missing_id"},
		}},
		ForeignKeys: []*ManualForeignKey{{
			Name:          "bad_fk",
			ChildTable:    ObjectNameFromString("orders"),
			ChildColumns:  []string{"missing_user_id"},
			ParentTable:   ObjectNameFromString("users"),
			ParentColumns: []string{"id"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	gotWarnings := strings.Join(warnings, "\n")
	for _, want := range []string{"missing_id", "missing_user_id"} {
		if !strings.Contains(gotWarnings, want) {
			t.Fatalf("expected warning containing %q, got:\n%s", want, gotWarnings)
		}
	}
	if got := strings.Join(primaryKeyColumns(virtual.Tables[0]), ","); got != "id" {
		t.Fatalf("invalid PK override should be skipped, got %s", got)
	}
}

func addTestPrimaryKey(table *MetaTable, name string, columns []string) {
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
