package xmeta

import (
	"strings"
	"testing"
)

func TestBuildDefaultAppSpecFromMetadata(t *testing.T) {
	meta := &MetaDatabase{
		Name:    "appdb",
		Options: map[string]string{"Driver": "postgres"},
		Tables: []*MetaTable{
			testAppTable("public.posts", "id", "user_id"),
			testAppTable("public.users", "id", "email"),
		},
	}

	spec, err := BuildDefaultAppSpec(meta, AppSpecOptions{
		Name: "Example",
		Auth: &AuthBinding{
			UserTable:      ObjectNameFromString("users"),
			UserIDColumn:   "id",
			LoginColumn:    "email",
			PasswordColumn: "passwd",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if spec.GetName() != "Example" || spec.GetDatasource().GetDriver() != "postgres" {
		t.Fatalf("unexpected app metadata: %#v", spec)
	}
	if got := componentNames(spec.GetComponents()); strings.Join(got, ",") != "posts,users" {
		t.Fatalf("components = %#v", got)
	}
	if len(spec.GetRoles()) != 1 {
		t.Fatalf("roles = %d, want 1", len(spec.GetRoles()))
	}
	role := spec.GetRoles()[0]
	if role.GetScope().GetMode() != FKTraversalMode_FKTraversalModeAuthUserDescendants {
		t.Fatalf("scope mode = %s", role.GetScope().GetMode())
	}
	if !role.GetScope().GetIncludeStartTable() {
		t.Fatal("default auth scope should include the auth table")
	}
}

func TestExpandRoleScopesTraversesChildrenAndDescendants(t *testing.T) {
	meta := &MetaDatabase{
		Name: "appdb",
		Tables: []*MetaTable{
			testAppTable("public.users", "id"),
			testAppTable("public.posts", "id", "user_id"),
			testAppTable("public.comments", "id", "post_id"),
			testAppTable("public.audit_log", "id"),
		},
	}
	addTestFK(meta.Tables[1], "posts_user_fk", []string{"user_id"}, "public.users", []string{"id"})
	addTestFK(meta.Tables[2], "comments_post_fk", []string{"post_id"}, "public.posts", []string{"id"})
	spec, err := BuildDefaultAppSpec(meta, AppSpecOptions{
		Name: "Example",
		Auth: &AuthBinding{
			UserTable:      ObjectNameFromString("users"),
			UserIDColumn:   "id",
			LoginColumn:    "email",
			PasswordColumn: "passwd",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	expanded, err := ExpandRoleScopes(meta, spec)
	if err != nil {
		t.Fatal(err)
	}

	grants := tableGrantKeys(expanded.GetTableGrants())
	if strings.Join(grants, ",") != "public.comments,public.posts,public.users" {
		t.Fatalf("table grants = %#v", grants)
	}
	if containsAppString(grants, "public.audit_log") {
		t.Fatalf("unrelated table should not be granted: %#v", grants)
	}
	if len(expanded.GetComponentGrants()) != 3 {
		t.Fatalf("component grants = %d, want 3", len(expanded.GetComponentGrants()))
	}
}

func TestExpandRoleScopesFallbackAllTablesIsExplicit(t *testing.T) {
	meta := &MetaDatabase{
		Name: "appdb",
		Tables: []*MetaTable{
			testAppTable("users", "id"),
			testAppTable("audit_log", "id"),
		},
	}
	spec, err := BuildDefaultAppSpec(meta, AppSpecOptions{
		Name: "Example",
		Auth: &AuthBinding{
			UserTable:      ObjectNameFromString("users"),
			UserIDColumn:   "id",
			LoginColumn:    "email",
			PasswordColumn: "passwd",
		},
		FallbackAllTables: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	expanded, err := ExpandRoleScopes(meta, spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(tableGrantKeys(expanded.GetTableGrants()), ","); got != "audit_log,users" {
		t.Fatalf("fallback grants = %s", got)
	}
}

func TestExpandRoleScopesUsesManualFKAndManualParentPK(t *testing.T) {
	meta := &MetaDatabase{
		Name: "appdb",
		Tables: []*MetaTable{
			testAppTable("users", "id", "public_id"),
			testAppTable("orders", "id", "user_public_id"),
			testAppTable("audit_log", "id"),
		},
	}
	spec, err := BuildDefaultAppSpec(meta, AppSpecOptions{
		Name: "Example",
		Auth: &AuthBinding{
			UserTable:      ObjectNameFromString("users"),
			UserIDColumn:   "id",
			LoginColumn:    "email",
			PasswordColumn: "passwd",
		},
		SchemaOverrides: &SchemaRelationshipOverrides{
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
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	expanded, err := ExpandRoleScopes(meta, spec)
	if err != nil {
		t.Fatal(err)
	}

	if got := strings.Join(tableGrantKeys(expanded.GetTableGrants()), ","); got != "orders,users" {
		t.Fatalf("table grants = %s", got)
	}
	if warnings := strings.Join(expanded.GetWarnings(), "\n"); !strings.Contains(warnings, "includes manual primary key users.(public_id)") {
		t.Fatalf("expected manual primary key traversal warning, got:\n%s", warnings)
	}
}

func TestExpandRoleScopesMergesManualAndPhysicalRelationships(t *testing.T) {
	meta := &MetaDatabase{
		Name: "appdb",
		Tables: []*MetaTable{
			testAppTable("users", "id", "public_id"),
			testAppTable("posts", "id", "user_id"),
			testAppTable("orders", "id", "user_public_id"),
		},
	}
	addTestFK(meta.Tables[1], "posts_user_fk", []string{"user_id"}, "users", []string{"id"})
	spec, err := BuildDefaultAppSpec(meta, AppSpecOptions{
		Name: "Example",
		Auth: &AuthBinding{
			UserTable:      ObjectNameFromString("users"),
			UserIDColumn:   "id",
			LoginColumn:    "email",
			PasswordColumn: "passwd",
		},
		SchemaOverrides: &SchemaRelationshipOverrides{
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
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	expanded, err := ExpandRoleScopes(meta, spec)
	if err != nil {
		t.Fatal(err)
	}

	if got := strings.Join(tableGrantKeys(expanded.GetTableGrants()), ","); got != "orders,posts,users" {
		t.Fatalf("manual and physical grants should merge, got %s", got)
	}
}

func TestExpandRoleScopesManualFKOverridesPhysicalConflict(t *testing.T) {
	meta := &MetaDatabase{
		Name: "appdb",
		Tables: []*MetaTable{
			testAppTable("users", "id"),
			testAppTable("accounts", "id"),
			testAppTable("posts", "id", "user_id"),
		},
	}
	addTestFK(meta.Tables[2], "posts_user_fk", []string{"user_id"}, "users", []string{"id"})
	spec, err := BuildDefaultAppSpec(meta, AppSpecOptions{
		Name: "Example",
		Auth: &AuthBinding{
			UserTable:      ObjectNameFromString("users"),
			UserIDColumn:   "id",
			LoginColumn:    "email",
			PasswordColumn: "passwd",
		},
		SchemaOverrides: &SchemaRelationshipOverrides{
			ForeignKeys: []*ManualForeignKey{{
				Name:          "posts_account_fk",
				ChildTable:    ObjectNameFromString("posts"),
				ChildColumns:  []string{"user_id"},
				ParentTable:   ObjectNameFromString("accounts"),
				ParentColumns: []string{"id"},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	expanded, err := ExpandRoleScopes(meta, spec)
	if err != nil {
		t.Fatal(err)
	}

	if got := strings.Join(tableGrantKeys(expanded.GetTableGrants()), ","); got != "users" {
		t.Fatalf("manual FK should override physical FK conflict, grants = %s", got)
	}
	if warnings := strings.Join(expanded.GetWarnings(), "\n"); !strings.Contains(warnings, "found no child foreign keys") {
		t.Fatalf("expected no-child warning, got:\n%s", warnings)
	}
}

func TestExpandRoleScopesWarnsForCyclesAndCompositeFKs(t *testing.T) {
	meta := &MetaDatabase{
		Name: "appdb",
		Tables: []*MetaTable{
			testAppTable("users", "id", "favorite_post_id"),
			testAppTable("posts", "id", "user_id"),
			testAppTable("memberships", "user_id", "org_id"),
		},
	}
	addTestFK(meta.Tables[0], "users_favorite_post_fk", []string{"favorite_post_id"}, "posts", []string{"id"})
	addTestFK(meta.Tables[1], "posts_user_fk", []string{"user_id"}, "users", []string{"id"})
	addTestFK(meta.Tables[2], "memberships_user_fk", []string{"user_id", "org_id"}, "users", []string{"id", "org_id"})
	spec, err := BuildDefaultAppSpec(meta, AppSpecOptions{
		Name: "Example",
		Auth: &AuthBinding{
			UserTable:      ObjectNameFromString("users"),
			UserIDColumn:   "id",
			LoginColumn:    "email",
			PasswordColumn: "passwd",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	expanded, err := ExpandRoleScopes(meta, spec)
	if err != nil {
		t.Fatal(err)
	}

	if got := strings.Join(tableGrantKeys(expanded.GetTableGrants()), ","); got != "posts,users" {
		t.Fatalf("table grants = %s", got)
	}
	warnings := strings.Join(expanded.GetWarnings(), "\n")
	if !strings.Contains(warnings, "cycle") {
		t.Fatalf("expected cycle warning, got:\n%s", warnings)
	}
	if !strings.Contains(warnings, "composite FK") {
		t.Fatalf("expected composite FK warning, got:\n%s", warnings)
	}
}

func testAppTable(name string, columns ...string) *MetaTable {
	table := &MetaTable{Name: ObjectNameFromString(name)}
	for _, column := range columns {
		table.Elements = append(table.Elements, &TableElement{
			TableElementClause: &TableElement_ColumnDefElement{
				ColumnDefElement: &ColumnDef{Name: column},
			},
		})
	}
	return table
}

func addTestFK(table *MetaTable, name string, local []string, foreignTable string, foreign []string) {
	table.Elements = append(table.Elements, &TableElement{
		TableElementClause: &TableElement_TableConstraintElement{
			TableConstraintElement: &TableConstraint{
				Name: name,
				Spec: &TableConstraintSpec{
					TableConstraintSpecClause: &TableConstraintSpec_ReferenceItem{
						ReferenceItem: &ReferentialTableConstraint{
							Columns: local,
							KeyExpr: &ReferenceKeyExpr{
								TableName: foreignTable,
								Columns:   foreign,
							},
						},
					},
				},
			},
		},
	})
}

func componentNames(components []*AppComponent) []string {
	var names []string
	for _, component := range components {
		names = append(names, component.GetName())
	}
	return names
}

func tableGrantKeys(grants []*ExpandedTableGrant) []string {
	var keys []string
	for _, grant := range grants {
		keys = append(keys, objectNameKey(grant.GetTableName()))
	}
	return keys
}
