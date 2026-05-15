package tavola

import (
	"strings"
	"testing"

	"github.com/genelet/sqlmeta/xmeta"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestBuildSpecMapsTableActionsAndWarnings(t *testing.T) {
	defaultAny, err := anypb.New(&wrapperspb.StringValue{Value: "now()"})
	if err != nil {
		t.Fatal(err)
	}
	meta := &xmeta.MetaDatabase{
		Name:    "appdb",
		Options: map[string]string{"Driver": "postgres"},
		Tables: []*xmeta.MetaTable{
			{
				Name: &xmeta.ObjectName{Idents: []string{"public", "users"}},
				Elements: []*xmeta.TableElement{
					column("id", intDT(), false, true, true, nil),
					column("email", textType(), false, false, false, nil),
					column("created", timestampType(), true, false, false, defaultAny),
				},
			},
			{
				Name: &xmeta.ObjectName{Idents: []string{"public", "events"}},
				Elements: []*xmeta.TableElement{
					column("user_id", intDT(), false, false, false, nil),
					column("event_id", intDT(), false, false, false, nil),
					{
						TableElementClause: &xmeta.TableElement_TableConstraintElement{
							TableConstraintElement: &xmeta.TableConstraint{
								Name: "events_pk",
								Spec: &xmeta.TableConstraintSpec{
									TableConstraintSpecClause: &xmeta.TableConstraintSpec_UniqueItem{
										UniqueItem: &xmeta.UniqueTableConstraint{IsPrimary: true, Columns: []string{"user_id", "event_id"}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	spec, err := BuildSpec(meta, Options{Project: "ExampleApp"})
	if err != nil {
		t.Fatal(err)
	}

	if spec.Datasource.Type != "PostgreSQL" {
		t.Fatalf("datasource type = %q", spec.Datasource.Type)
	}
	users := findTable(spec, "users")
	if users == nil {
		t.Fatal("users table not found")
	}
	if users.PrimaryKey != "id" || users.AutoKey != "id" {
		t.Fatalf("users key = %q auto = %q", users.PrimaryKey, users.AutoKey)
	}
	if strings.Join(users.Insert, ",") != "email,created" {
		t.Fatalf("users insert = %#v", users.Insert)
	}
	if strings.Contains(strings.Join(users.Update, ","), "id,id") {
		t.Fatalf("users update contains duplicate primary key: %#v", users.Update)
	}
	if !strings.Contains(users.Statement, "CREATE TABLE") {
		t.Fatalf("users statement was not generated: %q", users.Statement)
	}
	if len(spec.Introspection.Warnings) == 0 {
		t.Fatal("expected warnings for synthesized DDL and composite key")
	}
}

func TestBuildSpecAuthIsExplicit(t *testing.T) {
	meta := &xmeta.MetaDatabase{
		Name:    "appdb",
		Options: map[string]string{"Driver": "mysql"},
		Tables: []*xmeta.MetaTable{{
			Name: &xmeta.ObjectName{Idents: []string{"appdb", "app_user"}},
			Options: map[string]string{
				"CreateStatement": "CREATE TABLE app_user (id int primary key, email text, passwd text)",
			},
			Elements: []*xmeta.TableElement{
				column("id", intDT(), false, true, true, nil),
				column("email", textType(), false, false, false, nil),
				column("passwd", textType(), false, false, false, nil),
			},
		}},
	}

	spec, err := BuildSpec(meta, Options{
		Project: "ExampleApp",
		Auth: AuthOptions{
			Role:     "u",
			Table:    "appdb.app_user",
			ID:       "id",
			Login:    "email",
			Password: "passwd",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Roles) != 1 {
		t.Fatalf("roles = %d, want 1", len(spec.Roles))
	}
	if spec.Roles[0].Fields.Login != "email" || spec.Roles[0].Fields.Password != "passwd" {
		t.Fatalf("unexpected auth fields: %#v", spec.Roles[0].Fields)
	}
	if got := spec.Components[0].Roles["u"]; strings.Join(got, ",") != "startnew,insert,edit,update,delete,topics" {
		t.Fatalf("role actions = %#v", got)
	}
}

func TestBuildSpecAuthRoleUsesFKScope(t *testing.T) {
	meta := &xmeta.MetaDatabase{
		Name:    "appdb",
		Options: map[string]string{"Driver": "postgres"},
		Tables: []*xmeta.MetaTable{
			{
				Name: &xmeta.ObjectName{Idents: []string{"public", "users"}},
				Elements: []*xmeta.TableElement{
					column("id", intDT(), false, true, true, nil),
					column("email", textType(), false, false, false, nil),
					column("passwd", textType(), false, false, false, nil),
				},
			},
			{
				Name: &xmeta.ObjectName{Idents: []string{"public", "posts"}},
				Elements: []*xmeta.TableElement{
					column("id", intDT(), false, true, true, nil),
					column("user_id", intDT(), false, false, false, nil),
					fkConstraint("posts_user_fk", []string{"user_id"}, "public.users", []string{"id"}),
				},
			},
			{
				Name: &xmeta.ObjectName{Idents: []string{"public", "audit_log"}},
				Elements: []*xmeta.TableElement{
					column("id", intDT(), false, true, true, nil),
				},
			},
		},
	}

	spec, err := BuildSpec(meta, Options{
		Project: "ExampleApp",
		Auth: AuthOptions{
			Role:     "u",
			Table:    "users",
			ID:       "id",
			Login:    "email",
			Password: "passwd",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	rolesByTable := map[string]map[string][]string{}
	for _, component := range spec.Components {
		rolesByTable[component.Table] = component.Roles
	}
	if len(rolesByTable["users"]["u"]) == 0 {
		t.Fatal("users component should be granted to auth role")
	}
	if len(rolesByTable["posts"]["u"]) == 0 {
		t.Fatal("posts component should be granted to auth role via FK scope")
	}
	if got := rolesByTable["audit_log"]["u"]; len(got) != 0 {
		t.Fatalf("unrelated audit_log component should not be granted, got %#v", got)
	}
}

func TestBuildSpecUsesManualPrimaryKeyOverride(t *testing.T) {
	meta := &xmeta.MetaDatabase{
		Name:    "appdb",
		Options: map[string]string{"Driver": "postgres"},
		Tables: []*xmeta.MetaTable{{
			Name: &xmeta.ObjectName{Idents: []string{"public", "users"}},
			Elements: []*xmeta.TableElement{
				column("id", intDT(), false, true, true, nil),
				column("public_id", textType(), false, false, false, nil),
				column("email", textType(), false, false, false, nil),
			},
		}},
	}

	spec, err := BuildSpec(meta, Options{
		Project: "ExampleApp",
		SchemaOverrides: &xmeta.SchemaRelationshipOverrides{
			PrimaryKeys: []*xmeta.ManualPrimaryKey{{
				Name:      "users_public_id_pk",
				TableName: xmeta.ObjectNameFromString("users"),
				Columns:   []string{"public_id"},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	users := findTable(spec, "users")
	if users == nil {
		t.Fatal("users table not found")
	}
	if users.PrimaryKey != "public_id" {
		t.Fatalf("primaryKey = %q, want public_id", users.PrimaryKey)
	}
	if !strings.Contains(strings.Join(spec.Introspection.Warnings, "\n"), "manual primary key override") {
		t.Fatalf("expected manual PK warning, got %#v", spec.Introspection.Warnings)
	}
}

func TestBuildSpecSkipsInvalidManualPrimaryKeyOverride(t *testing.T) {
	meta := &xmeta.MetaDatabase{
		Name:    "appdb",
		Options: map[string]string{"Driver": "postgres"},
		Tables: []*xmeta.MetaTable{{
			Name: &xmeta.ObjectName{Idents: []string{"public", "users"}},
			Elements: []*xmeta.TableElement{
				column("id", intDT(), false, true, true, nil),
				column("email", textType(), false, false, false, nil),
			},
		}},
	}

	spec, err := BuildSpec(meta, Options{
		Project: "ExampleApp",
		SchemaOverrides: &xmeta.SchemaRelationshipOverrides{
			PrimaryKeys: []*xmeta.ManualPrimaryKey{{
				Name:      "users_bad_pk",
				TableName: xmeta.ObjectNameFromString("users"),
				Columns:   []string{"missing_id"},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	users := findTable(spec, "users")
	if users == nil {
		t.Fatal("users table not found")
	}
	if users.PrimaryKey != "id" {
		t.Fatalf("primaryKey = %q, want id", users.PrimaryKey)
	}
	if !strings.Contains(strings.Join(spec.Introspection.Warnings, "\n"), "missing_id") {
		t.Fatalf("expected missing column warning, got %#v", spec.Introspection.Warnings)
	}
}

func TestBuildSpecRequiresAuthUserTable(t *testing.T) {
	meta := &xmeta.MetaDatabase{
		Name:    "appdb",
		Options: map[string]string{"Driver": "postgres"},
		Tables: []*xmeta.MetaTable{{
			Name: &xmeta.ObjectName{Idents: []string{"public", "users"}},
			Elements: []*xmeta.TableElement{
				column("id", intDT(), false, true, true, nil),
				column("email", textType(), false, false, false, nil),
				column("passwd", textType(), false, false, false, nil),
			},
		}},
	}

	_, err := BuildSpec(meta, Options{
		Project: "ExampleApp",
		Auth: AuthOptions{
			Role:     "u",
			Table:    "missing_users",
			ID:       "id",
			Login:    "email",
			Password: "passwd",
		},
	})
	if err == nil {
		t.Fatal("expected missing auth user table error")
	}
}

func findTable(spec *Spec, name string) *Table {
	for i := range spec.Schema.Tables {
		if spec.Schema.Tables[i].Name == name {
			return &spec.Schema.Tables[i]
		}
	}
	return nil
}

func column(name string, typ *xmeta.DataType, notNull, primary, auto bool, def *anypb.Any) *xmeta.TableElement {
	col := &xmeta.ColumnDef{Name: name, DataType: typ, Default: def}
	if notNull {
		col.Constraints = append(col.Constraints, &xmeta.ColumnConstraint{
			Spec: &xmeta.ColumnConstraintSpec{
				ColumnConstraintSpecClause: &xmeta.ColumnConstraintSpec_NotNullItem{
					NotNullItem: xmeta.NotNullColumnSpec_NotNullColumnSpecConfirm,
				},
			},
		})
	}
	if primary {
		col.Constraints = append(col.Constraints, &xmeta.ColumnConstraint{
			Spec: &xmeta.ColumnConstraintSpec{
				ColumnConstraintSpecClause: &xmeta.ColumnConstraintSpec_UniqueItem{
					UniqueItem: &xmeta.UniqueColumnSpec{IsPrimaryKey: true},
				},
			},
		})
	}
	if auto {
		col.MyDecos = append(col.MyDecos, xmeta.AutoIncrement_AutoIncrementConfirm)
	}
	return &xmeta.TableElement{
		TableElementClause: &xmeta.TableElement_ColumnDefElement{ColumnDefElement: col},
	}
}

func fkConstraint(name string, local []string, foreignTable string, foreign []string) *xmeta.TableElement {
	return &xmeta.TableElement{
		TableElementClause: &xmeta.TableElement_TableConstraintElement{
			TableConstraintElement: &xmeta.TableConstraint{
				Name: name,
				Spec: &xmeta.TableConstraintSpec{
					TableConstraintSpecClause: &xmeta.TableConstraintSpec_ReferenceItem{
						ReferenceItem: &xmeta.ReferentialTableConstraint{
							Columns: local,
							KeyExpr: &xmeta.ReferenceKeyExpr{
								TableName: foreignTable,
								Columns:   foreign,
							},
						},
					},
				},
			},
		},
	}
}

func intDT() *xmeta.DataType {
	return &xmeta.DataType{TypeClause: &xmeta.DataType_IntData{IntData: &xmeta.Int{}}}
}

func textType() *xmeta.DataType {
	return &xmeta.DataType{TypeClause: &xmeta.DataType_TextData{TextData: xmeta.DataTypeSingle_Text}}
}

func timestampType() *xmeta.DataType {
	return &xmeta.DataType{TypeClause: &xmeta.DataType_TimestampData{TimestampData: &xmeta.Timestamp{}}}
}
