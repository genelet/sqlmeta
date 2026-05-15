package xmeta

import "fmt"

const (
	ContractScenarioManualPKFK       = "manual_pk_fk"
	ContractScenarioInvalidOverrides = "invalid_overrides"
	ContractScenarioMissingAuthTable = "missing_auth_table"

	ManualPKFKExpandedFixture       = ContractScenarioManualPKFK + ".expanded_app_spec.json"
	InvalidOverridesExpandedFixture = ContractScenarioInvalidOverrides + ".expanded_app_spec.json"
)

// ContractScenario describes the source inputs for a generated contract fixture.
type ContractScenario struct {
	Name            string
	AppName         string
	Meta            *MetaDatabase
	Auth            *AuthBinding
	RoleName        string
	SchemaOverrides *SchemaRelationshipOverrides
}

// LoadContractScenario returns the source inputs for a named contract scenario.
func LoadContractScenario(name string) (*ContractScenario, error) {
	switch name {
	case ContractScenarioManualPKFK:
		return manualPKFKScenario(), nil
	case ContractScenarioInvalidOverrides:
		return invalidOverridesScenario(), nil
	case ContractScenarioMissingAuthTable:
		return missingAuthTableScenario(), nil
	default:
		return nil, fmt.Errorf("unknown contract scenario %q", name)
	}
}

func manualPKFKScenario() *ContractScenario {
	return &ContractScenario{
		Name:    ContractScenarioManualPKFK,
		AppName: "Manual PK/FK",
		Meta: &MetaDatabase{
			Name:    "app.sqlite",
			Options: map[string]string{"Driver": "sqlite"},
			Tables: []*MetaTable{
				{
					Name: ObjectNameFromString("audit_log"),
					Type: "table",
					Options: map[string]string{
						"CreateStatement": "CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT NOT NULL)",
					},
					Elements: []*TableElement{
						contractColumn("id", contractIntType(), true, true),
						contractColumn("message", contractTextType(), true, false),
					},
				},
				{
					Name: ObjectNameFromString("posts"),
					Type: "table",
					Options: map[string]string{
						"CreateStatement": "CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_public_id TEXT NOT NULL, title TEXT NOT NULL, created TEXT)",
					},
					Elements: []*TableElement{
						contractColumn("id", contractIntType(), true, true),
						contractColumn("user_public_id", contractTextType(), true, false),
						contractColumn("title", contractTextType(), true, false),
						contractColumn("created", contractTextType(), false, false),
						contractFK("posts_physical_audit_fk", []string{"user_public_id"}, "audit_log", []string{"id"}),
					},
				},
				{
					Name: ObjectNameFromString("users"),
					Type: "table",
					Options: map[string]string{
						"CreateStatement": "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT NOT NULL UNIQUE, email TEXT NOT NULL, passwd TEXT NOT NULL, firstname TEXT, lastname TEXT)",
					},
					Elements: []*TableElement{
						contractColumn("id", contractIntType(), true, true),
						contractColumn("public_id", contractTextType(), true, false),
						contractColumn("email", contractTextType(), true, false),
						contractColumn("passwd", contractTextType(), true, false),
						contractColumn("firstname", contractTextType(), false, false),
						contractColumn("lastname", contractTextType(), false, false),
					},
				},
			},
		},
		Auth: &AuthBinding{
			UserTable:       ObjectNameFromString("users"),
			UserIDColumn:    "public_id",
			LoginColumn:     "email",
			PasswordColumn:  "passwd",
			FirstNameColumn: "firstname",
			LastNameColumn:  "lastname",
		},
		RoleName: "u",
		SchemaOverrides: &SchemaRelationshipOverrides{
			PrimaryKeys: []*ManualPrimaryKey{{
				Name:      "users_public_id_pk",
				TableName: ObjectNameFromString("users"),
				Columns:   []string{"public_id"},
			}},
			ForeignKeys: []*ManualForeignKey{{
				Name:         "posts_user_public_id_fk",
				ChildTable:   ObjectNameFromString("posts"),
				ChildColumns: []string{"user_public_id"},
				ParentTable:  ObjectNameFromString("users"),
			}},
		},
	}
}

func invalidOverridesScenario() *ContractScenario {
	return &ContractScenario{
		Name:    ContractScenarioInvalidOverrides,
		AppName: "Invalid Overrides",
		Meta: &MetaDatabase{
			Name:    "app.sqlite",
			Options: map[string]string{"Driver": "sqlite"},
			Tables: []*MetaTable{
				{
					Name: ObjectNameFromString("archive.teams"),
					Type: "table",
					Options: map[string]string{
						"CreateStatement": "CREATE TABLE archive_teams (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
					},
					Elements: []*TableElement{
						contractColumn("id", contractIntType(), true, true),
						contractColumn("name", contractTextType(), true, false),
					},
				},
				{
					Name: ObjectNameFromString("audit_log"),
					Type: "table",
					Options: map[string]string{
						"CreateStatement": "CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT NOT NULL)",
					},
					Elements: []*TableElement{
						contractColumn("id", contractIntType(), true, true),
						contractColumn("message", contractTextType(), true, false),
					},
				},
				{
					Name: ObjectNameFromString("memberships"),
					Type: "table",
					Options: map[string]string{
						"CreateStatement": "CREATE TABLE memberships (user_id INTEGER NOT NULL, org_id INTEGER NOT NULL, role TEXT NOT NULL)",
					},
					Elements: []*TableElement{
						contractColumn("user_id", contractIntType(), true, false),
						contractColumn("org_id", contractIntType(), true, false),
						contractColumn("role", contractTextType(), true, false),
					},
				},
				{
					Name: ObjectNameFromString("posts"),
					Type: "table",
					Options: map[string]string{
						"CreateStatement": "CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, team_id INTEGER, title TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id))",
					},
					Elements: []*TableElement{
						contractColumn("id", contractIntType(), true, true),
						contractColumn("user_id", contractIntType(), true, false),
						contractColumn("team_id", contractIntType(), false, false),
						contractColumn("title", contractTextType(), true, false),
						contractFK("posts_user_id_fk", []string{"user_id"}, "users", []string{"id"}),
					},
				},
				{
					Name: ObjectNameFromString("public.teams"),
					Type: "table",
					Options: map[string]string{
						"CreateStatement": "CREATE TABLE teams (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
					},
					Elements: []*TableElement{
						contractColumn("id", contractIntType(), true, true),
						contractColumn("name", contractTextType(), true, false),
					},
				},
				{
					Name: ObjectNameFromString("users"),
					Type: "table",
					Options: map[string]string{
						"CreateStatement": "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, org_id INTEGER, email TEXT NOT NULL, passwd TEXT NOT NULL, firstname TEXT, lastname TEXT)",
					},
					Elements: []*TableElement{
						contractColumn("id", contractIntType(), true, true),
						contractColumn("org_id", contractIntType(), false, false),
						contractColumn("email", contractTextType(), true, false),
						contractColumn("passwd", contractTextType(), true, false),
						contractColumn("firstname", contractTextType(), false, false),
						contractColumn("lastname", contractTextType(), false, false),
					},
				},
			},
		},
		Auth: &AuthBinding{
			UserTable:       ObjectNameFromString("users"),
			UserIDColumn:    "id",
			LoginColumn:     "email",
			PasswordColumn:  "passwd",
			FirstNameColumn: "firstname",
			LastNameColumn:  "lastname",
		},
		RoleName: "u",
		SchemaOverrides: &SchemaRelationshipOverrides{
			PrimaryKeys: []*ManualPrimaryKey{{
				Name:      "users_missing_public_id_pk",
				TableName: ObjectNameFromString("users"),
				Columns:   []string{"missing_public_id"},
			}},
			ForeignKeys: []*ManualForeignKey{{
				Name:          "posts_missing_child_fk",
				ChildTable:    ObjectNameFromString("posts"),
				ChildColumns:  []string{"missing_user_id"},
				ParentTable:   ObjectNameFromString("users"),
				ParentColumns: []string{"id"},
			}, {
				Name:          "posts_ambiguous_team_fk",
				ChildTable:    ObjectNameFromString("posts"),
				ChildColumns:  []string{"team_id"},
				ParentTable:   ObjectNameFromString("teams"),
				ParentColumns: []string{"id"},
			}, {
				Name:          "memberships_user_composite_fk",
				ChildTable:    ObjectNameFromString("memberships"),
				ChildColumns:  []string{"user_id", "org_id"},
				ParentTable:   ObjectNameFromString("users"),
				ParentColumns: []string{"id", "org_id"},
			}},
		},
	}
}

func missingAuthTableScenario() *ContractScenario {
	scenario := invalidOverridesScenario()
	scenario.Name = ContractScenarioMissingAuthTable
	scenario.AppName = "Missing Auth Table"
	scenario.Auth.UserTable = ObjectNameFromString("missing_users")
	return scenario
}

func contractColumn(name string, dataType *DataType, notNull, auto bool) *TableElement {
	col := &ColumnDef{Name: name, DataType: dataType}
	if notNull {
		col.Constraints = append(col.Constraints, &ColumnConstraint{
			Spec: &ColumnConstraintSpec{
				ColumnConstraintSpecClause: &ColumnConstraintSpec_NotNullItem{
					NotNullItem: NotNullColumnSpec_NotNullColumnSpecConfirm,
				},
			},
		})
	}
	if auto {
		col.MyDecos = append(col.MyDecos, AutoIncrement_AutoIncrementConfirm)
		col.Constraints = append(col.Constraints, &ColumnConstraint{
			Spec: &ColumnConstraintSpec{
				ColumnConstraintSpecClause: &ColumnConstraintSpec_UniqueItem{
					UniqueItem: &UniqueColumnSpec{IsPrimaryKey: true},
				},
			},
		})
	}
	return &TableElement{
		TableElementClause: &TableElement_ColumnDefElement{ColumnDefElement: col},
	}
}

func contractFK(name string, childColumns []string, parentTable string, parentColumns []string) *TableElement {
	return &TableElement{
		TableElementClause: &TableElement_TableConstraintElement{
			TableConstraintElement: &TableConstraint{
				Name: name,
				Spec: &TableConstraintSpec{
					TableConstraintSpecClause: &TableConstraintSpec_ReferenceItem{
						ReferenceItem: &ReferentialTableConstraint{
							Columns: childColumns,
							KeyExpr: &ReferenceKeyExpr{
								TableName:       parentTable,
								TableObjectName: ObjectNameFromString(parentTable),
								Columns:         parentColumns,
							},
						},
					},
				},
			},
		},
	}
}

func contractIntType() *DataType {
	return &DataType{TypeClause: &DataType_IntData{IntData: &Int{}}}
}

func contractTextType() *DataType {
	return &DataType{TypeClause: &DataType_TextData{TextData: DataTypeSingle_Text}}
}
