package xmeta

import "fmt"

const (
	ContractScenarioManualPKFK = "manual_pk_fk"

	ManualPKFKExpandedFixture = ContractScenarioManualPKFK + ".expanded_app_spec.json"
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
