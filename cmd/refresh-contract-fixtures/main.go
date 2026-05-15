package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/genelet/sqlmeta/tavola"
	"github.com/genelet/sqlmeta/xmeta"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	var root, tavolaOut string
	flag.StringVar(&root, "root", ".", "sqlmeta repository root")
	flag.StringVar(&tavolaOut, "tavola-out", "", "optional downstream Tavola project JSON path")
	flag.Parse()

	if err := run(root, tavolaOut); err != nil {
		fmt.Fprintln(os.Stderr, "refresh-contract-fixtures:", err)
		os.Exit(1)
	}
}

func run(root, tavolaOut string) error {
	meta := contractMetaDatabase()
	overrides := contractOverrides()
	app, err := xmeta.BuildDefaultAppSpec(meta, xmeta.AppSpecOptions{
		Name: "Manual PK/FK",
		Auth: &xmeta.AuthBinding{
			UserTable:       xmeta.ObjectNameFromString("users"),
			UserIDColumn:    "public_id",
			LoginColumn:     "email",
			PasswordColumn:  "passwd",
			FirstNameColumn: "firstname",
			LastNameColumn:  "lastname",
		},
		RoleName:        "u",
		SchemaOverrides: overrides,
	})
	if err != nil {
		return err
	}
	expanded, err := xmeta.ExpandRoleScopes(meta, app)
	if err != nil {
		return err
	}
	spec, err := tavola.BuildTavolaSpec(meta, expanded, tavola.Options{
		Project:            "SqlmetaApp",
		Script:             "/sqlmeta/app.php",
		PublicRole:         "p",
		OwnerLogin:         "local",
		OwnerEmail:         "local@example.test",
		OwnerTypeID:        1,
		DatasourceType:     "SQLite",
		DatasourceNickname: "sqlmeta",
		DatasourceDatabase: "app.sqlite",
		DatasourcePath:     "data/sqlmeta.sqlite",
	})
	if err != nil {
		return err
	}

	if err := writeProtoJSON(filepath.Join(root, "xmeta/testdata/contracts/manual_pk_fk.expanded_app_spec.json"), expanded); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(root, "tavola/testdata/contracts/manual_pk_fk.project.json"), spec); err != nil {
		return err
	}
	if err := writeWarnings(filepath.Join(root, "tavola/testdata/contracts/manual_pk_fk.warnings.txt"), specWarnings(spec)); err != nil {
		return err
	}
	if tavolaOut != "" {
		if err := writeJSON(tavolaOut, spec); err != nil {
			return err
		}
	}
	return nil
}

func contractMetaDatabase() *xmeta.MetaDatabase {
	return &xmeta.MetaDatabase{
		Name:    "app.sqlite",
		Options: map[string]string{"Driver": "sqlite"},
		Tables: []*xmeta.MetaTable{
			{
				Name: xmeta.ObjectNameFromString("audit_log"),
				Type: "table",
				Options: map[string]string{
					"CreateStatement": "CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, message TEXT NOT NULL)",
				},
				Elements: []*xmeta.TableElement{
					column("id", intType(), true, true),
					column("message", textType(), true, false),
				},
			},
			{
				Name: xmeta.ObjectNameFromString("posts"),
				Type: "table",
				Options: map[string]string{
					"CreateStatement": "CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_public_id TEXT NOT NULL, title TEXT NOT NULL, created TEXT)",
				},
				Elements: []*xmeta.TableElement{
					column("id", intType(), true, true),
					column("user_public_id", textType(), true, false),
					column("title", textType(), true, false),
					column("created", textType(), false, false),
					fk("posts_physical_audit_fk", []string{"user_public_id"}, "audit_log", []string{"id"}),
				},
			},
			{
				Name: xmeta.ObjectNameFromString("users"),
				Type: "table",
				Options: map[string]string{
					"CreateStatement": "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, public_id TEXT NOT NULL UNIQUE, email TEXT NOT NULL, passwd TEXT NOT NULL, firstname TEXT, lastname TEXT)",
				},
				Elements: []*xmeta.TableElement{
					column("id", intType(), true, true),
					column("public_id", textType(), true, false),
					column("email", textType(), true, false),
					column("passwd", textType(), true, false),
					column("firstname", textType(), false, false),
					column("lastname", textType(), false, false),
				},
			},
		},
	}
}

func contractOverrides() *xmeta.SchemaRelationshipOverrides {
	return &xmeta.SchemaRelationshipOverrides{
		PrimaryKeys: []*xmeta.ManualPrimaryKey{{
			Name:      "users_public_id_pk",
			TableName: xmeta.ObjectNameFromString("users"),
			Columns:   []string{"public_id"},
		}},
		ForeignKeys: []*xmeta.ManualForeignKey{{
			Name:         "posts_user_public_id_fk",
			ChildTable:   xmeta.ObjectNameFromString("posts"),
			ChildColumns: []string{"user_public_id"},
			ParentTable:  xmeta.ObjectNameFromString("users"),
		}},
	}
}

func column(name string, dataType *xmeta.DataType, notNull, auto bool) *xmeta.TableElement {
	col := &xmeta.ColumnDef{Name: name, DataType: dataType}
	if notNull {
		col.Constraints = append(col.Constraints, &xmeta.ColumnConstraint{
			Spec: &xmeta.ColumnConstraintSpec{
				ColumnConstraintSpecClause: &xmeta.ColumnConstraintSpec_NotNullItem{
					NotNullItem: xmeta.NotNullColumnSpec_NotNullColumnSpecConfirm,
				},
			},
		})
	}
	if auto {
		col.MyDecos = append(col.MyDecos, xmeta.AutoIncrement_AutoIncrementConfirm)
		col.Constraints = append(col.Constraints, &xmeta.ColumnConstraint{
			Spec: &xmeta.ColumnConstraintSpec{
				ColumnConstraintSpecClause: &xmeta.ColumnConstraintSpec_UniqueItem{
					UniqueItem: &xmeta.UniqueColumnSpec{IsPrimaryKey: true},
				},
			},
		})
	}
	return &xmeta.TableElement{
		TableElementClause: &xmeta.TableElement_ColumnDefElement{ColumnDefElement: col},
	}
}

func fk(name string, childColumns []string, parentTable string, parentColumns []string) *xmeta.TableElement {
	return &xmeta.TableElement{
		TableElementClause: &xmeta.TableElement_TableConstraintElement{
			TableConstraintElement: &xmeta.TableConstraint{
				Name: name,
				Spec: &xmeta.TableConstraintSpec{
					TableConstraintSpecClause: &xmeta.TableConstraintSpec_ReferenceItem{
						ReferenceItem: &xmeta.ReferentialTableConstraint{
							Columns: childColumns,
							KeyExpr: &xmeta.ReferenceKeyExpr{
								TableName:       parentTable,
								TableObjectName: xmeta.ObjectNameFromString(parentTable),
								Columns:         parentColumns,
							},
						},
					},
				},
			},
		},
	}
}

func intType() *xmeta.DataType {
	return &xmeta.DataType{TypeClause: &xmeta.DataType_IntData{IntData: &xmeta.Int{}}}
}

func textType() *xmeta.DataType {
	return &xmeta.DataType{TypeClause: &xmeta.DataType_TextData{TextData: xmeta.DataTypeSingle_Text}}
}

func writeProtoJSON(path string, msg proto.Message) error {
	data, err := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		EmitUnpopulated: false,
	}.Marshal(msg)
	if err != nil {
		return err
	}
	return writeFile(path, append(data, '\n'))
}

func specWarnings(spec *tavola.Spec) []string {
	if spec == nil || spec.Introspection == nil {
		return nil
	}
	return spec.Introspection.Warnings
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(path, append(data, '\n'))
}

func writeWarnings(path string, warnings []string) error {
	return writeFile(path, []byte(strings.Join(warnings, "\n")+"\n"))
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
