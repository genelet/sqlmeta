package xmeta

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSchemaRelationshipOverridesFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "overrides.textpb")
	data := []byte(`
PrimaryKeys: {
  Name: "users_public_id_pk"
  TableName: { Idents: "users" }
  Columns: "public_id"
}
ForeignKeys: {
  Name: "orders_user_fk"
  ChildTable: { Idents: "orders" }
  ChildColumns: "user_public_id"
  ParentTable: { Idents: "users" }
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	overrides, err := LoadSchemaRelationshipOverridesFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(overrides.GetPrimaryKeys()) != 1 || overrides.GetPrimaryKeys()[0].GetColumns()[0] != "public_id" {
		t.Fatalf("primary keys = %#v", overrides.GetPrimaryKeys())
	}
	if len(overrides.GetForeignKeys()) != 1 || overrides.GetForeignKeys()[0].GetChildColumns()[0] != "user_public_id" {
		t.Fatalf("foreign keys = %#v", overrides.GetForeignKeys())
	}
}
