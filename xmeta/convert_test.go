package xmeta

import (
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestPGTableToMetaTable(t *testing.T) {
	pgTbl := &PGTable{
		Name:    &ObjectName{Idents: []string{"public", "users"}},
		Comment: "User table",
		Columns: []*PGColumn{
			{
				Name:         "id",
				DefaultValue: "nextval('seq')",
				IsPrimaryKey: true,
				IsNullable:   false,
				Comment:      "Primary Key",
				IsIdentity:   true,
			},
		},
		Owner: "postgres",
	}

	meta := PGTableToMetaTable(pgTbl)
	if meta.Name.Idents[1] != "users" {
		t.Errorf("Expected table users, got %v", meta.Name)
	}
	if meta.Comment != "User table" {
		t.Errorf("Expected comment 'User table', got '%s'", meta.Comment)
	}
	if meta.Options["Owner"] != "postgres" {
		t.Errorf("Expected Owner postgres, got %s", meta.Options["Owner"])
	}

	if len(meta.Elements) != 1 {
		t.Fatal("Expected 1 element")
	}

	colElem := meta.Elements[0].GetColumnDefElement()
	if colElem == nil {
		t.Fatal("Expected ColumnDefElement")
	}

	if colElem.Name != "id" {
		t.Errorf("Expected col name id, got %s", colElem.Name)
	}
	if colElem.Comment != "Primary Key" {
		t.Errorf("Expected col comment 'Primary Key', got '%s'", colElem.Comment)
	}
	if colElem.Options["IsIdentity"] != "true" {
		t.Error("Expected IsIdentity option")
	}

	// Verify Default Packed
	if colElem.Default == nil {
		t.Fatal("Expected Default to be set")
	}
	sVal := &wrapperspb.StringValue{}
	if err := colElem.Default.UnmarshalTo(sVal); err != nil {
		t.Fatalf("Failed to unmarshal default: %v", err)
	}
	if sVal.Value != "nextval('seq')" {
		t.Errorf("Expected default 'nextval('seq')', got '%s'", sVal.Value)
	}
}

func TestPGConstraintToTableConstraint(t *testing.T) {
	pgCon := &PGConstraint{
		Name:    "uq_email",
		Type:    "u",
		Columns: []string{"email"},
	}

	tc := PGConstraintToTableConstraint(pgCon)
	if tc.Name != "uq_email" {
		t.Errorf("Expected name 'uq_email', got '%s'", tc.Name)
	}
	if u := tc.Spec.GetUniqueItem(); u != nil {
		if len(u.Columns) != 1 || u.Columns[0] != "email" {
			t.Errorf("Unexpected columns: %v", u.Columns)
		}
		if u.IsPrimary {
			t.Error("Expected not primary")
		}
	} else {
		t.Error("Expected UniqueItem")
	}
}

func TestMYIndexToTableConstraint(t *testing.T) {
	idx := &MYIndex{
		Name:     "PRIMARY",
		Columns:  []string{"id"},
		IsUnique: true,
	}

	tc := MYIndexToTableConstraint(idx)
	if tc == nil {
		t.Fatal("Expected table constraint")
	}
	if u := tc.Spec.GetUniqueItem(); u != nil {
		if !u.IsPrimary {
			t.Error("Expected IsPrimary=true")
		}
	} else {
		t.Error("Expected UniqueItem")
	}
}

func TestSQLiteColumnToColumnDef(t *testing.T) {
	liteCol := &SQLiteColumn{
		Name:         "age",
		DefaultValue: "18",
		IsNullable:   true,
	}

	colDef := SQLiteColumnToColumnDef(liteCol)
	if colDef.Default == nil {
		t.Fatal("Expected Default")
	}
	sVal := &wrapperspb.StringValue{}
	colDef.Default.UnmarshalTo(sVal)
	if sVal.Value != "18" {
		t.Errorf("Expected default '18', got '%s'", sVal.Value)
	}

	if len(colDef.Constraints) != 0 {
		t.Errorf("Expected no constraints for nullable column, got %d", len(colDef.Constraints))
	}
}
