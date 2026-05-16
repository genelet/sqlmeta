package xmeta

import (
	"testing"
)

func TestDiffDatabase_AddTable(t *testing.T) {
	current := &MetaDatabase{
		Name:   "testdb",
		Tables: []*MetaTable{},
	}
	desired := &MetaDatabase{
		Name: "testdb",
		Tables: []*MetaTable{
			{
				Name: &ObjectName{Idents: []string{"users"}},
				Elements: []*TableElement{
					{TableElementClause: &TableElement_ColumnDefElement{
						ColumnDefElement: &ColumnDef{Name: "id"},
					}},
				},
			},
		},
	}

	changes := DiffDatabase(current, desired)

	if len(changes) != 1 {
		t.Fatalf("Expected 1 change (AddTable), got %d", len(changes))
	}
	if _, ok := changes[0].(AddTable); !ok {
		t.Errorf("Expected AddTable, got %T", changes[0])
	}
}

func TestDiffDatabase_DropTable(t *testing.T) {
	current := &MetaDatabase{
		Name: "testdb",
		Tables: []*MetaTable{
			{
				Name:     &ObjectName{Idents: []string{"users"}},
				Elements: []*TableElement{},
			},
		},
	}
	desired := &MetaDatabase{
		Name:   "testdb",
		Tables: []*MetaTable{},
	}

	changes := DiffDatabase(current, desired)

	if len(changes) != 1 {
		t.Fatalf("Expected 1 change (DropTable), got %d", len(changes))
	}
	if _, ok := changes[0].(DropTable); !ok {
		t.Errorf("Expected DropTable, got %T", changes[0])
	}
	if changes[0].IsDestructive() != true {
		t.Error("DropTable should be destructive")
	}
}

func TestDiffDatabase_AddColumn(t *testing.T) {
	current := &MetaDatabase{
		Name: "testdb",
		Tables: []*MetaTable{
			{
				Name: &ObjectName{Idents: []string{"users"}},
				Elements: []*TableElement{
					{TableElementClause: &TableElement_ColumnDefElement{
						ColumnDefElement: &ColumnDef{Name: "id"},
					}},
				},
			},
		},
	}
	desired := &MetaDatabase{
		Name: "testdb",
		Tables: []*MetaTable{
			{
				Name: &ObjectName{Idents: []string{"users"}},
				Elements: []*TableElement{
					{TableElementClause: &TableElement_ColumnDefElement{
						ColumnDefElement: &ColumnDef{Name: "id"},
					}},
					{TableElementClause: &TableElement_ColumnDefElement{
						ColumnDefElement: &ColumnDef{Name: "email"},
					}},
				},
			},
		},
	}

	changes := DiffDatabase(current, desired)

	addColFound := false
	for _, c := range changes {
		if ac, ok := c.(AddColumn); ok && ac.Column.Name == "email" {
			addColFound = true
		}
	}
	if !addColFound {
		t.Error("Expected AddColumn for 'email'")
	}
}

func TestDiffDatabase_DropColumn(t *testing.T) {
	current := &MetaDatabase{
		Name: "testdb",
		Tables: []*MetaTable{
			{
				Name: &ObjectName{Idents: []string{"users"}},
				Elements: []*TableElement{
					{TableElementClause: &TableElement_ColumnDefElement{
						ColumnDefElement: &ColumnDef{Name: "id"},
					}},
					{TableElementClause: &TableElement_ColumnDefElement{
						ColumnDefElement: &ColumnDef{Name: "legacy_field"},
					}},
				},
			},
		},
	}
	desired := &MetaDatabase{
		Name: "testdb",
		Tables: []*MetaTable{
			{
				Name: &ObjectName{Idents: []string{"users"}},
				Elements: []*TableElement{
					{TableElementClause: &TableElement_ColumnDefElement{
						ColumnDefElement: &ColumnDef{Name: "id"},
					}},
				},
			},
		},
	}

	changes := DiffDatabase(current, desired)

	dropColFound := false
	for _, c := range changes {
		if dc, ok := c.(DropColumn); ok && dc.ColumnName == "legacy_field" {
			dropColFound = true
			if !dc.IsDestructive() {
				t.Error("DropColumn should be destructive")
			}
		}
	}
	if !dropColFound {
		t.Error("Expected DropColumn for 'legacy_field'")
	}
}

func TestDiffDatabase_AlterColumn(t *testing.T) {
	current := &MetaDatabase{
		Name: "testdb",
		Tables: []*MetaTable{
			{
				Name: &ObjectName{Idents: []string{"users"}},
				Elements: []*TableElement{
					{TableElementClause: &TableElement_ColumnDefElement{
						ColumnDefElement: &ColumnDef{
							Name:    "name",
							Comment: "User name",
						},
					}},
				},
			},
		},
	}
	desired := &MetaDatabase{
		Name: "testdb",
		Tables: []*MetaTable{
			{
				Name: &ObjectName{Idents: []string{"users"}},
				Elements: []*TableElement{
					{TableElementClause: &TableElement_ColumnDefElement{
						ColumnDefElement: &ColumnDef{
							Name:    "name",
							Comment: "Full name", // Changed comment
						},
					}},
				},
			},
		},
	}

	changes := DiffDatabase(current, desired)

	alterFound := false
	for _, c := range changes {
		if ac, ok := c.(AlterColumn); ok && ac.NewColumn.Name == "name" {
			alterFound = true
		}
	}
	if !alterFound {
		t.Error("Expected AlterColumn for 'name'")
	}
}

func TestAlterColumnIsDestructive(t *testing.T) {
	base := &ColumnDef{
		Name:     "status",
		DataType: testVarcharType(20),
		Default:  PackStringAny("'new'"),
		Comment:  "current status",
		Options:  map[string]string{"visible": "true"},
	}
	tests := []struct {
		name string
		new  *ColumnDef
		want bool
	}{
		{
			name: "type change",
			new: &ColumnDef{
				Name:     "status",
				DataType: testTextType(),
				Default:  PackStringAny("'new'"),
				Comment:  "current status",
				Options:  map[string]string{"visible": "true"},
			},
			want: true,
		},
		{
			name: "default only",
			new: &ColumnDef{
				Name:     "status",
				DataType: testVarcharType(20),
				Default:  PackStringAny("'active'"),
				Comment:  "current status",
				Options:  map[string]string{"visible": "true"},
			},
			want: false,
		},
		{
			name: "comment and options only",
			new: &ColumnDef{
				Name:        "status",
				DataType:    testVarcharType(20),
				Default:     PackStringAny("'new'"),
				Comment:     "updated status label",
				Options:     map[string]string{"visible": "false"},
				WithOptions: true,
			},
			want: false,
		},
		{
			name: "decorator change",
			new: &ColumnDef{
				Name:     "status",
				DataType: testVarcharType(20),
				Default:  PackStringAny("'new'"),
				Comment:  "current status",
				Options:  map[string]string{"visible": "true"},
				MyDecos:  []AutoIncrement{AutoIncrement_AutoIncrementConfirm},
			},
			want: true,
		},
		{
			name: "constraint change",
			new: &ColumnDef{
				Name:     "status",
				DataType: testVarcharType(20),
				Default:  PackStringAny("'new'"),
				Comment:  "current status",
				Options:  map[string]string{"visible": "true"},
				Constraints: []*ColumnConstraint{{
					Spec: &ColumnConstraintSpec{
						ColumnConstraintSpecClause: &ColumnConstraintSpec_NotNullItem{
							NotNullItem: NotNullColumnSpec_NotNullColumnSpecConfirm,
						},
					},
				}},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := (AlterColumn{OldColumn: base, NewColumn: tt.new}).IsDestructive()
			if got != tt.want {
				t.Fatalf("IsDestructive() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDiffDatabase_ChangeOrdering(t *testing.T) {
	// Verify that DropConstraint comes before DropTable
	current := &MetaDatabase{
		Name: "testdb",
		Tables: []*MetaTable{
			{
				Name: &ObjectName{Idents: []string{"orders"}},
				Elements: []*TableElement{
					{TableElementClause: &TableElement_TableConstraintElement{
						TableConstraintElement: &TableConstraint{
							Name: "fk_user",
							Spec: &TableConstraintSpec{
								TableConstraintSpecClause: &TableConstraintSpec_ReferenceItem{
									ReferenceItem: &ReferentialTableConstraint{},
								},
							},
						},
					}},
				},
			},
		},
	}
	desired := &MetaDatabase{
		Name:   "testdb",
		Tables: []*MetaTable{},
	}

	changes := DiffDatabase(current, desired)

	// Should have DropConstraint followed by DropTable (due to sorting)
	if len(changes) < 2 {
		t.Fatalf("Expected at least 2 changes, got %d", len(changes))
	}

	// First should be DropConstraint (priority 5 for FK)
	if _, ok := changes[0].(DropConstraint); !ok {
		t.Errorf("First change should be DropConstraint, got %T", changes[0])
	}
	// Second should be DropTable (priority 30)
	if _, ok := changes[1].(DropTable); !ok {
		t.Errorf("Second change should be DropTable, got %T", changes[1])
	}
}

func testVarcharType(size uint32) *DataType {
	return &DataType{TypeClause: &DataType_VarcharData{VarcharData: &VarcharType{Size: size}}}
}

func testTextType() *DataType {
	return &DataType{TypeClause: &DataType_TextData{TextData: DataTypeSingle_Text}}
}
