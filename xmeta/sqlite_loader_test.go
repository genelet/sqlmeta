package xmeta

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestLoadMetaDatabaseSQLiteIncludesDDLIndexesAndFKs(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	statements := []string{
		`CREATE TABLE orgs (id integer primary key, name text not null unique)`,
		`CREATE TABLE users (
			id integer primary key,
			org_id integer not null references orgs(id) on delete cascade,
			email text not null,
			created text default CURRENT_TIMESTAMP,
			unique(email)
		)`,
		`CREATE INDEX users_created_idx ON users(created)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}

	meta, err := LoadMetaDatabase(db, LoadOptions{Driver: "sqlite3", Database: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	users := findMetaTable(meta, "users")
	if users == nil {
		t.Fatalf("users table not found in %#v", meta.Tables)
	}
	if users.Options["CreateStatement"] == "" {
		t.Fatal("expected preserved sqlite CREATE TABLE statement")
	}
	if len(columnsFromElements(users.Elements)) != 4 {
		t.Fatalf("users columns = %d, want 4", len(columnsFromElements(users.Elements)))
	}
	if !hasReferenceConstraint(users, "orgs", "org_id", "id") {
		t.Fatalf("users FK to orgs(id) not found: %#v", users.Elements)
	}
	if !hasUniqueConstraint(users, "email") {
		t.Fatalf("users unique(email) not found: %#v", users.Elements)
	}
}

func findMetaTable(meta *MetaDatabase, name string) *MetaTable {
	for _, table := range meta.Tables {
		if objectNameKey(table.Name) == name {
			return table
		}
	}
	return nil
}

func hasReferenceConstraint(table *MetaTable, fkTable, local, foreign string) bool {
	for _, constraint := range constraintsFromElements(table.Elements) {
		ref := constraint.GetSpec().GetReferenceItem()
		if ref == nil || ref.KeyExpr == nil {
			continue
		}
		if ref.KeyExpr.TableName == fkTable && len(ref.Columns) == 1 && ref.Columns[0] == local && len(ref.KeyExpr.Columns) == 1 && ref.KeyExpr.Columns[0] == foreign {
			return true
		}
	}
	return false
}

func hasUniqueConstraint(table *MetaTable, col string) bool {
	for _, constraint := range constraintsFromElements(table.Elements) {
		unique := constraint.GetSpec().GetUniqueItem()
		if unique != nil && len(unique.Columns) == 1 && unique.Columns[0] == col {
			return true
		}
	}
	return false
}
