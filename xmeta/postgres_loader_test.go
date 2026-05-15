package xmeta

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

func TestLoadMetaDatabasePostgresGated(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := fmt.Sprintf("sqlmeta_it_%d", os.Getpid())
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
	if _, err := db.Exec(`CREATE TABLE ` + schema + `.orgs (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE ` + schema + `.users (id SERIAL PRIMARY KEY, org_id INT NOT NULL REFERENCES ` + schema + `.orgs(id) ON DELETE CASCADE, email TEXT NOT NULL UNIQUE)`); err != nil {
		t.Fatal(err)
	}

	meta, err := LoadMetaDatabase(db, LoadOptions{Driver: "postgres", Schemas: []string{schema}})
	if err != nil {
		t.Fatal(err)
	}
	users := findMetaTable(meta, schema+".users")
	if users == nil {
		t.Fatalf("users table not found in %#v", meta.Tables)
	}
	if !hasReferenceConstraint(users, schema+".orgs", "org_id", "id") {
		t.Fatalf("users FK to orgs(id) not found: %#v", users.Elements)
	}
	if !hasUniqueConstraint(users, "email") {
		t.Fatalf("users unique(email) not found: %#v", users.Elements)
	}
}
