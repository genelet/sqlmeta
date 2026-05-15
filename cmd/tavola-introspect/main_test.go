package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/genelet/sqlmeta/tavola"
)

func TestRunSQLiteWritesTavolaSpec(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "app.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE users (id integer primary key, email text not null unique)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(dir, "spec.json")
	err = run("sqlite3", dbPath, "app", nil, "SmokeApp", out, false, tavola.Options{
		Project:            "SmokeApp",
		OwnerLogin:         "local",
		OwnerEmail:         "local@example.test",
		DatasourceDatabase: "app",
		DatasourcePath:     dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var spec tavola.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Project.Name != "SmokeApp" {
		t.Fatalf("project = %q", spec.Project.Name)
	}
	if len(spec.Schema.Tables) != 1 || spec.Schema.Tables[0].Name != "users" {
		t.Fatalf("tables = %#v", spec.Schema.Tables)
	}
}
