package xmeta

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestLoadMetaDatabaseMySQLGated(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")
	database := os.Getenv("MYSQL_DATABASE")
	if dsn == "" || database == "" {
		t.Skip("MYSQL_DSN and MYSQL_DATABASE are not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	suffix := os.Getpid()
	orgs := fmt.Sprintf("sqlmeta_orgs_%d", suffix)
	users := fmt.Sprintf("sqlmeta_users_%d", suffix)
	defer db.Exec("DROP TABLE IF EXISTS " + quoteMySQLIdent(orgs))
	defer db.Exec("DROP TABLE IF EXISTS " + quoteMySQLIdent(users))
	if _, err := db.Exec("CREATE TABLE " + quoteMySQLIdent(orgs) + " (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL UNIQUE) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE " + quoteMySQLIdent(users) + " (id INT AUTO_INCREMENT PRIMARY KEY, org_id INT NOT NULL, email VARCHAR(255) NOT NULL UNIQUE, active TINYINT(1), CONSTRAINT fk_sqlmeta_users_org FOREIGN KEY (org_id) REFERENCES " + quoteMySQLIdent(orgs) + "(id) ON DELETE CASCADE) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}

	meta, err := LoadMetaDatabase(db, LoadOptions{Driver: "mysql", Database: database})
	if err != nil {
		t.Fatal(err)
	}
	table := findMetaTable(meta, database+"."+users)
	if table == nil {
		t.Fatalf("users table not found in %#v", meta.Tables)
	}
	if table.Options["CreateStatement"] == "" {
		t.Fatal("expected SHOW CREATE TABLE statement")
	}
	if !hasReferenceConstraint(table, database+"."+orgs, "org_id", "id") && !hasReferenceConstraint(table, orgs, "org_id", "id") {
		t.Fatalf("users FK to orgs(id) not found: %#v", table.Elements)
	}
	if !hasUniqueConstraint(table, "email") {
		t.Fatalf("users unique(email) not found: %#v", table.Elements)
	}
}
