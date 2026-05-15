package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/tabilet/sqlmeta/tavola"
	"github.com/tabilet/sqlmeta/xmeta"
)

type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			*s = append(*s, item)
		}
	}
	return nil
}

func main() {
	var schemas stringList
	var driver, dsn, database, project, script, ownerLogin, ownerEmail, out string
	var datasourceType, datasourceNickname, datasourceHost, datasourcePort, datasourceUser, datasourcePassword, datasourcePath string
	var force bool
	var auth tavola.AuthOptions

	flag.StringVar(&driver, "driver", "", "Database driver: mysql, postgres, sqlite3")
	flag.StringVar(&dsn, "dsn", "", "Database connection string")
	flag.StringVar(&database, "database", "", "Database/schema name")
	flag.StringVar(&database, "db", "", "Alias for --database")
	flag.Var(&schemas, "schema", "PostgreSQL schema to introspect; may be repeated or comma-separated")
	flag.StringVar(&project, "project", "", "Tavola project name")
	flag.StringVar(&script, "script", "", "Generated app script path")
	flag.StringVar(&ownerLogin, "owner-login", "local", "Tavola owner login")
	flag.StringVar(&ownerEmail, "owner-email", "local@example.test", "Tavola owner email")
	flag.StringVar(&datasourceType, "ds-type", "", "Override Tavola datasource type")
	flag.StringVar(&datasourceNickname, "ds-nickname", "", "Tavola datasource nickname")
	flag.StringVar(&datasourceHost, "ds-host", "", "Tavola datasource host")
	flag.StringVar(&datasourcePort, "ds-port", "", "Tavola datasource port")
	flag.StringVar(&datasourceUser, "ds-user", "", "Tavola datasource user")
	flag.StringVar(&datasourcePassword, "ds-password", "", "Tavola datasource password")
	flag.StringVar(&datasourcePath, "ds-path", "", "Tavola SQLite datasource path")
	flag.StringVar(&out, "out", "", "Output JSON file; stdout when omitted")
	flag.BoolVar(&force, "force", false, "Overwrite --out when it already exists")
	flag.StringVar(&auth.Role, "auth-role", "", "Protected role name")
	flag.StringVar(&auth.Table, "auth-table", "", "Authentication table name")
	flag.StringVar(&auth.ID, "auth-id", "", "Authentication user ID column")
	flag.StringVar(&auth.Login, "auth-login", "", "Authentication login column")
	flag.StringVar(&auth.Password, "auth-password", "", "Authentication password column")
	flag.StringVar(&auth.FirstName, "auth-firstname", "", "Authentication first-name column")
	flag.StringVar(&auth.LastName, "auth-lastname", "", "Authentication last-name column")
	flag.StringVar(&auth.Restriction, "auth-restriction", "", "Authentication role restriction")
	flag.Parse()

	if err := run(driver, dsn, database, []string(schemas), project, out, force, tavola.Options{
		Project:            project,
		Script:             script,
		OwnerLogin:         ownerLogin,
		OwnerEmail:         ownerEmail,
		OwnerTypeID:        1,
		DatasourceType:     datasourceType,
		DatasourceNickname: datasourceNickname,
		DatasourceDatabase: database,
		DatasourceHost:     datasourceHost,
		DatasourcePort:     datasourcePort,
		DatasourceUser:     datasourceUser,
		DatasourcePassword: datasourcePassword,
		DatasourcePath:     defaultString(datasourcePath, sqlitePath(driver, dsn)),
		Auth:               auth,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "tavola-introspect:", err)
		os.Exit(1)
	}
}

func run(driver, dsn, database string, schemas []string, project, out string, force bool, opts tavola.Options) error {
	if strings.TrimSpace(driver) == "" {
		return fmt.Errorf("--driver is required")
	}
	if strings.TrimSpace(dsn) == "" {
		return fmt.Errorf("--dsn is required")
	}
	if strings.TrimSpace(project) == "" {
		return fmt.Errorf("--project is required")
	}

	sqlDriver := sqlDriverName(driver)
	db, err := sql.Open(sqlDriver, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return err
	}

	meta, err := xmeta.LoadMetaDatabase(db, xmeta.LoadOptions{
		Driver:   driver,
		Database: database,
		Schemas:  schemas,
	})
	if err != nil {
		return err
	}

	spec, err := tavola.BuildSpec(meta, opts)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	if spec.Introspection != nil {
		for _, warning := range spec.Introspection.Warnings {
			fmt.Fprintln(os.Stderr, "warning:", warning)
		}
	}

	if out == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	if !force {
		if _, err := os.Stat(out); err == nil {
			return fmt.Errorf("%s exists; use --force to overwrite", out)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if dir := filepath.Dir(out); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(out, data, 0644)
}

func sqlDriverName(driver string) string {
	switch strings.ToLower(driver) {
	case "postgresql", "postgres", "pgsql":
		return "postgres"
	case "sqlite", "sqlite3":
		return "sqlite3"
	default:
		return "mysql"
	}
}

func sqlitePath(driver, dsn string) string {
	switch strings.ToLower(driver) {
	case "sqlite", "sqlite3":
		return dsn
	default:
		return ""
	}
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
