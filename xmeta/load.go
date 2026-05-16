package xmeta

import (
	"cmp"
	"database/sql"
	"fmt"
	"slices"
	"strings"
)

// LoadOptions selects the SQL dialect and schema scope for LoadMetaDatabase.
type LoadOptions struct {
	Driver   string
	Database string
	Schemas  []string
}

// LoadMetaDatabase introspects a live database and returns unified metadata.
func LoadMetaDatabase(db *sql.DB, opts LoadOptions) (*MetaDatabase, error) {
	switch normalizeDriver(opts.Driver) {
	case "postgres":
		schemas := opts.Schemas
		if len(schemas) == 0 {
			schemas = []string{"public"}
		}
		pgDB, err := LoadPostgresWithSchemas(db, schemas...)
		if err != nil {
			return nil, err
		}
		meta := &MetaDatabase{Name: pgDB.Name, Options: map[string]string{"Driver": "postgres"}}
		for _, schema := range pgDB.Schemas {
			for _, table := range schema.Tables {
				meta.Tables = append(meta.Tables, PGTableToMetaTable(table))
			}
		}
		sortMetaDatabase(meta)
		return meta, nil
	case "mysql":
		if opts.Database == "" {
			return nil, fmt.Errorf("mysql database name is required")
		}
		myDB, err := LoadMySQL(db, opts.Database)
		if err != nil {
			return nil, err
		}
		meta := &MetaDatabase{Name: myDB.Name, Options: map[string]string{"Driver": "mysql"}}
		for _, table := range myDB.Tables {
			meta.Tables = append(meta.Tables, MYTableToMetaTable(table))
		}
		sortMetaDatabase(meta)
		return meta, nil
	case "sqlite":
		sqliteDB, err := LoadSQLite(db)
		if err != nil {
			return nil, err
		}
		name := sqliteDB.Name
		if opts.Database != "" {
			name = opts.Database
		}
		meta := &MetaDatabase{Name: name, Options: map[string]string{"Driver": "sqlite"}}
		for _, table := range sqliteDB.Tables {
			mt := SQLiteTableToMetaTable(table)
			if mt == nil {
				continue
			}
			meta.Tables = append(meta.Tables, mt)
		}
		sortMetaDatabase(meta)
		return meta, nil
	default:
		return nil, fmt.Errorf("unsupported driver %q", opts.Driver)
	}
}

func normalizeDriver(driver string) string {
	normalized := strings.ToLower(strings.TrimSpace(driver))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	switch normalized {
	case "postgres", "postgresql", "pgsql":
		return "postgres"
	case "mysql", "mariadb":
		return "mysql"
	case "sqlite", "sqlite3":
		return "sqlite"
	default:
		return normalized
	}
}

func sortMetaDatabase(meta *MetaDatabase) {
	if meta == nil {
		return
	}
	slices.SortFunc(meta.Tables, func(a, b *MetaTable) int {
		return cmp.Compare(objectNameKey(a.GetName()), objectNameKey(b.GetName()))
	})
}
