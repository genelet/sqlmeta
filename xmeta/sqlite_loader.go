package xmeta

import (
	"database/sql"
	"fmt"
	"strings"
)

// LoadSQLite metadata into a SQLiteDatabase structure.
func LoadSQLite(db *sql.DB) (*SQLiteDatabase, error) {
	sqliteDB := &SQLiteDatabase{
		Name: "main",
	}

	// List tables
	tables, err := loadSQLiteTables(db)
	if err != nil {
		return nil, err
	}
	sqliteDB.Tables = tables

	return sqliteDB, nil
}

func loadSQLiteTables(db *sql.DB) ([]*SQLiteTable, error) {
	query := `SELECT name, sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'`
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query sqlite_schema: %w", err)
	}
	defer rows.Close()

	var tables []*SQLiteTable
	for rows.Next() {
		var name, sqlDef sql.NullString
		if err := rows.Scan(&name, &sqlDef); err != nil {
			return nil, err
		}

		table := &SQLiteTable{
			Name:       name.String,
			Type:       "table",
			Definition: sqlDef.String,
		}

		// Load Columns via PRAGMA
		cols, err := loadSQLiteColumns(db, name.String)
		if err != nil {
			return nil, err
		}
		table.Columns = cols

		tables = append(tables, table)
	}
	return tables, nil
}

func loadSQLiteColumns(db *sql.DB, tableName string) ([]*SQLiteColumn, error) {
	// PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
	query := fmt.Sprintf("PRAGMA table_info(%q)", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to pragma table_info for %s: %w", tableName, err)
	}
	defer rows.Close()

	var cols []*SQLiteColumn
	for rows.Next() {
		var cid int
		var name, typ, dflt sql.NullString
		var notnull, pk int

		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}

		col := &SQLiteColumn{
			Name:         name.String,
			DataType:     mapSQLiteTypeForProto(typ.String),
			IsNullable:   (notnull == 0),
			DefaultValue: dflt.String,
			IsPrimaryKey: (pk > 0),
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func mapSQLiteTypeForProto(typ string) *DataType {
	t := &DataType{}
	typ = strings.ToUpper(typ)

	// Basic Affinity mapping
	if strings.Contains(typ, "INT") {
		t.TypeClause = &DataType_IntData{IntData: &Int{}}
	} else if strings.Contains(typ, "CHAR") || strings.Contains(typ, "CLOB") || strings.Contains(typ, "TEXT") {
		t.TypeClause = &DataType_TextData{TextData: DataTypeSingle_Text}
	} else if strings.Contains(typ, "BLOB") {
		t.TypeClause = &DataType_ByteaData{ByteaData: DataTypeSingle_Bytea} // Approximate
	} else if strings.Contains(typ, "REAL") || strings.Contains(typ, "FLOA") || strings.Contains(typ, "DOUB") {
		t.TypeClause = &DataType_RealData{RealData: &Real{}}
	} else {
		// Fallback
		t.TypeClause = &DataType_CustomData{CustomData: &ObjectName{Idents: []string{typ}}}
	}
	return t
}
