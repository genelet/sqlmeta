package xmeta

import (
	"database/sql"
	"fmt"
	"sort"
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
	query := `SELECT name, type, sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query sqlite_schema: %w", err)
	}

	type tableRow struct {
		name       string
		tableType  string
		definition string
	}
	var tableRows []tableRow
	for rows.Next() {
		var name, tableType, sqlDef sql.NullString
		if err := rows.Scan(&name, &tableType, &sqlDef); err != nil {
			rows.Close()
			return nil, err
		}
		tableRows = append(tableRows, tableRow{name: name.String, tableType: tableType.String, definition: sqlDef.String})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	var tables []*SQLiteTable
	for _, row := range tableRows {
		table := &SQLiteTable{
			Name:         row.name,
			Type:         row.tableType,
			Definition:   row.definition,
			WithoutRowId: strings.Contains(strings.ToUpper(row.definition), "WITHOUT ROWID"),
			Strict:       strings.Contains(strings.ToUpper(row.definition), "STRICT"),
		}

		// Load Columns via PRAGMA
		cols, err := loadSQLiteColumns(db, row.name)
		if err != nil {
			return nil, err
		}
		table.Columns = cols

		indexes, err := loadSQLiteIndexes(db, row.name)
		if err != nil {
			return nil, err
		}
		table.Indexes = indexes

		fks, err := loadSQLiteForeignKeyConstraints(db, row.name)
		if err != nil {
			return nil, err
		}
		table.ForeignKeys = fks

		tables = append(tables, table)
	}
	return tables, nil
}

func loadSQLiteColumns(db *sql.DB, tableName string) ([]*SQLiteColumn, error) {
	// PRAGMA table_xinfo returns: cid, name, type, notnull, dflt_value, pk, hidden
	query := fmt.Sprintf("PRAGMA table_xinfo(%s)", sqlitePragmaArg(tableName))
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to pragma table_xinfo for %s: %w", tableName, err)
	}
	defer rows.Close()

	var cols []*SQLiteColumn
	for rows.Next() {
		var cid int
		var name, typ, dflt sql.NullString
		var notnull, pk, hidden int

		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk, &hidden); err != nil {
			return nil, err
		}
		if hidden == 1 {
			continue
		}

		col := &SQLiteColumn{
			Name:          name.String,
			DataType:      mapSQLiteTypeForProto(typ.String),
			IsNullable:    (notnull == 0),
			DefaultValue:  dflt.String,
			IsPrimaryKey:  (pk > 0),
			AutoIncrement: pk == 1 && strings.Contains(strings.ToUpper(typ.String), "INT"),
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return loadSQLiteColumnsCompat(db, tableName)
	}
	return cols, nil
}

func loadSQLiteColumnsCompat(db *sql.DB, tableName string) ([]*SQLiteColumn, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", sqlitePragmaArg(tableName))
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
		cols = append(cols, &SQLiteColumn{
			Name:          name.String,
			DataType:      mapSQLiteTypeForProto(typ.String),
			IsNullable:    notnull == 0,
			DefaultValue:  dflt.String,
			IsPrimaryKey:  pk > 0,
			AutoIncrement: pk == 1 && strings.Contains(strings.ToUpper(typ.String), "INT"),
		})
	}
	return cols, rows.Err()
}

func loadSQLiteIndexes(db *sql.DB, tableName string) ([]*SQLiteIndex, error) {
	query := fmt.Sprintf("PRAGMA index_list(%s)", sqlitePragmaArg(tableName))
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to pragma index_list for %s: %w", tableName, err)
	}

	type indexRow struct {
		name    string
		unique  bool
		origin  string
		partial bool
	}
	var indexRows []indexRow
	for rows.Next() {
		var seq int
		var name, origin string
		var unique, partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			rows.Close()
			return nil, err
		}
		indexRows = append(indexRows, indexRow{name: name, unique: unique != 0, origin: origin, partial: partial != 0})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	var indexes []*SQLiteIndex
	for _, row := range indexRows {
		idx := &SQLiteIndex{
			Name:      row.name,
			TableName: tableName,
			IsUnique:  row.unique,
			Origin:    row.origin,
			PartialWhere: func() string {
				if !row.partial {
					return ""
				}
				return "partial"
			}(),
		}
		cols, err := loadSQLiteIndexColumns(db, row.name)
		if err != nil {
			return nil, err
		}
		idx.Columns = cols
		idx.Definition = loadSQLiteIndexDefinition(db, row.name)
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

func loadSQLiteIndexColumns(db *sql.DB, indexName string) ([]string, error) {
	query := fmt.Sprintf("PRAGMA index_info(%s)", sqlitePragmaArg(indexName))
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to pragma index_info for %s: %w", indexName, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var seqno, cid int
		var name sql.NullString
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}
		if name.String != "" {
			cols = append(cols, name.String)
		}
	}
	return cols, rows.Err()
}

func loadSQLiteIndexDefinition(db *sql.DB, indexName string) string {
	var sqlDef sql.NullString
	_ = db.QueryRow(`SELECT sql FROM sqlite_schema WHERE type='index' AND name=?`, indexName).Scan(&sqlDef)
	return sqlDef.String
}

func loadSQLiteForeignKeyConstraints(db *sql.DB, tableName string) ([]*TableConstraint, error) {
	query := fmt.Sprintf("PRAGMA foreign_key_list(%s)", sqlitePragmaArg(tableName))
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to pragma foreign_key_list for %s: %w", tableName, err)
	}
	defer rows.Close()

	type fkPart struct {
		seq      int
		table    string
		from     string
		to       string
		onUpdate string
		onDelete string
		match    string
	}
	parts := map[int][]fkPart{}
	for rows.Next() {
		var id int
		var part fkPart
		if err := rows.Scan(&id, &part.seq, &part.table, &part.from, &part.to, &part.onUpdate, &part.onDelete, &part.match); err != nil {
			return nil, err
		}
		parts[id] = append(parts[id], part)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var ids []int
	for id := range parts {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	var constraints []*TableConstraint
	for _, id := range ids {
		list := parts[id]
		sort.Slice(list, func(i, j int) bool { return list[i].seq < list[j].seq })
		ref := &ReferentialTableConstraint{
			KeyExpr:  &ReferenceKeyExpr{TableName: list[0].table, TableObjectName: ObjectNameFromString(list[0].table)},
			OnUpdate: mapReferentialAction(list[0].onUpdate),
			OnDelete: mapReferentialAction(list[0].onDelete),
			Match:    mapMatchOption(list[0].match),
		}
		for _, part := range list {
			ref.Columns = append(ref.Columns, part.from)
			ref.KeyExpr.Columns = append(ref.KeyExpr.Columns, part.to)
		}
		constraints = append(constraints, &TableConstraint{
			Name: fmt.Sprintf("fk_%s_%d", tableName, id),
			Spec: &TableConstraintSpec{
				TableConstraintSpecClause: &TableConstraintSpec_ReferenceItem{ReferenceItem: ref},
			},
		})
	}
	return constraints, nil
}

func sqlitePragmaArg(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
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
	} else if strings.Contains(typ, "NUM") || strings.Contains(typ, "DEC") {
		t.TypeClause = &DataType_DecimalData{DecimalData: &Decimal{}}
	} else if strings.Contains(typ, "BOOL") {
		t.TypeClause = &DataType_BooleanData{BooleanData: DataTypeSingle_Boolean}
	} else {
		// Fallback
		t.TypeClause = &DataType_CustomData{CustomData: &ObjectName{Idents: []string{typ}}}
	}
	return t
}
