package xmeta

import (
	"database/sql"
	"fmt"
	"strings"
)

// LoadMySQL loads metadata into a MYDatabase structure.
func LoadMySQL(db *sql.DB, dbName string) (*MYDatabase, error) {
	// Get version
	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		return nil, fmt.Errorf("failed to get mysql version: %w", err)
	}

	myDB := &MYDatabase{
		Name: dbName,
	}

	// Load tables
	tables, err := loadMYTables(db, dbName)
	if err != nil {
		return nil, err
	}
	myDB.Tables = tables

	return myDB, nil
}

func loadMYTables(db *sql.DB, dbName string) ([]*MYTable, error) {
	query := `
		SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, TABLE_COMMENT, AUTO_INCREMENT
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
	`
	rows, err := db.Query(query, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []*MYTable
	for rows.Next() {
		var name, engine, collation, comment sql.NullString
		var autoInc sql.NullInt64

		if err := rows.Scan(&name, &engine, &collation, &comment, &autoInc); err != nil {
			return nil, err
		}

		table := &MYTable{
			Name: &ObjectName{
				Idents: []string{dbName, name.String},
			},
			Engine:        engine.String,
			Collation:     collation.String,
			Comment:       comment.String,
			AutoIncrement: autoInc.Int64,
		}

		// Load columns
		cols, err := loadMYColumns(db, dbName, name.String)
		if err != nil {
			return nil, err
		}
		table.Columns = cols

		// Load indexes
		indexes, err := loadMYIndexes(db, dbName, name.String)
		if err != nil {
			return nil, err
		}
		table.Indexes = indexes

		// Load foreign keys
		fks, err := loadMYForeignKeys(db, dbName, name.String)
		if err != nil {
			return nil, err
		}
		table.ForeignKeys = fks

		tables = append(tables, table)
	}
	return tables, nil
}

func loadMYColumns(db *sql.DB, dbName, tableName string) ([]*MYColumn, error) {
	query := `
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA, COLUMN_COMMENT, 
		       CHARACTER_SET_NAME, COLLATION_NAME, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var cols []*MYColumn
	for rows.Next() {
		var name, dataType, isNullable, defaultVal, colKey, extra, comment, charset, collation sql.NullString
		var precision, scale, length sql.NullInt64

		if err := rows.Scan(&name, &dataType, &isNullable, &defaultVal, &colKey, &extra, &comment,
			&charset, &collation, &precision, &scale, &length); err != nil {
			return nil, err
		}

		col := &MYColumn{
			Name:          name.String,
			DataType:      mapMySQLTypeForProto(dataType.String, precision.Int64, scale.Int64, length.Int64),
			IsNullable:    strings.ToUpper(isNullable.String) == "YES",
			DefaultValue:  defaultVal.String,
			IsPrimaryKey:  colKey.String == "PRI",
			AutoIncrement: strings.Contains(strings.ToLower(extra.String), "auto_increment"),
			Charset:       charset.String,
			Collation:     collation.String,
			Comment:       comment.String,
		}
		cols = append(cols, col)
	}
	return cols, nil
}

// Placeholder for type mapping
func mapMySQLTypeForProto(typ string, precision, scale, length int64) *DataType {
	t := &DataType{}
	typ = strings.ToLower(typ)

	switch typ {
	case "int", "integer", "mediumint":
		t.TypeClause = &DataType_IntData{IntData: &Int{}}
	case "bigint":
		t.TypeClause = &DataType_BigIntData{BigIntData: &BigInt{}}
	case "smallint":
		t.TypeClause = &DataType_SmallIntData{SmallIntData: &SmallInt{}}
	case "tinyint":
		// Often used as boolean
		t.TypeClause = &DataType_BooleanData{BooleanData: DataTypeSingle_Boolean}
	case "decimal", "numeric":
		t.TypeClause = &DataType_DecimalData{DecimalData: &Decimal{Precision: uint32(precision), Scale: uint32(scale)}}
	case "varchar", "char", "text", "mediumtext", "longtext", "tinytext":
		t.TypeClause = &DataType_TextData{TextData: DataTypeSingle_Text}
	default:
		t.TypeClause = &DataType_CustomData{CustomData: &ObjectName{Idents: []string{typ}}}
	}
	return t
}

func loadMYIndexes(db *sql.DB, dbName, tableName string) ([]*MYIndex, error) {
	// MySQL SHOW INDEX OR information_schema.STATISTICS
	query := `
		SELECT INDEX_NAME, NON_UNIQUE, INDEX_TYPE, COLUMN_NAME
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX
	`
	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	indexMap := make(map[string]*MYIndex)
	for rows.Next() {
		var indexName, indexType, colName string
		var nonUnique int

		if err := rows.Scan(&indexName, &nonUnique, &indexType, &colName); err != nil {
			return nil, err
		}

		idx, ok := indexMap[indexName]
		if !ok {
			idx = &MYIndex{
				Name: indexName,
				TableName: &ObjectName{
					Idents: []string{dbName, tableName},
				},
				IsUnique:  nonUnique == 0,
				IndexType: indexType,
			}
			indexMap[indexName] = idx
		}
		idx.Columns = append(idx.Columns, colName)
	}

	var indexes []*MYIndex
	for _, idx := range indexMap {
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

func loadMYForeignKeys(db *sql.DB, dbName, tableName string) ([]*MYForeignKey, error) {
	query := `
		SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_SCHEMA
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
	`
	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign keys: %w", err)
	}
	defer rows.Close()

	fkMap := make(map[string]*MYForeignKey)
	for rows.Next() {
		var constraintName, colName, refTableName, refColName, refSchema string

		if err := rows.Scan(&constraintName, &colName, &refTableName, &refColName, &refSchema); err != nil {
			return nil, err
		}

		fk, ok := fkMap[constraintName]
		if !ok {
			fk = &MYForeignKey{
				Name: constraintName,
				TableName: &ObjectName{
					Idents: []string{dbName, tableName},
				},
				ForeignTable: &ObjectName{
					Idents: []string{refSchema, refTableName},
				},
			}
			fkMap[constraintName] = fk
		}
		fk.LocalColumns = append(fk.LocalColumns, colName)
		fk.ForeignColumns = append(fk.ForeignColumns, refColName)
	}

	var fks []*MYForeignKey
	for _, fk := range fkMap {
		fks = append(fks, fk)
	}
	return fks, nil
}
