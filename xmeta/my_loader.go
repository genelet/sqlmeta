package xmeta

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
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
		createStatement, err := loadMYCreateStatement(db, dbName, name.String)
		if err != nil {
			return nil, err
		}
		table.CreateOptions = createStatement

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
		SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA, COLUMN_COMMENT,
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
		var name, dataType, columnType, isNullable, defaultVal, colKey, extra, comment, charset, collation sql.NullString
		var precision, scale, length sql.NullInt64

		if err := rows.Scan(&name, &dataType, &columnType, &isNullable, &defaultVal, &colKey, &extra, &comment,
			&charset, &collation, &precision, &scale, &length); err != nil {
			return nil, err
		}

		col := &MYColumn{
			Name:          name.String,
			DataType:      mapMySQLTypeForProto(dataType.String, columnType.String, precision.Int64, scale.Int64, length.Int64),
			IsNullable:    strings.ToUpper(isNullable.String) == "YES",
			DefaultValue:  defaultVal.String,
			IsPrimaryKey:  colKey.String == "PRI",
			AutoIncrement: strings.Contains(strings.ToLower(extra.String), "auto_increment"),
			Charset:       charset.String,
			Collation:     collation.String,
			Comment:       comment.String,
			IsUnsigned:    strings.Contains(strings.ToLower(columnType.String), "unsigned"),
			DisplayWidth:  uint32(parseMySQLDisplayWidth(columnType.String)),
		}
		cols = append(cols, col)
	}
	return cols, nil
}

// Placeholder for type mapping
func mapMySQLTypeForProto(typ, columnType string, precision, scale, length int64) *DataType {
	t := &DataType{}
	typ = strings.ToLower(typ)
	columnType = strings.ToLower(columnType)
	unsigned := strings.Contains(columnType, "unsigned")

	switch typ {
	case "int", "integer":
		t.TypeClause = &DataType_IntData{IntData: &Int{IsUnsigned: unsigned}}
	case "mediumint":
		t.TypeClause = &DataType_MediumIntData{MediumIntData: &MediumInt{IsUnsigned: unsigned}}
	case "bigint":
		t.TypeClause = &DataType_BigIntData{BigIntData: &BigInt{IsUnsigned: unsigned}}
	case "smallint":
		t.TypeClause = &DataType_SmallIntData{SmallIntData: &SmallInt{IsUnsigned: unsigned}}
	case "tinyint":
		if isMySQLBooleanTinyInt(columnType) {
			t.TypeClause = &DataType_BooleanData{BooleanData: DataTypeSingle_Boolean}
		} else {
			t.TypeClause = &DataType_TinyIntData{TinyIntData: &TinyInt{IsUnsigned: unsigned}}
		}
	case "decimal", "numeric":
		t.TypeClause = &DataType_DecimalData{DecimalData: &Decimal{Precision: uint32(precision), Scale: uint32(scale), IsUnsigned: unsigned}}
	case "varchar":
		t.TypeClause = &DataType_VarcharData{VarcharData: &VarcharType{Size: uint32(length)}}
	case "char":
		t.TypeClause = &DataType_CharData{CharData: &CharType{Size: uint32(length)}}
	case "text", "mediumtext", "longtext", "tinytext":
		t.TypeClause = &DataType_TextData{TextData: DataTypeSingle_Text}
	case "datetime", "timestamp":
		t.TypeClause = &DataType_TimestampData{TimestampData: &Timestamp{}}
	case "date":
		t.TypeClause = &DataType_DateData{DateData: DataTypeSingle_Date}
	case "time":
		t.TypeClause = &DataType_TimeData{TimeData: DataTypeSingle_Time}
	case "double":
		t.TypeClause = &DataType_DoubleData{DoubleData: &DoubleType{IsDoublePrecision: true}}
	case "float":
		t.TypeClause = &DataType_FloatData{FloatData: &Float{IsUnsigned: unsigned}}
	case "real":
		t.TypeClause = &DataType_RealData{RealData: &Real{IsUnsigned: unsigned}}
	case "json":
		t.TypeClause = &DataType_JSONData{JSONData: DataTypeSingle_JSON}
	case "year":
		t.TypeClause = &DataType_YearData{YearData: DataTypeSingle_Year}
	case "enum":
		t.TypeClause = &DataType_EnumData{EnumData: &EnumType{Values: parseMySQLStringList(columnType)}}
	case "set":
		t.TypeClause = &DataType_SetData{SetData: &SetType{Values: parseMySQLStringList(columnType)}}
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
		SELECT k.CONSTRAINT_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME,
		       k.REFERENCED_TABLE_SCHEMA, rc.UPDATE_RULE, rc.DELETE_RULE
		FROM information_schema.KEY_COLUMN_USAGE k
		LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
		  ON rc.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
		 AND rc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
		 AND rc.TABLE_NAME = k.TABLE_NAME
		WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ? AND k.REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION
	`
	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign keys: %w", err)
	}
	defer rows.Close()

	fkMap := make(map[string]*MYForeignKey)
	for rows.Next() {
		var constraintName, colName, refTableName, refColName, refSchema string
		var onUpdate, onDelete sql.NullString

		if err := rows.Scan(&constraintName, &colName, &refTableName, &refColName, &refSchema, &onUpdate, &onDelete); err != nil {
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
				OnUpdate: onUpdate.String,
				OnDelete: onDelete.String,
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

func loadMYCreateStatement(db *sql.DB, dbName, tableName string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE %s", quoteMySQLTable(dbName, tableName))
	var name, statement string
	if err := db.QueryRow(query).Scan(&name, &statement); err != nil {
		return "", fmt.Errorf("failed to show create table for %s.%s: %w", dbName, tableName, err)
	}
	return statement, nil
}

func quoteMySQLTable(dbName, tableName string) string {
	return quoteMySQLIdent(dbName) + "." + quoteMySQLIdent(tableName)
}

func quoteMySQLIdent(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func isMySQLBooleanTinyInt(columnType string) bool {
	columnType = strings.ToLower(strings.TrimSpace(columnType))
	return strings.HasPrefix(columnType, "tinyint(1)")
}

func parseMySQLDisplayWidth(columnType string) int {
	re := regexp.MustCompile(`^[a-z]+int\((\d+)\)`)
	matches := re.FindStringSubmatch(strings.ToLower(strings.TrimSpace(columnType)))
	if len(matches) != 2 {
		return 0
	}
	width, _ := strconv.Atoi(matches[1])
	return width
}

func parseMySQLStringList(columnType string) []string {
	start := strings.Index(columnType, "(")
	end := strings.LastIndex(columnType, ")")
	if start < 0 || end <= start {
		return nil
	}
	var values []string
	raw := columnType[start+1 : end]
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, "'")
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}
