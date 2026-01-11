package xmeta

import (
	"database/sql"
	"fmt"
	"strings"
)

// LoadPostgres metadata into a PGDatabase structure.
// Requires a connected database.
func LoadPostgres(db *sql.DB) (*PGDatabase, error) {
	// Get Version
	var version string
	row := db.QueryRow("SHOW server_version")
	if err := row.Scan(&version); err != nil {
		return nil, fmt.Errorf("failed to get server version: %w", err)
	}

	pgDB := &PGDatabase{
		Name:    "postgres", // Default or query current_database()
		Version: version,
		Schemas: []*PGSchema{},
	}

	// Query current database name
	dbNameRow := db.QueryRow("SELECT current_database()")
	if err := dbNameRow.Scan(&pgDB.Name); err != nil {
		// ignore error, stick to default
	}

	// Load Schemas
	schemas, err := loadPGSchemas(db)
	if err != nil {
		return nil, err
	}
	pgDB.Schemas = schemas

	return pgDB, nil
}

func loadPGSchemas(db *sql.DB) ([]*PGSchema, error) {
	query := `
		SELECT nspname, 
		       COALESCE(pg_catalog.pg_get_userbyid(nspowner), '') as owner
		FROM pg_catalog.pg_namespace
		WHERE nspname NOT LIKE 'pg_temp_%' 
		  AND nspname NOT LIKE 'pg_toast_%'
		  AND nspname NOT IN ('information_schema')
	`
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query schemas: %w", err)
	}
	defer rows.Close()

	var schemas []*PGSchema
	for rows.Next() {
		var name, owner string
		if err := rows.Scan(&name, &owner); err != nil {
			return nil, err
		}

		schema := &PGSchema{
			Name:  name,
			Owner: owner,
		}

		// Load Tables for this schema
		tables, err := loadPGTables(db, name)
		if err != nil {
			return nil, err
		}
		schema.Tables = tables

		// TODO: Load Views, Sequences

		schemas = append(schemas, schema)
	}
	return schemas, nil
}

func loadPGTables(db *sql.DB, schemaName string) ([]*PGTable, error) {
	query := `
		SELECT tablename, tableowner
	    FROM pg_catalog.pg_tables
		WHERE schemaname = $1
	`
	rows, err := db.Query(query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var tables []*PGTable
	for rows.Next() {
		var name, owner string
		if err := rows.Scan(&name, &owner); err != nil {
			return nil, err
		}

		table := &PGTable{
			Name: &ObjectName{
				Idents: []string{schemaName, name},
			},
			Owner:     owner,
			TableType: "BASE TABLE", // Approximation for now
		}

		// Load Columns
		cols, err := loadPGColumns(db, schemaName, name)
		if err != nil {
			return nil, err
		}
		table.Columns = cols

		tables = append(tables, table)
	}
	return tables, nil
}

func loadPGColumns(db *sql.DB, schemaName, tableName string) ([]*PGColumn, error) {
	query := `
		SELECT column_name, data_type, is_nullable, column_default, ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`
	rows, err := db.Query(query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var cols []*PGColumn
	for rows.Next() {
		var name, dataType, isNullableStr string
		var defaultVal sql.NullString
		var pos int32

		if err := rows.Scan(&name, &dataType, &isNullableStr, &defaultVal, &pos); err != nil {
			return nil, err
		}

		col := &PGColumn{
			Name:            name,
			DataType:        mapPostgresTypeForProto(dataType),
			IsNullable:      (strings.ToUpper(isNullableStr) == "YES"),
			DefaultValue:    defaultVal.String,
			OrdinalPosition: pos,
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func mapPostgresTypeForProto(pgType string) *DataType {
	// Simple mapping
	t := &DataType{}
	pgType = strings.ToLower(pgType)

	switch pgType {
	case "integer", "int", "int4":
		t.TypeClause = &DataType_IntData{IntData: &Int{}}
	case "bigint", "int8":
		t.TypeClause = &DataType_BigIntData{BigIntData: &BigInt{}}
	case "smallint", "int2":
		t.TypeClause = &DataType_SmallIntData{SmallIntData: &SmallInt{}}
	case "boolean", "bool":
		t.TypeClause = &DataType_BooleanData{BooleanData: DataTypeSingle_Boolean}
	case "text", "varchar", "character varying":
		t.TypeClause = &DataType_TextData{TextData: DataTypeSingle_Text}
	case "timestamp", "timestamp without time zone":
		t.TypeClause = &DataType_TimestampData{TimestampData: &Timestamp{WithTimeZone: false}}
	case "timestamptz", "timestamp with time zone":
		t.TypeClause = &DataType_TimestampData{TimestampData: &Timestamp{WithTimeZone: true}}
	default:
		// Fallback to custom
		t.TypeClause = &DataType_CustomData{CustomData: &ObjectName{Idents: []string{pgType}}}
	}
	return t
}
