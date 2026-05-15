package xmeta

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// LoadPostgres metadata into a PGDatabase structure.
// Requires a connected database.
func LoadPostgres(db *sql.DB) (*PGDatabase, error) {
	return LoadPostgresWithSchemas(db)
}

// LoadPostgresWithSchemas metadata into a PGDatabase structure, optionally
// restricted to the provided schema names. Use "*" to load all non-system
// schemas.
func LoadPostgresWithSchemas(db *sql.DB, schemas ...string) (*PGDatabase, error) {
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
	loadedSchemas, err := loadPGSchemas(db, schemas...)
	if err != nil {
		return nil, err
	}
	pgDB.Schemas = loadedSchemas

	return pgDB, nil
}

func loadPGSchemas(db *sql.DB, filters ...string) ([]*PGSchema, error) {
	query := `
		SELECT nspname, 
		       COALESCE(pg_catalog.pg_get_userbyid(nspowner), '') as owner
		FROM pg_catalog.pg_namespace
		WHERE nspname NOT LIKE 'pg_temp_%' 
		  AND nspname NOT LIKE 'pg_toast_%'
		  AND nspname NOT IN ('information_schema', 'pg_catalog')
		ORDER BY nspname
	`
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query schemas: %w", err)
	}
	defer rows.Close()

	allowAll := len(filters) == 0
	allowed := map[string]bool{}
	for _, filter := range filters {
		if filter == "*" {
			allowAll = true
			continue
		}
		if filter != "" {
			allowed[filter] = true
		}
	}

	var schemas []*PGSchema
	for rows.Next() {
		var name, owner string
		if err := rows.Scan(&name, &owner); err != nil {
			return nil, err
		}
		if !allowAll && !allowed[name] {
			continue
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
		SELECT c.relname,
		       COALESCE(pg_catalog.pg_get_userbyid(c.relowner), '') AS owner,
		       CASE c.relkind WHEN 'p' THEN 'PARTITIONED TABLE' ELSE 'BASE TABLE' END AS table_type,
		       c.relpersistence,
		       c.relrowsecurity,
		       c.relforcerowsecurity,
		       COALESCE(obj_description(c.oid, 'pg_class'), '') AS comment,
		       c.reltuples::bigint AS estimated_rows,
		       pg_total_relation_size(c.oid) AS total_bytes
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		  AND c.relkind IN ('r', 'p')
		ORDER BY c.relname
	`
	rows, err := db.Query(query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var tables []*PGTable
	for rows.Next() {
		var name, owner, tableType, persistence, comment string
		var hasRowSecurity, rowSecurityForced bool
		var estimatedRows, totalBytes int64
		if err := rows.Scan(&name, &owner, &tableType, &persistence, &hasRowSecurity, &rowSecurityForced, &comment, &estimatedRows, &totalBytes); err != nil {
			return nil, err
		}

		table := &PGTable{
			Name: &ObjectName{
				Idents: []string{schemaName, name},
			},
			Owner:             owner,
			TableType:         tableType,
			Persistence:       persistence,
			HasRowSecurity:    hasRowSecurity,
			RowSecurityForced: rowSecurityForced,
			Comment:           comment,
			EstimatedRows:     estimatedRows,
			TotalBytes:        totalBytes,
		}

		cols, err := loadPGColumns(db, schemaName, name)
		if err != nil {
			return nil, err
		}
		table.Columns = cols

		constraints, err := loadPGConstraints(db, schemaName, name)
		if err != nil {
			return nil, err
		}
		table.Constraints = constraints
		markPGPrimaryKeyColumns(table)

		fks, err := loadPGForeignKeys(db, schemaName, name)
		if err != nil {
			return nil, err
		}
		table.ForeignKeys = fks

		indexes, err := loadPGIndexes(db, schemaName, name)
		if err != nil {
			return nil, err
		}
		table.Indexes = indexes

		tables = append(tables, table)
	}
	return tables, nil
}

func loadPGColumns(db *sql.DB, schemaName, tableName string) ([]*PGColumn, error) {
	query := `
		SELECT a.attname,
		       pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
		       NOT a.attnotnull AS is_nullable,
		       pg_get_expr(ad.adbin, ad.adrelid) AS column_default,
		       a.attnum AS ordinal_position,
		       a.attidentity,
		       a.attgenerated,
		       COALESCE(col_description(a.attrelid, a.attnum), '') AS comment
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
		WHERE n.nspname = $1
		  AND c.relname = $2
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY a.attnum
	`
	rows, err := db.Query(query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var cols []*PGColumn
	for rows.Next() {
		var name, dataType, identity, generated, comment string
		var isNullable bool
		var defaultVal sql.NullString
		var pos int32

		if err := rows.Scan(&name, &dataType, &isNullable, &defaultVal, &pos, &identity, &generated, &comment); err != nil {
			return nil, err
		}

		col := &PGColumn{
			Name:                 name,
			DataType:             mapPostgresTypeForProto(dataType),
			IsNullable:           isNullable,
			DefaultValue:         defaultVal.String,
			OrdinalPosition:      pos,
			IsIdentity:           identity != "",
			IdentityGeneration:   mapPGIdentityGeneration(identity),
			IsGenerated:          generated != "",
			GenerationExpression: defaultVal.String,
			Comment:              comment,
		}
		cols = append(cols, col)
	}
	return cols, nil
}

func loadPGConstraints(db *sql.DB, schemaName, tableName string) ([]*PGConstraint, error) {
	query := `
		SELECT c.conname,
		       c.contype,
		       COALESCE(string_agg(a.attname, ',' ORDER BY cols.ord), '') AS columns,
		       pg_get_constraintdef(c.oid, true) AS definition,
		       c.condeferrable,
		       c.condeferred,
		       COALESCE(obj_description(c.oid, 'pg_constraint'), '') AS comment
		FROM pg_catalog.pg_constraint c
		JOIN pg_catalog.pg_class tbl ON tbl.oid = c.conrelid
		JOIN pg_catalog.pg_namespace ns ON ns.oid = tbl.relnamespace
		LEFT JOIN unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord) ON true
		LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = cols.attnum
		WHERE ns.nspname = $1
		  AND tbl.relname = $2
		  AND c.contype <> 'f'
		GROUP BY c.oid, c.conname, c.contype, c.condeferrable, c.condeferred
		ORDER BY c.conname
	`
	rows, err := db.Query(query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query constraints: %w", err)
	}
	defer rows.Close()

	var constraints []*PGConstraint
	for rows.Next() {
		var con PGConstraint
		var cols string
		if err := rows.Scan(&con.Name, &con.Type, &cols, &con.Definition, &con.IsDeferrable, &con.IsDeferred, &con.Comment); err != nil {
			return nil, err
		}
		con.TableName = &ObjectName{Idents: []string{schemaName, tableName}}
		con.Columns = splitCSV(cols)
		constraints = append(constraints, &con)
	}
	return constraints, rows.Err()
}

func loadPGForeignKeys(db *sql.DB, schemaName, tableName string) ([]*PGForeignKey, error) {
	query := `
		SELECT c.conname,
		       COALESCE(string_agg(la.attname, ',' ORDER BY l.ord), '') AS local_columns,
		       fns.nspname AS foreign_schema,
		       ftbl.relname AS foreign_table,
		       COALESCE(string_agg(fa.attname, ',' ORDER BY l.ord), '') AS foreign_columns,
		       c.confupdtype,
		       c.confdeltype,
		       c.confmatchtype,
		       pg_get_constraintdef(c.oid, true) AS definition,
		       COALESCE(obj_description(c.oid, 'pg_constraint'), '') AS comment
		FROM pg_catalog.pg_constraint c
		JOIN pg_catalog.pg_class tbl ON tbl.oid = c.conrelid
		JOIN pg_catalog.pg_namespace ns ON ns.oid = tbl.relnamespace
		JOIN pg_catalog.pg_class ftbl ON ftbl.oid = c.confrelid
		JOIN pg_catalog.pg_namespace fns ON fns.oid = ftbl.relnamespace
		LEFT JOIN unnest(c.conkey) WITH ORDINALITY AS l(attnum, ord) ON true
		LEFT JOIN unnest(c.confkey) WITH ORDINALITY AS r(attnum, ord) ON r.ord = l.ord
		LEFT JOIN pg_catalog.pg_attribute la ON la.attrelid = tbl.oid AND la.attnum = l.attnum
		LEFT JOIN pg_catalog.pg_attribute fa ON fa.attrelid = ftbl.oid AND fa.attnum = r.attnum
		WHERE ns.nspname = $1
		  AND tbl.relname = $2
		  AND c.contype = 'f'
		GROUP BY c.oid, c.conname, fns.nspname, ftbl.relname, c.confupdtype, c.confdeltype, c.confmatchtype
		ORDER BY c.conname
	`
	rows, err := db.Query(query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign keys: %w", err)
	}
	defer rows.Close()

	var fks []*PGForeignKey
	for rows.Next() {
		var fk PGForeignKey
		var localCols, foreignSchema, foreignTable, foreignCols, onUpdate, onDelete, match string
		if err := rows.Scan(&fk.Name, &localCols, &foreignSchema, &foreignTable, &foreignCols, &onUpdate, &onDelete, &match, &fk.Definition, &fk.Comment); err != nil {
			return nil, err
		}
		fk.TableName = &ObjectName{Idents: []string{schemaName, tableName}}
		fk.LocalColumns = splitCSV(localCols)
		fk.ForeignTable = &ObjectName{Idents: []string{foreignSchema, foreignTable}}
		fk.ForeignColumns = splitCSV(foreignCols)
		fk.OnUpdate = mapPGReferentialCode(onUpdate)
		fk.OnDelete = mapPGReferentialCode(onDelete)
		fk.MatchOption = mapPGMatchCode(match)
		fks = append(fks, &fk)
	}
	return fks, rows.Err()
}

func loadPGIndexes(db *sql.DB, schemaName, tableName string) ([]*PGIndex, error) {
	query := `
		SELECT ic.relname,
		       ix.indisunique,
		       ix.indisprimary,
		       ix.indisclustered,
		       ix.indisvalid,
		       am.amname,
		       COALESCE(string_agg(a.attname, ',' ORDER BY keys.ord) FILTER (WHERE a.attname IS NOT NULL), '') AS columns,
		       pg_get_indexdef(ix.indexrelid) AS definition,
		       COALESCE(obj_description(ic.oid, 'pg_class'), '') AS comment
		FROM pg_catalog.pg_index ix
		JOIN pg_catalog.pg_class tbl ON tbl.oid = ix.indrelid
		JOIN pg_catalog.pg_namespace ns ON ns.oid = tbl.relnamespace
		JOIN pg_catalog.pg_class ic ON ic.oid = ix.indexrelid
		JOIN pg_catalog.pg_am am ON am.oid = ic.relam
		LEFT JOIN unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ord) ON true
		LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = keys.attnum
		WHERE ns.nspname = $1
		  AND tbl.relname = $2
		GROUP BY ic.oid, ic.relname, ix.indisunique, ix.indisprimary, ix.indisclustered, ix.indisvalid, am.amname, ix.indexrelid
		ORDER BY ic.relname
	`
	rows, err := db.Query(query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	var indexes []*PGIndex
	for rows.Next() {
		var idx PGIndex
		var cols string
		if err := rows.Scan(&idx.Name, &idx.IsUnique, &idx.IsPrimary, &idx.IsClustered, &idx.IsValid, &idx.AccessMethod, &cols, &idx.Definition, &idx.Comment); err != nil {
			return nil, err
		}
		idx.TableName = &ObjectName{Idents: []string{schemaName, tableName}}
		idx.Columns = splitCSV(cols)
		indexes = append(indexes, &idx)
	}
	return indexes, rows.Err()
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
	case "text":
		t.TypeClause = &DataType_TextData{TextData: DataTypeSingle_Text}
	case "varchar", "character varying":
		t.TypeClause = &DataType_VarcharData{VarcharData: &VarcharType{}}
	case "date":
		t.TypeClause = &DataType_DateData{DateData: DataTypeSingle_Date}
	case "time", "time without time zone":
		t.TypeClause = &DataType_TimeData{TimeData: DataTypeSingle_Time}
	case "timestamp", "timestamp without time zone":
		t.TypeClause = &DataType_TimestampData{TimestampData: &Timestamp{WithTimeZone: false}}
	case "timestamptz", "timestamp with time zone":
		t.TypeClause = &DataType_TimestampData{TimestampData: &Timestamp{WithTimeZone: true}}
	case "uuid":
		t.TypeClause = &DataType_UUIDData{UUIDData: DataTypeSingle_UUID}
	case "json", "jsonb":
		t.TypeClause = &DataType_JSONData{JSONData: DataTypeSingle_JSON}
	default:
		if size, ok := parseTypeSize(pgType, "character varying"); ok {
			t.TypeClause = &DataType_VarcharData{VarcharData: &VarcharType{Size: uint32(size)}}
		} else if size, ok := parseTypeSize(pgType, "varchar"); ok {
			t.TypeClause = &DataType_VarcharData{VarcharData: &VarcharType{Size: uint32(size)}}
		} else if size, ok := parseTypeSize(pgType, "character"); ok {
			t.TypeClause = &DataType_CharData{CharData: &CharType{Size: uint32(size)}}
		} else if precision, scale, ok := parseDecimalType(pgType); ok {
			t.TypeClause = &DataType_DecimalData{DecimalData: &Decimal{Precision: uint32(precision), Scale: uint32(scale)}}
		} else {
			t.TypeClause = &DataType_CustomData{CustomData: &ObjectName{Idents: []string{pgType}}}
		}
	}
	return t
}

func markPGPrimaryKeyColumns(table *PGTable) {
	if table == nil {
		return
	}
	pks := map[string]bool{}
	for _, con := range table.Constraints {
		if con.Type != "p" {
			continue
		}
		for _, col := range con.Columns {
			pks[col] = true
		}
	}
	for _, col := range table.Columns {
		col.IsPrimaryKey = pks[col.Name]
	}
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func mapPGIdentityGeneration(code string) string {
	switch code {
	case "a":
		return "ALWAYS"
	case "d":
		return "BY DEFAULT"
	default:
		return ""
	}
}

func mapPGReferentialCode(code string) string {
	switch code {
	case "a":
		return "NO ACTION"
	case "r":
		return "RESTRICT"
	case "c":
		return "CASCADE"
	case "n":
		return "SET NULL"
	case "d":
		return "SET DEFAULT"
	default:
		return ""
	}
}

func mapPGMatchCode(code string) string {
	switch code {
	case "f":
		return "FULL"
	case "p":
		return "PARTIAL"
	case "s":
		return "SIMPLE"
	default:
		return ""
	}
}

func parseTypeSize(value, prefix string) (int, bool) {
	prefix = strings.ToLower(prefix) + "("
	if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, ")") {
		return 0, false
	}
	size, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(value, prefix), ")"))
	return size, err == nil
}

func parseDecimalType(value string) (int, int, bool) {
	for _, prefix := range []string{"numeric(", "decimal("} {
		if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, ")") {
			continue
		}
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(value, prefix), ")"), ",")
		if len(parts) == 0 || len(parts) > 2 {
			return 0, 0, false
		}
		precision, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return 0, 0, false
		}
		scale := 0
		if len(parts) == 2 {
			scale, err = strconv.Atoi(strings.TrimSpace(parts[1]))
			if err != nil {
				return 0, 0, false
			}
		}
		return precision, scale, true
	}
	return 0, 0, false
}
