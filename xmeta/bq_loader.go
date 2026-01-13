package xmeta

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// LoadBigQuery metadata into a BQProject structure.
// Uses the official BigQuery client.
func LoadBigQuery(ctx context.Context, client *bigquery.Client, projectID string) (*BQProject, error) {
	bqProj := &BQProject{
		ProjectId:    projectID,
		FriendlyName: projectID,
	}

	// List Datasets
	it := client.Datasets(ctx)
	it.ProjectID = projectID

	var datasets []*BQDataset
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list datasets: %w", err)
		}

		// Get Dataset Metadata
		md, err := ds.Metadata(ctx)
		if err != nil {
			// skip or error? generic loader should probably continue
			continue
		}

		bqDS := &BQDataset{
			Name:        &ObjectName{Idents: []string{ds.ProjectID, ds.DatasetID}},
			Location:    md.Location,
			Description: md.Description,
			Labels:      md.Labels,
		}

		// Load Tables
		tables, err := loadBQTables(ctx, ds)
		if err != nil {
			return nil, err
		}
		bqDS.Tables = tables

		datasets = append(datasets, bqDS)
	}
	bqProj.Datasets = datasets

	return bqProj, nil
}

func loadBQTables(ctx context.Context, ds *bigquery.Dataset) ([]*BQTable, error) {
	it := ds.Tables(ctx)
	var tables []*BQTable

	for {
		t, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list tables in %s: %w", ds.DatasetID, err)
		}

		md, err := t.Metadata(ctx)
		if err != nil {
			continue
		}

		bqT := &BQTable{
			Name:        &ObjectName{Idents: []string{t.ProjectID, t.DatasetID, t.TableID}},
			Type:        string(md.Type), // TABLE, VIEW, EXTERNAL
			NumRows:     int64(md.NumRows),
			TotalBytes:  md.NumBytes,
			Description: md.Description,
			Labels:      md.Labels,
		}

		// Schema
		if md.Schema != nil {
			bqT.Schema = mapBQSchema(md.Schema)
		}

		if md.ViewQuery != "" {
			bqT.ViewQuery = md.ViewQuery
		}

		tables = append(tables, bqT)
	}
	return tables, nil
}

func mapBQSchema(schema bigquery.Schema) []*BQColumn {
	var cols []*BQColumn
	for _, field := range schema {
		mode := "NULLABLE"
		if field.Required {
			mode = "REQUIRED"
		}
		if field.Repeated {
			mode = "REPEATED"
		}
		col := &BQColumn{
			Name:        field.Name,
			Mode:        mode,
			Description: field.Description,
		}

		// Map Type
		col.DataType = mapBQType(field)

		cols = append(cols, col)
	}
	return cols
}

func mapBQType(field *bigquery.FieldSchema) *DataType {
	t := &DataType{}
	fts := string(field.Type) // STRING, INTEGER, etc.

	switch fts {
	case "INTEGER", "INT64":
		t.TypeClause = &DataType_IntData{IntData: &Int{}}
	case "FLOAT", "FLOAT64":
		t.TypeClause = &DataType_FloatData{FloatData: &Float{}}
	case "BOOLEAN", "BOOL":
		t.TypeClause = &DataType_BooleanData{BooleanData: DataTypeSingle_Boolean}
	case "STRING":
		t.TypeClause = &DataType_TextData{TextData: DataTypeSingle_Text}
	case "BYTES":
		t.TypeClause = &DataType_ByteaData{ByteaData: DataTypeSingle_Bytea}
	case "STRUCT", "RECORD":
		// Recursive mapping for STRUCT
		var subCols []*ColumnDef
		for _, sub := range field.Schema {
			subCols = append(subCols, &ColumnDef{
				Name:     sub.Name,
				DataType: mapBQType(sub),
			})
		}
		t.TypeClause = &DataType_StructData{StructData: &StructData{Fields: subCols}}
	case "ARRAY":
		// Handle simple ARRAY definition, though usually caught by Repeated check below for basic fields.
		// If explicit ARRAY type string found, treat as Custom unless handled.
		t.TypeClause = &DataType_CustomData{CustomData: &ObjectName{Idents: []string{fts}}}
	default:
		t.TypeClause = &DataType_CustomData{CustomData: &ObjectName{Idents: []string{fts}}}
	}

	if field.Repeated {
		// Wrap in Array
		return &DataType{
			TypeClause: &DataType_ArrayData{
				ArrayData: &ArrayData{
					Type: t, // The element type
				},
			},
		}
	}

	return t
}
