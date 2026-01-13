package xmeta

// file_loader.go provides functions to load MetaDatabase from various file formats.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// LoadMetaDatabaseFromFile loads a MetaDatabase from a file.
// The format is detected from the file extension:
//   - .textpb, .txtpb, .pbtxt → Text proto format
//   - .json → JSON format
//   - .pb, .bin → Binary proto format
func LoadMetaDatabaseFromFile(path string) (*MetaDatabase, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	db := &MetaDatabase{}
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".textpb", ".txtpb", ".pbtxt":
		if err := prototext.Unmarshal(data, db); err != nil {
			return nil, fmt.Errorf("parsing text proto: %w", err)
		}
	case ".json":
		if err := protojson.Unmarshal(data, db); err != nil {
			return nil, fmt.Errorf("parsing JSON: %w", err)
		}
	case ".pb", ".bin":
		if err := proto.Unmarshal(data, db); err != nil {
			return nil, fmt.Errorf("parsing binary proto: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown file extension: %s (supported: .textpb, .json, .pb)", ext)
	}

	return db, nil
}

// LoadMetaTableFromFile loads a single MetaTable from a file.
// Useful when defining individual tables in separate files.
func LoadMetaTableFromFile(path string) (*MetaTable, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	table := &MetaTable{}
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".textpb", ".txtpb", ".pbtxt":
		if err := prototext.Unmarshal(data, table); err != nil {
			return nil, fmt.Errorf("parsing text proto: %w", err)
		}
	case ".json":
		if err := protojson.Unmarshal(data, table); err != nil {
			return nil, fmt.Errorf("parsing JSON: %w", err)
		}
	case ".pb", ".bin":
		if err := proto.Unmarshal(data, table); err != nil {
			return nil, fmt.Errorf("parsing binary proto: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown file extension: %s", ext)
	}

	return table, nil
}

// SaveMetaDatabaseToFile saves a MetaDatabase to a file.
// Format is determined by file extension.
func SaveMetaDatabaseToFile(db *MetaDatabase, path string) error {
	ext := strings.ToLower(filepath.Ext(path))
	var data []byte
	var err error

	switch ext {
	case ".textpb", ".txtpb", ".pbtxt":
		data, err = prototext.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(db)
	case ".json":
		data, err = protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(db)
	case ".pb", ".bin":
		data, err = proto.Marshal(db)
	default:
		return fmt.Errorf("unknown file extension: %s", ext)
	}

	if err != nil {
		return fmt.Errorf("marshaling: %w", err)
	}

	return os.WriteFile(path, data, 0644)
}

// LoadMetaDatabaseFromDir loads a MetaDatabase by scanning a directory for table files.
// Each file named *.table.textpb (or .json) is loaded as a MetaTable.
func LoadMetaDatabaseFromDir(dir string, dbName string) (*MetaDatabase, error) {
	db := &MetaDatabase{Name: dbName}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Match patterns like users.table.textpb or orders.table.json
		if strings.Contains(name, ".table.") {
			path := filepath.Join(dir, name)
			table, err := LoadMetaTableFromFile(path)
			if err != nil {
				return nil, fmt.Errorf("loading table %s: %w", name, err)
			}
			db.Tables = append(db.Tables, table)
		}
	}

	return db, nil
}
