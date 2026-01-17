# sqlmeta

`sqlmeta` is a Go package and Protobuf definition library for representing and loading database metadata in a standardized format. It is part of the `sqlproto` ecosystem, designed to bridge valid SQL ASTs (`sqlast`) with concrete database schemas.

## Overview

The package serves three main purposes:
1.  **Metadata Definitions**: Defines Protobuf messages for database objects (Tables, Columns, Schemas, etc.) and data types, shared across different SQL dialects (PostgreSQL, MySQL, BigQuery, SQLite).
2.  **Metadata Loaders**: Provides Go functions to introspect running databases and populate these Protobuf structures.
3.  **Unified Abstraction**: Offers a Unified Metadata Model (`MetaDatabase`, `MetaTable`) and conversion functions to bridge dialect-specific metadata into a single, canonical format suitable for generic tooling.

## Directory Structure

- **`proto/`**: Contains the Protocol Buffer definitions.
  - `types.proto`: **Core Unified Types** (`MetaTable`, `ColumnDef`, `DataType`).
  - `pg_meta.proto`: PostgreSQL-specific metadata structures.
  - `my_meta.proto`: MySQL-specific metadata structures.
  - `bq_meta.proto`: BigQuery-specific metadata structures.
  - `sqlite_meta.proto`: SQLite-specific metadata structures.

- **`xmeta/`**: Contains the generated Go code from the protos and the loader implementations.
  - `*_loader.go`: Dialect-specific loaders (e.g., `LoadPostgres`, `LoadMySQL`).
  - `convert.go`: **Conversion Layer** to transform dialect-specific structs into Unified Metadata.

## Core Unified Types

`sqlmeta` introduces a Unified Metadata Model in `types.proto` to act as a pivot format.

### `MetaDatabase` & `MetaTable`
A high-level container for an entire database schema and its tables.
```protobuf
message MetaTable {
    ObjectName Name = 1;
    string Type = 2; // BASE TABLE, VIEW, etc.
    repeated TableElement Elements = 3; // Columns and Constraints
    string Comment = 4;
    map<string, string> Options = 5; // Dialect-specific options (e.g. Engine, Owner)
}
```

### `ColumnDef`
A unified column definition. Defaults and Check Expressions are stored as `google.protobuf.Any` to support both simple strings (`wrapperspb.StringValue`) and complex AST nodes.
```protobuf
message ColumnDef {
    string Name = 1;
    DataType DataType = 2;
    google.protobuf.Any Default = 3;
    repeated ColumnConstraint Constraints = 5;
    string Comment = 6;
    map<string, string> Options = 7;
}
```

## Usage

### 1. Loading Dialect-Specific Metadata

You can use the loaders in the `xmeta` package to inspect a connected database (e.g., Postgres).

```go
import (
    "database/sql"
    _ "github.com/lib/pq"
    "github.com/genelet/sqlmeta/xmeta"
)

func main() {
    db, _ := sql.Open("postgres", "...")
    defer db.Close()

    // Load Postgres-specific metadata
    pgMeta, err := xmeta.LoadPostgres(db)
    if err != nil { panic(err) }
}
```

### 2. Converting to Unified Metadata

Once loaded, you can convert the dialect-specific structs into the Unified Format. This allows you to write generic logic that works for any database.

```go
    // Convert a specific Postgres table to the Unified MetaTable
    pgTable := pgMeta.Schemas[0].Tables[0]
    unifiedTable := xmeta.PGTableToMetaTable(pgTable)

    fmt.Printf("Unified Table: %s\n", unifiedTable.Name.Idents)
    fmt.Printf("Comment: %s\n", unifiedTable.Comment)
    
    for _, elem := range unifiedTable.Elements {
        if col := elem.GetColumnDefElement(); col != nil {
            fmt.Printf("  Column: %s (%s)\n", col.Name, col.DataType)
        }
    }
```

### 3. Comparing Schemas (Migration Support)

The **Diff Engine** compares two `MetaDatabase` states and outputs a list of changes. This enables declarative migrations and drift detection.

```go
    // Load live database
    currentDB, _ := xmeta.LoadPostgres(db)
    currentMeta := xmeta.ConvertToMetaDatabase(currentDB) // If conversion helper exists

    // Load desired state (from proto definition or parsed SQL)
    desiredMeta := &xmeta.MetaDatabase{...}

    // Compute diff
    changes := xmeta.DiffDatabase(currentMeta, desiredMeta)

    for _, change := range changes {
        switch c := change.(type) {
        case xmeta.AddTable:
            fmt.Printf("ADD TABLE %s\n", c.Table.Name.Idents)
        case xmeta.DropColumn:
            fmt.Printf("DROP COLUMN %s.%s\n", c.TableName.Idents, c.ColumnName)
        case xmeta.AlterColumn:
            fmt.Printf("ALTER COLUMN %s.%s\n", c.TableName.Idents, c.NewColumn.Name)
        }
    }
```

**Features:**
- `IsDestructive()` method identifies dangerous changes (DropTable, DropColumn).
- Changes are automatically sorted for safe execution order (drop constraints before tables).
- Diffs are schema-aware: table identity uses the full `ObjectName.Idents` chain (e.g., `schema.table`).

## Complete Migration Workflow Example

This example demonstrates the full declarative migration cycle:

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/lib/pq"
    "github.com/genelet/sqlmeta/xmeta"
)

func main() {
    // =========================================================================
    // Step 1: Connect to Postgres and read current metadata
    // =========================================================================
    db, err := sql.Open("postgres", "postgres://user:pass@localhost:5432/mydb?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    pgMeta, err := xmeta.LoadPostgres(db)
    if err != nil {
        log.Fatal(err)
    }

    // =========================================================================
    // Step 2: Convert dialect-specific metadata to unified MetaDatabase
    // =========================================================================
    currentDB := &xmeta.MetaDatabase{Name: "mydb"}
    for _, schema := range pgMeta.Schemas {
        for _, table := range schema.Tables {
            currentDB.Tables = append(currentDB.Tables, xmeta.PGTableToMetaTable(table))
        }
    }

    // Save current state to a text proto file for version control
    xmeta.SaveMetaDatabaseToFile(currentDB, "schema_current.textpb")
    fmt.Println("Current schema saved to schema_current.textpb")

    // =========================================================================
    // Step 3: Define desired state (add a new column)
    // =========================================================================
    // Option A: Load from a text proto file you edited
    // desiredDB, _ := xmeta.LoadMetaDatabaseFromFile("schema_desired.textpb")

    // Option B: Programmatically modify the current state
    desiredDB := cloneMetaDatabase(currentDB)

    // Find the "users" table and add a "phone" column
    for _, table := range desiredDB.Tables {
        if xmeta.TableName(table.Name) == "users" {
            table.Elements = append(table.Elements, &xmeta.TableElement{
                TableElementClause: &xmeta.TableElement_ColumnDefElement{
                    ColumnDefElement: &xmeta.ColumnDef{
                        Name:     "phone",
                        DataType: &xmeta.DataType{TypeClause: &xmeta.DataType_VarcharData{VarcharData: &xmeta.VarcharType{Size: 20}}},
                        Comment:  "User phone number",
                    },
                },
            })
        }
    }

    // Save desired state
    xmeta.SaveMetaDatabaseToFile(desiredDB, "schema_desired.textpb")
    fmt.Println("Desired schema saved to schema_desired.textpb")

    // =========================================================================
    // Step 4: Compute the diff (migration plan)
    // =========================================================================
    changes := xmeta.DiffDatabase(currentDB, desiredDB)

    fmt.Printf("\n=== Migration Plan (%d changes) ===\n", len(changes))
    for _, change := range changes {
        destructive := ""
        if change.IsDestructive() {
            destructive = " [DESTRUCTIVE]"
        }

        switch c := change.(type) {
        case xmeta.AddTable:
            fmt.Printf("  ADD TABLE %s%s\n", c.Table.Name.Idents, destructive)
        case xmeta.DropTable:
            fmt.Printf("  DROP TABLE %s%s\n", c.TableName.Idents, destructive)
        case xmeta.AddColumn:
            fmt.Printf("  ADD COLUMN %s.%s%s\n", c.TableName.Idents, c.Column.Name, destructive)
        case xmeta.DropColumn:
            fmt.Printf("  DROP COLUMN %s.%s%s\n", c.TableName.Idents, c.ColumnName, destructive)
        case xmeta.AlterColumn:
            fmt.Printf("  ALTER COLUMN %s.%s%s\n", c.TableName.Idents, c.NewColumn.Name, destructive)
        }
    }

    // =========================================================================
    // Step 5: Apply changes (generate SQL or use your migration tool)
    // =========================================================================
    // For each change, generate the appropriate SQL:
    // - AddColumn -> "ALTER TABLE users ADD COLUMN phone VARCHAR(20)"
    // - DropColumn -> "ALTER TABLE users DROP COLUMN legacy_field"
    // This step is left to the consumer or a future SQL generator.
}

// Helper: Deep clone a MetaDatabase (simplified)
func cloneMetaDatabase(src *xmeta.MetaDatabase) *xmeta.MetaDatabase {
    // In production, use proto.Clone() for proper deep copy
    dst := &xmeta.MetaDatabase{Name: src.Name}
    for _, t := range src.Tables {
        dst.Tables = append(dst.Tables, t) // Shallow copy for demo
    }
    return dst
}
```

### Text Proto Schema File Example

After running `SaveMetaDatabaseToFile()`, you get a human-readable file:

```protobuf
# schema_desired.textpb
name: "mydb"
tables {
  name { idents: ["public", "users"] }
  comment: "User accounts"
  elements {
    column_def_element {
      name: "id"
      data_type { int_data {} }
    }
  }
  elements {
    column_def_element {
      name: "email"
      data_type { varchar_data { size: 255 } }
    }
  }
  elements {
    column_def_element {
      name: "phone"
      data_type { varchar_data { size: 20 } }
      comment: "User phone number"
    }
  }
}
```

You can edit this file directly and reload it to drive migrations.

## Development

If you modify the `.proto` files, you must regenerate the Go code. The output location is fixed to `xmeta/`.

```bash
# From proto/
cd /home/peter/Workspace/sqlmeta/proto
protoc -I=. --go_out=../xmeta --go_opt=paths=source_relative *.proto
```

Ensure you have the Protobuf compiler and Go plugins installed.
