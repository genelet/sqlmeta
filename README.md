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

## Development

If you modify the `.proto` files, you must regenerate the Go code:

```bash
# From the project root
protoc -I=. --go_out=xmeta --go_opt=paths=source_relative proto/*.proto
```

Ensure you have the Protobuf compiler and Go plugins installed.
