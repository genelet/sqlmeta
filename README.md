# sqlmeta

`sqlmeta` is a Go package and Protobuf definition library for representing and loading database metadata in a standardized format. It is part of the `sqlproto` ecosystem, designed to bridge valid SQL ASTs (`sqlast`) with concrete database schemas.

## Overview

The package serves two main purposes:
1.  **Metadata Definitions**: Defines Protobuf messages for database objects (Tables, Columns, Schemas, etc.) and data types, shared across different SQL dialects (PostgreSQL, BigQuery, SQLite).
2.  **Metadata Loaders**: Provides Go functions to introspect running databases and populate these Protobuf structures.

## Directory Structure

- **`proto/`**: Contains the Protocol Buffer definitions.
  - `types.proto`: Core types used across dialects (e.g., `ObjectName`, `DataType`).
  - `pg_meta.proto`: PostgreSQL-specific metadata structures.
  - `bq_meta.proto`: BigQuery-specific metadata structures.
  - `sqlite_meta.proto`: SQLite-specific metadata structures.

- **`xmeta/`**: Contains the generated Go code from the protos and the loader implementations.
  - `*_loader.go`: Go functions to connect to a database `*sql.DB` and load metadata.

## Usage

### Loading Metadata

You can use the loaders in the `xmeta` package to inspect a connected database.

```go
import (
    "database/sql"
    _ "github.com/lib/pq"
    "github.com/genelet/sqlmeta/xmeta"
)

func main() {
    // Connect to your database
    db, _ := sql.Open("postgres", "postgres://user:pass@localhost:5432/dbname?sslmode=disable")
    defer db.Close()

    // Load metadata
    pgMeta, err := xmeta.LoadPostgres(db)
    if err != nil {
        panic(err)
    }

    // Access schema information
    for _, schema := range pgMeta.Schemas {
        fmt.Printf("Schema: %s\n", schema.Name)
        for _, table := range schema.Tables {
            fmt.Printf("  Table: %s (Columns: %d)\n", table.Name.Idents, len(table.Columns))
        }
    }
}
```

## Core Types

### `ObjectName`
A unified representation for identifiers, handling qualified names (e.g., `schema.table`).

```protobuf
message ObjectName {
    repeated string Idents = 1;
}
```

### `DataType`
A comprehensive union of supported SQL data types, allowing precise type mapping.

## Development

If you modify the `.proto` files, you must regenerate the Go code:

```bash
# From the project root
protoc -I. --go_out=. proto/*.proto
```

Ensure you have the Protobuf compiler and Go plugins installed.
