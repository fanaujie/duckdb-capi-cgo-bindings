# DuckDB C API Go Bindings

Go bindings for [DuckDB](https://duckdb.org/) C API using CGO.

> **Warning**: This project is for **learning and testing purposes only**. Do NOT use in production environments.
>
> For production use, please use the official DuckDB Go client: https://github.com/duckdb/duckdb-go

## Overview

This project provides CGO bindings to DuckDB's C API, offering a learning resource for:
- Understanding how to wrap C libraries in Go using CGO
- Exploring DuckDB's C API functionality
- Testing and experimenting with DuckDB features

Originally developed in 2022 with DuckDB v0.3.4. Contributions to update and evolve the project are welcome.

## Prerequisites

- Go 1.18 or later
- DuckDB v0.3.4 library and headers

## Installation

### 1. Download DuckDB v0.3.4

Download the appropriate release for your platform from:
https://github.com/duckdb/duckdb/releases/tag/v0.3.4

### 2. Install Library and Headers

Extract and install the DuckDB library (`libduckdb.so` / `libduckdb.dylib`) and header file (`duckdb.h`) to your system paths, for example:

```bash
# Linux example
sudo cp libduckdb.so /usr/local/lib/
sudo cp duckdb.h /usr/local/include/
sudo ldconfig
```

### 3. Get the Package

```bash
go get github.com/fanaujie/duckdb-capi-cgo-bindings
```

## Usage Examples

### Basic Query

```go
package main

import (
    "fmt"
    duckdbcapi "github.com/fanaujie/duckdb-capi-cgo-bindings"
)

func main() {
    // Open in-memory database
    db, _ := duckdbcapi.Open("")
    defer db.Close()

    // Create connection
    conn, _ := db.Connection()
    defer conn.Disconnect()

    // Execute queries
    conn.Query("CREATE TABLE test (id INTEGER, name VARCHAR)", nil)
    conn.Query("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')", nil)

    // Query with results
    var result duckdbcapi.Result
    conn.Query("SELECT * FROM test", &result)
    defer result.Destroy()

    // Iterate results
    for row := uint64(0); row < result.RowCount(); row++ {
        id := result.ValueInt32(0, row)
        name := result.ValueVarChar(1, row)
        fmt.Printf("id: %d, name: %s\n", id, name)
    }
}
```

### Query Parquet Files

```go
// DuckDB can query Parquet files directly
query := "SELECT * FROM read_parquet('data.parquet') LIMIT 10"
conn.Query(query, &result)
```

See the `examples/` directory for complete runnable examples:
- `examples/basic-query/` - Basic SQL operations
- `examples/parquet-query/` - Querying Parquet files

## Running Tests

```bash
CGO_CFLAGS="-I/usr/local/include" \
CGO_LDFLAGS="-L/usr/local/lib -lduckdb" \
go test -v
```

## Building Examples

```bash
cd examples/basic-query
CGO_CFLAGS="-I/usr/local/include" \
CGO_LDFLAGS="-L/usr/local/lib -lduckdb" \
go build main.go
```

## Contributing

Contributions are welcome! This project was originally developed in 2022 and would benefit from:

- Updating to newer DuckDB versions
- Adding more examples
- Improving documentation
- Bug fixes and enhancements

Feel free to open issues or submit pull requests.

## License

MIT License
