package main

import (
	"fmt"
	"os"

	duckdbcapi "github.com/fanaujie/duckdb-capi-cgo-bindings"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Open in-memory database
	db, err := duckdbcapi.Open("")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Create connection
	conn, err := db.Connection()
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}
	defer conn.Disconnect()

	// Create table
	if err := conn.Query("CREATE TABLE integers(i INTEGER, j INTEGER);", nil); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert data
	if err := conn.Query("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", nil); err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	// Query data
	var result duckdbcapi.Result
	if err := conn.Query("SELECT * FROM integers", &result); err != nil {
		return fmt.Errorf("failed to query data: %w", err)
	}
	defer result.Destroy()

	// Print column names
	columnCount := result.ColumnCount()
	for i := uint64(0); i < columnCount; i++ {
		name, err := result.ColumnName(i)
		if err != nil {
			return fmt.Errorf("failed to get column name: %w", err)
		}
		fmt.Printf("%s ", name)
	}
	fmt.Println()

	// Print data
	rowCount := result.RowCount()
	for rowIdx := uint64(0); rowIdx < rowCount; rowIdx++ {
		for colIdx := uint64(0); colIdx < columnCount; colIdx++ {
			val := result.ValueVarChar(colIdx, rowIdx)
			fmt.Printf("%s ", val)
		}
		fmt.Println()
	}

	return nil
}
