package main

import (
	"fmt"
	"os"
	"path/filepath"

	duckdbcapi "github.com/fanaujie/duckdb-capi-cgo-bindings"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Get the directory where this executable is located
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}
	execDir := filepath.Dir(execPath)

	// Parquet file path (relative to examples directory)
	parquetFile := filepath.Join(execDir, "..", "yellow_tripdata_2022-01.parquet")

	// For go run, use relative path from current working directory
	if _, err := os.Stat(parquetFile); os.IsNotExist(err) {
		parquetFile = filepath.Join("examples", "yellow_tripdata_2022-01.parquet")
	}

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

	// Query parquet file directly
	query := fmt.Sprintf("SELECT * FROM read_parquet('%s') LIMIT 10", parquetFile)
	var result duckdbcapi.Result
	if err := conn.Query(query, &result); err != nil {
		return fmt.Errorf("failed to query parquet file: %w", err)
	}
	defer result.Destroy()

	// Print column names
	columnCount := result.ColumnCount()
	for i := uint64(0); i < columnCount; i++ {
		name, err := result.ColumnName(i)
		if err != nil {
			return fmt.Errorf("failed to get column name: %w", err)
		}
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(name)
	}
	fmt.Println()

	// Print separator
	for i := uint64(0); i < columnCount; i++ {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print("--------")
	}
	fmt.Println()

	// Print data
	rowCount := result.RowCount()
	for rowIdx := uint64(0); rowIdx < rowCount; rowIdx++ {
		for colIdx := uint64(0); colIdx < columnCount; colIdx++ {
			if colIdx > 0 {
				fmt.Print("\t")
			}
			val := result.ValueVarChar(colIdx, rowIdx)
			fmt.Print(val)
		}
		fmt.Println()
	}

	fmt.Printf("\nTotal rows returned: %d\n", rowCount)

	return nil
}
