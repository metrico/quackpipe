package db

import (
	"database/sql"
	"fmt"
	_ "github.com/marcboeker/go-duckdb/v2" // load duckdb driver
)

// ConnectDuckDB opens and returns a connection to DuckDB.
func ConnectDuckDB(filePath string) (*sql.DB, error) {
	// Open DuckDB connection (this will create a DuckDB instance in the specified file)
	db, err := sql.Open("duckdb", filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// Test the connection
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to DuckDB: %w", err)
	}

	return db, nil
}
