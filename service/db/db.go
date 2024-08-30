package db

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/marcboeker/go-duckdb" // load duckdb driver
	"os"
	"quackpipe/model"
	"time"
)

func Quack(appFlags model.CommandLineFlags, query string, stdin bool, params string, hashdb string) (*sql.Rows, time.Duration, error) {
	var err error
	alias := *appFlags.Alias
	motherduck, md := os.LookupEnv("motherduck_token")

	if len(hashdb) > 0 {
		params = hashdb + "?" + params
	}

	db, err := sql.Open("duckdb", params)
	if err != nil {
		return nil, 0, err
	}
	defer db.Close()

	if !stdin {
		check(db.ExecContext(context.Background(), "LOAD httpfs; LOAD json; LOAD parquet;"))
		check(db.ExecContext(context.Background(), "SET autoinstall_known_extensions=1;"))
		check(db.ExecContext(context.Background(), "SET autoload_known_extensions=1;"))
	}

	if alias {
		check(db.ExecContext(context.Background(), "LOAD chsql;"))
	}

	if (md) && (motherduck != "") {
		check(db.ExecContext(context.Background(), "LOAD motherduck; ATTACH 'md:';"))
	}
	startTime := time.Now()
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return nil, 0, err
	}
	elapsedTime := time.Since(startTime)
	return rows, elapsedTime, nil
}

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}

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

	fmt.Println("Connected to DuckDB successfully.")
	return db, nil
}

// CreateTablesTable creates the metadata table if it doesn't already exist
func CreateTablesTable(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS tables (
		name VARCHAR PRIMARY KEY, 
		path VARCHAR,
		field_names VARCHAR[],
		field_types VARCHAR[],
		order_by VARCHAR[],
		engine VARCHAR[],
		timestamp_field VARCHAR[],
		timestamp_precision VARCHAR[],
		partition_by VARCHAR[]
	);
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create tables metadata: %w", err)
	}
	return nil
}

func InsertTableMetadata(db *sql.DB, name, path string, fieldNames, fieldTypes, orderBy, engine, timestampField, timestampPrecision, partitionBy []string) error {
	query := `
	INSERT INTO tables (name, path, field_names, field_types, order_by, engine, timestamp_field, timestamp_precision, partition_by)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(name) DO UPDATE SET
		path = excluded.path,
		field_names = excluded.field_names,
		field_types = excluded.field_types,
		order_by = excluded.order_by,
		engine = excluded.engine,
		timestamp_field = excluded.timestamp_field,
		timestamp_precision = excluded.timestamp_precision,
		partition_by = excluded.partition_by;
	`

	_, err := db.Exec(query, name, path, fieldNames, fieldTypes, orderBy, engine, timestampField, timestampPrecision, partitionBy)
	if err != nil {
		return fmt.Errorf("failed to insert table metadata: %w", err)
	}
	return nil
}
