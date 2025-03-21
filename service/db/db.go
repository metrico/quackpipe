package db

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/marcboeker/go-duckdb/v2" // load duckdb driver
	"github.com/metrico/quackpipe/model"
	"os"
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
		if alias {
			check(db.ExecContext(context.Background(), "LOAD chsql; LOAD chsql_native;"))
		}
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

	return db, nil
}
