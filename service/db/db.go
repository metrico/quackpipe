package db

import (
	"context"
	"database/sql"
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
	
	check(db.ExecContext(context.Background(), "LOAD chsql;"))
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
