package utils

import (
	"database/sql"
	"fmt"
	_ "github.com/marcboeker/go-duckdb/v2" // load duckdb driver
	"sync"
	"sync/atomic"
	"time"
)

var poolMap sync.Map

type dbWrapper struct {
	*sql.DB
	initedAt time.Time
}

var dbHeld int32
var poolSize int32

func init() {
	t := time.NewTicker(time.Second * 30)
	go func() {
	    for range t.C {
	        active := atomic.LoadInt32(&dbHeld)
	        idle := atomic.LoadInt32(&poolSize)
	        // Print when usage is high
	        if active >= idle-2 {
	            fmt.Printf("Duckdb pool stats: %d active / %d idle\n", active, idle)
	        }
	    }
	}()

}

// ConnectDuckDB opens and returns a connection to DuckDB.
func ConnectDuckDB(filePath string) (*sql.DB, func(), error) {
	// Open DuckDB connection (this will create a DuckDB instance in the specified file)
	pool, _ := poolMap.LoadOrStore(filePath, &sync.Pool{})
	db := pool.(*sync.Pool).Get()
	cancel := func() {
		atomic.AddInt32(&dbHeld, -1)
		if time.Now().Sub(db.(*dbWrapper).initedAt).Minutes() > 5 || atomic.LoadInt32(&poolSize) > 5 {
			db.(*dbWrapper).Close()
			return
		}
		atomic.AddInt32(&poolSize, 1)
		pool.(*sync.Pool).Put(db.(*dbWrapper))
	}
	if db != nil {
		atomic.AddInt32(&poolSize, -1)
		atomic.AddInt32(&dbHeld, 1)
		return db.(*dbWrapper).DB, cancel, nil
	}
	db, err := sql.Open("duckdb", filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}
	db = &dbWrapper{db.(*sql.DB), time.Now()}
	// Test the connection
	if err = db.(*dbWrapper).Ping(); err != nil {
		db.(*dbWrapper).Close()
		return nil, nil, fmt.Errorf("failed to connect to DuckDB: %w", err)
	}
	atomic.AddInt32(&dbHeld, 1)
	return db.(*dbWrapper).DB, cancel, nil
}
