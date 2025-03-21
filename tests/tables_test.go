package tests

import (
	"database/sql"
	"github.com/google/uuid"
	"github.com/metrico/quackpipe/repository"
	db2 "github.com/metrico/quackpipe/service/db"
	"log"
	"os"
	"testing"
)

var dbFilePath string
var db *sql.DB

// TestMain is called by the testing framework before any tests are run.
func TestMain(m *testing.M) {
	var err error
	dbFilePath = "test2" + ".db"
	// Initialize the database
	db, err = db2.ConnectDuckDB(dbFilePath)
	if err != nil {
		log.Fatalf("failed to open DuckDB database: %v", err)
	}

	// Create necessary tables
	if err := repository.CreateDuckDBTablesTable(db); err != nil {
		db.Close() // Ensure DB is closed on error
		log.Fatalf("failed to create DuckDB tables table: %v", err)
	}

	// Run the tests
	code := m.Run()

	// Teardown - close the database
	db.Close()

	// Exit with the test result code
	os.Exit(code)
}

func TestPersistentStorage(t *testing.T) {
	// Insert some metadata
	if err := repository.InsertTableMetadata(db,
		"test_table"+uuid.New().String(),
		"/path/to/table_!", []string{"field3"},
		[]string{"VARCHAR 2"},
		[]string{"field1 ASC 2"}, "some_engine_1",
		[]string{"created_at_2"},
		[]string{"SECOND_2"},
		[]string{"partition_field_2"}); err != nil {
		t.Fatalf("failed to insert table metadata: %v", err)
	}

	db.Close()

	// Reconnect to the database
	var err error
	db, err = db2.ConnectDuckDB(dbFilePath)
	if err != nil {
		t.Fatalf("failed to reopen DuckDB database: %v", err)
	}
	defer db.Close()

	// Display all data from the 'tables' table
	if err := repository.DisplayAllData(db, "tables"); err != nil {
		t.Fatalf("failed to display table data: %v", err)
	}

}

func generateUniqueID() string {
	id := uuid.New()
	return id.String()
}
