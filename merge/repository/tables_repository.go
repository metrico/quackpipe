package repository

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"quackpipe/model"
	"sync"
)

var dbMtx sync.Mutex

func CreateDuckDBTablesTable(db *sql.DB) error {
	// Adjusted schema using DuckDB's ARRAY type
	query := `
	CREATE TABLE IF NOT EXISTS tables (
		name VARCHAR PRIMARY KEY, 
		paths VARCHAR[],
		fs_path VARCHAR,
		field_names  VARCHAR[],     
		field_types VARCHAR[],     
		order_by VARCHAR[],       
		engine VARCHAR,     
		timestamp_field VARCHAR,    
		timestamp_precision VARCHAR,
		partition_by VARCHAR   
	);
	`

	// Execute the query to create the table if it doesn't exist
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create 'tables' table in DuckDB: %v", err)
	}

	return nil
}

func InsertTableMetadata(db *sql.DB, t *model.Table) error {
	fieldNames := make([]string, len(t.Fields))
	fieldTypes := make([]string, len(t.Fields))
	for i, field := range t.Fields {
		fieldNames[i] = field[0]
		fieldTypes[i] = field[1]
	}
	fieldNamesJSON, err := json.Marshal(fieldNames)
	if err != nil {
		return err
	}
	fieldTypesJSON, err := json.Marshal(fieldTypes)
	if err != nil {
		return err
	}
	orderByJSON, err := json.Marshal(t.OrderBy)
	if err != nil {
		return err
	}
	pathsJson, err := json.Marshal(t.Paths)

	query := `
INSERT INTO tables (
	name, paths, fs_path, field_names, field_types, order_by, engine, timestamp_field, timestamp_precision, partition_by
) SELECT ?, ?::JSON::VARCHAR[], ?, ?::JSON::VARCHAR[], ?::JSON::VARCHAR[], ?::JSON::VARCHAR[], ?, ?, ?, ? 
ON CONFLICT DO NOTHING`
	_, err = db.Exec(query,
		t.Name, string(pathsJson), t.FSPath,
		string(fieldNamesJSON), string(fieldTypesJSON), string(orderByJSON),
		t.Engine, t.TimestampField, t.TimestampPrecision, t.PartitionBy)

	return err
}

func DisplayAllData(db *sql.DB, tableName string) error {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table data: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	fmt.Println("Table Data:")
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		for i, col := range values {
			if col != nil {
				fmt.Printf("%s: %v\t", columns[i], col)
			}
		}
		fmt.Println()
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error occurred during row iteration: %w", err)
	}

	return nil
}
