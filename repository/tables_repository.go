package repository

import (
	"database/sql"
	"fmt"
	"strings"
)

func CreateDuckDBTablesTable(db *sql.DB) error {
	// Adjusted schema using DuckDB's ARRAY type
	query := `
	CREATE TABLE IF NOT EXISTS tables (
		name VARCHAR PRIMARY KEY, 
		path VARCHAR,
		field_names  VARCHAR[],     
		field_types VARCHAR[],     
		order_by VARCHAR[],       
		engine VARCHAR,     
		timestamp_field VARCHAR,    
		timestamp_precision VARCHAR,
		partition_by VARCHAR[]   
	);
	`

	// Execute the query to create the table if it doesn't exist
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create 'tables' table in DuckDB: %v", err)
	}

	return nil
}

func InsertTableMetadata(db *sql.DB, name, path string, fieldNames []string, fieldTypes []string, orderBy []string, engine string, timestampField, timestampPrecision, partitionBy []string) error {

	fieldNamesStr := fmt.Sprintf("[%s]", strings.Join(fieldNames, ", "))
	fieldTypesStr := fmt.Sprintf("[%s]", strings.Join(fieldTypes, ", "))
	orderByStr := fmt.Sprintf("[%s]", strings.Join(orderBy, ", "))
	partitionByStr := fmt.Sprintf("[%s]", strings.Join(partitionBy, ", "))
	timestampPrecisionStr := fmt.Sprintf("[%s]", strings.Join(timestampPrecision, ", "))
	timestampFieldStr := fmt.Sprintf("[%s]", strings.Join(timestampField, ", "))
	query := `INSERT INTO tables (
        name, path, field_names, field_types, order_by, engine, timestamp_field, timestamp_precision, partition_by
    ) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (name) DO UPDATE SET
        path = excluded.path,
        order_by = excluded.order_by,
        engine = excluded.engine,
        timestamp_field = excluded.timestamp_field,
        timestamp_precision = excluded.timestamp_precision,
        partition_by = excluded.partition_by;`
	// Prepare the SQL statement
	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("failed to prepare SQL statement: %w", err)
	}
	defer stmt.Close()

	// Execute the SQL statement with the converted array literals
	_, err = stmt.Exec(name, path, fieldNamesStr, fieldTypesStr, orderByStr, engine, timestampFieldStr, timestampPrecisionStr, partitionByStr)
	if err != nil {
		return fmt.Errorf("failed to insert table metadata: %w", err)
	}
	return nil
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
