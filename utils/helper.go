package utils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"quackpipe/model"
	"regexp"
	"strings"
	"time"
)

// ExtractAndRemoveFormat extracts the FORMAT clause from the query and returns the query without the FORMAT clause
func ExtractAndRemoveFormat(input string) (string, string) {
	re := regexp.MustCompile(`(?i)\bFORMAT\s+(\w+)\b`)
	match := re.FindStringSubmatch(input)
	if len(match) != 2 {
		return input, ""
	}
	format := match[1]
	return re.ReplaceAllString(input, ""), format
}

func ConversationOfRows(rows *sql.Rows, default_format string, duration time.Duration) (string, error) {

	switch default_format {
	case "JSONCompact", "JSON":
		result, err := rowsToJSON(rows, duration)
		if err != nil {
			return "", err
		}
		return result, nil
	case "CSVWithNames":
		result, err := rowsToCSV(rows, true)
		if err != nil {
			return "", err
		}
		return result, nil
	case "TSVWithNames", "TabSeparatedWithNames":

		result, err := rowsToTSV(rows, true)
		if err != nil {
			return "", err
		}
		return result, nil
	case "TSV", "TabSeparated":
		result, err := rowsToTSV(rows, true)
		if err != nil {
			return "", err
		}
		return result, nil

	}

	return "", nil
}

// rowsToJSON converts the rows to JSON string
func rowsToJSON(rows *sql.Rows, elapsedTime time.Duration) (string, error) {
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}

	// Create a slice to store maps of column names and their corresponding values
	var results model.OutputJSON
	results.Meta = make([]model.Metadata, len(columns))
	results.Data = make([][]interface{}, 0)

	for i, column := range columns {
		results.Meta[i].Name = column
	}

	for rows.Next() {
		// Create a slice to hold pointers to the values of the columns
		values := make([]interface{}, len(columns))
		for i := range columns {
			values[i] = new(interface{})
		}

		// Scan the values from the row into the pointers
		err := rows.Scan(values...)
		if err != nil {
			return "", err
		}

		// Create a slice to hold the row data
		rowData := make([]interface{}, len(columns))
		for i, value := range values {
			// Convert the value to the appropriate Go type
			switch v := (*(value.(*interface{}))).(type) {
			case []byte:
				rowData[i] = string(v)
			default:
				rowData[i] = v
			}
		}
		results.Data = append(results.Data, rowData)
	}

	err = rows.Err()
	if err != nil {
		return "", err
	}

	results.Rows = len(results.Data)
	results.RowsBeforeLimitAtLeast = len(results.Data)

	// Populate the statistics object with number of rows, bytes, and elapsed time
	results.Statistics.Elapsed = elapsedTime.Seconds()
	results.Statistics.RowsRead = results.Rows
	// Note: bytes_read is an approximation, it's just the number of rows * number of columns
	// results.Statistics.BytesRead = results.Rows * len(columns) * 8 // Assuming each value takes 8 bytes
	jsonData, err := json.Marshal(results)
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}

// rowsToTSV converts the rows to TSV string
func rowsToTSV(rows *sql.Rows, cols bool) (string, error) {
	var result []string
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}

	if cols {
		// Append column names as the first row
		result = append(result, strings.Join(columns, "\t"))
	}

	// Fetch rows and append their values as tab-delimited lines
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return "", err
		}

		var lineParts []string
		for _, v := range values {
			lineParts = append(lineParts, fmt.Sprintf("%v", v))
		}
		result = append(result, strings.Join(lineParts, "\t"))
	}

	if err := rows.Err(); err != nil {
		return "", err
	}

	return strings.Join(result, "\n"), nil
}

// rowsToCSV converts the rows to CSV string
func rowsToCSV(rows *sql.Rows, cols bool) (string, error) {
	var result []string
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}

	if cols {
		// Append column names as the first row
		result = append(result, strings.Join(columns, ","))
	}

	// Fetch rows and append their values as CSV rows
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			return "", err
		}

		var lineParts []string
		for _, v := range values {
			lineParts = append(lineParts, fmt.Sprintf("%v", v))
		}
		result = append(result, strings.Join(lineParts, ","))
	}

	if err := rows.Err(); err != nil {
		return "", err
	}

	return strings.Join(result, "\n"), nil
}
