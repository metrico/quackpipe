package main

import (
	"bufio"
	"database/sql"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb" // load duckdb driver
)

//go:embed play.html
var staticPlay string

//go:embed aliases.sql
var staticAliases string

// params for Flags
type CommandLineFlags struct {
	Host   *string `json:"host"`
	Port   *string `json:"port"`
	Stdin  *bool   `json:"stdin"`
	Format *string `json:"format"`
	Params *string `json:"params"`
}

var appFlags CommandLineFlags

var db *sql.DB

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}

func quack(query string, stdin bool, format string, params string) (string, error) {
	var err error

	db, err = sql.Open("duckdb", params)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if !stdin {
		check(db.Exec("LOAD httpfs; LOAD json; LOAD parquet;"))
	}
	
	if staticAliases != "" {
		check(db.Exec(staticAliases))
	}
	
	startTime := time.Now()
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	elapsedTime := time.Since(startTime)

	switch format {
	case "JSONCompact", "JSON":
		return rowsToJSON(rows, elapsedTime)
	case "CSVWithNames":
		return rowsToCSV(rows, true)
	case "TSVWithNames", "TabSeparatedWithNames":
		return rowsToTSV(rows, true)
	case "TSV", "TabSeparated":
		return rowsToTSV(rows, false)
	default:
		return rowsToTSV(rows, false)
	}
}

// initFlags initializes the command line flags
func initFlags() {
	appFlags.Host = flag.String("host", "0.0.0.0", "API host. Default 0.0.0.0")
	appFlags.Port = flag.String("port", "8123", "API port. Default 8123")
	appFlags.Format = flag.String("format", "JSONCompact", "API port. Default JSONCompact")
	appFlags.Params = flag.String("params", "", "DuckDB optional parameters. Default to none.")
	appFlags.Stdin = flag.Bool("stdin", false, "STDIN query. Default false")
	flag.Parse()
}

// extractAndRemoveFormat extracts the FORMAT clause from the query and returns the query without the FORMAT clause
func extractAndRemoveFormat(input string) (string, string) {
	re := regexp.MustCompile(`(?i)\bFORMAT\s+(\w+)\b`)
	match := re.FindStringSubmatch(input)
	if len(match) != 2 {
		return input, ""
	}
	format := match[1]
	return re.ReplaceAllString(input, ""), format
}

// Metadata is the metadata for a column
type Metadata struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Statistics is the statistics for a query
type Statistics struct {
	Elapsed   float64 `json:"elapsed"`
	RowsRead  int     `json:"rows_read"`
	BytesRead int     `json:"bytes_read"`
}

// OutputJSON is the JSON output for a query
type OutputJSON struct {
	Meta                   []Metadata      `json:"meta"`
	Data                   [][]interface{} `json:"data"`
	Rows                   int             `json:"rows"`
	RowsBeforeLimitAtLeast int             `json:"rows_before_limit_at_least"`
	Statistics             Statistics      `json:"statistics"`
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
	var results OutputJSON
	results.Meta = make([]Metadata, len(columns))
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

func main() {
	initFlags()
	default_format := *appFlags.Format
	default_params := *appFlags.Params
	if *appFlags.Stdin {
		scanner := bufio.NewScanner((os.Stdin))
		query := ""
		for scanner.Scan() {
			query = query + "\n" + scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "reading standard input:", err)
		}
		cleanquery, format := extractAndRemoveFormat(query)
		if len(format) > 0 {
			query = cleanquery
			default_format = format
		}
		result, err := quack(query, true, default_format, default_params)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		} else {
			fmt.Println(result)
		}
	} else {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			var bodyBytes []byte
			var query string
			var err error

			// handle query parameter
			if r.URL.Query().Get("query") != "" {
				// query = r.FormValue("query")
				query = r.URL.Query().Get("query")
			} else if r.Body != nil {
				bodyBytes, err = ioutil.ReadAll(r.Body)
				if err != nil {
					fmt.Printf("Body reading error: %v", err)
					return
				}
				defer r.Body.Close()
				query = string(bodyBytes)
			}

			switch r.Header.Get("Accept") {
			case "application/json":
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
			case "application/xml":
				w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			case "text/css":
				w.Header().Set("Content-Type", "text/css; charset=utf-8")
			default:
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
			}
			
			// format handling
			if r.URL.Query().Get("default_format") != "" {
				default_format = r.URL.Query().Get("default_format")
			}
			// param handling
			if r.URL.Query().Get("default_params") != "" {
				default_params = r.URL.Query().Get("default_params")
			}
			// extract FORMAT from query and override the current `default_format`
			cleanquery, format := extractAndRemoveFormat(query)
			if len(format) > 0 {
				query = cleanquery
				default_format = format
			}

			if len(query) == 0 {
				_, _ = w.Write([]byte(staticPlay))
			} else {
				result, err := quack(query, false, default_format, default_params)
				if err != nil {
					_, _ = w.Write([]byte(err.Error()))
				} else {
					_, _ = w.Write([]byte(result))
				}
			}
		})

		fmt.Printf("QuackPipe API Running: %s:%s\n", *appFlags.Host, *appFlags.Port)
		if err := http.ListenAndServe(*appFlags.Host+":"+*appFlags.Port, nil); err != nil {
			panic(err)
		}
	}
}
