package main

import (
	"database/sql"
	_ "embed"
	"encoding/json"
//	"encoding/csv"
	"flag"
	"fmt"
	"time"
	"io/ioutil"
	"log"
	"net/http"
	"bufio"
	"os"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

//go:embed play.html
var staticPlay string

// params for Flags
type CommandLineFlags struct {
	Host *string `json:"host"`
	Port *string `json:"port"`
	Stdin *bool `json:"stdin"`
	Format *string `json:"format"`
}

var appFlags CommandLineFlags

var (
	db *sql.DB
)

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}

func quack(query string, stdin bool, format string) string {

	var err error

	db, err = sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if stdin != true {
		check(db.Exec("LOAD httpfs; LOAD json; LOAD parquet;"))
	}

	startTime := time.Now()
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Sprint(err.Error())
	}
	elapsedTime := time.Since(startTime)

	if (format == "JSONCompact") || (format == "JSON") {
		jsonData, err := rowsToJSON(rows, elapsedTime)
		if err != nil {
			return fmt.Sprint(err.Error())
		}
		return string(jsonData)
	} else if format == "CSV" {
		csvData, err := rowsToCSV(rows, false)
		if err != nil {
			return fmt.Sprint(err.Error())
		}
		return string(csvData)
	} else if format == "CSVWithNames" {
		csvData, err := rowsToCSV(rows, true)
		if err != nil {
			return fmt.Sprint(err.Error())
		}
		return string(csvData)
	} else if (format == "TSVWithNames") || (format == "TabSeparatedWithNames") {
		tsvData, err := rowsToTSV(rows, true)
		if err != nil {
			return fmt.Sprint(err.Error())
		}
		return string(tsvData)
	} else if (format == "TSV") || (format == "TabSeparated") {
		tsvData, err := rowsToTSV(rows, false)
		if err != nil {
			return fmt.Sprint(err.Error())
		}
		return string(tsvData)
	} else {
		tsvData, err := rowsToTSV(rows, false)
		if err != nil {
			return fmt.Sprint(err.Error())
		}
		return string(tsvData)
	}
}

/* init flags */
func initFlags() {
	appFlags.Host = flag.String("host", "0.0.0.0", "API host. Default 0.0.0.0")
	appFlags.Port = flag.String("port", "8123", "API port. Default 8123")
	appFlags.Format = flag.String("format", "JSONCompact", "API port. Default JSONCompact")
	appFlags.Stdin = flag.Bool("stdin", false, "STDIN query. Default false")
	flag.Parse()
}

/* JSONCompact formatter */
type MetaData struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Statistics struct {
	Elapsed   float64 `json:"elapsed"`
	RowsRead  int     `json:"rows_read"`
	BytesRead int     `json:"bytes_read"`
}

type OutputJSON struct {
	Meta                   []MetaData      `json:"meta"`
	Data                   [][]interface{} `json:"data"`
	Rows                   int             `json:"rows"`
	RowsBeforeLimitAtLeast int             `json:"rows_before_limit_at_least"`
	Statistics             Statistics      `json:"statistics"`
}

func rowsToJSON(rows *sql.Rows, elapsedTime time.Duration) ([]byte, error) {
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create a slice to store maps of column names and their corresponding values
	var results OutputJSON
	results.Meta = make([]MetaData, len(columns))
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
			return nil, err
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
		return nil, err
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
		return nil, err
	}

	return jsonData, nil
}

/* TSV formatter */
func rowsToTSV(rows *sql.Rows, cols bool) (string, error) {
	var result []string
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}

	if cols == true {
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

/* CSV Formatter */
func rowsToCSV(rows *sql.Rows, cols bool) (string, error) {
	var result []string
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}

	if cols == true {
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



/* main */

func main() {

   initFlags()
   if *appFlags.Stdin == true {

		scanner := bufio.NewScanner((os.Stdin))
		inputString := ""
		for scanner.Scan() {
			inputString = inputString + "\n" + scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "reading standard input:", err)
		}
		result := quack(inputString, true, *appFlags.Format)
		fmt.Println(result)

   } else {	

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		var bodyBytes []byte
		var query string
		var err error

		/* Query Handler */
		if r.URL.Query().Get("query") != "" {
			query = r.Form.Get("query")
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
		
		/* Format Handler */
		default_format := *appFlags.Format
		if r.URL.Query().Get("default_format") != "" {
			default_format = r.URL.Query().Get("default_format")
		}
		
		/* TODO: Extract FORMAT from query and override the current `default_format` */

		if len(query) == 0 {
			w.Write([]byte(staticPlay))
		} else {
			result := quack(query, false, default_format)
			w.Write([]byte(result))
		}
	})

	fmt.Printf("API Running: %s:%s\n", *appFlags.Host, *appFlags.Port)
	if err := http.ListenAndServe(*appFlags.Host+":"+*appFlags.Port, nil); err != nil {
		panic(err)
	}

   }
}
