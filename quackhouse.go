package main

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"time"
	"io/ioutil"
	"log"
	"net/http"

	_ "github.com/marcboeker/go-duckdb"
)

//go:embed play.html
var staticPlay string

// params for Flags
type CommandLineFlags struct {
	Host *string `json:"host"`
	Port *string `json:"port"`
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

func quack(query string) string {

	var err error

	db, err = sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	check(db.Exec("INSTALL httpfs;"))
	check(db.Exec("LOAD httpfs;"))
	check(db.Exec("INSTALL json;"))
	check(db.Exec("LOAD json;"))
	check(db.Exec("INSTALL parquet;"))
	check(db.Exec("LOAD parquet;"))

	startTime := time.Now()
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Sprint(err.Error())
	}
	elapsedTime := time.Since(startTime)

	jsonData, err := rowsToJSON(rows, elapsedTime)
	if err != nil {
		return fmt.Sprint(err.Error())
	}

	return string(jsonData)

}

/* init flags */
func initFlags() {
	appFlags.Host = flag.String("host", "0.0.0.0", "API host. Default 0.0.0.0")
	appFlags.Port = flag.String("port", "8123", "API port. Default 8123")
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

/* main */

func main() {

	initFlags()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		var bodyBytes []byte
		var query string
		var err error

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

		if len(query) == 0 {
			w.Write([]byte(staticPlay))
		} else {
			result := quack(query)
			w.Write([]byte(result))
		}
	})

	fmt.Printf("API Running: %s:%s\n", *appFlags.Host, *appFlags.Port)
	if err := http.ListenAndServe(*appFlags.Host+":"+*appFlags.Port, nil); err != nil {
		panic(err)
	}
}
