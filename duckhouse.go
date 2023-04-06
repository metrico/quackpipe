// Run with:
// CGO_LDFLAGS="-L<path to libduckdb_static.a>" CGO_CFLAGS="-I<path to duckdb.h>" DYLD_LIBRARY_PATH="<path to libduckdb.dylib>" go run examples/test.go

package main

import (
	_ "embed"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"database/sql"
	"log"

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

	var (
		result    []string
		container []string
		pointers  []interface{}
	)
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

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Sprint(err.Error())
		// panic(err.Error())
	}

	cols, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}

	length := len(cols)

	for rows.Next() {
		pointers = make([]interface{}, length)
		container = make([]string, length)

		for i := range pointers {
			pointers[i] = &container[i]
		}

		err = rows.Scan(pointers...)
		if err != nil {
			panic(err.Error())
		}

		result = append(result, fmt.Sprint(container))
	}

	output := strings.Join(result,"\n")
	return output
}


/* init flags */
func initFlags() {
	appFlags.Host = flag.String("host", "0.0.0.0", "API host. Default 0.0.0.0")
	appFlags.Port = flag.String("port", "8123", "API port. Default 8123")
	flag.Parse()
}

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
