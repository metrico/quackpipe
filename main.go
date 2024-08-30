package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"quackpipe/model"
	"quackpipe/repository"
	"quackpipe/router"
	"quackpipe/service/db"
	"quackpipe/utils"
)

// initFlags initializes the command line flags
func initFlags() *model.CommandLineFlags {

	appFlags := &model.CommandLineFlags{}
	appFlags.Host = flag.String("host", "0.0.0.0", "API host. Default 0.0.0.0")
	appFlags.Port = flag.String("port", "8123", "API port. Default 8123")
	appFlags.Format = flag.String("format", "JSONCompact", "API port. Default JSONCompact")
	appFlags.Params = flag.String("params", "", "DuckDB optional parameters. Default to none.")
	appFlags.DBPath = flag.String("dbpath", "/tmp/", "DuckDB DB storage path. Default to /tmp/")
	appFlags.Stdin = flag.Bool("stdin", false, "STDIN query. Default false")
	appFlags.Alias = flag.Bool("alias", false, "Built-in CH Aliases. Default true")
	flag.Parse()

	return appFlags
}

var appFlags *model.CommandLineFlags

func main() {

	dbConn, err := db.ConnectDuckDB("test.db")
	if err != nil {
		log.Fatalf("failed to connect to DuckDB: %v", err)
	}
	defer dbConn.Close()
	err = repository.CreateDuckDBTablesTable(dbConn)
	if err != nil {
		log.Fatalf("failed to create metadata table: %v", err)
	}

	appFlags = initFlags()
	if *appFlags.Stdin {
		rows, duration, format, err := utils.ReadFromScanner(*appFlags)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		results, err := utils.ConversationOfRows(rows, format, duration)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		} else {
			fmt.Println(results)
		}

	} else {
		r := router.NewRouter(appFlags)
		fmt.Printf("QuackPipe API Running: %s:%s\n", *appFlags.Host, *appFlags.Port)
		if err := http.ListenAndServe(*appFlags.Host+":"+*appFlags.Port, r); err != nil {
			panic(err)
		}

	}

}
