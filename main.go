package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"quackpipe/model"
	"quackpipe/router"
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
	appFlags.Alias = flag.Bool("alias", true, "Built-in CH Aliases. Default true")
	flag.Parse()

	return appFlags
}

var appFlags *model.CommandLineFlags

func main() {
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
