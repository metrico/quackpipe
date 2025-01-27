package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"quackpipe/config"
	"quackpipe/merge"
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
	appFlags.Config = flag.String("config", "", "path to the configuration file")
	appFlags.Params = flag.String("params", "", "DuckDB optional parameters. Default to none.")
	appFlags.Stdin = flag.Bool("stdin", false, "STDIN query. Default false")
	appFlags.Alias = flag.Bool("alias", true, "Built-in CH Aliases. Default true")
	flag.Parse()

	return appFlags
}

func main() {
	config.AppFlags = initFlags()
	if *config.AppFlags.Stdin {
		rows, duration, format, err := utils.ReadFromScanner(*config.AppFlags)
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
		return
	}
	config.InitConfig(*config.AppFlags.Config)
	if config.Config.QuackPipe.Enabled {
		merge.Init()
	}
	r := router.NewRouter(config.AppFlags)
	fmt.Printf("QuackPipe API Running: %s:%s\n", *config.AppFlags.Host, *config.AppFlags.Port)
	if err := http.ListenAndServe(*config.AppFlags.Host+":"+*config.AppFlags.Port, r); err != nil {
		panic(err)
	}

}
