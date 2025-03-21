package main

import (
	"flag"
	"fmt"
	"github.com/metrico/quackpipe/config"
	"github.com/metrico/quackpipe/merge"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/router"
	"github.com/metrico/quackpipe/utils"
	"net/http"
	"os"
)

// initFlags initializes the command line flags
func initFlags() *model.CommandLineFlags {

	appFlags := &model.CommandLineFlags{}
	appFlags.Host = flag.String("host", "0.0.0.0", "API host. Default 0.0.0.0")
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
	port := "8080"
	if os.Getenv("PORT") != "" {
		port = os.Getenv("PORT")
	}
	config.AppFlags.Port = &port
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
