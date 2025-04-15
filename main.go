package main

import (
	"flag"
	"fmt"
	"github.com/metrico/quackpipe/config"
	"github.com/metrico/quackpipe/merge"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/router"
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
	port := "7971"
	if os.Getenv("PORT") != "" {
		port = os.Getenv("PORT")
	}
	config.InitConfig("")
	merge.Init()
	r := router.NewRouter(config.AppFlags)
	fmt.Printf("GigAPI Running: %s:%s\n", *config.AppFlags.Host, *config.AppFlags.Port)
	if err := http.ListenAndServe(*config.AppFlags.Host+":"+*config.AppFlags.Port, r); err != nil {
		panic(err)
	}
}
