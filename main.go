package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"quackpipe/config"
	"quackpipe/model"
	"quackpipe/router"
	"quackpipe/utils"
	"sync"
)

var (
	db     *sql.DB
	dbOnce sync.Once
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

	// Load configuration
	config, err := config.LoadConfig("/Users/mac/Documents/go/src/github.com/metrico/quackpipe/config/config.yml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	//go func() {
	//	// Create a new cron instance
	//	fmt.Println("goroutine trigger")
	//	// Schedule jobs based on configuration
	//	for _, job := range config.CronJobs {
	//
	//		fmt.Println("job trigger")
	//		utils.ScheduleJob(job)
	//	}
	//
	//}()
	//
	db := getDBInstance()
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatalf("Failed to close database: %v", err)
		}
	}()

	// Create a context with the database connection
	ctx := context.WithValue(context.Background(), "db", db)
	utils.ExecuteOnStartQueries(ctx, config.OnStart.Queries)

	go func() {
		for _, job := range config.CronJobs {

			fmt.Println("job trigger")
			utils.CronTrigger(ctx, job)
		}
	}()

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

func getDBInstance() *sql.DB {
	dbOnce.Do(func() {
		var err error
		db, err = sql.Open("duckdb", "")
		if err != nil {
			log.Fatalf("Failed to open database: %v", err)
		}
	})
	return db
}
