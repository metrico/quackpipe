package utils

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"github.com/robfig/cron/v3"
	"log"
	"os"
	"quackpipe/model"
	"quackpipe/service/db"
	"time"
)

func ReadFromScanner(appFlags model.CommandLineFlags) (*sql.Rows, time.Duration, string, error) {
	defaultFormat := *appFlags.Format
	defaultParams := *appFlags.Params
	scanner := bufio.NewScanner((os.Stdin))
	query := ""
	for scanner.Scan() {
		query = query + "\n" + scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, "", err
	}

	cleanQuery, format := ExtractAndRemoveFormat(query)
	if len(format) > 0 {
		query = cleanQuery
		defaultFormat = format
	}
	result, duration, err := db.Quack(appFlags, query, nil, true, defaultParams, "")
	if err != nil {
		return nil, 0, "", err
	}

	return result, duration, defaultFormat, nil

}

func ExecuteOnStartQueries(ctx context.Context, queries []string) {
	db := ctx.Value("db").(*sql.DB)
	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			log.Printf("Failed to execute onStart query %q: %v", query, err)
		} else {
			log.Printf("Successfully executed onStart query %q", query)
		}
	}
}

func CronTrigger(ctx context.Context, job model.JobConfig) {
	c := cron.New()
	_, err := c.AddFunc(job.Cron, func() {
		executeQueries(ctx, job.Queries)
	})
	if err != nil {
		log.Fatalf("Failed to schedule job: %v", err)
	}
	c.Run()
	defer c.Stop()
}

func executeQueries(ctx context.Context, queries []string) {
	fmt.Println("executeQueries trigger")
	db := ctx.Value("db").(*sql.DB)
	fmt.Println(db)
	for _, query := range queries {
		fmt.Println("query", query)
		_, err := db.Exec(query)
		if err != nil {
			fmt.Println(err.Error())
			log.Printf("Failed to execute query %q: %v", query, err)
		} else {
			log.Printf("Successfully executed query %q", query)
		}
	}
}
