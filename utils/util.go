package utils

import (
	"bufio"
	"database/sql"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/service/db"
	"os"
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
	result, duration, err := db.Quack(appFlags, query, true, defaultParams, "")
	if err != nil {
		return nil, 0, "", err
	}

	return result, duration, defaultFormat, nil

}
