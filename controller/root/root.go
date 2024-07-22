package root

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"quackpipe/model"
	"quackpipe/service/db"
	"quackpipe/utils"
)

func QueryOperation(flagInformation *model.CommandLineFlags, query string, r *http.Request, default_path string, default_format string, default_params string) (string, error) {
	// auth to hash based temp file storage
	username, password, ok := r.BasicAuth()
	hashdb := ""
	if ok && len(password) > 0 {
		hash := sha256.Sum256([]byte(username + password))
		hashdb = fmt.Sprintf("%s/%x.db", default_path, hash)
	}
	// extract FORMAT from query and override the current `default_format`
	cleanquery, format := utils.ExtractAndRemoveFormat(query)
	if len(format) > 0 {
		query = cleanquery
		default_format = format
	}

	if len(format) > 0 {
		query = cleanquery
		default_format = format
	}

	if len(query) == 0 {
		return "", errors.New("query length is empty")
	} else {
		rows, duration, err := db.Quack(*flagInformation, query, false, default_params, hashdb)
		if err != nil {
			return "", err
		} else {

			result, err := utils.ConversationOfRows(rows, default_format, duration)
			if err != nil {
				return "", err
			}
			return result, nil
		}
	}

	return "", nil
}
