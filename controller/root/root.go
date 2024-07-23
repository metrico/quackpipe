package root

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"quackpipe/model"
	"quackpipe/service/db"
	"quackpipe/utils"
)

func QueryOperation(flagInformation *model.CommandLineFlags, query string, r *http.Request, defaultPath string, defaultFormat string, defaultParams string) (string, error) {
	// auth to hash based temp file storage
	username, password, ok := r.BasicAuth()
	hashdb := ""
	if ok && len(password) > 0 {
		hash := sha256.Sum256([]byte(username + password))
		hashdb = fmt.Sprintf("%s/%x.db", defaultPath, hash)
	}
	rows, duration, err := db.Quack(*flagInformation, query, false, defaultParams, hashdb)
	if err != nil {
		return "", err
	} else {
		result, err := utils.ConversationOfRows(rows, defaultFormat, duration)
		if err != nil {
			return "", err
		}
		return result, nil
	}

}
