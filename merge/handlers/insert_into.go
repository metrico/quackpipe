package handlers

import (
	"compress/gzip"
	"context"
	"github.com/gigapi/gigapi/merge/parsers"
	"github.com/gigapi/gigapi/merge/repository"
	"github.com/gigapi/gigapi/utils"
	"github.com/gorilla/mux"
	"io"
	"net/http"
)

func getDatabase(r *http.Request) string {
	if db := r.URL.Query().Get("db"); db != "" {
		return db
	}
	vars := mux.Vars(r)
	if db, ok := vars["db"]; ok {
		return db
	}
	return ""
}

func InsertIntoHandler(w http.ResponseWriter, r *http.Request) error {
	contentType := r.Header.Get("Content-Type")
	parser, err := parsers.GetParser(contentType, nil, nil)

	database := getDatabase(r)

	ctx := r.Context()
	precision := r.URL.Query().Get("precision")
	if precision != "" {
		ctx = context.WithValue(ctx, "precision", precision)
	}

	if err != nil {
		return err
	}

	// Handle gzip compression
	var reader io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		gzipReader, err := gzip.NewReader(r.Body)
		if err != nil {
			return err
		}
		defer gzipReader.Close()
		reader = gzipReader
	}

	res, err := parser.ParseReader(ctx, reader)
	if err != nil {
		return err
	}
	var promises []utils.Promise[int32]
	for _res := range res {
		if _res.Error != nil {
			go func() {
				for range res {
				}
			}()
			return _res.Error
		}
		_database := database
		if _database == "" {
			database = _res.Database
		}
		promises = append(promises, repository.Store(_database, _res.Table, _res.Data))
	}
	for _, p := range promises {
		_, err = p.Get()
		if err != nil {
			return err
		}
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
