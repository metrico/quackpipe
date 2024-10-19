package handlers

import (
	"github.com/gorilla/mux"
	"net/http"
	"quackpipe/merge/parsers"
	"quackpipe/merge/repository"
	"quackpipe/utils/promise"
)

func InsertIntoHandler(w http.ResponseWriter, r *http.Request) error {
	contentType := r.Header.Get("Content-Type")
	parameters := mux.Vars(r)
	tableName := parameters["table"]
	table, err := repository.GetTable(tableName)
	if err != nil {
		return err
	}

	var fieldNames []string
	var fieldTypes []string
	for _, field := range table.Table.Fields {
		fieldNames = append(fieldNames, field[0])
		fieldTypes = append(fieldTypes, field[1])
	}

	parser, err := parsers.GetParser(contentType, fieldNames, fieldTypes)
	if err != nil {
		return err
	}
	res, err := parser.ParseReader(r.Body)
	if err != nil {
		return err
	}
	var promises []*promise.Promise[int32]
	for _res := range res {
		if _res.Error != nil {
			go func() {
				for range res {
				}
			}()
			return _res.Error
		}
		promises = append(promises, table.Store(_res.Data))
	}
	for _, p := range promises {
		_, err = p.Get()
		if err != nil {
			return err
		}
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ok"))
	return nil
}
