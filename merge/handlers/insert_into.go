package handlers

import (
	"context"
	"github.com/metrico/quackpipe/merge/parsers"
	"github.com/metrico/quackpipe/merge/repository"
	"github.com/metrico/quackpipe/utils"
	"net/http"
)

func InsertIntoHandler(w http.ResponseWriter, r *http.Request) error {
	contentType := r.Header.Get("Content-Type")
	parser, err := parsers.GetParser(contentType, nil, nil)

	ctx := r.Context()
	precision := r.URL.Query().Get("precision")
	if precision != "" {
		ctx = context.WithValue(ctx, "precision", precision)
	}

	if err != nil {
		return err
	}
	res, err := parser.ParseReader(ctx, r.Body)
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
		promises = append(promises, repository.Store("", _res.Table, _res.Data))
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
