package handlers

import (
	_ "embed"
	"fmt"
	"github.com/metrico/quackpipe/config"
	"github.com/metrico/quackpipe/controller/root"
	"github.com/metrico/quackpipe/utils"
	"io"
	"net/http"
)

//go:embed play.html
var staticPlay string

type Handler struct {
}

func (u *Handler) Handlers(w http.ResponseWriter, r *http.Request) error {
	var bodyBytes []byte
	var query string
	var err error
	defaultFormat := *config.AppFlags.Format
	defaultParams := *config.AppFlags.Params
	defaultPath := *config.AppFlags.DBPath
	// handle query parameter
	if r.URL.Query().Get("query") != "" {
		query = r.URL.Query().Get("query")
	} else if r.Body != nil {
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("Body reading error: %v", err)
			return nil
		}
		defer r.Body.Close()
		query = string(bodyBytes)
	}

	switch r.Header.Get("Accept") {
	case "application/json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	case "application/xml":
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	case "text/css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	default:
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	}
	// format handling
	if r.URL.Query().Get("default_format") != "" {
		defaultFormat = r.URL.Query().Get("default_format")
	}
	// param handling
	if r.URL.Query().Get("default_params") != "" {
		defaultParams = r.URL.Query().Get("default_params")
	}

	// extract FORMAT from query and override the current `default_format`
	cleanQuery, format := utils.ExtractAndRemoveFormat(query)
	if len(format) > 0 {
		query = cleanQuery
		defaultFormat = format
	}
	if len(query) == 0 {
		_, _ = w.Write([]byte(staticPlay))

	} else {
		result, err := root.QueryOperation(config.AppFlags, query, r, defaultPath, defaultFormat, defaultParams)
		if err != nil {
			_, _ = w.Write([]byte(err.Error()))
		} else {
			_, _ = w.Write([]byte(result))
		}
	}
	return nil
}
