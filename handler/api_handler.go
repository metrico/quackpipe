package handlers

import (
	_ "embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"quackpipe/controller/root"
	"quackpipe/model"
)

//go:embed play.html
var staticPlay string
var (
	EmptyQuery = errors.New("length of query is empty")
)

type Handler struct {
	FlagInformation *model.CommandLineFlags
}

func (u *Handler) Handlers(w http.ResponseWriter, r *http.Request) {
	var bodyBytes []byte
	var query string
	var err error
	defaultFormat := *u.FlagInformation.Format
	defaultParams := *u.FlagInformation.Params
	defaultPath := *u.FlagInformation.DBPath
	// handle query parameter
	if r.URL.Query().Get("query") != "" {
		query = r.URL.Query().Get("query")
	} else if r.Body != nil {
		bodyBytes, err = io.ReadAll(r.Body)
		if err != nil {
			fmt.Printf("Body reading error: %v", err)
			return
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

	result, err := root.QueryOperation(u.FlagInformation, query, r, defaultPath, defaultFormat, defaultParams)
	if err != nil {
		if errors.Is(err, EmptyQuery) {
			_, _ = w.Write([]byte(staticPlay))
		} else {
			_, _ = w.Write([]byte(err.Error()))
		}
	} else {
		_, _ = w.Write([]byte(result))
	}

}
