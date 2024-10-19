package router

import (
	"github.com/gorilla/mux"
	"net/http"
	"quackpipe/model"
)

type Route struct {
	Path    string
	Methods []string
	Handler func(w http.ResponseWriter, r *http.Request) error
}

func WithErrorHandle(hndl func(w http.ResponseWriter, r *http.Request) error,
) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		err := hndl(w, r)
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
		}
	}
}

var handlerRegistry []*Route = nil

func RegisterRoute(r *Route) {
	handlerRegistry = append(handlerRegistry, r)
}

func NewRouter(flagInformation *model.CommandLineFlags) *mux.Router {
	router := mux.NewRouter()
	for _, r := range handlerRegistry {
		router.HandleFunc(r.Path, WithErrorHandle(r.Handler)).Methods(r.Methods...)
	}
	return router
}
