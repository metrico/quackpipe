package router

import (
	"github.com/gigapi/gigapi/v2/modules"
	"github.com/gorilla/mux"
	"net/http"
)

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

var handlerRegistry []*modules.Route = nil

func RegisterRoute(r *modules.Route) {
	handlerRegistry = append(handlerRegistry, r)
}

func NewRouter() *mux.Router {
	router := mux.NewRouter()
	for _, r := range handlerRegistry {
		router.HandleFunc(r.Path, WithErrorHandle(r.Handler)).Methods(r.Methods...)
	}
	return router
}

func GetPathParams(r *http.Request) map[string]string {
	return mux.Vars(r)
}
