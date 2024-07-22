package route

import (
	"github.com/gorilla/mux"
	"quackpipe/model"
	"quackpipe/route/root"
)

func NewRouter(flagInformation *model.CommandLineFlags) *mux.Router {
	router := mux.NewRouter()
	// Register root module routes
	root.RootHandler(router, flagInformation)
	return router
}
