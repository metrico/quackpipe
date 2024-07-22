package route

import (
	"github.com/gorilla/mux"
	"quackpipe/model"
)

func NewRouter(flagInformation *model.CommandLineFlags) *mux.Router {
	router := mux.NewRouter()
	// Register  module routes
	RootHandler(router, flagInformation)
	return router
}
