package router

import (
	"github.com/gorilla/mux"
	"quackpipe/model"
)

func NewRouter(flagInformation *model.CommandLineFlags) *mux.Router {
	router := mux.NewRouter()
	// Register  module routes

	APIHandler(router, flagInformation)
	return router
}
