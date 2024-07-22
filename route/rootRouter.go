package route

import (
	"github.com/gorilla/mux"
	handlers "quackpipe/handler"
	"quackpipe/model"
)

// RootHandler function for the root endpoint
func RootHandler(router *mux.Router, FlagInformation *model.CommandLineFlags) handlers.Handler {
	HandlerInfo := handlers.Handler{FlagInformation: FlagInformation}
	router.HandleFunc("/", HandlerInfo.Handlers).Methods("POST", "GET")
	return HandlerInfo
}
