package router

import (
	"github.com/gorilla/mux"
	handlers "quackpipe/handler"
	"quackpipe/model"
)

// APIHandler function for the root endpoint
func APIHandler(router *mux.Router, FlagInformation *model.CommandLineFlags) handlers.Handler {
	HandlerInfo := handlers.Handler{FlagInformation: FlagInformation}
	router.HandleFunc("/", HandlerInfo.Handlers).Methods("POST", "GET")
	//router.HandleFunc("/ch-insert", HandlerInfo.CHHandlers).Methods("POST", "GET")
	router.HandleFunc("/ch-insert", HandlerInfo.InsertHandler).Methods("POST", "GET")
	return HandlerInfo
}
