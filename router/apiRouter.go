package router

import (
	handlers "quackpipe/handler"
)

var _ = func() int {
	HandlerInfo := handlers.Handler{}
	r := Route{
		Path:    "/",
		Methods: []string{"POST", "GET"},
		Handler: HandlerInfo.Handlers,
	}
	RegisterRoute(&r)
	return 0
}()
