package router

import (
	handlers "github.com/metrico/quackpipe/handler"
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
