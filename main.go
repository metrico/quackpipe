package main

import (
	"fmt"
	"github.com/gigapi/gigapi/config"
	"github.com/gigapi/gigapi/merge"
	"github.com/gigapi/gigapi/router"
	"github.com/gigapi/gigapi/stdin"
	"net/http"
)

func initModules() {
	stdin.Init()
	merge.Init()
}

func main() {
	config.InitConfig("")
	initModules()
	r := router.NewRouter()
	fmt.Printf("GigAPI Running: %s:%d\n", config.Config.Host, config.Config.Port)
	if err := http.ListenAndServe(fmt.Sprintf("%s:%d", config.Config.Host, config.Config.Port), r); err != nil {
		panic(err)
	}
}
