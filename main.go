package main

import (
	"fmt"
	"github.com/gigapi/gigapi/config"
	"github.com/gigapi/gigapi/merge"
	"github.com/gigapi/gigapi/router"
	"net/http"
)

func initModules() {
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
