package merge

import (
	"os"
	"quackpipe/config"
	"quackpipe/merge/handlers"
	"quackpipe/merge/repository"
	"quackpipe/router"
	"quackpipe/service/db"
)

func Init() {
	err := os.MkdirAll(config.Config.QuackPipe.Root, 0750)
	if err != nil {
		panic(err)
	}
	conn, err := db.ConnectDuckDB(config.Config.QuackPipe.Root + "/ddb.db")
	if err != nil {
		panic(err)
	}

	_, err = conn.Exec("INSTALL json; LOAD json;")
	if err != nil {
		panic(err)
	}

	err = repository.CreateDuckDBTablesTable(conn)
	if err != nil {
		panic(err)
	}

	err = repository.InitRegistry(conn)
	if err != nil {
		panic(err)
	}

	InitHandlers()
}

func InitHandlers() {
	router.RegisterRoute(&router.Route{
		Path:    "/quackdb/create",
		Methods: []string{"POST"},
		Handler: handlers.CreateTableHandler,
	})
	router.RegisterRoute(&router.Route{
		Path:    "/quackdb/{table}/insert",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
}
