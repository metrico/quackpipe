package merge

import (
	"quackpipe/config"
	"quackpipe/merge/handlers"
	"quackpipe/merge/repository"
	"quackpipe/router"
	"quackpipe/service/db"
)

func Init() {
	conn, err := db.ConnectDuckDB(config.Config.DBPath + "/ddb.db")
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
