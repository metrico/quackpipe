package merge

import (
	"github.com/metrico/quackpipe/config"
	"github.com/metrico/quackpipe/merge/handlers"
	"github.com/metrico/quackpipe/merge/repository"
	"github.com/metrico/quackpipe/router"
	"github.com/metrico/quackpipe/service/db"
	"os"
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
		Path:    "/quackdb/insert",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
}
