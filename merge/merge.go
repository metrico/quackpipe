package merge

import (
	"github.com/gigapi/gigapi/config"
	"github.com/gigapi/gigapi/merge/handlers"
	"github.com/gigapi/gigapi/merge/repository"
	"github.com/gigapi/gigapi/merge/utils"
	"github.com/gigapi/gigapi/router"
	"net/http"
	"os"
)

func Init() {
	err := os.MkdirAll(config.Config.Gigapi.Root, 0750)
	if err != nil {
		panic(err)
	}
	conn, cancel, err := utils.ConnectDuckDB(config.Config.Gigapi.Root + "/ddb.db")
	if err != nil {
		panic(err)
	}
	defer cancel()

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
		Path:    "/gigapi/create",
		Methods: []string{"POST"},
		Handler: handlers.CreateTableHandler,
	})
	router.RegisterRoute(&router.Route{
		Path:    "/gigapi/insert",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})

	router.RegisterRoute(&router.Route{
		Path:    "/gigapi/write/{db}",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
	router.RegisterRoute(&router.Route{
		Path:    "/gigapi/write",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})

	// InfluxDB 2+3 compatibility endpoints
	router.RegisterRoute(&router.Route{
		Path:    "/write",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
	router.RegisterRoute(&router.Route{
		Path:    "/api/v2/write",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
	router.RegisterRoute(&router.Route{
		Path:    "/api/v3/write_lp",
		Methods: []string{"POST"},
		Handler: handlers.InsertIntoHandler,
	})
	router.RegisterRoute(&router.Route{
		Path:    "/health",
		Methods: []string{"GET"},
		Handler: func(w http.ResponseWriter, r *http.Request) error {
			response := `{"checks": [], "commit": "null-commit", "message": "Service is healthy", "name": "GigAPI", "status": "pass", "version": "0.0.0"}`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(response + "\n"))
			return nil
		},
	})
	router.RegisterRoute(&router.Route{
		Path:    "/ping",
		Methods: []string{"GET"},
		Handler: func(w http.ResponseWriter, r *http.Request) error {
			w.WriteHeader(http.StatusNoContent)
			return nil
		},
	})

}
