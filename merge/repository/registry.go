package repository

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"quackpipe/config"
	"quackpipe/merge/service"
	"quackpipe/model"
	"quackpipe/service/db"
	"quackpipe/utils/promise"
	"regexp"
	"strings"
	"sync"
	"time"
)

var conn *sql.DB

var registry = make(map[string]*service.MergeTreeService)
var mergeTicker *time.Ticker
var registryMtx sync.Mutex

func InitRegistry(_conn *sql.DB) error {
	var err error
	if _conn == nil {
		_conn, err = db.ConnectDuckDB(config.Config.QuackPipe.Root + "/ddb.db")
		if err != nil {
			return err
		}
	}
	conn = _conn
	err = CreateDuckDBTablesTable(conn)
	if err != nil {
		return err
	}
	err = PopulateRegistry()
	if err != nil {
		return err
	}
	if !config.Config.QuackPipe.NoMerges {
		go RunMerge()
	}
	return nil
}

func GetTable(name string) (*service.MergeTreeService, error) {
	table, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("table %q not found", name)
	}
	return table, nil
}

func RunMerge() {
	mergeTicker = time.NewTicker(time.Second * 10)
	for range mergeTicker.C {
		_registry := make(map[string]*service.MergeTreeService, len(registry))
		func() {
			registryMtx.Lock()
			defer registryMtx.Unlock()
			for k, v := range registry {
				_registry[k] = v
			}
		}()
		for _, table := range _registry {
			plan, err := table.PlanMerge()
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = table.Merge(plan)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

var tableNameCheck = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func Store(name string, columns map[string]any) *promise.Promise[int32] {
	table := registry[name]
	if table == nil {
		err := RegisterSimpleTable(name)
		if err != nil {
			return promise.Fulfilled(err, int32(0))
		}
		table = registry[name]
	}
	return table.Store(columns)
}

func RegisterSimpleTable(name string) error {
	table := &model.Table{
		Name:               name,
		Engine:             "Merge",
		OrderBy:            []string{"__timestamp"},
		TimestampField:     "__timestamp",
		TimestampPrecision: "ns",
		PartitionBy:        "",
		AutoTimestamp:      true,
	}
	return RegisterNewTable(table)
}

func RegisterNewTable(table *model.Table) error {
	if !tableNameCheck.MatchString(table.Name) {
		return fmt.Errorf("invalid table name, only letters and _ are accepted: %q", table.Name)
	}
	if table.Path == "" {
		table.Path = filepath.Join(config.Config.QuackPipe.Root, table.Name)
	}
	if _, ok := registry[table.Name]; ok {
		return nil
	}
	_table := *table
	if strings.HasPrefix(table.Path, "s3://") {
		_table.Path = path.Join(config.Config.QuackPipe.Root, table.Name)
	}
	err := createTableFolders(&_table)
	if err != nil {
		return err
	}
	err = InsertTableMetadata(conn, table)
	if err != nil {
		return err
	}
	registryMtx.Lock()
	defer registryMtx.Unlock()
	registry[table.Name], err = service.NewMergeTreeService(table)
	if err != nil {
		return err
	}
	registry[table.Name].Run()
	return nil
}

func PopulateRegistry() error {
	tables, err := GetAllTableMetadata(conn)
	if err != nil {
		return err
	}
	for _, table := range tables {
		registryMtx.Lock()
		registry[table.Name], err = service.NewMergeTreeService(table)
		if err != nil {
			registryMtx.Unlock()
			return err
		}
		registry[table.Name].Run()
		registryMtx.Unlock()
	}
	return nil

}

func createTableFolders(table *model.Table) error {
	if !tableNameCheck.MatchString(table.Name) {
		return fmt.Errorf("invalid table name, only letters and _ are accepted: %q", table.Name)
	}
	err := os.MkdirAll(filepath.Join(table.Path, "tmp"), 0755)
	if err != nil {
		return err
	}
	return os.MkdirAll(filepath.Join(table.Path, "data"), 0755)
}
