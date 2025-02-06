package repository

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"quackpipe/config"
	"quackpipe/merge/service"
	"quackpipe/model"
	"quackpipe/service/db"
	"regexp"
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

func RegisterNewTable(table *model.Table) error {
	if !tableNameCheck.MatchString(table.Name) {
		return fmt.Errorf("invalid table name, only letters and _ are accepted: %q", table.Name)
	}
	if table.FSPath == "" {
		table.FSPath = filepath.Join(config.Config.QuackPipe.Root, table.Name)
	}
	for i := range table.Paths {
		if table.Paths[i] == "" {
			table.Paths[i] = filepath.Join(table.FSPath)
		}
	}
	if _, ok := registry[table.Name]; ok {
		return nil
	}
	fieldNames := make([]string, len(table.Fields))
	fieldTypes := make([]string, len(table.Fields))
	for i, field := range table.Fields {
		fieldNames[i] = field[0]
		fieldTypes[i] = field[1]
	}
	err := createTableFolders(table)
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
	res, err := conn.Query(`
SELECT name,paths, fs_path, field_names, field_types, order_by, engine, timestamp_field, timestamp_precision, partition_by
FROM tables
`)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.Next() {
		var table model.Table
		var (
			fieldNames []any
			fieldTypes []any
			orderBy    []any
			paths      []any
		)
		err = res.Scan(
			&table.Name, &paths, &table.FSPath,
			&fieldNames, &fieldTypes, &orderBy,
			&table.Engine, &table.TimestampField, &table.TimestampPrecision, &table.PartitionBy,
		)
		for i := range table.Paths {
			table.Paths[i] = paths[i].(string)
		}
		if err != nil {
			return err
		}
		for i, n := range fieldNames {
			table.Fields = append(table.Fields, [2]string{n.(string), fieldTypes[i].(string)})
		}
		for _, n := range orderBy {
			table.OrderBy = append(table.OrderBy, n.(string))
		}
		registryMtx.Lock()
		registry[table.Name], err = service.NewMergeTreeService(&table)
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
	err := os.MkdirAll(filepath.Join(table.FSPath, "tmp"), 0755)
	if err != nil {
		return err
	}
	return os.MkdirAll(filepath.Join(table.FSPath, "data"), 0755)
}
