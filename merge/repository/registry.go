package repository

import (
	"database/sql"
	"fmt"
	"github.com/metrico/quackpipe/config"
	"github.com/metrico/quackpipe/merge/index"
	"github.com/metrico/quackpipe/merge/service"
	"github.com/metrico/quackpipe/model"
	"github.com/metrico/quackpipe/service/db"
	"github.com/metrico/quackpipe/utils/promise"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var conn *sql.DB

var registry = make(map[string]service.MergeService)
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

func GetTable(name string) (service.MergeService, error) {
	table, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("table %q not found", name)
	}
	return table, nil
}

func RunMerge() {
	mergeTicker = time.NewTicker(time.Second * 10)
	for range mergeTicker.C {
		_registry := make(map[string]service.MergeService, len(registry))
		func() {
			registryMtx.Lock()
			defer registryMtx.Unlock()
			for k, v := range registry {
				_registry[k] = v
			}
		}()

		for _, table := range _registry {
			err := table.DoMerge()
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}
}

var tableNameCheck = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
var m sync.Mutex

func Store(name string, columns map[string]any) promise.Promise[int32] {
	m.Lock()
	table := registry[name]
	if table == nil {
		err := RegisterSimpleTable(name)
		if err != nil {
			m.Unlock()
			return promise.Fulfilled(err, int32(0))

		}
		table = registry[name]
	}
	m.Unlock()
	return table.Store(columns)
}

func RegisterSimpleTable(name string) error {
	table := &model.Table{
		Name:    name,
		Engine:  "HiveMerge",
		OrderBy: []string{"__timestamp"},
		Path:    path.Join(config.Config.QuackPipe.Root, name),
		PartitionBy: func(m map[string]*model.ColumnStore) ([]model.PartitionDesc, error) {
			tsCol, ok := m["__timestamp"]
			if !ok {
				return nil, fmt.Errorf("table %q does not have a '__timestamp' column", name)
			}
			tsData, ok := tsCol.Data.([]int64)
			if !ok {
				return nil, fmt.Errorf("column '__timestamp' has non-int64 data type")
			}

			parts := make(map[int64]*model.PartitionDesc)
			lastPartId := int64(0)
			var lastPart *model.PartitionDesc
			for i, ts := range tsData {
				id := int64(ts / 86400000000000)
				if lastPart == nil || lastPartId != id {
					lastPartId = id
					if _, ok := parts[id]; !ok {
						parts[id] = &model.PartitionDesc{
							Values: [][2]string{
								{"date", time.Unix(0, ts).UTC().Format("2006-01-02")},
								{"hour", time.Unix(0, ts).UTC().Format("15")},
							},
							IndexMap: make([]byte, (len(tsData)+7)/8),
						}
					}
					lastPart = parts[id]
				}
				lastPart.IndexMap[i/8] |= 1 << (uint(i) % 8)
			}
			res := make([]model.PartitionDesc, 0, len(parts))
			for _, desc := range parts {
				res = append(res, *desc)
			}
			return res, nil

		},

		/*func(i int64, m map[string]*model.ColumnStore) ([][2]string, error) {
			res := [2][2]string{{"date", ""}, {"hour", ""}}
			tsCol, ok := m["__timestamp"]
			if !ok {
				return nil, fmt.Errorf("table %q does not have a '__timestamp' column", name)
			}
			tsData, ok := tsCol.Data.([]int64)
			if !ok {
				return nil, fmt.Errorf("column '__timestamp' has non-int64 data type")
			}
			if int64(len(tsData)) <= i {
				return nil, fmt.Errorf("index out of range")
			}
			ts := time.Unix(0, tsData[i])
			res[0][1] = ts.Format("2006-01-02")
			res[1][1] = ts.Format("15:00")
			return res[:], nil
		},*/
		AutoTimestamp: true,
	}
	var err error
	table.Index, err = index.NewJSONIndex(table)
	if err != nil {
		return err
	}
	table.Index.Run()
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
	/*err = InsertTableMetadata(conn, table)
	if err != nil {
		return err
	}*/
	registryMtx.Lock()
	defer registryMtx.Unlock()
	switch table.Engine {
	case "Merge":
		registry[table.Name], err = service.NewMergeTreeService(table)
	case "HiveMerge":
		registry[table.Name] = service.NewMultithreadHiveMergeTreeService(0, table)
	}
	if err != nil {
		return err
	}
	registry[table.Name].Run()
	return nil
}

func PopulateRegistry() error {
	/*tables, err := GetAllTableMetadata(conn)
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
	}*/
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
