<img src="https://user-images.githubusercontent.com/1423657/230504468-39bdecf5-b1c1-462c-bb11-91d147cde8d3.png" width=180 />

> quack, motherducker

# quackhouse

_This is an unholy experiment. Can we silently replace ClickHouse with DuckDB for selected usecases?_

### Status

- [x] DuckDB [cgo binding](https://github.com/marcboeker/go-duckdb)
- [x] GO API
  - [x] FORMAT Emulation _(CSV,TSV,JSON)_
  - [x] Web Playground _(borrowed from ClickHouse)_
- [x] STDIN Execution

### Usage

##### Parameters

| params | usage | default |
|-- |-- |-- |
| `--port` | HTTP API Port | `8123` |
| `--host` | HTTP API Host | `0.0.0.0` |
| `--stdin` | STDIN query mode | `false` |

#### Playground
Execute queries using the embedded playground

![image](https://user-images.githubusercontent.com/1423657/230783859-1c69910b-6bf2-42df-8b1d-876b94fc3419.png)

#### API
Execute queries using the POST API
```
curl -X POST https://quackhouse.fly.dev 
   -H "Content-Type: application/json"
   -d 'SELECT version()'  
```

#### STDIN
Execute queries using STDIN
```
# echo "SELECT version()" | go run quackhouse.go --stdin
{"meta":[{"name":"version()","type":""}],"data":[["v0.7.1"]],"rows":1,"rows_before_limit_at_least":1,"statistics":{"elapsed":0.000266523,"rows_read":1,"bytes_read":0}}
```
