<img src="https://user-images.githubusercontent.com/1423657/230504468-39bdecf5-b1c1-462c-bb11-91d147cde8d3.png" width=180 />

> quack, motherducker

# quackhouse

_DuckDB core with HTTP API and a few extra compatibility bits. If you know, you know._

### Status

- [x] DuckDB Core [^1]
  - [c] [cgo binding](https://github.com/marcboeker/go-duckdb) 
- [x] GO REST API [^3]
  - [x] FORMAT Emulation _(CSV,TSV,JSON)_
  - [x] Web Playground _(from ClickkHouse, Apache2 Licensed)_ [^2]
- [x] STDIN Execution

### Usage

##### Parameters

| params | usage | default |
|-- |-- |-- |
| `--port` | HTTP API Port | `8123` |
| `--host` | HTTP API Host | `0.0.0.0` |
| `--stdin` | STDIN query mode | `false` |
| `--format` | FORMAT handler | `JSONCompact` |


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
# echo "SELECT 'hello', version() as version FORMAT CSV" | go run quackhouse.go --stdin
hello,v0.7.1
```

### Extensions
Several extensions are pre-installed by default in Docker images, including _parquet, json, httpfs_
When using HTTP API,   _parquet, json, httpfs_

-------

[^1]: DuckDB ® is a trademark of MotherDuck. No direct affiliation or endorsement.
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement.
[^3]: Released under the MIT license. All rights reserved by their respective owners.

