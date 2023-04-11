<img src="https://user-images.githubusercontent.com/1423657/230504468-39bdecf5-b1c1-462c-bb11-91d147cde8d3.png" width=180 />

> _quack, motherducker!_

# :baby_chick: quackhouse

_QuackHouse is an API built on top of DuckDB with a few extra compatibility bits. If you know, you know._

:hatched_chick:	[public demo](https://quackhouse.fly.dev) _(1x-shared-vcpu, 256Mb)_

<br>

#### Features

- [x] DuckDB Core [^1]
  - [x] [cgo](https://github.com/marcboeker/go-duckdb) binding
  - [x] Extension preloading
- [x] REST API [^3]
  - [x] FORMAT Emulation _(CSV,TSV,JSON)_
  - [x] Web Playground _(from ClickkHouse, Apache2 Licensed)_ [^2]
- [x] STDIN Fast Query Execution
- [x] NO Files. Cloud Storage only _(s3/r2/minio, httpfs, etc)_

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
When using HTTP API, _httpfs, parquet, json_ extensions are automatically pre-loaded.

<br>

-------

###### :black_joker: Disclaimers 

[^1]: DuckDB ® is a trademark of MotherDuck. No direct affiliation or endorsement.
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement.
[^3]: Released under the MIT license. All rights reserved by their respective owners.

