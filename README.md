<a href="https://quackpipe.fly.dev" target="_blank"><img src="https://user-images.githubusercontent.com/1423657/231310060-aae46ee6-c748-44c9-905e-20a4eba0a814.png" width=220 /></a>

> _quack, motherducker!_

# :baby_chick: quackpipe

_QuackPipe is an OLAP API built on top of DuckDB with a few extra compatibility bits. If you know, you know._

Play with DuckDB SQL though a familiar API, without giving up old habits and integrations.

:hatched_chick:	[public demo](https://quackpipe.fly.dev) _(1x-shared-vcpu, 256Mb, minimal resources)_


### Feature Status
- [x] DuckDB Core [^1]
  - [x] [cgo](https://github.com/marcboeker/go-duckdb) binding
  - [x] Extension preloading
  - [ ] Aliases Extension
- [x] REST API [^3] [^4]
  - [x] FORMAT Emulation
    - [x] CSV, CSVWithNames
    - [x] TSV, TSVWithNames
    - [x] JSONCompact
    - [ ] Native
  - [x] Web Playground _(from ClickkHouse, Apache2 Licensed)_ [^2]
- [x] STDIN Fast Query Execution
- [x] `:memory:` table + Cloud Storage _(s3/r2/minio, httpfs, etc)_

<br>

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
curl -X POST https://quackpipe.fly.dev 
   -H "Content-Type: application/json"
   -d 'SELECT version()'  
```

#### STDIN
Execute queries using STDIN
```
# echo "SELECT 'hello', version() as version FORMAT CSV" | ./quackpipe --stdin
hello,v0.7.1
```

### Extensions
Several extensions are pre-installed by default in Docker images, including _parquet, json, httpfs_<br>
When using HTTP API, _httpfs, parquet, json_ extensions are automatically pre-loaded.

<br>

-------

###### :black_joker: Disclaimers 

[^1]: DuckDB ® is a trademark of MotherDuck. No direct affiliation or endorsement.
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement.
[^3]: Released under the MIT license. See LICENSE for details. All rights reserved by their respective owners.
[^4]: Elements of this experiments (including potential bugs) were co-authored by ChatGPT.
