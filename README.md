<a href="https://quackpipe.fly.dev" target="_blank"><img src="https://user-images.githubusercontent.com/1423657/231310060-aae46ee6-c748-44c9-905e-20a4eba0a814.png" width=220 /></a>

> _quack, motherducker!_

# :baby_chick: quackpipe

_QuackPipe is an OLAP API built on top of DuckDB with a few extra compatibility bits. If you know, you know._

Play with DuckDB SQL and Cloud storage though a familiar API, without giving up old habits and integrations.

:hatched_chick: try our [public demo playground](https://quackpipe.fly.dev) and run a [sample query](https://quackpipe.fly.dev/?user=default#U0VMRUNUCiAgICB0b3duLAogICAgZGlzdHJpY3QsCiAgICBjb3VudCgpIEFTIGMsCkZST00gcmVhZF9wYXJxdWV0KCdodHRwczovL2RhdGFzZXRzLWRvY3VtZW50YXRpb24uczMuZXUtd2VzdC0zLmFtYXpvbmF3cy5jb20vaG91c2VfcGFycXVldC9ob3VzZV8wLnBhcnF1ZXQnKQpXSEVSRSByZWFkX3BhcnF1ZXQudG93biA9PSAnTE9ORE9OJwpHUk9VUCBCWQogICAgdG93biwKICAgIGRpc3RyaWN0Ck9SREVSIEJZIGMgREVTQwpMSU1JVCA1) _(1x-shared-vcpu, 256Mb, minimal resources)_



<br>

### :seedling: Get Started
Download a [binary release](https://github.com/metrico/quackpipe/releases/), use [docker](https://github.com/metrico/quackpipe/pkgs/container/quackpipe) or build from source

#### 🐋 Using Docker
```bash
docker pull ghcr.io/metrico/quackpipe:latest
docker run -ti --rm -p 8123:8123 ghcr.io/metrico/quackpipe:latest
```

#### 📦 Download Binary
```bash
curl -fsSL github.com/metrico/quackpipe/releases/latest/download/quackpipe-amd64 --output quackpipe \
&& chmod +x quackpipe
```
##### 🔌 Start Server w/ parameters
```bash
./quackpipe --port 8123
```

##### 🔌 Start Server w/ file database, READ-ONLY access
```bash
./quackpipe --port 8123 --params ""/tmp/test.db?access_mode=READ_ONLY"
```

Run with `-h` for a full list of parameters

##### Parameters

| params | usage | default |
|-- |-- |-- |
| `--port` | HTTP API Port | `8123` |
| `--host` | HTTP API Host | `0.0.0.0` |
| `--stdin` | STDIN query mode | `false` |
| `--format` | FORMAT handler | `JSONCompact` |
| `--params` | Optional Parameters |  |
<br>

#### :point_right: Playground
Execute queries using the embedded playground

![image](https://user-images.githubusercontent.com/1423657/230783859-1c69910b-6bf2-42df-8b1d-876b94fc3419.png)

#### :point_right: API
Execute queries using the POST API
```
curl -X POST https://quackpipe.fly.dev 
   -H "Content-Type: application/json"
   -d 'SELECT version()'  
```

#### :point_right: STDIN
Execute queries using STDIN
```
# echo "SELECT 'hello', version() as version FORMAT CSV" | ./quackpipe --stdin
hello,v0.7.1
```

### :fist_right: Extensions
Several extensions are pre-installed by default in Docker images, including _parquet, json, httpfs_<br>
When using HTTP API, _httpfs, parquet, json_ extensions are automatically pre-loaded.

<br>

-------

### :construction: Feature Status
- [x] DuckDB Core [^1]
  - [x] [cgo](https://github.com/marcboeker/go-duckdb) binding
  - [x] Extension preloading
  - [ ] Aliases Extension
- [x] REST API [^3] [^4]
  - [x] CH FORMAT Emulation
    - [x] CSV, CSVWithNames
    - [x] TSV, TSVWithNames
    - [x] JSONCompact
    - [ ] Native
  - [x] Web Playground _(from ClickkHouse, Apache2 Licensed)_ [^2]
- [x] STDIN Fast Query Execution
- [x] `:memory:` table + Cloud Storage _(s3/r2/minio, httpfs, etc)_


-------

### Contributors

&nbsp;&nbsp;&nbsp;&nbsp;[![Contributors @metrico/quackpipe](https://contrib.rocks/image?repo=metrico/quackpipe)](https://github.com/metrico/quackpipe/graphs/contributors)

### Community

[![Stargazers for @metrico/quackpipe](https://reporoster.com/stars/metrico/quackpipe)](https://github.com/metrico/quackpipe/stargazers)

<!-- [![Forkers for @metrico/quackpipe](https://reporoster.com/forks/metrico/quackpipe)](https://github.com/metrico/quackpipe/network/members) -->


###### :black_joker: Disclaimers 

[^1]: DuckDB ® is a trademark of MotherDuck. No direct affiliation or endorsement.
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement.
[^3]: Released under the MIT license. See LICENSE for details. All rights reserved by their respective owners.
[^4]: Elements of this experiments (including potential bugs) were co-authored by ChatGPT.
