<a href="https://quackpipe.fly.dev" target="_blank"><img src="https://github.com/user-attachments/assets/790afacb-96d9-4cdf-af48-1948049e0385" width=220 /></a>

> _a data pipe for quackheads_

# :baby_chick: quackpipe

_QuackPipe is a serverless OLAP API built on top of DuckDB emulating and aliasing the ClickHouse HTTP API_

Play with DuckDB SQL and Cloud storage though a familiar API, without giving up old habits and integrations.

### :hatched_chick: Demos
:hatched_chick: try a [sample s3/parquet query](https://quackpipe.fly.dev/?user=default#U0VMRUNUCiAgICB0b3duLAogICAgZGlzdHJpY3QsCiAgICBjb3VudCgpIEFTIGMsCkZST00gcmVhZF9wYXJxdWV0KCdodHRwczovL2RhdGFzZXRzLWRvY3VtZW50YXRpb24uczMuZXUtd2VzdC0zLmFtYXpvbmF3cy5jb20vaG91c2VfcGFycXVldC9ob3VzZV8wLnBhcnF1ZXQnKQpXSEVSRSByZWFkX3BhcnF1ZXQudG93biA9PSAnTE9ORE9OJwpHUk9VUCBCWQogICAgdG93biwKICAgIGRpc3RyaWN0Ck9SREVSIEJZIGMgREVTQwpMSU1JVCAxMA==) in our [miniature playground](https://quackpipe.fly.dev) _(fly.io free tier, 1x-shared-vcpu, 256Mb)_ <br>
:hatched_chick: launch your own _free instance_ on fly.io

<a href="https://flyctl.sh/shell?repo=metrico/quackpipe" target="_blank">
  <img src="https://user-images.githubusercontent.com/1423657/236479471-a1cb0484-dfd3-4dc2-8d62-121debd7faa3.png" width=300>
</a>

<br>

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
./quackpipe --port 8123 --params "/tmp/test.db?access_mode=READ_ONLY"
```

##### 🔌 Start Server w/ Motherduck authentication token
###### Using DuckDB Params
```bash
./quackpipe --port 8123 --params "/tmp/test.db?motherduck_token=YOUR_TOKEN_HERE"
```
###### Using System ENV
```bash
export motherduck_token='<token>'
./quackpipe --port 8123 
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
Execute stateless queries w/o persistence using the embedded playground

<a href="https://quackpipe.fly.dev" target=_blank><img src="https://github.com/metrico/quackpipe/assets/1423657/fa0c8b8f-7480-4bd1-b8b2-bee24ee39186" width=800></a>

##### 👉 Stateful Queries
Execute stateful queries with data persistence by adding unique HTTP Authentication. No registration required.

<a href="https://quackpipe.fly.dev" target=_blank><img src="https://github.com/metrico/quackpipe/assets/1423657/b0546f2a-fa0b-4cbf-b336-a6cdeaa86863" width=800></a>



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
hello,v1.1.1
```

### :fist_right: Clickhouse SQL (chsql)
Quackpipe speaks a little ClickHouse SQL using the [chsql](https://community-extensions.duckdb.org/extensions/chsql.html) DuckDB Extension providing users with [100+ ClickHouse SQL Command Macros](https://community-extensions.duckdb.org/extensions/chsql.html#added-functions) two clients _(HTTP/S and Native)_ to interact with remote ClickHouse APIs

#### Example
```sql
--- Install and load chsql
D INSTALL chsql FROM community;
D LOAD chsql;

--- Use any of the 100+ ClickHouse Function Macros
D SELECT IPv4StringToNum('127.0.0.1'), IPv4NumToString(2130706433);
┌──────────────────────────────┬─────────────────────────────┐
│ ipv4stringtonum('127.0.0.1') │ ipv4numtostring(2130706433) │
│            int32             │           varchar           │
├──────────────────────────────┼─────────────────────────────┤
│                   2130706433 │ 127.0.0.1                   │
└──────────────────────────────┴─────────────────────────────┘
```

### Remote Queries
The built-in `ch_scan` function can be used to query remote ClickHouse servers using the HTTP/s API
```sql
--- Set optional X-Header Authentication
D CREATE SECRET extra_http_headers (
      TYPE HTTP,
      EXTRA_HTTP_HEADERS MAP{
          'X-ClickHouse-User': 'user',
          'X-ClickHouse-Key': 'password'
      }
  );
--- Query using the HTTP API
D SELECT * FROM ch_scan("SELECT number * 2 FROM numbers(10)", "https://play.clickhouse.com");
```

### :fist_right: Extensions
Several extensions are pre-installed by default in [Docker images](https://github.com/metrico/quackpipe/blob/main/Dockerfile#L9), including _parquet, json, httpfs_<br>
When using HTTP API, _httpfs, parquet, json_ extensions are automatically pre-loaded by the wrapper.

Users can pre-install extensions and execute quackpipe using a custom parameters:
```
echo "INSTALL httpfs;" | ./quackpipe --stdin --params "?extension_directory=/tmp/"
./quackpipe --port 8123 --host 0.0.0.0 --params "?extension_directory=/tmp/"
```

### <img src="https://github.com/metrico/quackpipe/assets/1423657/f66fd8f8-a756-40a6-bee9-7979b09f2576" height=20 > ClickHouse HTTP

Quackpipe can be used to query a remote instance of itself and/or ClickHouse using the HTTP API

```sql
CREATE OR REPLACE MACRO quackpipe(query, server := 'https://play.clickhouse.com', user := 'play', format := 'JSONEachRow') AS TABLE
    SELECT * FROM read_json_auto(concat(server, '/?default_format=', format, '&user=', user, '&query=', query));

SELECT * FROM quackpipe("SELECT number as once, number *2 as twice FROM numbers(10)")
```

### <img src="https://github.com/metrico/quackpipe/assets/1423657/f66fd8f8-a756-40a6-bee9-7979b09f2576" height=20 > ClickHouse UDF

Quackpipe can be used as [executable UDF](https://clickhouse.com/docs/en/engines/table-functions/executable) to get DuckDB data IN/OUT of ClickHouse queries:

```sql
SELECT *
FROM executable('quackpipe -stdin -format TSV', TSV, 'id UInt32, num UInt32', (
    SELECT 'SELECT 1, 2'
))
Query id: dd878948-bec8-4abe-9e06-2f5813653c3a
┌─id─┬─num─┐
│  1 │   2 │
└────┴─────┘
1 rows in set. Elapsed: 0.155 sec.
```

🃏 What is this? Think of it as a SELECT within a SELECT with a different syntax.<br>
🃏 Format confusion? Make DuckDB SQL feel like ClickHouse with the included [ClickHouse Macro Aliases](https://github.com/metrico/quackpipe/blob/main/aliases.sql)


<br>

-------

### :construction: Feature Status
- [x] DuckDB Core [^1]
  - [x] [cgo](https://github.com/marcboeker/go-duckdb) binding
  - [x] Extension preloading
  - [ ] Aliases Extension
- [x] REST API [^3]
  - [x] CH FORMAT Emulation
    - [x] CSV, CSVWithNames
    - [x] TSV, TSVWithNames
    - [x] JSONCompact
    - [ ] Native
  - [x] Web Playground _(from ClickkHouse, Apache2 Licensed)_ [^2]
- [x] STDIN Fast Query Execution
- [x] ClickHouse Executable UDF
- [x] `:memory:` mode Cloud Storage _(s3/r2/minio, httpfs, etc)_
- [x] `:file:` mode using optional _parameters_

-------

### Contributors

&nbsp;&nbsp;&nbsp;&nbsp;[![Contributors @metrico/quackpipe](https://contrib.rocks/image?repo=metrico/quackpipe)](https://github.com/metrico/quackpipe/graphs/contributors)

### Community

[![Stargazers for @metrico/quackpipe](https://reporoster.com/stars/metrico/quackpipe)](https://github.com/metrico/quackpipe/stargazers)

<!-- [![Forkers for @metrico/quackpipe](https://reporoster.com/forks/metrico/quackpipe)](https://github.com/metrico/quackpipe/network/members) -->


###### :black_joker: Disclaimers 

[^1]: DuckDB ® is a trademark of DuckDB Foundation. All rights reserved by their respective owners.
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement.
[^3]: Released under the MIT license. See LICENSE for details. All rights reserved by their respective owners.
