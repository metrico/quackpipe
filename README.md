# <img src="https://github.com/user-attachments/assets/5b0a4a37-ecab-4ca6-b955-1a2bbccad0b4" />

# <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=25 /> GigAPI: The Infinite Timeseries Lakehouse

Like a durable parquet floor, GigAPI provides rock-solid data foundation for your queries and analytics

> GigAPI by Gigapipe is our twist on future query engines – one where you focus on your data, not your infrastructure, servers or capacity. By combining the performance of DuckDB with cloud-native architecture principles we've created a simple and light solution designed for unlimited time series and analytical datasets that makes traditional server-based OLAP databases feel like costly relics and decimating infrastructure costs by 50-90% without performance loss. All 100% opensource - no open core cloud gimmicks.

> [!WARNING]  
> GigAPI is an open beta developed in public. Bugs and changes should be expected. Use at your own risk.
> 

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> Features

* Fast: DuckDB SQL + Parquet powered OLAP API Engine
* Flexible: Schema-less Parquet Ingestion & Compaction
* Simple: Low Maintenance, Portable, Infinitely Scalable
* Smart: Independent storage/write and compute/read components
* Extensible: Built-In Query Engine _(DuckDB)_ or DIY _(ClickHouse, Datafusion, etc)_

## <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=20 /> Usage

> S3 Support Coming Soon

```yml
services:
  gigapi:
    image: ghcr.io/gigapi/gigapi:latest
    container_name: gigapi
    hostname: gigapi
    restart: unless-stopped
    volumes:
      - ./data:/data
    ports:
      - "7971:7971"
    environment:
      - GIGAPI_ENABLED=true
      - GIGAPI_MERGE_TIMEOUT_S=10
      - GIGAPI_ROOT=/data
      - PORT=7971
  gigapi-querier:
    image: ghcr.io/gigapi/gigapi-querier:latest
    container_name: gigapi-querier
    hostname: gigapi-querier
    volumes:
      - ./data:/data
    ports:
      - "7972:7972"
    environment:
      - DATA_DIR=/data
      - PORT=7972
```
### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> Settings

| Env Var Name           | Description                                 | Default Value       |
|------------------------|---------------------------------------------|---------------------|
| GIGAPI_ROOT            | Root directory for the databases and tables | <current directory> |
| GIGAPI_MERGE_TIMEOUT_S | Merge timeout in seconds                    | 10                  |
| GIGAPI_SAVE_TIMEOUT_S  | Save timeout in seconds                     | 1.0                 |
| GIGAPI_NO_MERGES       | Disables merges when set to true            | false               |
| PORT                   | Port number for the server to listen on     | 7971                |


## <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=20 /> Write Support
As write requests come in to GigAPI they are parsed and progressively appeanded to parquet files alongside their metadata. The ingestion buffer is flushed to disk at configurable intervals using a hive partitioning schema. Generated parquet files and their respective metadata are progressively compacted and sorted over time based on configuration parameters.

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> API
GigAPI provides an HTTP API for clients to write, currently supporting the InfluxDB Line Protocol format 

```bash
cat <<EOF | curl -X POST "http://localhost:7971/write?db=mydb" --data-binary @/dev/stdin
weather,location=us-midwest,season=summer temperature=82
weather,location=us-east,season=summer temperature=80
weather,location=us-west,season=summer temperature=99
EOF
```

> [!NOTE]
> _more ingestion protocols coming soon!_

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> Data Schema
GigAPI is a schema-on-write database managing databases, tables and schemas on the fly. New columns can be added or removed over time, leaving reconciliation up to readers.

```bash
/data
  /mydb
    /weather
      /date=2025-04-10
        /hour=14
          *.parquet
          metadata.json
        /hour=15
          *.parquet
          metadata.json
```

GigAPI managed parquet files use the following naming schema:
```
{UUID}.{LEVEL}.parquet
```

### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=18 /> Parquet Compactor
GigAPI files are progressively compacted based on the following logic _(subject to future changes)_


| Merge Level   | Source | Target | Frequency              | Max Size |
|---------------|--------|--------|------------------------|----------|
| Level 1 -> 2  | `.1`   | `.2`   | `MERGE_TIMEOUT_S` = `10` | 100 MB   |
| Level 2 -> 3  | `.2`   | `.3`   | `MERGE_TIMEOUT_S` * `10` | 400 MB   |
| Level 3 -> 4  | `.3`   | `.3`   | `MERGE_TIMEOUT_S` * `10` * `10` | 4 GB     |



## <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=20 /> Read Support
As read requests come in to GigAPI they are parsed and transpiled using the GigAPI Metadata catalog to resolve data location based on database, table and timerange in requests. Series can be used with or without time ranges, ie for calculating averages, etc.

Query Data
```bash
$ curl -X POST "http://localhost:7972/query?db=mydb" \
  -H "Content-Type: application/json"  \
  -d {"query": "SELECT time, temperature FROM weather WHERE time >= epoch_ns('2025-04-24T00:00:00'::TIMESTAMP)"}
```

Series can be used with or without time ranges, ie for counting, calculating averages, etc.

```bash
$ curl -X POST "http://localhost:7972/query?db=mydb" \
  -H "Content-Type: application/json"  \
  -d '{"query": "SELECT count(*), avg(temperature) FROM weather"}'
```
```json
{"results":[{"avg(temperature)":87.025,"count_star()":"40"}]}
```

> GigAPI readers can be implemented in any language and with any OLAP engine supporting Parquet files.

<br>

## <img src="https://github.com/user-attachments/assets/74a1fa93-5e7e-476d-93cb-be565eca4a59" height=20 />  GigAPI Diagram
```mermaid
%%{
  init: {
    'theme': 'base',
    'themeVariables': {
      'primaryColor': '#6a329f',
      'primaryTextColor': '#fff',
      'primaryBorderColor': '#7C0000',
      'lineColor': '#6f329f',
      'secondaryColor': '#006100',
      'tertiaryColor': '#fff'
    }
  }
}%%

  graph TD;
      GigAPI-->ParquetWriter;
      ParquetWriter-->Storage;
      ParquetWriter-->Metadata;
      Storage-->Compactor;
      Compactor-->Storage;
      Compactor-->Metadata;
      Storage-.->LocalFS;
      Storage-.->S3;
      HTTP-API-- GET/POST --> GigAPI;
      DuckDB-->Storage;
      DuckDB-->Metadata;

      subgraph GigAPI[GigAPI Server]
        ParquetWriter
        Compactor
        Metadata;
        DuckDB;
      end

```

### Contributors

&nbsp;&nbsp;&nbsp;&nbsp;[![Contributors @metrico/quackpipe](https://contrib.rocks/image?repo=gigapi/gigapi)](https://github.com/gigapi/gigapi/graphs/contributors)

### Community

[![Stargazers for @metrico/quackpipe](https://reporoster.com/stars/gigapi/gigapi)](https://github.com/gigapi/gigapi/stargazers)


###### :black_joker: Disclaimers 

[^1]: DuckDB ® is a trademark of DuckDB Foundation. All rights reserved by their respective owners. [^1]
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement. [^2]
[^3]: InfluxDB ® is a trademark of InfluxData. No direct affiliation or endorsement. [^3]
[^4]: Released under the MIT license. See LICENSE for details. All rights reserved by their respective owners. [^4]
