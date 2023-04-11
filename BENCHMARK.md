## Query Performance Benchmarks

Non authoritative, hacky and experimental benchmarks for sparking discussion, improvements and corrections.


### CLI: Remote Parquet
```
LOAD parquet; SELECT town, district, count() AS c, round(avg(price)) AS price FROM read_parquet('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_1.parquet') GROUP BY town, district LIMIT 10;
```
#### QuackHouse CLI
##### :icecream: Cold Query
```
real	0m0.888s
user	0m0.634s
sys     0m0.042s
```
#### DuckDB CLI
##### :icecream: Cold Query
```
real	0m1.390s
user	0m0.782s
sys     0m0.048s
```
##### :hot_pepper: Cached Query
```
real	0m0.730s
user	0m0.470s
sys     0m0.047s
```

#### Clickhouse-local
```
real	0m3.438s
user	0m1.899s
sys     0m0.259s
```

### API: Remote Parquet
```
LOAD parquet; SELECT town, district, count() AS c, round(avg(price)) AS price FROM read_parquet('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_1.parquet') GROUP BY town, district LIMIT 10;
```
#### Quackhouse 
##### :icecream: CURL, HTTP API
```
real	0m0.875s
user	0m0.675s
sys     0m0.073s
```
#### Clickhouse-server 
##### :icecream: CURL, HTTP API
```
real	0m1.884s
user	0m0.015s
sys     0m0.004s
```
