## Query Performance Benchmarks

Non authoritative, hacky and experimental benchmarks for sparking discussion, improvements and corrections.

![image](https://user-images.githubusercontent.com/1423657/231174042-35eb47fa-1015-4e18-9045-c15255394881.png)



### CLI: Remote Parquet
```
LOAD parquet; SELECT town, district, count() AS c, round(avg(price)) AS price FROM read_parquet('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_1.parquet') GROUP BY town, district LIMIT 10;
```
#### QuackPipe CLI `v0.7.1.6`
##### :icecream: Cold Query
```
real	0m0.888s
user	0m0.634s
sys     0m0.042s
```
#### DuckDB CLI `v0.7.1`
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

#### Clickhouse-local `v23.4.1.1`
##### :icecream: Cold Query
```
real	0m3.438s
user	0m1.899s
sys     0m0.259s
```

### API: Remote Parquet
```
LOAD parquet; SELECT town, district, count() AS c, round(avg(price)) AS price FROM read_parquet('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_1.parquet') GROUP BY town, district LIMIT 10;
```
#### QuackPipe `v0.7.1.6`
##### :icecream: CURL, HTTP API
```
real	0m0.875s
user	0m0.675s
sys     0m0.073s
```
#### Clickhouse-server `v23.4.1.1`
##### :icecream: CURL, HTTP API
```
real	0m1.884s
user	0m0.015s
sys     0m0.004s
```
