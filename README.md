<img src="https://user-images.githubusercontent.com/1423657/230504468-39bdecf5-b1c1-462c-bb11-91d147cde8d3.png" width=180 />

> quack, motherducker

# quackhouse

_This is an unholy experiment. Can we silently replace ClickHouse with DuckDB for selected usecases?_

### Status

- [x] DuckDB [cgo binding](https://github.com/marcboeker/go-duckdb)
- [x] GO API
- [x] JSONCompact output
- [x] ClickHouse Playground


### Usage
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
