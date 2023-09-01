# pg_json_bench

### Spin up docker compose with monitoring stack
```shell
docker compose up -d
```

### Prepare the data
```shell
mkdir ./data
wget https://raw.githubusercontent.com/algolia/datasets/master/movies/records.json -O ./data/records.json
```

### Prepare the schema
```shell
psql -c "create database test;"
psql -d test < ./main.sql
```

### Build
```shell
go build -o pg_json_bench
```

### Run Benchmark
Example:
```shell
DBCONN="dbname=test sslmode=disable" ./pg_json_bench query count_score_over_7 btree_idx_score,gin_idx,gin_idx_path
```

On the output the link for the metrics will be displayed, maybe you need to refresh.
Grafana user and password is `admin`