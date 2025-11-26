# Mongo Builders

Build and structure MongoDB from Postgres using simple version configs.

## Structure

```
mongo/
├── run_mongo.py         # Entry point
├── main.py              # Consolidated build logic (copy + structured)
├── config.yaml.example  # Optional Mongo connection settings
├── versions/            # Version YAMLs (source/targets + tables)
│   ├── groundtruthsmall.yaml
│   └── groundtruth0.yaml
├── benchmarks/          # Query benchmark samples
│   ├── benchmark.json
│   └── benchmark_structured.json
└── queries/
    └── queries.py       # Simple runner for benchmark queries
```

## Connection

- Postgres: uses `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` env vars
- Mongo: uses `MONGO_URI` (defaults to `mongodb://localhost:27017`)
- You can also copy `config.yaml.example` → `config.yaml` to set `mongo_uri`

## Build

```powershell
# Copy raw tables from Postgres to Mongo
python mongo/run_mongo.py groundtruthsmall copy

# Build structured Mongo from raw copy
python mongo/run_mongo.py groundtruthsmall structured

# Do both steps
python mongo/run_mongo.py groundtruthsmall all
```

Version YAML (example):
```yaml
source_postgres: groundtruthsmall
target_mongo_copy: groundtruthsmall_copy
target_mongo_structured: groundtruthsmall_structured

tables:
- eierskap
- politikere
- aksjeeiebok
- person
- selskap
```

## Queries

```powershell
# Uses MONGO_URI and DB_NAME env (DB_NAME defaults to groundtruthsmall)
python mongo/queries/queries.py
```

- Benchmarks load from `mongo/benchmarks/benchmark.json`
- Edit or add more queries by updating the JSON
