# Postgres Database Builder

Build and query PostgreSQL databases from YAML configurations.

## Structure

```
postgres/
├── run.py              # Main entry point
├── main.py             # Core database building logic
├── config.yaml.example # Example connection config
├── versions/           # Database version configurations
│   ├── groundtruth0.yaml
│   └── groundtruthsmall.yaml
├── schemas/            # SQL schema files
│   ├── tables/         # Table definitions
│   │   ├── aksjeeiebok.sql
│   │   ├── eierskap.sql
│   │   ├── konkurs.sql
│   │   ├── person.sql
│   │   ├── politikere.sql
│   │   └── selskap.sql
│   ├── post_load/      # Post-load SQL scripts
│   │   ├── cleanup.sql
│   │   └── view.sql
│   └── indexes.sql     # Index definitions
└── queries/            # Query collections
    └── queries.py
```

## Setup

### Connection Configuration

Choose one of these methods:

1. Environment variables (recommended for multi-PC usage):
   ```
   PGHOST=localhost
   PGPORT=5432
   PGUSER=youruser
   PGPASSWORD=yourpassword
   PGDATABASE=defaultdb
   ```

2. Local config file (for single PC):
   - Copy `config.yaml.example` to `config.yaml`
   - Edit with your connection details
   - Add `config.yaml` to `.gitignore`

Environment variables take priority over config file.

## Usage

### Build Database

```bash
python run.py <version_name>
```

Example:
```bash
python run.py groundtruthsmall
```

### Run Queries

```bash
python queries/queries.py <query_name>
```

Example:
```bash
python queries/queries.py konkurs_orgnr_dato
```

## Adding New Versions

1. Create a new YAML file in `versions/` (e.g., `versions/myversion.yaml`)
2. Specify database name, CSV paths, and table configurations
3. Run: `python run.py myversion`

Version YAML format:
```yaml
database: mydb
csv_base_path: /path/to/csv/files
schema_base_path: postgres/schemas/tables

tables:
  - name: tablename
    schema: tablename.sql
    file: datafile.csv
    delimiter: ","
    drop_cols: []
    date_style: "SQL, DMY"

post_load_sql:
  - postgres/schemas/post_load/cleanup.sql
  - postgres/schemas/indexes.sql
```
