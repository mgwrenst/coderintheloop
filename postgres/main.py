import os
import yaml
import psycopg
from pathlib import Path
from psycopg import sql


def get_connection_info(dbname=None):
    conninfo = {}
    if os.getenv("PGHOST"):
        conninfo["host"] = os.getenv("PGHOST")
    if os.getenv("PGPORT"):
        conninfo["port"] = os.getenv("PGPORT")
    if os.getenv("PGUSER"):
        conninfo["user"] = os.getenv("PGUSER")
    if os.getenv("PGPASSWORD"):
        conninfo["password"] = os.getenv("PGPASSWORD")
    
    if dbname:
        conninfo["dbname"] = dbname
    elif os.getenv("PGDATABASE"):
        conninfo["dbname"] = os.getenv("PGDATABASE")
    
    config_file = Path(__file__).parent / "config.yaml"
    if config_file.exists() and not conninfo:
        with open(config_file) as f:
            file_config = yaml.safe_load(f)
            conninfo.update(file_config.get("connection", {}))
            if dbname:
                conninfo["dbname"] = dbname
    
    return conninfo


def connect_db(dbname):
    conninfo = get_connection_info(dbname)
    return psycopg.connect(**conninfo)


def load_version_config(version_name):
    versions_dir = Path(__file__).parent / "versions"
    config_path = versions_dir / f"{version_name}.yaml"
    
    if not config_path.exists():
        print(f"ERROR: Version config not found: {config_path}")
        print("Available versions:")
        for f in versions_dir.glob("*.yaml"):
            print(f"  - {f.stem}")
        return None
    
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    if "csv_base_path" in config:
        config["csv_base_path"] = Path(config["csv_base_path"])
    
    if "schema_base_path" in config:
        config["schema_base_path"] = Path(config["schema_base_path"])
    
    for table in config.get("tables", []):
        if "schema" in table and config.get("schema_base_path"):
            table["schema"] = config["schema_base_path"] / table["schema"]
        if "file" in table and config.get("csv_base_path"):
            table["file"] = config["csv_base_path"] / table["file"]
    
    return config


def run_sql_file(cur, filepath):
    filepath = Path(filepath)
    with open(filepath, encoding="utf-8") as f:
        cur.execute(f.read())


def load_table(cur, name, schema_path, csv_path, delimiter=",", drop_cols=None, date_style=None):
    print(f"Loading table: {name}")
    
    if date_style:
        cur.execute(f"SET datestyle = {date_style};")
    
    with open(schema_path, encoding="utf-8") as f:
        cur.execute(f.read())
    
    cur.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY;").format(sql.Identifier(name)))
    
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    copy_sql = f"COPY {name} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER '{delimiter}')"
    
    with open(csv_path, encoding="utf-8") as f, cur.copy(copy_sql) as copy:
        for line in f:
            copy.write(line)
    
    if drop_cols:
        drops = ", ".join([f"DROP COLUMN IF EXISTS {col}" for col in drop_cols])
        cur.execute(f"ALTER TABLE {name} {drops};")
    
    print(f"  - Table '{name}' loaded")


def build_database(version_name):
    print(f"Loading version config: {version_name}")
    
    config = load_version_config(version_name)
    if not config:
        return False
    
    dbname = config["database"]
    tables = config.get("tables", [])
    post_sql_files = config.get("post_load_sql", [])
    
    print(f"Building database: {dbname}")
    
    with connect_db(dbname) as conn, conn.cursor() as cur:
        for table_cfg in tables:
            name = table_cfg["name"]
            
            try:
                cur.execute("SAVEPOINT before_table_load;")
                
                load_table(
                    cur=cur,
                    name=name,
                    schema_path=table_cfg["schema"],
                    csv_path=table_cfg["file"],
                    delimiter=table_cfg.get("delimiter", ","),
                    drop_cols=table_cfg.get("drop_cols"),
                    date_style=table_cfg.get("date_style"),
                )
                
            except Exception as e:
                print(f"ERROR: Failed loading table '{name}': {e}")
                cur.execute("ROLLBACK TO SAVEPOINT before_table_load;")
        
        if post_sql_files:
            print("Running post-load SQL scripts")
        
        for sql_file in post_sql_files:
            sql_path = Path(sql_file)
            if not sql_path.exists():
                print(f"WARNING: Post-load SQL file not found: {sql_path}")
                continue
            
            print(f"  - Running: {sql_path.name}")
            run_sql_file(cur, sql_path)
        
        conn.commit()
    
    print(f"Database '{dbname}' built successfully")
    return True
