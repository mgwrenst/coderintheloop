import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

def get_conninfo(dbname=None):
    env_map = {
        "host": "PGHOST",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "dbname": "PGDATABASE",
    }
    conninfo = {k: os.getenv(v) for k, v in env_map.items() if os.getenv(v)}
    if dbname:
        conninfo["dbname"] = dbname
    return conninfo

def run_sql_file(cur, filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        cur.execute(f.read())

def load_table(cur, name, schema_path, csv_path, delimiter=",", drop_cols=None, date_style=None):
    """Create, truncate, and load a table from CSV."""
    print(f"[INFO] Loading {name} ...")

    if date_style:
        cur.execute(f"SET datestyle = {date_style};")

    # create table
    with open(schema_path, encoding="utf-8") as f:
        cur.execute(f.read())

    # load data
    cur.execute(f"TRUNCATE {name} RESTART IDENTITY;")
    with open(csv_path, encoding="utf-8") as f, cur.copy(
        f"COPY {name} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER '{delimiter}')"
    ) as copy:
        for line in f:
            copy.write(line)

    # drop unwanted columns
    if drop_cols:
        drops = ", ".join([f"DROP COLUMN IF EXISTS {c}" for c in drop_cols])
        cur.execute(f"ALTER TABLE {name} {drops};")

    print(f"[OK] {name} loaded.")