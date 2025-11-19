import os
import psycopg
from pathlib import Path
from dotenv import load_dotenv

# Load .env once at import time
load_dotenv()


def get_conninfo(dbname: str | None = None) -> dict:
    """
    Build a psycopg connection info dict using environment variables.
    If dbname is provided, it overrides PGDATABASE.
    """
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

def connect_to_database(dbname: str) -> psycopg.Connection:
    conninfo = get_conninfo(dbname=dbname)
    return psycopg.connect(**conninfo)