import os
from dotenv import load_dotenv

load_dotenv()

def get_pg_conninfo(dbname: str) -> dict:
    """Return psycopg connection info for a specific Postgres database."""
    return {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "dbname": dbname,
    }
