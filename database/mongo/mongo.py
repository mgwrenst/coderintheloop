import os
from dotenv import load_dotenv
import psycopg
from pymongo import MongoClient
import datetime

# Load environment variables from .env file
load_dotenv()

def get_pg_conninfo_dict():
    keys = [
        ("PGHOST", "host"),
        ("PGPORT", "port"),
        ("PGUSER", "user"),
        ("PGPASSWORD", "password"),
        ("PGDATABASE", "dbname"),
    ]
    return {pg_key: os.environ.get(env_key) for env_key, pg_key in keys if os.environ.get(env_key)}

def convert_dates(obj):
    """Recursively convert datetime.date to datetime.datetime in dicts/lists for MongoDB."""
    if isinstance(obj, dict):
        return {k: convert_dates(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates(v) for v in obj]
    elif isinstance(obj, datetime.date) and not isinstance(obj, datetime.datetime):
        # Convert date to datetime (MongoDB understands datetime, not date)
        return datetime.datetime(obj.year, obj.month, obj.day)
    else:
        return obj

def transfer_table(pg_cur, mongo_db, pg_table, mongo_collection, query=None, chunk_size=1000):
    print(f"[INFO] Transferring table '{pg_table}' to MongoDB collection '{mongo_collection}' ...")
    query = query or f"SELECT * FROM {pg_table};"
    pg_cur.execute(query)
    columns = [desc[0] for desc in pg_cur.description]
    total = 0
    while True:
        rows = pg_cur.fetchmany(chunk_size)
        if not rows:
            break
        docs = [convert_dates(dict(zip(columns, row))) for row in rows]
        if docs:
            mongo_db[mongo_collection].insert_many(docs)
            total += len(docs)
            print(f"[OK] Inserted {len(docs)} rows to '{mongo_collection}' (total: {total}).")
    print(f"[SUCCESS] Table '{pg_table}' transferred ({total} documents).")

def main():
    # PostgreSQL connection
    pg_conninfo = get_pg_conninfo_dict()
    with psycopg.connect(**pg_conninfo) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            # MongoDB connection
            mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
            mongo_db_name = os.environ.get("MONGO_DATABASE", pg_conninfo.get("dbname", "groundtruth0"))
            mongo_client = MongoClient(mongo_url)
            mongo_db = mongo_client[mongo_db_name]

            # List of tables to transfer: (Postgres table name, MongoDB collection name)
            tables = [
                ("aksjeeiebok", "aksjeeiebok"),
                ("politikere", "politikere"),
                ("konkurs", "konkurs"),
                ("selskap", "selskap"),
                ("person", "person"),
                ("eierskap", "eierskap"),
            ]

            for pg_table, mongo_collection in tables:
                transfer_table(pg_cur, mongo_db, pg_table, mongo_collection)

            print("[COMPLETE] All tables transferred from PostgreSQL to MongoDB.")

if __name__ == "__main__":
    main()