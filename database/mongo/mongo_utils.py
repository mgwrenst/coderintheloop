import os
import datetime
from datetime import datetime, date

def get_pg_conninfo(dbname=None):
    return {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "dbname": dbname or os.getenv("PGDATABASE"),
    }

def transfer_table(pg_conn, mongo_db, pg_table_name, mongo_collection_name, batch_size=5000):
    collection = mongo_db[mongo_collection_name]
    print(f"Transferring {pg_table_name} -> {mongo_collection_name}")

    # open a named, server-side cursor for streaming
    with pg_conn.cursor(name=f"cur_{pg_table_name}") as pg_cur:
        pg_cur.itersize = batch_size
        pg_cur.execute(f"SELECT * FROM {pg_table_name}")
        col_names = [desc[0] for desc in pg_cur.description]
        total = 0

        while True:
            rows = pg_cur.fetchmany(batch_size)
            if not rows:
                break

            docs = []
            for row in rows:
                doc = {}
                for col, val in zip(col_names, row):
                    if isinstance(val, date) and not isinstance(val, datetime):
                        val = datetime(val.year, val.month, val.day)
                    doc[col] = val
                docs.append(doc)

            if docs:
                collection.insert_many(docs, ordered=False)
                total += len(docs)

        print(f"Done ({total} rows)")
