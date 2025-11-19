from pymongo import MongoClient
import psycopg
from .base_loader import pg_row_to_bson

def build_copy(pg_conninfo, mongo_dbname, tables):
    """Copy each Postgres table into MongoDB as-is."""
    import psycopg
    from pymongo import MongoClient

    # Connect
    pg = psycopg.connect(**pg_conninfo)
    mongo = MongoClient("mongodb://localhost:27017")[mongo_dbname]

    # clear collections
    for table in tables:
        mongo[table].drop()

    for table in tables:
        print(f"[COPY] {table} -> Mongo")

        collection = mongo[table]

        with pg.cursor(name=f"cur_{table}") as cur:
            cur.itersize = 5000
            cur.execute(f"SELECT * FROM {table}") # type: ignore
            col_names = [desc[0] for desc in cur.description] # type: ignore

            while (rows := cur.fetchmany(5000)):
                docs = [pg_row_to_bson(col_names, r) for r in rows]
                collection.insert_many(docs, ordered=False)

    print("[DONE] Raw copy complete")
    pg.close()
