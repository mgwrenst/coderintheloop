import os
import datetime
from dotenv import load_dotenv

def get_pg_conninfo(dbname=None):
    """Return connection info dict for psycopg.connect()"""
    load_dotenv()
    return {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "dbname": dbname or os.getenv("PGDATABASE"),
    }

def convert_dates(obj):
    """Recursively convert datetime.date → datetime.datetime for MongoDB."""
    if isinstance(obj, dict):
        return {k: convert_dates(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_dates(v) for v in obj]
    elif isinstance(obj, datetime.date) and not isinstance(obj, datetime.datetime):
        return datetime.datetime(obj.year, obj.month, obj.day)
    return obj

def transfer_table(pg_cur, mongo_db, pg_table, mongo_collection, query=None, chunk_size=1000):
    """Copy a Postgres table into a MongoDB collection."""
    print(f"[INFO] Transferring '{pg_table}' → '{mongo_collection}' ...")
    query = query or f'SELECT * FROM "{pg_table}";'
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
            print(f"[OK] Inserted {len(docs)} docs (total: {total}).")
    print(f"[SUCCESS] {pg_table} → {mongo_collection} ({total} docs).")


def formater_eierskap(rad):
    return {
        "uuid": rad.get("eierskapuuid"),
        "år": rad.get("eierskapår"),
        "andel": {
            "antall": rad.get("eierskapantall"),
            "prosent": rad.get("eierskapandel"),
            "stemmer": {
                "prosent": rad.get("eierskapstemmeandel"),
                "antall": rad.get("eierskapstemmeantall"),
                "totalt_stemmer": rad.get("eierskaptotalstemmeantall"),
            },
            "totalt_aksjer": rad.get("eierskaptotalantall"),
        },
        "eier": {
            "uuid": rad.get("eierpersonuuid"),
            "navn": {"full": rad.get("eierpersonnavn")},
            "fødsel": {
                "dato": rad.get("eierpersonfødselsdato"),
                "år": rad.get("eierpersonfødselsår"),
                "kjønnuuid": rad.get("eierpersonkjønnuuid"),
            },
            "adresse": {
                "gate": rad.get("eierpersonadresse"),
                "postnummer": rad.get("eierpersonpostkode"),
                "poststed": rad.get("eierpersonpoststed"),
                "kommune": {
                    "nr": rad.get("eierpersonkommunenr"),
                    "navn": rad.get("eierpersonkommune"),
                },
            },
        },
        "utsteder": {
            "uuid": rad.get("utstederuuid"),
            "orgnr": rad.get("utstederorgnr"),
            "navn": rad.get("utstedernavn"),
        },
    }
