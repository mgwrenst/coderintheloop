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

def build_all_companies(pg_cur, mongo_db, chunk_size=1000):
    print("[INFO] Building all_companies with embedded persons...")

    # 1️⃣ Get all companies (active + bankrupt)
    pg_cur.execute("""
        SELECT navn, "uuid", "orgnr", "konkursflagg", "likvidasjonflagg",
               "nacekode", "organisasjonstype", "oppløstdato", "etablertdato"
        FROM (
            SELECT * FROM selskap
            UNION ALL
            SELECT * FROM konkurs
        ) AS all_companies;
    """)
    cols = [d[0] for d in pg_cur.description]

    inserted = 0
    while True:
        companies = pg_cur.fetchmany(chunk_size)
        if not companies:
            break

        docs = []
        for row in companies:
            company = dict(zip(cols, row))
            # 2️⃣ Find persons linked to this company
            pg_cur.execute("""
                SELECT "uuid", "navn", "fødselsdato", "fødselsår", "kjønnuuid", "postnummer", "land", "poststed", "adresse", "landkode", "registrerttid", "oppdaterttid", "kommunenr",  "kommunenavn", "selskapnavn", "selskapuuid", "selskaporgnr", "selskapregistrert", "selskapoppdatert", "selskaprolleuuid", "selskaprolle", "rolleregistrert", "rolleoppdatert", "selskaprollerang", "rolleuuid", "rollesluttdato", "rollestartdato"
                FROM person
                WHERE "selskapuuid" = %s;
            """, (company["uuid"],))
            persons = [dict(zip([d[0] for d in pg_cur.description], r)) for r in pg_cur.fetchall()]
            company["persons"] = persons

            # Convert dates
            docs.append(convert_dates(company))

        # 3️⃣ Insert into MongoDB
        if docs:
            mongo_db["all_companies"].insert_many(docs)
            inserted += len(docs)
            print(f"[OK] Inserted {len(docs)} companies (total: {inserted}).")

    print(f"[SUCCESS] all_companies created with {inserted} documents.")


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
            mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
            mongo_db_name = os.environ.get("MONGO_DATABASE", pg_conninfo.get("dbname", "groundtruthsmall"))
            mongo_client = MongoClient(mongo_url)
            mongo_db = mongo_client[mongo_db_name]

            # First, drop old collections if re-running
            for coll in ["all_companies", "ownerships", "politicians", "sharebooks"]:
                mongo_db[coll].drop()

            # Build all_companies with embedded persons
            build_all_companies(pg_cur, mongo_db)

            # Copy remaining flat tables
            transfer_table(pg_cur, mongo_db, "eierskap", "ownerships")
            transfer_table(pg_cur, mongo_db, "politikere", "politicians")
            transfer_table(pg_cur, mongo_db, "aksjeeiebok", "sharebooks")

            print("[COMPLETE] MongoDB ETL done.")

if __name__ == "__main__":
    main()