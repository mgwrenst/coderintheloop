import os
from dotenv import load_dotenv
import psycopg
from pymongo import MongoClient
import datetime
from mongo_utils import convert_dates, get_pg_conninfo, transfer_table

# Load environment variables from .env file
load_dotenv()


def build_selskap(pg_cur, mongo_db, chunk_size=1000):
    print("[INFO] Building selskap with embedded persons...")

    # 1️⃣ Get all companies (active + bankrupt)
    pg_cur.execute("""
        SELECT navn, "uuid", "orgnr", "konkursflagg", "likvidasjonflagg",
               "nacekode", "organisasjonstype", "oppløstdato", "etablertdato"
        FROM (
            SELECT * FROM selskap
            UNION ALL
            SELECT * FROM konkurs
        ) AS selskap;
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
                SELECT "uuid", "navn", "fødselsdato", "fødselsår", "kjønnuuid", "postnummer", "land", "poststed", 
                       "adresse", "landkode", "registrerttid", "oppdaterttid", "kommunenr", "kommunenavn", 
                       "selskapnavn", "selskapuuid", "selskaporgnr", "selskapregistrert", "selskapoppdatert", 
                       "selskaprolleuuid", "selskaprolle", "rolleregistrert", "rolleoppdatert", 
                       "selskaprollerang", "rolleuuid", "rollesluttdato", "rollestartdato"
                FROM person
                WHERE "selskapuuid" = %s;
            """, (company["uuid"],))
            raw_persons = [dict(zip([d[0] for d in pg_cur.description], r)) for r in pg_cur.fetchall()]
            cleaned_persons = []

            for p in raw_persons:
                cleaned_persons.append({
                    "uuid": p["uuid"],
                    "name": {"full": p.get("navn")},
                    "birth": {
                        "date": p.get("fødselsdato"),
                        "year": p.get("fødselsår"),
                        "gender_uuid": p.get("kjønnuuid"),
                    },
                    "address": {
                        "street": p.get("adresse"),
                        "postal_code": p.get("postnummer"),
                        "city": p.get("poststed"),
                        "municipality": {
                            "nr": p.get("kommunenr"),
                            "name": p.get("kommunenavn"),
                        },
                        "country": {
                            "name": p.get("land"),
                            "code": p.get("landkode"),
                        },
                    },
                    "role": {
                        "type": p.get("selskaprolle"),
                        "uuid": p.get("selskaprolleuuid"),
                        "start_date": p.get("rollestartdato"),
                        "end_date": p.get("rollesluttdato"),
                    },
                    "meta": {
                        "registered": p.get("registrerttid"),
                        "updated": p.get("oppdaterttid"),
                    },
                })

            company["persons"] = cleaned_persons

            # Convert dates
            docs.append(convert_dates(company))

        # 3️⃣ Insert into MongoDB
        if docs:
            mongo_db["selskap"].insert_many(docs)
            inserted += len(docs)
            print(f"[OK] Inserted {len(docs)} companies (total: {inserted}).")

    print(f"[SUCCESS] selskap created with {inserted} documents.")


def add_ownership_refs(mongo_db):
    """Add lightweight ownership_refs to selskap based on eierskap."""
    print("[INFO] Adding ownership_refs to selskap...")

    selskap_col = mongo_db["selskap"]
    eierskap_col = mongo_db["eierskap"]

    # Build a mapping of orgnr -> ownership_refs
    ownership_map = {}
    for doc in eierskap_col.find(
        {"utstederorgnr": {"$ne": None}},
        {"_id": 0, "utstederorgnr": 1, "eierskapuuid": 1, "eierpersonnavn": 1}
    ):
        orgnr = doc["utstederorgnr"]
        ref = {
            "eierskapuuid": doc["eierskapuuid"],
            "eierpersonnavn": doc.get("eierpersonnavn")
        }
        ownership_map.setdefault(orgnr, []).append(ref)

    print(f"[INFO] Built ownership map for {len(ownership_map)} companies.")

    # Add references to selskap
    updated = 0
    for orgnr, refs in ownership_map.items():
        result = selskap_col.update_one({"orgnr": orgnr}, {"$set": {"ownership_refs": refs}})
        updated += result.modified_count

    print(f"[SUCCESS] Added ownership_refs to {updated} selskap documents.")


def main():
    # PostgreSQL connection
    pg_conninfo = get_pg_conninfo()
    with psycopg.connect(**pg_conninfo) as pg_conn, pg_conn.cursor() as pg_cur:
        mongo_client = MongoClient("mongodb://localhost:27017")
        mongo_db = mongo_client["groundtruthsmall"]

        # Clear collections before loading
        for coll in ["selskap", "eierskap", "politikere", "aksjeeiebok"]:
            mongo_db[coll].drop()
        print("[INFO] Old collections cleared.")

        # Build selskap with embedded persons
        build_selskap(pg_cur, mongo_db)

        # Transfer remaining tables
        transfer_table(pg_cur, mongo_db, "eierskap", "eierskap")
        transfer_table(pg_cur, mongo_db, "politikere", "politikere")
        transfer_table(pg_cur, mongo_db, "aksjeeiebok", "aksjeeiebok")

        # 🧩 Add ownership_refs *after* selskap and eierskap exist
        add_ownership_refs(mongo_db)

        print("[COMPLETE] MongoDB ETL finished successfully.")


if __name__ == "__main__":
    main()
