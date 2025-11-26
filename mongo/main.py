import os
import yaml
from pathlib import Path
from datetime import date, datetime

import psycopg
from pymongo import MongoClient, ASCENDING


# ----------------------------
# Connection helpers
# ----------------------------

def get_pg_conninfo(dbname: str) -> dict:
    return {
        "host": os.getenv("PGHOST"),
        "port": os.getenv("PGPORT"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "dbname": dbname,
    }

def get_mongo_uri() -> str:
    # Try env, fallback to config.yaml if present
    uri = os.getenv("MONGO_URI")
    if uri:
        return uri
    cfg_path = Path(__file__).parent / "config.yaml"
    if cfg_path.exists():
        cfg = yaml.safe_load(cfg_path.read_text())
        uri = (cfg or {}).get("mongo_uri")
        if uri:
            return uri
    return "mongodb://localhost:27017"


# ----------------------------
# Version config loader
# ----------------------------

def load_version(version_name: str) -> dict:
    versions_dir = Path(__file__).parent / "versions"
    cfg_path = versions_dir / f"{version_name}.yaml"
    if not cfg_path.exists():
        print(f"[ERROR] Version config not found: {cfg_path}")
        print("Available versions:")
        for f in versions_dir.glob("*.yaml"):
            print(" -", f.stem)
        return {}
    return yaml.safe_load(cfg_path.read_text()) or {}


# ----------------------------
# Small utils
# ----------------------------

def pg_row_to_bson(col_names, row):
    doc = {}
    for col, val in zip(col_names, row):
        if isinstance(val, date) and not isinstance(val, datetime):
            val = datetime(val.year, val.month, val.day)
        doc[col] = val
    return doc


# ----------------------------
# Build: raw copy Postgres -> Mongo
# ----------------------------

def build_copy(pg_conninfo: dict, mongo_dbname: str, tables: list[str]):
    print(f"[INFO] Copying tables to Mongo database '{mongo_dbname}'")

    client = MongoClient(get_mongo_uri())
    mongo = client[mongo_dbname]

    with psycopg.connect(**pg_conninfo) as pg:
        # clear target collections
        for table in tables:
            mongo[table].drop()

        for table in tables:
            print(f"[COPY] {table}")
            coll = mongo[table]
            with pg.cursor(name=f"cur_{table}") as cur:  # server-side cursor
                cur.itersize = 5000
                cur.execute(f"SELECT * FROM {table}")  # type: ignore[arg-type]
                col_names = [d[0] for d in cur.description]  # type: ignore[index]
                while (rows := cur.fetchmany(5000)):
                    docs = [pg_row_to_bson(col_names, r) for r in rows]
                    if docs:
                        coll.insert_many(docs, ordered=False)

    client.close()
    print("[OK] Raw copy complete")


# ----------------------------
# Build: structure Mongo from raw
# ----------------------------

def build_structured(raw_dbname: str, structured_dbname: str):
    print(f"[INFO] Structuring '{raw_dbname}' -> '{structured_dbname}'")

    client = MongoClient(get_mongo_uri())
    raw = client[raw_dbname]
    structured = client[structured_dbname]

    # reset target collections
    for coll in ["selskap", "person", "eierskap", "aksjeeiebok", "politikere"]:
        structured[coll].drop()

    _build_selskap(raw, structured)
    _build_person(raw, structured)
    _build_eierskap(raw, structured)
    _build_aksjeeiebok(raw, structured)
    _build_politikere(raw, structured)

    client.close()
    print("[OK] Structured build complete")


def _build_selskap(raw, structured):
    src = raw["selskap"]
    dst = structured["selskap"]

    bulk = []
    for s in src.find({}):
        bulk.append({
            "orgnr": s.get("orgnr"),
            "uuid": s.get("uuid"),
            "navn": s.get("navn"),
            "organisasjonstype": s.get("organisasjonstype"),
            "nacekode": s.get("nacekode"),
            "etablertdato": s.get("etablertdato"),
            "oppløstdato": s.get("oppløstdato"),
            "konkursflagg": s.get("konkursflagg"),
            "likvidasjonflagg": s.get("likvidasjonflagg"),
        })
        if len(bulk) >= 1000:
            dst.insert_many(bulk)
            bulk = []
    if bulk:
        dst.insert_many(bulk)


def _build_person(raw, structured):
    src = raw["person"]
    dst = structured["person"]

    dst.create_index([("uuid", ASCENDING)], unique=True)

    grouped = {}
    for p in src.find({}):
        uuid = p.get("uuid")
        if not uuid:
            continue
        if uuid not in grouped:
            grouped[uuid] = {
                "uuid": uuid,
                "navn": p.get("navn"),
                "foedselsdato": p.get("fødselsdato"),
                "foedselsaar": p.get("fødselsår"),
                "kjonnuuid": p.get("kjønnuuid"),
                "adresse": {
                    "adresse": p.get("adresse"),
                    "postnummer": p.get("postnummer"),
                    "poststed": p.get("poststed"),
                    "land": p.get("land"),
                    "landkode": p.get("landkode"),
                },
                "kommune": {
                    "nr": p.get("kommunenr"),
                    "navn": p.get("kommunenavn"),
                },
                "roles": [],
            }
        grouped[uuid]["roles"].append({
            "orgnr": p.get("selskaporgnr"),
            "selskapuuid": p.get("selskapuuid"),
            "selskapnavn": p.get("selskapnavn"),
            "rolle": p.get("selskaprolle"),
            "rolleuuid": p.get("rolleuuid"),
            "rolleregistrert": p.get("rolleregistrert"),
            "rolleoppdatert": p.get("rolleoppdatert"),
            "rollestartdato": p.get("rollestartdato"),
            "rollesluttdato": p.get("rollesluttdato"),
            "rollerang": p.get("selskaprollerang"),
        })
    if grouped:
        dst.insert_many(grouped.values())


def _build_eierskap(raw, structured):
    src = raw["eierskap"]
    dst = structured["eierskap"]

    dst.create_index([("company.orgnr", ASCENDING)])
    dst.create_index([("owner.uuid", ASCENDING)])

    bulk = []
    for e in src.find({}):
        bulk.append({
            "ownership_uuid": e.get("eierskapuuid"),
            "year": e.get("eierskapår"),
            "owner": {
                "uuid": e.get("eierpersonuuid"),
                "navn": e.get("eierpersonnavn"),
                "foedselsdato": e.get("eierpersonfødselsdato"),
                "foedselsaar": e.get("eierpersonfødselsår"),
                "adresse": e.get("eierpersonadresse"),
                "postkode": e.get("eierpersonpostkode"),
                "poststed": e.get("eierpersonpoststed"),
                "kommunenr": e.get("eierpersonkommunenr"),
                "kommune": e.get("eierpersonkommune"),
            },
            "company": {
                "orgnr": e.get("utstederorgnr"),
                "uuid": e.get("utstederuuid"),
                "navn": e.get("utstedernavn"),
            },
            "andel": e.get("eierskapandel"),
            "antall": e.get("eierskapantall"),
            "stemmeandel": e.get("eierskapstemmeandel"),
            "totalantall": e.get("eierskaptotalantall"),
            "stemmeantall": e.get("eierskapstemmeantall"),
            "totalsstemmeantall": e.get("eierskaptotalstemmeantall"),
        })
        if len(bulk) >= 2000:
            dst.insert_many(bulk)
            bulk = []
    if bulk:
        dst.insert_many(bulk)


def _build_aksjeeiebok(raw, structured):
    src = raw["aksjeeiebok"]
    dst = structured["aksjeeiebok"]

    dst.create_index([("orgnr", ASCENDING)])
    dst.create_index([("år", ASCENDING)])

    bulk = []
    for a in src.find({}):
        bulk.append({
            "orgnr": a.get("orgnr"),
            "selskap": a.get("selskap"),
            "år": a.get("år"),
            "aksjeklasse": a.get("aksjeklasse"),
            "aksjonær": {
                "navn": a.get("aksjonærnavn"),
                "nr": a.get("aksjonærnr"),
                "poststed": a.get("poststed"),
                "landkode": a.get("landkode"),
            },
            "antallaksjer": a.get("antallaksjer"),
            "antallaksjerselskap": a.get("antallaksjerselskap"),
        })
        if len(bulk) >= 2000:
            dst.insert_many(bulk)
            bulk = []
    if bulk:
        dst.insert_many(bulk)


def _build_politikere(raw, structured):
    src = raw["politikere"]
    dst = structured["politikere"]

    dst.create_index([("navn", ASCENDING)])
    dst.create_index([("parti", ASCENDING)])
    dst.create_index([("kommunenr", ASCENDING)])

    docs = []
    for p in src.find({}):
        docs.append({
            "navn": p.get("navn"),
            "parti": p.get("parti"),
            "kommunenr": p.get("kommunenr"),
            "kommune": p.get("kommune"),
            "foedselsdato": p.get("fødselsdato"),
            "listeplass": p.get("listeplass"),
            "stemmetillegg": p.get("stemmetillegg"),
            "personstemmer": p.get("persontemmer"),
            "slengere": p.get("slengere"),
            "endeligrangering": p.get("endeligrangering"),
            "innvalgt": p.get("innvalgt"),
        })
    if docs:
        dst.insert_many(docs)


# ----------------------------
# Orchestration
# ----------------------------

def build(version: str, mode: str) -> bool:
    cfg = load_version(version)
    if not cfg:
        return False

    source_pg = cfg["source_postgres"]
    target_copy = cfg["target_mongo_copy"]
    target_structured = cfg["target_mongo_structured"]
    tables = cfg.get("tables", [])

    print(f"[INFO] Using Postgres '{source_pg}', mode='{mode}'")

    if mode == "copy":
        build_copy(get_pg_conninfo(source_pg), target_copy, tables)
    elif mode == "structured":
        build_structured(target_copy, target_structured)
    elif mode == "all":
        build_copy(get_pg_conninfo(source_pg), target_copy, tables)
        build_structured(target_copy, target_structured)
    else:
        print("[ERROR] Unknown mode. Use 'copy', 'structured', or 'all'.")
        return False

    return True
