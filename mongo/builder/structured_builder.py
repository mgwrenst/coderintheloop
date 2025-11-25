from datetime import datetime
from pymongo import MongoClient, ASCENDING


def build_structured(raw_dbname: str, structured_dbname: str):
    """
    Build structured MongoDB from raw database using recommended schema:
      - selskap
      - person (roles embedded)
      - eierskap
      - aksjeeiebok
      - politikere
    """

    client = MongoClient("mongodb://localhost:27017")
    raw = client[raw_dbname]
    structured = client[structured_dbname]

    # Drop existing
    for coll in ["selskap", "person", "eierskap", "aksjeeiebok", "politikere"]:
        structured[coll].drop()

    print(f"⚙️ Building structured Mongo database: {structured_dbname}")

    _build_selskap(raw, structured)
    _build_person(raw, structured)
    _build_eierskap(raw, structured)
    _build_aksjeeiebok(raw, structured)
    _build_politikere(raw, structured)

    print(f"✅ Structured Mongo database '{structured_dbname}' completed.")


# -------------------------------------------------------------
# COMPANIES  (selskap + konkurs)
# -------------------------------------------------------------
def _build_selskap(raw, structured):
    selskap_raw = raw["selskap"]
    selskap = structured["selskap"]   # <-- THIS LINE WAS MISSING

    bulk = []
    for s in selskap_raw.find({}):
        orgnr = s.get("orgnr")

        doc = {
            "orgnr": orgnr,
            "uuid": s.get("uuid"),
            "navn": s.get("navn"),
            "organisasjonstype": s.get("organisasjonstype"),
            "nacekode": s.get("nacekode"),
            "etablertdato": s.get("etablertdato"),
            "oppløstdato": s.get("oppløstdato"),
            "konkursflagg": s.get("konkursflagg"),
            "likvidasjonflagg": s.get("likvidasjonflagg")
        }

        bulk.append(doc)
        if len(bulk) >= 1000:
            selskap.insert_many(bulk)
            bulk = []

    if bulk:
        selskap.insert_many(bulk)


# -------------------------------------------------------------
# PERSONS  (group all rows by person uuid, embed roles)
# -------------------------------------------------------------
def _build_person(raw, structured):
    person_raw = raw["person"]
    person = structured["person"]

    person.create_index([("uuid", ASCENDING)], unique=True)

    grouped = {}

    for p in person_raw.find({}):
        uuid = p.get("uuid")
        if uuid is None:
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
                    "landkode": p.get("landkode")
                },
                "kommune": {
                    "nr": p.get("kommunenr"),
                    "navn": p.get("kommunenavn")
                },
                "roles": []
            }

        # Append role
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
            "rollerang": p.get("selskaprollerang")
        })

    if grouped:
        person.insert_many(grouped.values())


# -------------------------------------------------------------
# OWNERSHIPS (eierskap)
# -------------------------------------------------------------
def _build_eierskap(raw, structured):
    eierskap_raw = raw["eierskap"]
    eierskap = structured["eierskap"]

    eierskap.create_index([("company.orgnr", ASCENDING)])
    eierskap.create_index([("owner.uuid", ASCENDING)])

    bulk = []
    for e in eierskap_raw.find({}):
        doc = {
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
                "kommune": e.get("eierpersonkommune")
            },

            "company": {
                "orgnr": e.get("utstederorgnr"),
                "uuid": e.get("utstederuuid"),
                "navn": e.get("utstedernavn")
            },

            "andel": e.get("eierskapandel"),
            "antall": e.get("eierskapantall"),
            "stemmeandel": e.get("eierskapstemmeandel"),
            "totalantall": e.get("eierskaptotalantall"),
            "stemmeantall": e.get("eierskapstemmeantall"),
            "totalsstemmeantall": e.get("eierskaptotalstemmeantall")
        }

        bulk.append(doc)
        if len(bulk) >= 2000:
            eierskap.insert_many(bulk)
            bulk = []

    if bulk:
        eierskap.insert_many(bulk)


# -------------------------------------------------------------
# SHAREBOOKS (aksjeeiebok)
# -------------------------------------------------------------
def _build_aksjeeiebok(raw, structured):
    raw_book = raw["aksjeeiebok"]
    aksjeeiebok = structured["aksjeeiebok"]

    aksjeeiebok.create_index([("orgnr", ASCENDING)])
    aksjeeiebok.create_index([("år", ASCENDING)])

    bulk = []
    for a in raw_book.find({}):
        doc = {
            "orgnr": a.get("orgnr"),
            "selskap": a.get("selskap"),
            "år": a.get("år"),
            "aksjeklasse": a.get("aksjeklasse"),
            "aksjonaer": {
                "navn": a.get("aksjonærnavn"),
                "nr": a.get("aksjonærnr"),
                "poststed": a.get("poststed"),
                "landkode": a.get("landkode")
            },
            "antallaksjer": a.get("antallaksjer"),
            "antallaksjerselskap": a.get("antallaksjerselskap")
        }

        bulk.append(doc)
        if len(bulk) >= 2000:
            aksjeeiebok.insert_many(bulk)
            bulk = []

    if bulk:
        aksjeeiebok.insert_many(bulk)


# -------------------------------------------------------------
# POLITICIANS
# -------------------------------------------------------------
def _build_politikere(raw, structured):
    raw_pol = raw["politikere"]
    pol = structured["politikere"]

    pol.create_index([("navn", ASCENDING)])
    pol.create_index([("parti", ASCENDING)])
    pol.create_index([("kommunenr", ASCENDING)])

    docs = []
    for p in raw_pol.find({}):
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
            "innvalgt": p.get("innvalgt")
        })

    if docs:
        pol.insert_many(docs)
