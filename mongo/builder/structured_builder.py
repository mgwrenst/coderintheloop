from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING


def build_structured(pg_dbname: str, mongo_dbname: str):
    """
    Read from raw MongoDB database <pg_dbname>
    and build a structured MongoDB database <mongo_dbname>.
    """

    client = MongoClient("mongodb://localhost:27017")
    raw = client[pg_dbname]
    structured = client[mongo_dbname]

    # Drop structured collections (idempotent)
    for coll in ["selskap", "eierskap", "aksjeeiebok", "politikere"]:
        structured[coll].drop()

    print(f"⚙️ Building structured Mongo database: {mongo_dbname}")

    _build_selskap(raw, structured)
    _build_eierskap(raw, structured)
    _build_aksjeeiebok(raw, structured)
    _build_politikere(raw, structured)

    print(f"✅ Structured Mongo database '{mongo_dbname}' completed.\n")


# -------------------------------------------------------------
# 1. Structured selskap (with embedded personer)
# -------------------------------------------------------------
def _build_selskap(raw, structured):
    from collections import defaultdict
    from datetime import datetime, timezone
    from pymongo import ASCENDING

    print("→ Structuring 'selskap' (UUID-based join)...")

    all_companies = raw["alleselskaper"]  # or raw["selskap"] depending on DB
    persons_raw = raw["person"]
    out = structured["selskap"]

    # --- Map persons by selskapUUID ---
    person_roles_by_uuid = defaultdict(list)

    for p in persons_raw.find({}, {"_id": 0}):
        for comp in p.get("companies", []):
            comp_uuid = comp.get("uuid")
            if not comp_uuid:
                continue

            person_entry = {
                "uuid": p.get("uuid"),
                "name": p.get("name"),
                "roles": comp.get("roles", [])
            }
            person_roles_by_uuid[comp_uuid].append(person_entry)

    buffer = []
    batch_size = 2000

    # --- Process companies ---
    for s in all_companies.find({}, {"_id": 0}):

        company_uuid = s.get("UUID")  # critical field name

        doc = {
            "company": {
                "orgnr": s.get("orgNr"),
                "uuid": company_uuid,
                "name": s.get("navn"),
                "type": s.get("organisasjonstype"),
                "nace": s.get("naceKode")
            },

            "status": {
                "bankrupt": bool(s.get("konkursFlagg")),
                "liquidated": bool(s.get("likvidasjonFlagg")),
                "dates": {
                    "founded": s.get("etablertDato"),
                    "dissolved": s.get("oppløstDato")
                }
            },

            "persons": person_roles_by_uuid.get(company_uuid, []),

            "meta": {
                "nr": s.get("nr"),
                "imported_at": datetime.now(timezone.utc)
            }
        }

        buffer.append(doc)

        if len(buffer) >= batch_size:
            out.insert_many(buffer, ordered=False)
            buffer.clear()

    if buffer:
        out.insert_many(buffer, ordered=False)

    out.create_index([("company.orgnr", ASCENDING)])
    out.create_index([("company.uuid", ASCENDING)], unique=True)
    out.create_index([("persons.uuid", ASCENDING)])

    print("✓ selskap done.")




# -------------------------------------------------------------
# 2. Structured eierskap (owner → issuer)
# -------------------------------------------------------------
def _build_eierskap(raw, structured):
    from datetime import datetime, timezone
    from pymongo import ASCENDING

    print("→ Structuring 'eierskap' (full schema, batch mode)...")

    src = raw["eierskap"]
    out = structured["eierskap"]

    buffer = []
    batch_size = 5000

    for e in src.find({}, {"_id": 0}):

        # Determine owner type
        owner_type = "person" if e.get("eierpersonuuid") else "company"

        # Calculate percentages
        share_count = e.get("eierskapantall")
        share_total = e.get("eierskaptotalantall")
        percent = None
        if share_count and share_total and share_total > 0:
            percent = share_count / share_total

        voting_count = e.get("eierskapstemmeantall")
        voting_total = e.get("eierskaptotalstemmeantall")
        voting_percent = None
        if voting_count and voting_total and voting_total > 0:
            voting_percent = voting_count / voting_total

        doc = {
            "owner": {
                "type": owner_type,
                "person": None,
                "company": None
            },

            "issuer": {
                "name": e.get("utstedernavn"),
                "uuid": e.get("utstederuuid"),
                "orgnr": e.get("utstederorgnr"),
                "company_uuid": e.get("company_share_ownership_share_issuer_company_uuid")
            },

            "ownership": {
                "uuid": e.get("eierskapuuid"),
                "year": e.get("eierskapår"),
                "share_count": share_count,
                "share_total": share_total,
                "percent": percent,
                "voting_percent": voting_percent,
                "voting_total": voting_total,
                "interval": {
                    "percent": {
                        "lower": e.get("company_share_ownership_ownership_lower"),
                        "upper": e.get("company_share_ownership_ownership_upper"),
                    },
                    "voting": {
                        "lower": e.get("company_share_ownership_voting_ownership_lower"),
                        "upper": e.get("company_share_ownership_voting_ownership_upper"),
                    }
                }
            },

            "meta": {
                "source": "eierskap",
                "imported_at": datetime.now(timezone.utc),
                "nr": e.get("nr"),
                "aksjonær_string": e.get("eierskapaksjonær"),
            }
        }

        # Owner details
        if owner_type == "person":
            doc["owner"]["person"] = {
                "uuid": e.get("eierpersonuuid"),
                "name": e.get("eierpersonnavn"),
                "birth": {
                    "date": e.get("eierpersonfødselsdato"),
                    "year": e.get("eierpersonfødselsår"),
                    "day": e.get("shareholder_person_birth_day"),
                    "month": e.get("shareholder_person_birth_month"),
                },
                "gender_uuid": e.get("eierpersonkjønnuuid"),
                "address": {
                    "adresse": e.get("eierpersonadresse"),
                    "postkode": e.get("eierpersonpostkode"),
                    "poststed": e.get("eierpersonpoststed"),
                    "kommune": {
                        "nr": e.get("eierpersonkommunenr"),
                        "navn": e.get("eierpersonkommune"),
                    }
                }
            }
        else:
            doc["owner"]["company"] = {
                "name": e.get("eierselskapnavn"),
                "uuid": e.get("eierselskapuuid"),
                "orgnr": e.get("eierselskaporgnr")
            }

        buffer.append(doc)

        if len(buffer) >= batch_size:
            out.insert_many(buffer, ordered=False)
            buffer.clear()

    if buffer:
        out.insert_many(buffer, ordered=False)

    # Indexing
    out.create_index([("owner.person.uuid", ASCENDING)])
    out.create_index([("owner.company.orgnr", ASCENDING)])
    out.create_index([("issuer.orgnr", ASCENDING)])
    out.create_index([("ownership.year", ASCENDING)])
    out.create_index([("ownership.percent", ASCENDING)])

    print("✓ eierskap done.")



# -------------------------------------------------------------
# 3. Structured aksjeeiebok (shareholder → company)
# -------------------------------------------------------------
def _build_aksjeeiebok(raw, structured):
    from datetime import datetime, timezone
    from pymongo import ASCENDING

    print("→ Structuring 'aksjeeiebok' (full schema, batch mode)...")

    src = raw["aksjeeiebok"]
    out = structured["aksjeeiebok"]

    buffer = []
    batch_size = 5000

    for a in src.find({}, {"_id": 0}):

        antall = a.get("antallaksjer")
        total = a.get("antallaksjerselskap")
        pct = None
        if isinstance(antall, (int, float)) and isinstance(total, (int, float)) and total > 0:
            pct = antall / total

        doc = {
            "company": {
                "orgnr": a.get("orgnr"),
                "name": a.get("selskap"),
                "share_class": a.get("aksjeklasse"),
            },
            "shareholder": {
                "name": a.get("aksjonærnavn"),
                "id_number": a.get("aksjonærnr"),
                "location": {
                    "poststed": a.get("poststed"),
                    "landkode": a.get("landkode"),
                }
            },
            "holdings": {
                "count": antall,
                "company_total": total,
                "ownership_pct": pct,
            },
            "year": a.get("år"),
            "meta": {
                "source": "aksjeeiebok",
                "imported_at": datetime.now(timezone.utc),
            }
        }

        buffer.append(doc)

        if len(buffer) >= batch_size:
            out.insert_many(buffer, ordered=False)
            buffer.clear()

    if buffer:
        out.insert_many(buffer, ordered=False)

    out.create_index([("company.orgnr", ASCENDING)])
    out.create_index([("shareholder.name", ASCENDING)])
    out.create_index([("year", ASCENDING)])
    out.create_index([("holdings.ownership_pct", ASCENDING)])

    print("✓ aksjeeiebok done.")



# -------------------------------------------------------------
# 4. Structured politikere
# -------------------------------------------------------------
def _build_politikere(raw, structured):
    from datetime import datetime, timezone
    from pymongo import ASCENDING

    print("→ Structuring 'politikere'...")

    src = raw["politikere"]
    out = structured["politikere"]

    buffer = []
    batch_size = 2000

    for p in src.find({}, {"_id": 0}):

        doc = {
            "name": p.get("navn"),
            "birthdate": p.get("fødselsdato"),

            "party": p.get("parti"),

            "municipality": {
                "nr": p.get("kommunenr"),
                "name": p.get("kommune")
            },

            "ballot": {
                "position": p.get("listeplass"),
                "final_rank": p.get("endeligrangering"),
                "vote_bonus": (p.get("stemmetillegg") == "Ja")
            },

            "votes": {
                "personal": p.get("personstemmer"),
                "stray": p.get("slengere")
            },

            "elected": (p.get("innvalgt") == "Ja"),

            "meta": {
                "imported_at": datetime.now(timezone.utc)
            }
        }

        buffer.append(doc)

        if len(buffer) >= batch_size:
            out.insert_many(buffer, ordered=False)
            buffer.clear()

    if buffer:
        out.insert_many(buffer, ordered=False)

    out.create_index([("name", ASCENDING)])
    out.create_index([("party", ASCENDING)])
    out.create_index([("municipality.nr", ASCENDING)])
    out.create_index([("elected", ASCENDING)])

    print("✓ politikere done.")

