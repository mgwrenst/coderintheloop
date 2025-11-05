from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING

def structure_collections():
    client = MongoClient("mongodb://localhost:27017")
    raw = client["groundtruthsmall"]
    structured = client["groundtruth_structured"]

    # Drop existing collections
    for coll in ["selskap", "eierskap", "aksjeeiebok", "politikere"]:
        structured[coll].drop()

    print("⚙️ Structuring data into 'groundtruth_structured'...")

    # --- Structure selskap with embedded person documents ---
    selskap_coll = raw["selskap"]
    person_coll = raw["person"]
    structured_selskap = structured["selskap"]

    for selskap_doc in selskap_coll.find({}, {"_id": 0}):
        orgnr = selskap_doc.get("orgnr")
        personer_docs = []
        for p in person_coll.find({"selskaporgnr": orgnr}, {"_id": 0}):
            personer_docs.append({
                "navn": p.get("navn"),
                "rolle": p.get("selskaprolle"),
                "rolleuuid": p.get("rolleuuid"),
                "rollestartdato": p.get("rollestartdato"),
                "rollesluttdato": p.get("rollesluttdato"),
                "kommunenavn": p.get("kommunenavn")
            })
        selskap_doc["personer"] = personer_docs
        selskap_doc["oppdatert_tid"] = datetime.now(timezone.utc)
        structured_selskap.insert_one(selskap_doc)

    structured_selskap.create_index([("orgnr", ASCENDING)], unique=True)
    structured_selskap.create_index([("navn", ASCENDING)])
    structured_selskap.create_index([("personer.navn", ASCENDING)])
    print("🏢 selskap structured successfully.")

    # --- Structure eierskap ---
    structured_eierskap = structured["eierskap"]

    for e in raw["eierskap"].find({}, {"_id": 0}):
        structured_doc = {
            "eier": {
                "navn": e.get("eierpersonnavn") or e.get("eierselskapnavn"),
                "uuid": e.get("eierpersonuuid") or e.get("eierselskapuuid"),
                "type": "person" if e.get("eierpersonuuid") else "selskap",
                "poststed": e.get("eierpersonpoststed")
            },
            "utsteder": {
                "navn": e.get("utstedernavn"),
                "orgnr": e.get("utstederorgnr"),
                "uuid": e.get("utstederuuid")
            },
            "andel": e.get("eierskapandel"),
            "antall": e.get("eierskapantall"),
            "stemmeandel": e.get("eierskapstemmeandel"),
            "år": e.get("eierskapår"),
            "kilde": "eierskap"
        }
        structured_eierskap.insert_one(structured_doc)

    structured_eierskap.create_index([("eier.navn", ASCENDING)])
    structured_eierskap.create_index([("utsteder.orgnr", ASCENDING)])
    structured_eierskap.create_index([("år", ASCENDING)])
    print("🔗 eierskap structured successfully.")

    # --- Structure aksjeeiebok ---
    structured_aksjeeiebok = structured["aksjeeiebok"]

    for a in raw["aksjeeiebok"].find({}, {"_id": 0}):
        structured_doc = {
            "selskap": {
                "navn": a.get("selskap"),
                "orgnr": a.get("orgnr")
            },
            "aksjonær": {
                "navn": a.get("aksjonærnavn"),
                "poststed": a.get("poststed"),
                "landkode": a.get("landkode")
            },
            "aksjeklasse": a.get("aksjeklasse"),
            "antallaksjer": a.get("antallaksjer"),
            "antallaksjerselskap": a.get("antallaksjerselskap"),
            "år": a.get("år")
        }
        structured_aksjeeiebok.insert_one(structured_doc)

    structured_aksjeeiebok.create_index([("selskap.orgnr", ASCENDING)])
    structured_aksjeeiebok.create_index([("aksjonær.navn", ASCENDING)])
    structured_aksjeeiebok.create_index([("år", ASCENDING)])
    print("📚 aksjeeiebok structured successfully.")

    # --- Structure politikere ---
    structured_politikere = structured["politikere"]

    for p in raw["politikere"].find({}, {"_id": 0}):
        structured_doc = {
            "navn": p.get("navn"),
            "parti": p.get("parti"),
            "kommune": p.get("kommune"),
            "innvalgt": p.get("innvalgt") == "Ja",
            "fødselsdato": p.get("fødselsdato"),
            "personstemmer": p.get("personstemmer")
        }
        structured_politikere.insert_one(structured_doc)

    structured_politikere.create_index([("navn", ASCENDING)])
    structured_politikere.create_index([("parti", ASCENDING)])
    structured_politikere.create_index([("kommune", ASCENDING)])
    print("🏛️ politikere structured successfully.")

    print("✅ Structuring complete! All collections written to groundtruth_structured.")


if __name__ == "__main__":
    structure_collections()
