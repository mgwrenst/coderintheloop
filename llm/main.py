import os
import json
from datetime import datetime
from pathlib import Path
from openai import OpenAI
from pymongo import MongoClient


def get_mongo_uri():
    uri = os.getenv("MONGO_URI")
    if uri:
        return uri
    cfg_path = Path(__file__).parent / "config.yaml"
    if cfg_path.exists():
        import yaml
        cfg = yaml.safe_load(cfg_path.read_text())
        uri = (cfg or {}).get("mongo_uri")
        if uri:
            return uri
    return "mongodb://localhost:27017"


def get_openai_key():
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        cfg_path = Path(__file__).parent / "config.yaml"
        if cfg_path.exists():
            import yaml
            cfg = yaml.safe_load(cfg_path.read_text())
            key = (cfg or {}).get("openai_api_key")
    return key


def load_baseline_prompt():
    baseline_file = Path(__file__).parent / "baseline_prompt.txt"
    if not baseline_file.exists():
        print(f"[ERROR] Baseline prompt not found: {baseline_file}")
        return None
    return baseline_file.read_text(encoding="utf-8")


def generate_query(db_description: str, question: str, model: str = "gpt-4o-mini"):
    baseline = load_baseline_prompt()
    if not baseline:
        return None
    
    prompt = baseline.replace("{db_description}", db_description)
    prompt = prompt.replace("{question}", question)
    
    api_key = get_openai_key()
    if not api_key:
        print("[ERROR] No OpenAI API key found")
        return None
    
    client = OpenAI(api_key=api_key)
    
    print("[INFO] Generating query")
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    
    return response.choices[0].message.content


def execute_query(query_json: str, db_name: str, mongo_uri: str = None):
    try:
        query = json.loads(query_json)
    except json.JSONDecodeError as e:
        return {"error": f"Invalid JSON: {e}"}
    
    collection = query.get("collection")
    operation = query.get("operation", "find")
    
    if not collection:
        return {"error": "No collection specified"}
    
    uri = mongo_uri if mongo_uri else get_mongo_uri()
    client = MongoClient(uri)
    db = client[db_name]
    coll = db[collection]
    
    print(f"[INFO] Executing {operation} on '{collection}'")
    
    results = []
    if operation == "find":
        filter_doc = query.get("filter", {})
        projection = query.get("projection")
        limit = query.get("limit", 100)
        cursor = coll.find(filter_doc, projection).limit(limit)
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)
    elif operation == "aggregate":
        pipeline = query.get("pipeline", [])
        cursor = coll.aggregate(pipeline)
        for doc in cursor:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
            results.append(doc)
    else:
        results = {"error": f"Unsupported operation: {operation}"}
    
    client.close()
    return results


def inspect_database(mongo_uri: str, db_name: str):
    print("[INFO] Inspecting database")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    
    db_info = {
        "database": db_name,
        "collections": []
    }
    
    for coll_name in db.list_collection_names():
        coll = db[coll_name]
        
        sample_docs = list(coll.find().limit(5))
        
        fields = {}
        for doc in sample_docs:
            for key, value in doc.items():
                if key not in fields:
                    fields[key] = type(value).__name__
        
        coll_info = {
            "name": coll_name,
            "count": coll.count_documents({}),
            "fields": fields,
            "sample": sample_docs[0] if sample_docs else None
        }
        
        db_info["collections"].append(coll_info)
    
    client.close()
    return db_info


def format_db_description(db_info: dict) -> str:
    lines = [f"Databasen heter '{db_info['database']}' og inneholder følgende data:\n"]
    
    for coll in db_info["collections"]:
        lines.append(f"Tabell '{coll['name']}' med {coll['count']} dokumenter:")
        lines.append("Felter:")
        for field, field_type in coll["fields"].items():
            lines.append(f"  - {field}: {field_type}")
        lines.append("")
    
    return "\n".join(lines)


def save_query(generated_query: str, execution_result=None, mode: str = "manual"):
    results_dir = Path(__file__).parent / "results" / mode
    results_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}.json"
    
    filepath = results_dir / filename
    
    if execution_result is not None:
        data = {
            "query": json.loads(generated_query) if generated_query.strip().startswith("{") else generated_query,
            "result": execution_result
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    else:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(generated_query)
    
    print(f"[INFO] Saved to {filepath}")
