from main import inspect_database, format_db_description, generate_query, execute_query, save_query


def main():
    print("[INFO] Auto-inspect mode: Database schema inspection")
    print()
    
    mongo_uri = input("Enter MongoDB URI: ").strip()
    if not mongo_uri:
        print("[ERROR] MongoDB URI required")
        return
    
    db_name = input("Enter database name: ").strip()
    if not db_name:
        print("[ERROR] Database name required")
        return
    
    print()
    print("[INFO] Inspecting database...")
    
    db_info = inspect_database(mongo_uri, db_name)
    db_description = format_db_description(db_info)
    
    print()
    print("[DATABASE SCHEMA]")
    print(db_description)
    print()
    
    question = input("What data are you interested in?\n> ").strip()
    if not question:
        print("[ERROR] Question required")
        return
    
    print()
    
    generated = generate_query(db_description, question)
    
    if not generated:
        print("[ERROR] Failed to generate query")
        return
    
    print("[GENERATED QUERY]")
    print(generated)
    print()
    
    execute = input("Execute query? (y/n): ").strip().lower()
    
    result = None
    if execute == "y":
        result = execute_query(generated, db_name, mongo_uri)
        print()
        print("[RESULT]")
        if isinstance(result, list):
            print(f"Found {len(result)} documents")
            for doc in result[:5]:
                print(doc)
            if len(result) > 5:
                print(f"... and {len(result) - 5} more")
        else:
            print(result)
    
    print()
    save_query(generated, result, mode="auto_inspect")


if __name__ == "__main__":
    main()
