from main import generate_query, execute_query, save_query


def main():
    print("[INFO] Manual prompt mode")
    print()
    
    print("Enter database description:")
    db_description = input("> ").strip()
    print()
    
    if not db_description:
        print("[ERROR] Database description required")
        return
    
    print("Enter your question:")
    question = input("> ").strip()
    print()
    
    if not question:
        print("[ERROR] Question required")
        return
    
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
        db_name = input("Database name: ").strip()
        if db_name:
            result = execute_query(generated, db_name)
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
    save_query(generated, result, mode="manual")


if __name__ == "__main__":
    main()
