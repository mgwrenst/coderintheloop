import sys
from pathlib import Path
from main import generate_query, execute_query, save_query


def main():
    if len(sys.argv) < 2:
        print("Usage: python from_file.py <prompt_file>")
        print("Example: python from_file.py prompts/example.txt")
        return
    
    prompt_file = Path(sys.argv[1])
    
    if not prompt_file.exists():
        prompt_file = Path(__file__).parent / sys.argv[1]
    
    if not prompt_file.exists():
        print(f"[ERROR] File not found: {sys.argv[1]}")
        return
    
    print(f"[INFO] Reading from {prompt_file}")
    
    content = prompt_file.read_text(encoding="utf-8").strip()
    
    parts = content.split("---")
    
    if len(parts) != 2:
        print("[ERROR] File must contain database description and question separated by '---'")
        print("Format:")
        print("Database description here")
        print("---")
        print("Question here")
        return
    
    db_description = parts[0].strip()
    question = parts[1].strip()
    
    print()
    print("[DATABASE DESCRIPTION]")
    print(db_description)
    print()
    print("[QUESTION]")
    print(question)
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
    save_query(generated, result, mode="from_file")


if __name__ == "__main__":
    main()
