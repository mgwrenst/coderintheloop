import json
import os
from dotenv import load_dotenv
from pymongo import MongoClient

from mongo_executor import execute_query
from models.query_generator import QueryGenerator
from config.prompts import BASE_PROMPT


def main():
    load_dotenv()

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set.")
        return

    # User describes the database schema
    user_schema = input("\nBeskriv databasen din (skjema) i naturlig språk:\n> ")

    # User writes a natural language question
    user_question = input("\nSkriv spørsmålet ditt på norsk:\n> ")

    # Instantiate generator
    generator = QueryGenerator(api_key=api_key)

    # Generate query using LLM
    output = generator.generate(user_schema, user_question)

    print("\n=== LLM-GENERERT QUERY ===")
    print(json.dumps(output, indent=4))

    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017")
    db = client["groundtruthsmall_copy"]   # <-- change if necessary

    # Execute generated query
    print("\n=== RESULTAT FRA MONGO ===")
    try:
        results = execute_query(db, output)
        for r in results:
            print(r)
    except Exception as e:
        print(f"\nFEIL VED KJØRING AV MONGO-SPØRRING: {e}")


if __name__ == "__main__":
    main()
