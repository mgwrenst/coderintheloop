import os
from dotenv import load_dotenv
from pipeline.query_pipeline import QueryPipeline

def main():
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")

    print("Velg modus:")
    print("1) Kun brukerinput")
    print("2) Base-prompt + brukerinput")
    print("3) Base-prompt + skjema (manuelt)")
    print("4) Base-prompt + automatisk MongoDB-skjema")

    mode = int(input("Valg: "))

    question = input("\nSkriv spørsmålet ditt:\n> ")

    # --- MODE LOGIC ---
    pipeline = None
    manual_schema = None

    if mode == 1:
        pipeline = QueryPipeline(api_key, use_base_prompt=False)

    elif mode == 2:
        pipeline = QueryPipeline(api_key, use_base_prompt=True)

    elif mode == 3:
        manual_schema = input("Lim inn databaseskjema:\n> ")
        pipeline = QueryPipeline(api_key, use_base_prompt=True, include_db_schema=False)

    elif mode == 4:
        uri = input("MongoDB URI:\n> ")
        db = input("Database-navn:\n> ")
        pipeline = QueryPipeline(
            api_key,
            use_base_prompt=True,
            include_db_schema=True,
            db_uri=uri,
            db_name=db
    )

    else:
        print("\nUgyldig valg. Velg et tall fra 1 til 4.")
        return

    result = pipeline.run(question, manual_schema=manual_schema)
    print("\n=== LLM OUTPUT ===")
    print(result)


if __name__ == "__main__":
    main()
