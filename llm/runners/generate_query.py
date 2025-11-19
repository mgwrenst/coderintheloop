from llm.models.query_generator import QueryGenerator
from llm.models.validator import QueryValidator

def run():
    with open("llm/examples/schema_description_example.txt") as f:
        schema = f.read()

    gen = QueryGenerator(api_key="YOUR_KEY", schema_text=schema)
    validator = QueryValidator()

    user_question = input("Brukerspørsmål: ")

    query = gen.generate(user_question)
    validator.validate(query)

    print("GENERERT QUERY:")
    print(query)

if __name__ == "__main__":
    run()
