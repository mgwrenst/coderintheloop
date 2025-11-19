import json
import re
from openai import OpenAI
from config.prompts import BASE_PROMPT

class QueryGenerator:
    def __init__(self, api_key):
        self.client = OpenAI(api_key=api_key)

    def generate(self, user_schema: str, user_question: str):
        prompt = f"""
{BASE_PROMPT}

SKJEMA (fra bruker):
{user_schema}

BRUKERSPØRSMÅL:
{user_question}

Svar KUN med JSON.
"""

        response = self.client.responses.create(
            model="gpt-4o-mini",
            input=prompt
        )

        # Extract the raw text from the model
        text = response.output_text.strip()

        # Remove ```json ... ``` or ``` ... ```
        text = re.sub(r"```json", "", text)
        text = re.sub(r"```", "", text)

        # Remove any text before the first '{' and after the last '}'
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise ValueError(f"Model did not return valid JSON: {text}")

        cleaned = match.group(0).strip()

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            raise ValueError(f"Cleaned output is not valid JSON:\n{cleaned}")
