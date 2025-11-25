from config.prompts import BASE_PROMPT, USER_PROMPT_TEMPLATE, SCHEMA_PROMPT_TEMPLATE
from typing import Optional

class PromptBuilder:
    def __init__(self, use_base=False, include_schema=False):
        self.use_base = use_base
        self.include_schema = include_schema

    def build(self, question: str, schema: Optional[str] = None):
        parts = []

        if self.use_base:
            parts.append(BASE_PROMPT)

        if self.include_schema and schema:
            parts.append(SCHEMA_PROMPT_TEMPLATE.format(schema=schema))

        parts.append(USER_PROMPT_TEMPLATE.format(question=question))

        return "\n".join(parts)
