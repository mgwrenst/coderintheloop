from providers.openai_provider import OpenAIProvider
from pipeline.prompt_builder import PromptBuilder
from models.schema_builder import SchemaBuilder
from mongo.inspector import MongoInspector
from typing import Optional

class QueryPipeline:
    def __init__(
        self, api_key,
        use_base_prompt=True,
        include_db_schema=False,
        db_uri=None,
        db_name=None
    ):
        self.llm = OpenAIProvider(api_key)
        self.use_base = use_base_prompt
        self.include_schema = include_db_schema

        self.schema = None
        if include_db_schema and db_uri and db_name:
            inspector = MongoInspector(db_uri, db_name)
            raw_schema = inspector.get_schema_overview()
            self.schema = SchemaBuilder.build_human_readable(raw_schema)

    def run(self, user_question: str, manual_schema: Optional[str] = None):
        builder = PromptBuilder(
            use_base=self.use_base,
            include_schema=self.include_schema
        )

        schema_to_use = self.schema if self.schema else manual_schema

        final_prompt = builder.build(
            question=user_question,
            schema=schema_to_use
        )

        messages = [
            {"role": "user", "content": final_prompt}
        ]

        content = self.llm.generate(messages)

        return content
