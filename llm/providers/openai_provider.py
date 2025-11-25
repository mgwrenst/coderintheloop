from openai import OpenAI

class OpenAIProvider:
    def __init__(self, api_key, model="gpt-5-nano", temperature=0):
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.temperature = temperature

    def generate(self, messages: list):
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
        )
        return response.choices[0].message.content
