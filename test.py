from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

# Import the tokenizer and model
tokenizer = AutoTokenizer.from_pretrained("norallm/normistral-11b-warm")
model = AutoModelForCausalLM.from_pretrained("norallm/normistral-11b-warm").cuda().eval()

# Define zero-shot translation prompt template
prompt = """Engelsk: {0}
Bokmål:"""

# Define tokens that should end the generation (any token with a newline)
eos_token_ids = [
    token_id
    for token_id in range(tokenizer.vocab_size)
    if '\n' in tokenizer.decode([token_id])
]

# Generation function
@torch.no_grad()
def generate(text):
    text = prompt.format(text)
    input_ids = tokenizer(text, return_tensors='pt').input_ids.cuda()
    prediction = model.generate(
        input_ids,
        max_new_tokens=64,
        do_sample=False,
        eos_token_id=eos_token_ids
    )
    return tokenizer.decode(prediction[0, input_ids.size(1):]).strip()

# Example usage
generate("I'm excited to try this new Norwegian language model!")
# > Expected output: 'Jeg er spent på å prøve denne nye norske språkmodellen!'
