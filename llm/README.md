# LLM Query Generator

Generate MongoDB queries from natural language using OpenAI. Two simple modes: manual input or from file.

## Structure

```
llm/
├── manual.py              # Interactive mode: type description + question
├── from_file.py           # File mode: read from text file
├── auto_inspect.py        # Auto-inspect mode: connect to DB and inspect schema
├── main.py                # Core: generate_query, execute_query, save_query
├── baseline_prompt.txt    # Base prompt with Norwegian instructions
├── config.yaml.example    # Optional API key and Mongo URI config
├── prompts/               # Example prompt files
│   ├── example.txt
│   └── simple_example.txt
└── results/               # Generated queries saved as JSON
    ├── manual/            # Results from manual mode
    ├── from_file/         # Results from file mode
    └── auto_inspect/      # Results from auto-inspect mode
```

## Setup

Set environment variables or copy `config.yaml.example` to `config.yaml`:
- `OPENAI_API_KEY`
- `MONGO_URI` (defaults to `mongodb://localhost:27017`)

## Usage

### Manual Mode
Type database description and question interactively:

```powershell
python llm/manual.py
```

You'll be prompted for:
1. Database description (collections and fields)
2. Your question
3. Whether to execute the query (optional)

Results saved to `results/manual/`

### File Mode
Read database description and question from a text file:

```powershell
python llm/from_file.py prompts/simple_example.txt
```

File format:
```
Database description here
---
Question here
```

Example files are provided in `prompts/` folder.

Results saved to `results/from_file/`

### Auto-Inspect Mode
Connect to a MongoDB database and automatically inspect its schema:

```powershell
python llm/auto_inspect.py
```

You'll be prompted for:
1. MongoDB URI (e.g., `mongodb://localhost:27017`)
2. Database name
3. The system inspects all collections and fields
4. You enter your question
5. Query is generated using the inspected schema
6. Optionally execute the query

Results saved to `results/auto_inspect/`

## How It Works

1. Baseline prompt (`baseline_prompt.txt`) contains instructions in Norwegian for the LLM
2. User provides database description + question (manual or from file)
3. System combines baseline + description + question and sends to OpenAI
4. Generated query is displayed and saved to `results/` folder
5. Optionally execute query against MongoDB database

## Results

Generated queries are saved as timestamped JSON files in `results/`:
- Format: `YYYYMMDD_HHMMSS.json`
- Contains only the generated MongoDB query JSON
- Simple and clean for easy comparison

## Baseline Prompt

Edit `baseline_prompt.txt` to modify:
- Language (currently Norwegian)
- Query format requirements
- Generation instructions
- Model behavior

The baseline uses placeholders:
- `{db_description}` - Replaced with database description
- `{question}` - Replaced with user question
