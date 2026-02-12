# Baseball Eval Flow

ETL flow for baseball ML training data

## Development and testing

Use the project virtual environment for all Python package installs and test runs:

```bash
# Activate venv (use .venv or venv, whichever you use)
source .venv/bin/activate   # or: source venv/bin/activate

# Install dependencies (including dev/test)
pip install -r requirements.txt

# Run tests
pytest tests/ -v
```

To install only new packages (e.g. after adding to `requirements.txt`) without touching the rest of the system:

```bash
.venv/bin/pip install -r requirements.txt
```