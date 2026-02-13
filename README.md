# Baseball Eval Flow

ETL and prediction pipeline for baseball player performance metrics

## Core features

Pulls player-level performance data from the MLB's Statcast API, validates and regularizes data inputs, and uses resulting data set to make next-game performance predictions using machine learning 

### Technology stack
- [Apace Airflow](https://airflow.apache.org/) (ETL)
- [PostgreSQL](https://www.postgresql.org/) (Database)
- [Scikit-Learn](https://pypi.org/project/scikit-learn/) (Predictive modeling)

### Data architecture

A one-page visual overview of the data flow (sources → pipelines → database):

- **[docs/data_architecture.html](docs/data_architecture.html)** — Open in a browser to view the diagram and SVG; use **File → Print → Save as PDF** for a shareable copy.
- **[docs/data_architecture.md](docs/data_architecture.md)** — Mermaid diagram and summary; renders on GitHub and can be exported from Mermaid Live or your editor.

## Development and testing

### Development

Run the project in development using Docker Compose. 

**Warning:** The Airflow Docker distribution can be extremely resource intensive. You'll want to allocate at least 8GB worth of memory to Docker before running the application locally

```bash
docker compose up --build
```

### Testing

Use a Python virtual environment for all Python package installs and test runs:

```bash
# Activate venv (use .venv or venv, whichever you use)
source .venv/bin/activate   # or: source venv/bin/activate

# Install dependencies (including dev/test)
pip install -r requirements.txt

# Run tests
pytest tests/ -v
```