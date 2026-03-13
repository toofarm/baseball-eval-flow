# Baseball Eval Flow

ELT and prediction pipeline for baseball player performance metrics

## Core features

Pulls player-level performance data from the MLB's Statcast API, loads raw data into PostgreSQL, transforms it with dbt (including advanced metrics like wOBA, wRC+, FIP), and uses the resulting data set to make next-game performance predictions using machine learning.

### Technology stack
- [Apache Airflow](https://airflow.apache.org/) (orchestration)
- [dbt](https://www.getdbt.com/) (transform)
- [PostgreSQL](https://www.postgresql.org/) (database)
- [Scikit-Learn](https://pypi.org/project/scikit-learn/) (predictive modeling)

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

#### Database migrations

Add columns to a database post-facto by running `00a_add_staging_column.sql`

Configure the `table_name` and `column_name` values, then:

```bash
docker compose exec postgres psql -U airflow -d airflow -v ON_ERROR_STOP=1 -f /schema/00a_add_staging_column.sql
```

### dbt

The dbt project lives in `dbt/`. Run transforms locally:

```bash
cd dbt
dbt seed                    # Load stat constants
dbt run --vars '{"as_of_date": "2024-01-15"}'   # Build models (pass as_of_date for rolling stats)
```

Connection is configured via `profiles.yml` (uses env vars: `DBT_HOST`, `DBT_USER`, `DBT_PASSWORD`, `DBT_DATABASE`, `DBT_SCHEMA`).

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