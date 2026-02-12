# Baseball Eval Flow

ETL and prediction pipeline for baseball player performance metrics

## Core features

Pulls player-level performance data from the MLB's Statcast API, validates and regularizes data inputs, and uses resulting data set to make next-game performance predictions using machine learning 

### Technology stack
- [Apace Airflow](https://airflow.apache.org/) (ETL)
- [PostgreSQL](https://www.postgresql.org/) (Database)
- [Scikit-Learn](https://pypi.org/project/scikit-learn/) (Predictive modeling)

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