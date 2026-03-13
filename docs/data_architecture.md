# Baseball Eval Flow — Data Architecture

One-page view of data flow: **sources → pipelines → database**.

---

## Diagram

```mermaid
flowchart TB
  subgraph external["External sources"]
    api["MLB Stats API (statsapi.mlb.com)"]
  end

  subgraph elt["mlb_player_stats_pipeline — 2:00 UTC (ELT)"]
    E1[Check MLB data ready]
    E2[Extract schedule · Validate]
    E3[Fetch boxscores]
    E4[Load to staging tables]
    E5[dbt seed + dbt run]
    E6[Record audit]
    E1 --> E2 --> E3 --> E4 --> E5 --> E6
  end

  subgraph ml["ml_predictions_pipeline — 6:00 UTC"]
    M1[Check freshness + rolling stats]
    M2[Today's schedule]
    M3[Train batter wOBA · pitcher FIP]
    M4[Generate predictions]
    M5[Load predictions · audit]
    M1 --> M2 --> M3 --> M4 --> M5
  end

  subgraph postgres["PostgreSQL"]
    staging[staging_schedule, staging_player_stats]
    dims[Dimensions: dim_player, dim_team, dim_game, dim_date, dim_stat_constants]
    fact[fact_game_state]
    rolling[player_rolling_stats]
    preds[predictions]
    audit[pipeline_load_audit]
  end

  api -->|schedule, boxscore| E1
  api -.->|today's schedule| M2
  E4 --> staging
  E5 --> dims
  E5 --> fact
  E5 --> rolling
  E6 --> audit
  M4 --> preds
  M5 --> audit
  dims --> M3
  fact --> M3
  rolling --> M4
```

---

## Flow summary

| Layer | Description |
|--------|-------------|
| **External** | MLB Stats API (schedule + boxscore per game). |
| **ELT (2:00 UTC)** | Sensor → extract yesterday’s games → validate → fetch boxscores → load raw data into staging tables → dbt seed + dbt run (dims, fact, rolling stats) → record load audit. |
| **ML (6:00 UTC)** | Check upstream freshness and rolling stats → get today’s schedule → train batter (wOBA) and pitcher (FIP) models → generate predictions → load into `predictions` and record audit. |
| **PostgreSQL** | Staging tables, star schema (dimensions, `fact_game_state`), `player_rolling_stats`, `predictions`, and `pipeline_load_audit` for dependencies and freshness. |

---

For a **printable one-page visual**, open [data_architecture.html](data_architecture.html) in a browser and use **File → Print → Save as PDF**.
