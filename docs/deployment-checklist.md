# Production deployment checklist (DigitalOcean + Docker Compose)

Use this when bringing up the stack on a droplet or validating a release. Airflow’s upstream docs recommend Kubernetes for large production deployments; this project intentionally runs **Docker Compose** on a single host with occasional downtime acceptable.

## 1. GitHub and CI

- [ ] **Packages:** For GHCR, allow GitHub Actions to publish (`Settings` → `Actions` → `General` → workflow read/write as needed; first push may require accepting the `ghcr.io` package for the repo).
- [ ] **Workflow:** Pushes to **`main`** build and push **`Dockerfile.prod`** (see [.github/workflows/production-image.yml](../.github/workflows/production-image.yml)). Tags: `:main` and a tag equal to the commit SHA.
- [ ] **Image reference on droplet:** Set `AIRFLOW_IMAGE_NAME` in the server `.env` to the lowercase GHCR image, e.g. `ghcr.io/<owner>/baseball-eval-flow:main`.

## 2. Droplet

- [X] Ubuntu LTS, **≥ 8 GB RAM** recommended for this stack.
- [X] Install **Docker Engine** and **Docker Compose v2** (`docker compose`, not legacy `docker-compose`). See [DigitalOcean Docker Compose on Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-22-04) and [Docker’s Compose install docs](https://docs.docker.com/compose/install/).
- [X] Non-root sudo user, SSH keys, firewall (**UFW**): allow **22**, **80**, **443**; deny public access to Postgres and Airflow’s internal API port.

## 3. Repo layout on the server

- [X] Clone this repository (or copy at least `docker-compose.yaml`, `docker-compose.prod.yaml`, `deploy/Caddyfile`, and `schema/`). **Postgres schema init** bind-mounts `./schema`; keep paths aligned with `AIRFLOW_PROJ_DIR` / project root when you run Compose.

## 4. Environment file (do not commit)

Create `.env` in the project root (same directory as the compose files). Restrict with `chmod 600 .env`.

Required / strongly recommended:

| Variable | Notes |
|----------|--------|
| `AIRFLOW_IMAGE_NAME` | GHCR image + tag, e.g. `ghcr.io/org/baseball-eval-flow:main` |
| `AIRFLOW__CORE__FERNET_KEY` | Stable Fernet key ([Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets.html#fernet-key)); changing it invalidates encrypted secrets |
| `POSTGRES_PASSWORD` | Strong password; must match URLs below |
| `POSTGRES_USER` / `POSTGRES_DB` | If changed from defaults, keep consistent in any custom URLs |
| `_AIRFLOW_WWW_USER_PASSWORD` | Fab UI admin password |
| `AIRFLOW_PUBLIC_HOST` | Public DNS hostname only (used by Caddy for TLS), e.g. `airflow.example.com` |
| `AIRFLOW__API__BASE_URL` | Public UI URL with scheme, e.g. `https://airflow.example.com` (must match what users open in the browser) |
| `SNOWFLAKE_*` | For dbt `--target prod` and DAG offload writes: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_SCHEMA` |
| `MLB_OFFLOAD_CONN_ID` | Optional override; defaults to `snowflake-baseball` (see `Dockerfile.prod`). Selects the Airflow connection used by both DAGs for offload writes |
| `DBT_TARGET` | Optional override; defaults to `prod` (see `Dockerfile.prod`). Selects the `dbt/profiles.yml` target used by `run_dbt_task` |

Optional: `AIRFLOW_UID` on Linux for file ownership on the `airflow-logs` volume.

## 5. TLS (Caddy)

- [X] **DNS:** `A` / `AAAA` for `AIRFLOW_PUBLIC_HOST` pointing at the droplet.
- [ ] Caddy obtains **Let’s Encrypt** certificates automatically when the hostname resolves publicly on **:80** / **:443**.
- [ ] `AIRFLOW__FAB__ENABLE_PROXY_FIX` is set to `true` in production compose so `X-Forwarded-*` headers from Caddy are honored.

Local smoke without real DNS: use a throwaway hostname and hosts file, or temporarily run without the prod override’s Caddy service (not recommended for real production).

## 6. Snowflake

- [X] Outbound **HTTPS** from droplet to Snowflake allowed.
- [X] If using **network policies** in Snowflake, allow the droplet’s egress IP.
- [X] **Airflow connection `snowflake-baseball`** created in the UI (or via `airflow connections add`) with the same credentials referenced by the `SNOWFLAKE_*` env vars. Both DAGs read this conn via `MLB_OFFLOAD_CONN_ID` (default `snowflake-baseball`) — see [src/load/connection.py](../src/load/connection.py).
- [X] Snowflake DDL for the offload tables (`staging_schedule`, `staging_player_stats`, `dim_*`, `fact_game_state`, `player_rolling_stats`, `predictions`, `pipeline_load_audit`) is in place. The repo's `schema/snowflak` directory contains DDL for application directly in Snowflake.

## 7. Start and verify

```bash
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml pull
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d
```

- [X] `curl -fsS https://<AIRFLOW_PUBLIC_HOST>/api/v2/version` (or open the UI).
- [X] Scheduler / worker health checks green (`docker compose ... ps`).
- [X] Run a small DAG smoke test.

## 8. Backups

- [X] **Postgres metadata DB**: daily `pg_dump` to an S3-compatible store (DigitalOcean Spaces). See [scripts/backup.sh](../scripts/backup.sh). Required env in the cron user's environment: `BACKUP_BUCKET`, `S3_ENDPOINT`, and `awscli` credentials with write access to that bucket only.
- [ ] **Retention**: lifecycle rule on the bucket to expire objects after 30 days (set via the Spaces UI or `aws s3api put-bucket-lifecycle-configuration`). The script does not delete old backups itself.
- [ ] **Cron**: schedule `backup.sh` daily (UTC 03:00 is a reasonable default — DAG runs at 02:00 UTC, so this captures the day's metadata):
  ```
  0 3 * * * BACKUP_BUCKET=s3://baseball-eval-backups S3_ENDPOINT=https://nyc3.digitaloceanspaces.com /home/common/opt/baseball-eval-flow/scripts/backup.sh >> /var/log/baseball-eval-backup.log 2>&1
  ```
- [ ] **Restore drill**: documented and exercised at least quarterly. Restoring loses no data only if the **Fernet key** (`AIRFLOW__CORE__FERNET_KEY`) on the new host matches the one the dump was taken under — otherwise encrypted connection secrets are unrecoverable. The Fernet key and `rsa_key.p8` are held in the operator's password manager, not in this backup pipeline.
- [ ] **Snowflake**: confirm Time Travel retention is ≥ 7 days on `baseball_eval_prod`:
  ```sql
  SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN DATABASE baseball_eval_prod;
  ```
  Snowflake's Time Travel + Fail-Safe is the right tool for warehouse-side recovery; no warehouse export needed from this host.

## 9. Deploy updates after a merge to `main`

- [X] CI finishes pushing a new `:main` image.
- [X] On the droplet: `docker compose -f docker-compose.yaml -f docker-compose.prod.yaml pull && docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d`.

Optional: add a GitHub Actions job that SSHs into the droplet and runs the same commands; store `SSH_PRIVATE_KEY`, host, and `known_hosts` in repo secrets.
