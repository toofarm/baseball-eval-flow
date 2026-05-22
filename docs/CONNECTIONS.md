# Connections & Authentication

How each component in `baseball-eval-flow` proves its identity to the next. Use this as the first stop when an auth-related failure shows up in a task log.

## At a glance

```
                +--------------+
                | MLB Stats API|  (anonymous, public)
                +------+-------+
                       |
                       v
+-----------+   +-------+--------+   +-----------+   +-----------+
| GitHub    |   | Airflow worker |-->| Snowflake | <-| dbt CLI   |
| Actions   |-->| (in container) |   | account   |   | (subproc) |
|  (CI)     |   +-------+--------+   +-----------+   +-----------+
+-----+-----+           |                                  ^
      |                 v                                  |
      v             +-------+                              |
   GHCR image    +-->|Postgres|<-- Airflow metadata + FAB users
                    +--------+
                        ^
                        |
                      Redis  (Celery broker, no auth)
```

## Inventory

### 1. Airflow worker → Snowflake (DAG `load_staging_task`)

- **What it does**: Bulk-inserts schedule rows and player stats into `staging_schedule` / `staging_player_stats` from `src/load/`.
- **How it auths**: Airflow connection `snowflake-baseball`, looked up by `src/load/connection.py` (`MLB_OFFLOAD_CONN_ID` env, default `snowflake-baseball`).
- **Auth method**: Snowflake key-pair (no password). The connection's **Extras** holds either `private_key_content` (PEM as text) or `private_key_file` (path inside container).
- **Where configured**: Airflow UI → Admin → Connections. The `Password` field must be **empty** because the key is unencrypted; populating it triggers `TypeError: Password was given but private key is not encrypted`.
- **Secrets**: Only inside Airflow's encrypted metadata DB; nothing in `.env` for this path.

### 2. dbt → Snowflake (DAG `run_dbt_task`)

- **What it does**: `dbt seed` + `dbt run` invoked as a subprocess from `dags/mlb_player_stats_pipeline.py`.
- **How it auths**: `dbt/profiles.yml` `prod` target reads `private_key_path` from `SNOWFLAKE_PRIVATE_KEY_PATH` (default `/opt/airflow/rsa_key.p8`).
- **Auth method**: Same Snowflake key-pair as above. dbt-snowflake reads the PEM directly from the file at startup.
- **Where the key lives**: Host file at `/home/common/opt/baseball-eval-flow/rsa_key.p8`, bind-mounted read-only into the container as `/opt/airflow/rsa_key.p8` (see `docker-compose.prod.yaml` volumes block). Host file is `chown 50001:0 chmod 400` so the in-container `default` user can read it.
- **Where dbt finds its profile**: `DBT_PROFILES_DIR=/app/dbt` in `docker-compose.prod.yaml`. Without this, dbt falls back to `~/.dbt/profiles.yml` which doesn't exist.
- **Other dbt connection inputs** (all from `.env`, passed through compose `environment:`): `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_SCHEMA`.

### 3. Snowflake side — what was provisioned

- User: `baseball_eval_service`. Public key registered with `ALTER USER ... SET RSA_PUBLIC_KEY = '...';`.
- Role: `service`. Granted `USAGE` on warehouse `PC_DBT_WH`, `USAGE` + `CREATE TABLE` + `CREATE VIEW` on `baseball_eval_prod.PUBLIC`.
- The public IP of the droplet is allowed in any Snowflake network policy attached to this user.

### 4. Airflow internals → Postgres metadata DB

- **What for**: DAG state, XCom, task logs metadata, FAB user table.
- **Auth method**: Basic password.
- **Connection string**: `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` in `docker-compose.prod.yaml`, interpolated from `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` in `.env` (defaults `airflow` / `airflow` / `airflow`).
- **Note**: This Postgres is the Airflow metadata store, **not** the analytical warehouse. Real data lives in Snowflake.

### 5. Airflow web UI users

- **Auth manager**: `airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` (FAB).
- **Bootstrap user**: `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` from `.env`. Created by the `airflow-init` service on first boot.
- **Where stored**: FAB tables inside the Postgres metadata DB.

### 6. Celery worker ↔ Redis broker

- **Auth method**: None. `AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0` — empty password, internal Docker network only. Redis port is not published outside the compose network.

### 7. External HTTP edge — Caddy → Airflow API

- **TLS termination**: Caddy (`deploy/Caddyfile`), automatic Let's Encrypt cert for `AIRFLOW_PUBLIC_HOST`.
- **Upstream auth**: None at the Caddy hop; Caddy reverse-proxies to `airflow-apiserver` over the Docker network. Authentication happens at the Airflow API layer (FAB session cookies from step 5).
- **Required env**: `AIRFLOW_PUBLIC_HOST`, `AIRFLOW__API__BASE_URL` (must match the public URL; otherwise FAB login redirects break).
- **Trust the proxy**: `AIRFLOW__FAB__ENABLE_PROXY_FIX=true` so Airflow honors `X-Forwarded-Proto` / `X-Forwarded-For` from Caddy.

### 8. CI/CD — GitHub Actions → GHCR

- **Workflow**: `.github/workflows/production-image.yml`.
- **Auth**: `secrets.GITHUB_TOKEN` (auto-provisioned per run), scope `packages: write`.
- **Pushes to**: `ghcr.io/<owner>/<repo>:main` and `:<commit-sha>` (owner/repo are lowercased via `tr` in the workflow).
- **Droplet pull**: `docker compose pull` uses the image name from `.env`'s `AIRFLOW_IMAGE_NAME`. Verify this matches the GHCR path the workflow publishes — they got out of sync once in this project's history and silently masqueraded an unmodified upstream Apache image as our build. Confirm via `docker history` if in doubt.

### 9. External: MLB Stats API

- **What it is**: `statsapi.mlb.com`, the public MLB Stats API consumed by `MLB-StatsAPI` Python client (`src/extract/`).
- **Auth**: None. Anonymous HTTPS.
- **Failure mode**: Rate limiting / outage surfaces as the sensor (`check_mlb_data_readiness`) returning empty payloads; the sensor is configured to fail rather than reschedule forever.

## Quick troubleshooting reference

| Symptom | Most likely culprit | Where to look |
|---|---|---|
| `TypeError: Password was given but private key is not encrypted` | Private key pasted into the connection's Password field instead of Extras | Airflow UI → Connections → `snowflake-baseball` |
| `Could not deserialize key data ... ASN.1 parsing error` | dbt receiving base64-encoded PEM instead of decoded PEM, or wrong file path | `SNOWFLAKE_PRIVATE_KEY_PATH` env; `ls -la /opt/airflow/rsa_key.p8` inside the worker |
| `Connection test skipped since no profile was found` | dbt invoked without `DBT_PROFILES_DIR` and no `~/.dbt/profiles.yml` | `docker compose ... exec airflow-worker printenv DBT_PROFILES_DIR` |
| `Permission denied: '/app/dbt/target'` | `/app/dbt` not group-writable for `uid=50001` | `ls -la /app/dbt` — needs `drwxrwxr-x` and group=`0` |
| `FileNotFoundError: 'dbt'` from subprocess | dbt subprocess `cwd=` set to a path that doesn't exist (NOT the binary being missing) | Confirm `AIRFLOW_PROJ_DIR=/app` in the container env |
| Container running unrelated `apache/airflow` image instead of our build | `AIRFLOW_IMAGE_NAME` in `.env` points at a different GHCR path than the workflow publishes | `docker history <image>` should show our COPY layers |

## Key rotation runbook (Snowflake)

1. Generate new key pair on a trusted host:
   ```
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key_new.p8 -nocrypt
   openssl rsa -in rsa_key_new.p8 -pubout -out rsa_key_new.pub
   ```
2. In Snowflake, set the new public key (this immediately invalidates the old one if you use `SET RSA_PUBLIC_KEY` rather than `RSA_PUBLIC_KEY_2`):
   ```sql
   ALTER USER baseball_eval_service SET RSA_PUBLIC_KEY_2 = '<contents of rsa_key_new.pub minus header/footer>';
   ```
   Using `RSA_PUBLIC_KEY_2` lets you run both keys in parallel during the swap.
3. SCP `rsa_key_new.p8` to the droplet, replacing `/home/common/opt/baseball-eval-flow/rsa_key.p8` (back up the old one first). `chown 50001:0`, `chmod 400`.
4. Update the Airflow connection's Extras `private_key_file` (or paste new content into `private_key_content`).
5. Trigger the DAG and confirm both `load_staging_task` and `run_dbt_task` succeed.
6. Once verified, retire the old key:
   ```sql
   ALTER USER baseball_eval_service UNSET RSA_PUBLIC_KEY;
   ```
