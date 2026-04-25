# Production deployment checklist (DigitalOcean + Docker Compose)

Use this when bringing up the stack on a droplet or validating a release. Airflow’s upstream docs recommend Kubernetes for large production deployments; this project intentionally runs **Docker Compose** on a single host with occasional downtime acceptable.

## 1. GitHub and CI

- [ ] **Packages:** For GHCR, allow GitHub Actions to publish (`Settings` → `Actions` → `General` → workflow read/write as needed; first push may require accepting the `ghcr.io` package for the repo).
- [ ] **Workflow:** Pushes to **`main`** build and push **`Dockerfile.prod`** (see [.github/workflows/production-image.yml](../.github/workflows/production-image.yml)). Tags: `:main` and a tag equal to the commit SHA.
- [ ] **Image reference on droplet:** Set `AIRFLOW_IMAGE_NAME` in the server `.env` to the lowercase GHCR image, e.g. `ghcr.io/<owner>/baseball-eval-flow:main`.

## 2. Droplet

- [X] Ubuntu LTS, **≥ 8 GB RAM** recommended for this stack.
- [X] Install **Docker Engine** and **Docker Compose v2** (`docker compose`, not legacy `docker-compose`). See [DigitalOcean Docker Compose on Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-22-04) and [Docker’s Compose install docs](https://docs.docker.com/compose/install/).
- [ ] Non-root sudo user, SSH keys, firewall (**UFW**): allow **22**, **80**, **443**; deny public access to Postgres and Airflow’s internal API port.

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
| `SNOWFLAKE_*` | For dbt `--target prod` from DAGs: account, user, password, role, database, warehouse, schema |

Optional: `AIRFLOW_UID` on Linux for file ownership on the `airflow-logs` volume.

## 5. TLS (Caddy)

- [X] **DNS:** `A` / `AAAA` for `AIRFLOW_PUBLIC_HOST` pointing at the droplet.
- [ ] Caddy obtains **Let’s Encrypt** certificates automatically when the hostname resolves publicly on **:80** / **:443**.
- [ ] `AIRFLOW__FAB__ENABLE_PROXY_FIX` is set to `true` in production compose so `X-Forwarded-*` headers from Caddy are honored.

Local smoke without real DNS: use a throwaway hostname and hosts file, or temporarily run without the prod override’s Caddy service (not recommended for real production).

## 6. Snowflake

- [ ] Outbound **HTTPS** from droplet to Snowflake allowed.
- [ ] If using **network policies** in Snowflake, allow the droplet’s egress IP.

## 7. Start and verify

```bash
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml pull
docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d
```

- [ ] `curl -fsS https://<AIRFLOW_PUBLIC_HOST>/api/v2/version` (or open the UI).
- [ ] Scheduler / worker health checks green (`docker compose ... ps`).
- [ ] Run a small DAG smoke test.

## 8. Backups

- [ ] Schedule **Postgres** backups (`pg_dump` or volume snapshots). Metadata loss loses DAG history, variables, and connection secrets stored in Airflow.

## 9. Deploy updates after a merge to `main`

- [ ] CI finishes pushing a new `:main` image.
- [ ] On the droplet: `docker compose -f docker-compose.yaml -f docker-compose.prod.yaml pull && docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d`.

Optional: add a GitHub Actions job that SSHs into the droplet and runs the same commands; store `SSH_PRIVATE_KEY`, host, and `known_hosts` in repo secrets.
