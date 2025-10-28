# Audit Webhook

C-based webhook for ingesting elevator audit ZIP bundles, normalizing their contents, and loading them into Postgres tables. Expects ZIP payloads matching the structure demonstrated in `405 LEXINGTON AVENUE - 31 - 1p17993.zip`.

## Build

```sh
make
```

Produces the executable `audit_webhook`.

Dependencies:
- POSIX environment with `gcc`, `libpq` headers/libraries, and `unzip` in `$PATH`.
- Optional `.env` configuration file (see below).

## Database Schema

Apply the migrations in `sql/schema.sql` before running the service:

```sh
psql "$DATABASE_URL" -f sql/schema.sql
```

Tables created:
- `audits` — single row per audit, with canonical column names.
- `audit_photos` — binary photo storage (`BYTEA`) keyed by audit.
- `audit_deficiencies` — parsed violation data for each audit.

## Configuration

At start-up the service loads environment variables from the file specified in `ENV_FILE` (defaults to `.env` if present). See `env.example` for a template:

```env
API_KEY=super-secret
DATABASE_URL=postgres://user:pass@db-host:5432/dbname
# WEBHOOK_PORT=8080
```

Required variables

| Variable       | Description                                                         |
|----------------|---------------------------------------------------------------------|
| `API_KEY`      | Shared secret required in `X-API-Key`.                               |
| `DATABASE_URL` | Postgres connection string. `POSTGRES_DSN` is accepted as an alias. |

Optional variables

| Variable       | Description                                    |
|----------------|------------------------------------------------|
| `WEBHOOK_PORT` | Listening port (default `8080`).                |
| `ENV_FILE`     | Path to env file (defaults to `.env`).          |

## Running

```sh
./audit_webhook
```

POST a ZIP audit package (containing the CSV, JSON, and referenced photos) directly as the request body to `/webhook` (content-type `application/zip`). Include the required API key header `X-API-Key: <your API_KEY value>`. Example using `curl`:

```sh
curl -X POST \
     -H 'Content-Type: application/zip' \
     -H "X-API-Key: $API_KEY" \
     --data-binary @"405 LEXINGTON AVENUE - 31 - 1p17993.zip" \
     http://localhost:8080/webhook
```

Successful responses resemble:

```json
{"status":"ok","audits":["30f6d8db-415e-4297-b32d-4144083e6c78"]}
```

Errors return:

```json
{"status":"error","message":"why it failed"}
```

The service extracts each ZIP into a temporary directory, parses the CSV for labeled field values, enriches with JSON-only data (metadata, door width, photo manifest, deficiencies), stores photos as `BYTEA`, and replaces any prior rows for the same `audit_uuid` within a single transaction. Payloads should include only the audit CSV, audit JSON, and referenced photo files.

When deployed publicly, the webhook is expected to serve under `https://auditforms.citywideportal.io` (ensure TLS termination and request routing at that hostname).

## Docker

A multi-stage `Dockerfile` is provided.

```sh
docker build -t audit-webhook .
```

Run with production secrets via env file:

```sh
docker run \
  --env-file ./production.env \
  -p 8080:8080 \
  ghcr.io/OWNER/REPO:latest
```

GitHub Actions workflow `.github/workflows/docker.yml` automatically builds and publishes images to GitHub Container Registry (`ghcr.io/<owner>/<repo>`) on pushes to `main`/`master` or version tags.

## Systemd (Debian)

For VM/bare-metal deployments, a sample unit lives at `systemd/audit-webhook.service`. Install with:

```sh
sudo useradd --system --home /srv/audit-webhook --shell /usr/sbin/nologin audit
sudo cp audit_webhook /usr/local/bin/
sudo mkdir -p /srv/audit-webhook
sudo cp -r sql README.md .env /srv/audit-webhook/
sudo chown -R audit:audit /srv/audit-webhook
sudo cp systemd/audit-webhook.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now audit-webhook.service
```

Systemd ensures the service starts at boot and restarts after crashes/reboots.
