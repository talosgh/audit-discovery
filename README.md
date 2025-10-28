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
| `API_PREFIX`   | URL prefix for API routes (default `/webhook`). |
| `STATIC_DIR`   | Directory containing built dashboard assets (default `./static`). |

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

## HTTP API

Read-only JSON endpoints are exposed to power dashboards and integrations (CORS headers are emitted automatically):

| Method | Path (`{API_PREFIX}` defaults to `/webhook`) | Description                                                     |
|--------|----------------------------------------------|-----------------------------------------------------------------|
| GET    | `{API_PREFIX}` or `{API_PREFIX}/health`       | Simple heartbeat returning `{"status":"ok"}`.                |
| GET    | `{API_PREFIX}/audits`                         | Recent audit summaries (latest 100, ordered by submission).     |
| GET    | `{API_PREFIX}/audits/{uuid}`                  | Detailed audit payload with metadata, deficiencies, and photos. |
| PATCH  | `{API_PREFIX}/audits/{uuid}/deficiencies/{id}` | Toggle a deficiency’s closed state (`{"resolved":true|false}`). |

`/audits/{uuid}` responses follow the shape:

```json
{
  "audit": { "audit_uuid": "…", "building_address": "…", "device_type": "…" },
  "deficiencies": [ { "violation_note": "Expired cat 1 tag" } ],
  "photos": [ { "photo_filename": "foo.jpg", "content_type": "image/jpeg", "photo_bytes": "<base64>" } ]
}
```

These GET endpoints do **not** require the ingest API key, which makes browser-based clients straightforward.

## Docker

A multi-stage `Dockerfile` is provided.

```sh
docker build -t audit-webhook .
```

Run with production secrets via env file (replace `OWNER/REPO` with your repository path):

```sh
docker pull ghcr.io/OWNER/REPO:main
docker run \
  --env-file ./production.env \
  -p 8080:8080 \
  ghcr.io/OWNER/REPO:main
```

GitHub Actions workflow `.github/workflows/docker.yml` automatically builds and publishes images to GitHub Container Registry (`ghcr.io/<owner>/<repo>`) on pushes to `main`/`master` or version tags.

The container serves the SolidJS dashboard from the web root and exposes API routes under the configured `API_PREFIX` (default `/webhook`).

## Systemd (Debian)

For VM/bare-metal deployments, a sample unit that runs the container lives at `systemd/audit-webhook.service`. Customize the `IMAGE=ghcr.io/OWNER/REPO:main` line, place your `.env` at `/srv/audit-webhook/.env`, and install with:

```sh
sudo mkdir -p /srv/audit-webhook
sudo cp production.env /srv/audit-webhook/.env
sudo chmod 600 /srv/audit-webhook/.env
sudo cp systemd/audit-webhook.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now audit-webhook.service
```

The unit pulls the latest image, runs it with `--env-file`, binds port `8080`, and restarts automatically after crashes or reboots.

## Dashboard (SolidJS)

A SolidJS single-page application lives under `dashboard/` for exploring the data captured by the webhook.

```sh
cd dashboard
npm install
npm run dev
# or build a production bundle
npm run build
```

Expose the API to the dashboard by setting:

- `VITE_API_BASE_URL` – the origin serving the webhook (defaults to the current origin when omitted).
- `VITE_API_PATH` – API prefix (defaults to `/webhook`).

For local development, create `dashboard/.env` with:

```
VITE_API_BASE_URL=http://localhost:8080
VITE_API_PATH=/webhook
```

Routes:

- `/` — searchable list of audits including building, owner, device, and submission timestamps.
- `/audits/:id` — detail page with rich metadata, deficiency breakdown (close/reopen actions), and inline photo gallery (photos are base64 data URLs generated by the API).

Production builds emit to `dashboard/dist/` via Vite.

When hosting both API and UI on the same domain (e.g., `audit-webhook.citywideportal.io`), serve the static bundle at the root path and forward `/webhook/*` to the webhook container so browser requests hit `/webhook/audits…` transparently.
The dashboard uses the History API, so ensure that all non-API routes (e.g., `/audits/<id>`) fall back to `index.html` when served by your reverse proxy.
