# dw-dev

A locally runnable Data Warehouse development environment (Podman/Docker
Compose). Modular ETL/ELT pipeline:

```
SOURCE/API → EXTRACTOR → LANDING → LOADER → STAGING → CORE → MART → REPORTING

ORCHESTRATOR (schedule, run, GUI)
```

The local stack runs the extractor, loader, orchestrator, staging, core, and
mart services. Sources can be configured for full snapshots or incremental
upserts.

Each extraction writes a readable payload filename and a matching manifest:

```
data/landing/
  products_20260923T120000Z_<uuid>.json
  products_20260923T120000Z_<uuid>.manifest.json
```

The manifest contains the run ID, source and URL, final URL, fetch time,
HTTP status, content type, byte size, SHA-256 checksum, load mode, checkpoint
before/after, and whether pagination appears complete. The loader only loads
a payload when its manifest exists, names the same payload, and contains a
matching checksum. Both files are written atomically by the extractor.
## First
Copy .env file template as .env file. You may use default values in dev. NEVER use default values in production!
```bash
cd dw-dev
cp .env-example .env
```

## Build environment
```bash
podman compose up --build
```
First boot creates the databases, roles, and a demo user/department
dataset. The orchestrator also seeds an enabled `demo-products` source
(`local://demo`, every two minutes). The extractor serves its bundled
`extractor/conf/demo.json` fixture for that URL, so the pipeline can run
without an external API. The allowlist still controls which HTTP(S) hosts the
extractor may contact. It follows `Link` headers with `rel="next"`, JSON
`next` URLs, DummyJSON-style `total`/`skip`/`limit` metadata, and `has_more`
page-number responses, combining result lists before loading. Every page and
redirect is checked against the allowlist; unknown or incomplete pagination
fails closed.

## Delete environment
```bash
podman compose down --remove-orphans -v
```
Don't use -v if you want to keep database volumes

## GUI
http://localhost:8080  (HTTP Basic Auth — see `ORCH_USER` / `ORCH_PASSWORD`
in `.env`, default `admin` / `admin`)

Shows system health, source load modes, missing-key policies, checkpoints,
last successful runs, recent errors and pipeline runs. The source editor
configures key fields, deletion policy, an incremental query parameter and a
watermark field. Incremental sources use
`updated_since` and `updated_at` by default; their first run is always a full
snapshot. The source API must actually honor the configured query parameter
to return a delta; otherwise each incremental upsert may still download the
entire response. The full reload action runs a snapshot immediately and
advances an incremental source's checkpoint only after the entire pipeline
succeeds.

## Health
- http://localhost:8080/health — open liveness check
- http://localhost:8080/api/health — authenticated, aggregated dependency health
  (extractor, loader, staging, core, mart)

Each internal service also exposes its own unauthenticated health check,
used by container healthchecks:
- extractor → `GET /health`
- loader → `GET /health`
- staging / core / mart → `SELECT 1` (checked over psycopg by the orchestrator)

## Database ports (host)
| Database | Port |
|----------|------|
| staging  | 5433 |
| core     | 5434 |
| mart     | 5435 |
| users    | 5436 |

Connect e.g. `psql -h localhost -p 5433 -U admin -d staging` (password in `.env`).

## Pipeline
```
scheduler (croniter, checks every 5s)
  → extractor  (fetch the source URL, write JSON and a run manifest to data/landing/)
  → loader     (flatten JSON → staging.* tables, generic + schema-adapting)
  → staging    (stores original data in database format) 
  → core       (core.sync_users(), batch-scoped core.sync_from_staging() — SCD2 dimensions)
  → mart       (mart.refresh() — publishes only is_current = true, no SCD columns)
  → reporting  (read-only role, SELECT-only on mart.*)
```

## Tests
Run the unit tests in their service containers:
```bash
podman compose build extractor loader orchestrator
podman compose run --rm --no-deps \
  -v "$PWD:/workspace:z" -w /workspace \
  extractor python -m unittest discover -s extractor/tests -v
podman compose run --rm --no-deps \
  -v "$PWD:/workspace:z" -w /workspace \
  loader python -m unittest discover -s loader/tests -v
podman compose run --rm --no-deps \
  -v "$PWD:/workspace:z" -w /workspace \
  orchestrator python -m unittest discover -s orchestrator/tests -v
```

The extractor-to-mart integration tests use the local compose databases and
are opt-in. Start the database services, build the orchestrator image, then
run the suite inside that image (no host Python packages are needed):
```bash
podman compose up -d staging users core mart
podman compose build orchestrator
podman compose run --rm --no-deps \
  -v "$PWD:/workspace:z" -w /workspace \
  -e DW_E2E_TESTS=1 \
  -e DW_TEST_STAGING_HOST=staging -e DW_TEST_STAGING_PORT=5432 \
  -e DW_TEST_CORE_HOST=core -e DW_TEST_CORE_PORT=5432 \
  -e DW_TEST_MART_HOST=mart -e DW_TEST_MART_PORT=5432 \
  orchestrator python -m unittest discover -s tests -v
```

The integration suite creates a unique source and removes its staging, core,
and mart tables afterward. `mart.refresh()` rebuilds all published mart
tables, so run it against the local development stack rather than a shared
environment. The `DW_TEST_*_HOST` and `DW_TEST_*_PORT` variables can be
changed when the databases use different compose service names or internal
ports. The suite sends generated flat-array, DummyJSON `/products`-shaped
paginated/nested product, and nested object/array API payloads through
Extractor and Loader, then verifies the resulting records and flattened
columns through the read-only mart role.

Every run is recorded in the orchestrator's own SQLite database
(`orchestrator/data/orchestrator.db`) with its current step and, on failure,
the error message. The same source can never run twice concurrently —
triggering a source that is already running returns HTTP 409.

## Adding a new source
Add it through the GUI (or `POST /api/sources`) with a name, URL, cron
expression, and load mode. For incremental upserts, configure the
`updated_since` query parameter and ISO timestamp watermark field. The loader
creates/adapts staging tables automatically; the batch-scoped
`core.sync_from_staging(batch_id, source, mode, key_fields)` discovers new
staging tables and builds/maintains the matching
`core.dim_*` SCD2 tables; `mart.refresh()` picks up every `core.dim_*`
table automatically.

## Cron examples
```
0 1 * * *       daily at 01:00
0 3 * * *       daily at 03:00
*/15 * * * *    every 15 minutes
*/2 * * * *     every 2 minutes (used by the seeded demo source)
```

## Security notes (dev-only shortcuts, documented on purpose)
- All credentials live in `.env` (git-ignored). Passwords are dev-grade and
  identical across databases on purpose, per the project brief.
- `ADMIN_USER` is created via `POSTGRES_USER`/`POSTGRES_PASSWORD` by the
  official postgres image, which makes it a full superuser in each
  database — slightly more than the "admin, but not quite superuser" ideal
  described in the brief. Acceptable for a local sandbox; if you need a
  truly reduced-privilege admin, revoke `SUPERUSER` post-init and grant the
  specific privileges you need instead.
- `core.sync_users(mode)`, `core.sync_from_staging(batch_id, source, mode, key_fields)` and `mart.refresh()` are
  `SECURITY DEFINER` with a locked-down `search_path` and `EXECUTE` revoked
  from `PUBLIC` — only `core_service` / `mart_service` (the orchestrator)
  and the schema owner can call them.
- Row Level Security is enabled on `core.dim_user`, scoped to
  `users`/`core`'s department data (see `postgres/core/init.sql`). Set
  `SELECT set_config('core.department_code', 'ENG', false);` in a session
  to see it filter.
- All dynamic SQL (table/column names built at runtime by the loader and by
  `core.sync_from_staging(...)` / `mart.refresh()`) goes through
  `format()`/`%I`/`%L` (SQL) or `psycopg.sql.Identifier()` (Python) — never
  raw string concatenation — and source-derived identifiers are normalized
  (lower-cased, non-alphanumerics replaced, length-capped, leading digits
  prefixed) before use.

## Ingestion modes and deletion policy
Incremental extraction adds the configured query parameter (default
`updated_since`) with the saved checkpoint. The configured watermark field
(default `updated_at`) must contain ISO-8601 timestamps in returned records.
Incremental mode requires the source API to filter on that query parameter.
DummyJSON `/products` supports offset pagination but does not filter by
`updated_since`; using it in `incremental_upsert` mode therefore upserts the
full product response on each run rather than extracting only changed
products.
The extractor follows common pagination conventions (`Link` header
`rel="next"`, JSON `next` URLs, offset/limit/total including DummyJSON, and
`has_more` with a page number) and combines the returned records. Every page
and redirect must pass the host allowlist, and pagination is capped at 1000
pages. Unrecognized signals such as cursor-only feeds fail closed. CDC
deletes, restricted snapshots and append-only feeds are not implemented yet.

Only a completed `full_snapshot` with `close_on_full_snapshot` policy closes
current dimension rows whose business keys are absent; `never_close` disables
absence-based closure even for snapshots. An `incremental_upsert` only inserts
or updates keys present in its exact staging batch; absence never means
deletion. The fixed `users`
sync follows the same mode and continues to write only to `core.dim_user` and
`core.dim_department`; RLS remains on `core.dim_user`, not on mart tables.

For existing persistent development databases, reapply updated schema and
procedure definitions after upgrading the checkout. The staging init script
adds new batch metadata columns as the database owner:
`podman compose exec -T staging psql -v ON_ERROR_STOP=1 -U "$ADMIN_USER" -d "$STAGING_DB" -f /docker-entrypoint-initdb.d/init.sql`.
Reapply the core procedures:
`podman compose exec -T core psql -v ON_ERROR_STOP=1 -U "$ADMIN_USER" -d "$CORE_DB" -f /docker-entrypoint-initdb.d/procedures.sql`.
The orchestrator migrates its SQLite metadata database automatically.

## Project layout
```
dw-dev/
  .env                  secrets / config (git-ignored)
  compose.yml
  data/landing/           landing zone (host-mounted)
  postgres/
    staging/  init.sh init.sql
    users/    init.sh init.sql
    core/     init.sh init.sql procedures.sql
    mart/     init.sh init.sql procedures.sql
  extractor/  Dockerfile requirements.txt app/main.py
  loader/     Dockerfile requirements.txt app/{main,db,flatten}.py
  orchestrator/
    Dockerfile requirements.txt
    app/{main,db,auth,pipeline,runner,scheduler}.py
    app/static/index.html   (GUI, no build step / no frontend framework)
    data/                   orchestrator SQLite DB (host-mounted)
```
