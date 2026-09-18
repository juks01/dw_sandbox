# dw-dev

A locally runnable Data Warehouse development environment (Podman/Docker
Compose). Modular ETL/ELT pipeline:

```
SOURCE/API → EXTRACTOR → LANDING → LOADER → STAGING → CORE → MART → REPORTING

ORCHESTRATOR (schedule, run, GUI)
```
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
First boot creates the databases, roles, a demo user/department
dataset, and one fully offline demo pipeline source (`local://demo`) so you
can see data flow through the whole system without any external API.
For another data source you can use for example https://dummyjson.com/products .

## Delete environment
```bash
podman compose down --remove-orphans -v
```
Don't use -v if you want to keep database volumes

## GUI
http://localhost:8080  (HTTP Basic Auth — see `ORCH_USER` / `ORCH_PASSWORD`
in `.env`, default `admin` / `admin`)

Shows system health, the source list (URL, cron, next run, Run/Delete),
and recent pipeline runs with status, current step and error message.

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
  → extractor  (GET the source URL, write raw JSON to data/landing/)
  → loader     (flatten JSON → staging.* tables, generic + schema-adapting)
  → staging    (stores original data in database format) 
  → core       (core.sync_users(), core.sync_from_staging() — SCD2 dimensions)
  → mart       (mart.refresh() — publishes only is_current = true, no SCD columns)
  → reporting  (read-only role, SELECT-only on mart.*)
```

Every run is recorded in the orchestrator's own SQLite database
(`orchestrator/data/orchestrator.db`) with its current step and, on failure,
the error message. The same source can never run twice concurrently —
triggering a source that is already running returns HTTP 409.

## Adding a new source
Nothing else in the pipeline needs to change. Add it through the GUI (or
`POST /api/sources`) with a name, a URL (or `local://demo` for the offline
fixture), and a cron expression. The loader creates/adapts staging tables
automatically; `core.sync_from_staging()` discovers new staging tables and
builds/maintains the matching `core.dim_*` SCD2 tables; `mart.refresh()`
picks up every `core.dim_*` table automatically.

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
- `core.sync_users()`, `core.sync_from_staging()` and `mart.refresh()` are
  `SECURITY DEFINER` with a locked-down `search_path` and `EXECUTE` revoked
  from `PUBLIC` — only `core_service` / `mart_service` (the orchestrator)
  and the schema owner can call them.
- Row Level Security is enabled on `core.dim_user`, scoped to
  `users`/`core`'s department data (see `postgres/core/init.sql`). Set
  `SELECT set_config('core.department_code', 'ENG', false);` in a session
  to see it filter.
- All dynamic SQL (table/column names built at runtime by the loader and by
  `core.sync_from_staging()` / `mart.refresh()`) goes through
  `format()`/`%I`/`%L` (SQL) or `psycopg.sql.Identifier()` (Python) — never
  raw string concatenation — and source-derived identifiers are normalized
  (lower-cased, non-alphanumerics replaced, length-capped, leading digits
  prefixed) before use.

## Deletion policy (explicit, on purpose)
If a business key that is currently `is_current = true` in a dimension no
longer appears in its source table, the sync procedures **close** that row
(`valid_to = now()`, `is_current = false`) and insert **no replacement**.
The row's history is preserved, but it disappears from `mart.*` (which only
ever selects `is_current = true`).

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
