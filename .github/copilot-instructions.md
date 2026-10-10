# Copilot instructions

## Project purpose

This repository is a local, container-first development environment for a
modular data warehouse pipeline. Keep development convenient in Podman Compose
while preserving service boundaries so pipeline stages can later run on
separate physical or virtual machines.

## Architecture and boundaries

The current flow is:

```text
Source/API -> Extractor -> landing files -> Loader -> Staging -> Core -> Mart -> Reporting
                     ^                    ^
                     +-- Orchestrator -----+
                         API, GUI, cron, run state and pipeline control
Users database ---------------------------> Core user/department dimensions
```

- **Extractor** fetches source data, follows supported pagination, checks
  destinations against its host allowlist, and writes payloads and manifests.
- **Loader** validates manifests and checksums, flattens generic JSON, and
  writes staging batches.
- **Staging** stores raw batches and dynamically shaped source tables.
- **Core** synchronizes an explicit staging batch into SCD2 dimensions.
- **Mart** publishes current reporting data; keep RLS on `core.dim_user`, not
  on materialized reporting tables.
- **Orchestrator** owns source configuration, cron scheduling, checkpoints,
  run status, health checks, and the web UI.

`users` is a separate PostgreSQL service containing the demo/master user and
department records consumed by Core. It is a source database, not another
reporting stage.

Keep service responsibilities and contracts explicit. **Current development
couplings that must not be mistaken for distributed-ready features:**

- Compose mounts the same host `data/landing/` directory into Extractor and
  Loader. Extractor writes a payload and manifest; Loader reads both by file
  name. This is not a remote object-store API.
- Core imports `staging` and `users` tables with PostgreSQL FDW. Mart imports
  Core tables with FDW. `postgres/core/init.sh` and `postgres/mart/init.sh`
  currently hard-code Compose DNS names (`staging`, `users`, `core`) and port
  `5432`.
- Although `.env-template` defines several database hosts and ports, not every
  consumer honors them. Orchestrator's Core/Staging/Mart port constants are
  currently `5432`; Loader receives fixed Compose values; Core/Mart FDW
  endpoints are fixed in init scripts. Treat these as known gaps, not as
  configurable capabilities.
- The current Compose bridge network and volume mounts provide local service
  discovery and file sharing. They are not the future cross-machine transport
  or storage contract.

When implementing multi-machine support, make service endpoints and database
hosts/ports configurable end to end, and replace the shared local landing
directory with a storage contract that both Extractor and Loader can use.
Update persistent FDW server definitions/mappings as part of the deployment
change. Keep database credentials in configuration/secrets and do not make
firewall or port-opening instructions part of this project.

Network firewall and port-opening guidance is out of scope.

## Development environment

- Use `.env-template` as the source for a local `.env`; never commit `.env`,
  credentials, tokens, or production secrets. Development defaults are not
  production credentials.
- The actual first-run command is `cp .env-template .env`. `.env` is
  git-ignored.
- Run builds, Python commands, unit tests, database commands, and integration
  tests inside the appropriate Compose service container. Do not depend on
  host-installed Python packages or a host virtual environment.
- The Python service images currently use Python 3.14. Keep dependency changes
  in that service's requirements file and Docker build context.
- Start the stack with `podman compose up --build` (Docker Compose may also be
  used where configured). Rebuild and recreate an application service after
  changing code when verifying the running stack.
- Service containers are `extractor`, `loader`, and `orchestrator`; PostgreSQL
  containers are `staging`, `users`, `core`, and `mart`. Compose service names
  work only on the current Compose network.
- `.env-template` currently sets `TZ=Europe/Helsinki`,
  `ORCH_SCHEDULER_INTERVAL_SECONDS=10`, Orchestrator host port `8088`, and
  database host ports `5433` (staging), `5434` (core), `5435` (mart), and
  `5436` (users). These are development defaults, not deployment guarantees.
- Cron expressions use the Orchestrator's `TZ` (`ZoneInfo`); `next_run` is
  stored with an offset and `next_run_timezone` tracks the zone used to
  calculate it. A timezone change causes the scheduler to recalculate the
  stored next run. UI timestamps and cron labels use the same configured zone.
- Do not remove persistent database volumes as routine cleanup. In particular,
  never add `-v` to `podman compose down` unless explicitly asked to erase
  development database data.
- Treat `data/landing/` and `orchestrator/data/` as runtime data. Do not delete
  or rewrite existing user data to clean up a test or implementation.

## Testing

Run focused tests in the service container that owns the changed code:

```bash
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

The Extractor-to-Mart suite is opt-in and writes to the local Compose
databases. Run it only against the development stack, with `DW_E2E_TESTS=1`
and the `DW_TEST_*` connection variables documented in `README.md`. It runs
under the Orchestrator image because that image has the test dependencies.
Prefer focused tests first, then the relevant integration suite for changes
that cross service or database boundaries.

Use these container commands when reproducing all current unit-test layers:

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

For the opt-in E2E test, start the DB services and run inside the Orchestrator
container:

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

The test reads database names and credentials from `.env`: `STAGING_DB`,
`CORE_DB`, `MART_DB`, `ADMIN_USER`, `POSTGRES_PASSWORD`,
`LOADER_WRITER_USER/PASSWORD`, `CORE_SERVICE_USER/PASSWORD`,
`CORE_READER_USER/PASSWORD`, and `REPORTING_USER/PASSWORD`. Run E2E
separately, not concurrently with another test or pipeline using those same
databases: it creates unique test tables, drops them in cleanup, and calls
`mart.refresh()`, which rebuilds the published Mart tables.

## Data and correctness invariants

- Extractor manifests describe the exact payload, run, mode, checkpoints, and
  pagination completeness. They include requested/final URL, fetch timestamp,
  HTTP status, content type, byte size, SHA-256, and payload filename. Payload
  and manifest are written atomically as separate files. Loader verifies the
  manifest/payload relationship, run/source/mode/checkpoint metadata, and
  checksum before writing to staging.
- Extractor routes are `GET /health` and `POST /extract`. Loader routes are
  `GET /health` and `POST /load`. Do not bypass Loader's manifest validation
  by writing directly into staging from Extractor.
- Extractor supports JSON `next`/`links.next`, HTTP `Link` `rel="next"`,
  `total` + `skip`/`offset` + `limit` (including DummyJSON), and `has_more`
  page-number pagination. It merges a common or unique list collection across
  pages, caps extraction at 1000 pages, and limits redirects to 11 requests.
  The initial URL and every pagination/redirect target must pass
  `extractor/conf/allowed_hosts`. A cursor-only signal such as `next_cursor`
  without a supported continuation marks the payload incomplete; Orchestrator
  refuses to load an incomplete response.
- `local://demo` serves the checked-in `extractor/conf/demo.json` fixture and
  does not call an external API.
- Loader flattening rules: top-level objects/arrays become a source table;
  nested objects become prefixed columns; nested arrays become child tables;
  technical `_row_id`, `_source_batch_id`, `_parent_id`, `_row_index` track
  batch and parent/order. SQL identifiers must be normalized and quoted.
- Staging persists the original JSON payload and run metadata in
  `staging.raw_batches`; dynamic flattened tables hold per-row records.
  Duplicate payload filenames are idempotently skipped.
- Core synchronization is batch-scoped. Never select an implicit “latest”
  staging batch for a run.
- A configured stable business key, or a source `id` column, is required for
  each non-empty source table synchronized into SCD2. When `key_fields` is
  empty, Core automatically uses `id`; when configured, every configured
  field must exist in that non-empty table or Core raises an error. Empty
  tables can be synchronized without a key. Keyless nested arrays therefore
  need explicit stable keys or must not be sent through generic Core SCD2.
  Content hashes over source data columns (excluding loader technical columns)
  detect changes; they are not business keys.
- Core SCD2 stores `valid_from`, `valid_to`, `is_current`, `_business_key`,
  and `_content_hash`. Changed content for a stable business key closes its
  current row and inserts a new current version. Mart refreshes from all
  `core.dim_*` tables, filters `is_current = true`, and excludes SCD metadata
  (`_sk`, validity fields, `_content_hash`, `_business_key`) from reporting.
- Incremental upserts process keys present in the batch. Absence is not a
  deletion signal. Only a completed full snapshot may close missing keys under
  the configured deletion policy.
- Source load modes are `full_snapshot` and `incremental_upsert`; deletion
  policies are `close_on_full_snapshot` and `never_close`. There is no CDC,
  append-only, or scoped-snapshot implementation.
- `key_fields` and `watermark_field` are independently configured. A blank
  key list means auto-use `id`; it does not create a content-hash ID. A blank
  watermark means each incremental run fetches the whole endpoint and uses no
  timestamp checkpoint/query parameter. With a watermark, the API request
  adds the configured `incremental_param` (default `updated_since`), and the
  returned watermark field (default `updated_at`) must be ISO-8601. The API
  itself must implement that query filter; the pipeline cannot enforce it.
- A watermark-based incremental source with no saved checkpoint runs a
  `full_snapshot` first. `/api/sources/{id}/full-reload` also forces a snapshot.
  The new checkpoint is committed only after Extractor, Loader, Core, and Mart
  all succeed. Failed runs retain the prior checkpoint for retry.
- Each source may have only one in-flight run. Manual duplicate triggers
  return HTTP 409. Scheduled runs are checked by the Orchestrator's cron loop.
- Orchestrator's GUI and `/api/*` routes require HTTP Basic Auth; `/health` is
  unauthenticated liveness. Current routes are `GET /api/health`,
  `GET/POST /api/sources`, `PUT/DELETE /api/sources/{id}`,
  `POST /api/sources/{id}/run`,
  `POST /api/sources/{id}/full-reload`, `GET /api/runs`, and
  `GET /api/runs/{id}`. Source create/update accepts name, URL, cron, enabled,
  load mode, delete policy, key fields, incremental parameter, and watermark
  field. Defaults are enabled, `full_snapshot`,
  `close_on_full_snapshot`, empty `key_fields`, `updated_since`, and
  `updated_at`. Duplicate source names return 409; invalid source settings
  return 422.
- Updating a source clears its saved checkpoint if its URL, load mode,
  incremental parameter, or watermark field changes. Editing the cron,
  deletion policy, key fields, or enabled state alone retains the checkpoint.
- Each orchestrated run performs dependency preflight health checks, extracts,
  loads, runs `core.sync_users()` and the exact-batch
  `core.sync_from_staging(...)`, refreshes Mart, then marks the run complete
  and advances a returned checkpoint. `core.sync_users()` keeps the static
  user/department dimensions up to date; user RLS remains on
  `core.dim_user`.
- Keep source schemas generic. Do not assume all APIs have the same columns or
  response shape; preserve supported pagination and nested JSON behavior.
- Keep existing Row-Level Security behavior scoped to `core.dim_user`.

## Database and security changes

- Database init scripts run when a database volume is first initialized; edits
  to an init script do not automatically migrate an existing persistent
  database. For schema/procedure changes, provide or document the required
  migration/re-apply step and test it against the local Compose databases.
- Staging uses `postgres/staging/init.sql`; users master data uses
  `postgres/users/init.sql`; Core schema/RLS/roles use
  `postgres/core/init.sql` and `init.sh`; sync functions live in
  `postgres/core/procedures.sql`; Mart setup and refresh live in
  `postgres/mart/init.sql`, `init.sh`, and `procedures.sql`. Orchestrator's
  SQLite metadata schema is migrated by `orchestrator/app/db.py::init_db()`.
- Existing database volumes require explicit SQL re-application for changed
  init/procedure definitions; do not assume rebuilding a container reruns
  Postgres initialization. Follow the commands in `README.md`. If changing
  FDW endpoints, also migrate existing foreign servers and user mappings; an
  init-script edit alone does not alter an already-created server.
- Use least-privilege service accounts and preserve the separation between
  loader writer, staging/core readers, core service, mart service, and
  reporting roles.
- Parameterize SQL values. Quote dynamic SQL identifiers with the existing
  safe helpers (`psycopg.sql.Identifier` or PostgreSQL `format('%I', ...)`);
  never interpolate source-controlled identifiers directly into SQL.
- Preserve the Extractor allowlist checks for initial URLs, pagination URLs,
  and redirects. Do not weaken URL validation as a shortcut for a test.
- Orchestrator's Basic Auth protects the GUI and `/api/*` routes. `/health` is
  unauthenticated liveness; `/api/health` is authenticated and performs
  dependency checks. Each dependency health result is logged by Orchestrator;
  access-log filters suppress duplicate `/health` entries in service logs.
- Keep errors visible with their pipeline phase and useful context. Avoid broad
  catches that silently convert failures into success-shaped results.

## Code and documentation

- Follow existing service-local structure, naming, typing, and formatting.
- Service entry points and main files: `extractor/app/main.py`,
  `loader/app/main.py`, and `orchestrator/app/main.py`. The GUI is plain
  HTML/JavaScript in `orchestrator/app/static/index.html`; no frontend build
  step is used.
- Prefer FastAPI lifespan handlers for new startup/shutdown work. Extractor
  currently uses lifespan; Loader and Orchestrator still have legacy
  `@app.on_event` handlers and should be migrated when those lifecycle sections
  are next changed, without losing access-log setup, database initialization,
  scheduler start, or scheduler stop behavior.
- Make surgical changes and preserve unrelated worktree modifications.
- Add short comments or docstrings for important boundaries and non-obvious
  invariants; do not narrate obvious code.
- Update `README.md` when commands, configuration, service behavior, database
  migrations, or test workflows change.
- Before finishing, run the smallest relevant test set and `git diff --check`;
  report tests that could not be run and why.
