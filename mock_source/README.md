# Standalone mock source

The mock source is a development/test fixture, not a warehouse stage. Its
database, API, Python code and lifecycle are kept in `mock_source/`; Compose
connects the API to the warehouse network only so the Extractor can read it.
This module has its own documentation and dependencies.

## Three commands

Create `.env` from `.env-template` in the repository root if you have not
already done so. Run all commands below from the repository root.

### `up` — build, start and initialize

```bash
./mock_source/dev.sh up
```

Builds the mock API image, creates/starts its containers and database volume,
waits for both services to become healthy, then initializes the data. The
generator prints row progress for each table and batch; the large dataset can
take a while. By default, it targets about 100 MB for `medium_records` and
1 GB for `large_records`.

If all four tables already contain data, `up` leaves it unchanged and reports
that initialization was skipped. This makes repeated `up` commands safe and
preserves mutations. If only some tables contain data, initialization fails
instead of silently leaving a partial fixture. Optional generator flags can
follow `up`, for example `./mock_source/dev.sh up --seed 7`.

### `mutate` — update existing rows and add rows

```bash
./mock_source/dev.sh mutate
```

By default, updates 20 rows and adds 20 customers. Select another table or
change the counts with generator options, for example:

```bash
./mock_source/dev.sh mutate --table medium_records --update 25 --add 10 --seed 7
```

Run `./mock_source/dev.sh mutate --help` for all options.

### `down` — remove the mock source

```bash
./mock_source/dev.sh down
```

Stops and removes the mock containers, permanently deletes the mock database
volume/data and removes the locally built API image. This does not delete the
downloaded PostgreSQL base image, the shared warehouse network, or any
warehouse container/volume. Run `up` again to create a fresh fixture.

The mock database uses PostgreSQL 18. Because `down` deletes its data, old
PostgreSQL 17 mock data is discarded rather than migrated.

The generator creates:

- `departments`: 1,000 rows
- `customers`: 1,000 rows referencing departments
- `medium_records`: a paged JSON API representation of size of approximately 100MB
- `large_records`: a paged JSON API representation of size of approximately 1GB

Every table includes `created_at` (randomly 1–3 years in the past) and
`updated_at` (randomly 0–1 years in the past). The `medium_records` and
`large_records` tables also include `record_type`, `score`, JSON `attributes`,
and nested JSON `metadata` fields in addition to `payload`.

Bulk tables are generated in batches, with a default payload of about 1,900
characters per row. The target sizes refer to compact API JSON, not
PostgreSQL's on-disk size; actual response sizes vary slightly with IDs and
page envelopes.

## Update data for incremental and SCD2 tests

After the initial full snapshot has established a checkpoint, mutate a
dataset. The command updates existing stable IDs, changes `updated_at` and
adds new rows:

```bash
./mock_source/dev.sh mutate --table customers --update 25 --add 10 --seed 7
```

Supported tables are `departments`, `customers`, `medium_records` and
`large_records`. The default is 20 updates and 20 additions in `customers`.
Mutations preserve `created_at` on existing rows, set `updated_at` to current
UTC time and give new rows a randomized `created_at`; changes can therefore
be selected by the next incremental request. To test incremental loading,
configure a Source with the API URL below, `incremental_upsert`, key field
`id`, query parameter `updated_since` and watermark field `updated_at`. Run
the initial snapshot, mutate, then run the Source again.

## API endpoints and Sources configuration

The API publishes all four tables:

```text
http://mock-source-api:8000/api/departments
http://mock-source-api:8000/api/customers
http://mock-source-api:8000/api/medium_records
http://mock-source-api:8000/api/large_records
```

For a browser or host-side client use `http://localhost:8090/...` instead.
Each route accepts `skip`, `limit` (maximum 5,000, default 1,000) and
`updated_since` query parameters. It returns `items`, `total`, `skip`, `limit`
and `has_more`; the Extractor follows the existing `total`/`skip`/`limit`
pagination convention. The Loader stores the `items` as rows of the selected
source table rather than treating the pagination envelope as a data row.
`updated_since` is an exclusive ISO-8601 timestamp.

Add a table as a Source in the GUI or API with `id` as its key, `updated_at`
as its watermark field and `updated_since` as its incremental parameter. A
blank watermark field instead reads all pages on every upsert run.

## Large-payload limitation

The API is paginated so clients can read bounded pages. The warehouse
Extractor appends each fetched page to the landing file rather than
aggregating all pages in memory, and Loader parses and writes the landing
records to Staging in batches of 500 top-level records. Loader retains one
top-level record and its nested rows in memory at a time. The landing JSON is
the durable raw copy; `staging.raw_batches` stores its filename, byte size and
SHA-256 checksum instead of copying the full JSON into PostgreSQL.

The full batch is committed as one Staging transaction, so very large loads
can keep that transaction open for a long time. Extractor memory is bounded by
the current API page, so use a reasonable API page size. The 100 MB and 1 GB
fixtures can now be tried through the warehouse pipeline, subject to disk
space for the landing file, Staging data, and transaction/WAL overhead.
Keep landing files for as long as you may need to audit or replay raw batches.
The Orchestrator allows up to 900 seconds for each Extractor and Loader HTTP
request by default, configured with `ORCH_PIPELINE_REQUEST_TIMEOUT_SECONDS`.
Increase that `.env` setting if a large fixture needs more time; recreate the
Orchestrator container after changing it.

## Isolation

The mock database has its own Compose volume and credentials and is not used
by warehouse ETL stages. No mock data is inserted into staging, core or mart
unless a user explicitly adds a mock API Source and runs it through the
pipeline.
