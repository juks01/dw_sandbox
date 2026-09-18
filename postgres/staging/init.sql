-- Staging database schema.
-- Holds raw ingested batches plus dynamically-created flattened tables
-- (the loader service creates/alters those tables at runtime).

CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION CURRENT_USER;

CREATE TABLE IF NOT EXISTS staging.raw_batches (
    id              BIGSERIAL PRIMARY KEY,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    filename        TEXT NOT NULL UNIQUE,
    source          TEXT NOT NULL,
    source_url      TEXT,
    payload         JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_batches_source ON staging.raw_batches (source);
CREATE INDEX IF NOT EXISTS idx_raw_batches_payload_gin ON staging.raw_batches USING GIN (payload);

-- ---------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------
GRANT USAGE ON SCHEMA staging TO loader_writer, staging_reader;

GRANT SELECT, INSERT, UPDATE, REFERENCES ON staging.raw_batches TO loader_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO loader_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT, INSERT, UPDATE, REFERENCES ON TABLES TO loader_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT USAGE, SELECT ON SEQUENCES TO loader_writer;

GRANT SELECT ON staging.raw_batches TO staging_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO staging_reader;

-- Loader needs to be able to create new dynamic tables in the staging schema.
GRANT CREATE ON SCHEMA staging TO loader_writer;

-- Admin gets full rights (already superuser via POSTGRES_USER bootstrap,
-- but be explicit for the schema too).
GRANT ALL PRIVILEGES ON SCHEMA staging TO CURRENT_USER;
