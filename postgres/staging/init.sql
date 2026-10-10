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
    payload         JSONB,
    payload_filename TEXT,
    payload_sha256  TEXT,
    payload_size    BIGINT,
    run_id          TEXT,
    load_mode       TEXT NOT NULL DEFAULT 'full_snapshot',
    delete_policy   TEXT NOT NULL DEFAULT 'close_on_full_snapshot',
    checkpoint_before TEXT,
    checkpoint_after TEXT,
    pagination_complete BOOLEAN NOT NULL DEFAULT true,
    table_names JSONB NOT NULL DEFAULT '[]'::jsonb
);

ALTER TABLE staging.raw_batches ADD COLUMN IF NOT EXISTS run_id TEXT;
ALTER TABLE staging.raw_batches ADD COLUMN IF NOT EXISTS payload_filename TEXT;
ALTER TABLE staging.raw_batches ADD COLUMN IF NOT EXISTS payload_sha256 TEXT;
ALTER TABLE staging.raw_batches ADD COLUMN IF NOT EXISTS payload_size BIGINT;
ALTER TABLE staging.raw_batches ALTER COLUMN payload DROP NOT NULL;
ALTER TABLE staging.raw_batches ADD COLUMN IF NOT EXISTS load_mode TEXT NOT NULL DEFAULT 'full_snapshot';
ALTER TABLE staging.raw_batches ADD COLUMN IF NOT EXISTS delete_policy TEXT NOT NULL DEFAULT 'close_on_full_snapshot';
ALTER TABLE staging.raw_batches ADD COLUMN IF NOT EXISTS checkpoint_before TEXT;
ALTER TABLE staging.raw_batches ADD COLUMN IF NOT EXISTS checkpoint_after TEXT;
ALTER TABLE staging.raw_batches ADD COLUMN IF NOT EXISTS pagination_complete BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE staging.raw_batches ADD COLUMN IF NOT EXISTS table_names JSONB NOT NULL DEFAULT '[]'::jsonb;

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
