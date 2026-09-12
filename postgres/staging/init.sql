CREATE ROLE admin LOGIN PASSWORD 'admin';
CREATE ROLE loader LOGIN PASSWORD 'loader';

GRANT pg_read_all_data TO admin;
ALTER ROLE admin BYPASSRLS;

CREATE SCHEMA staging AUTHORIZATION loader;

CREATE TABLE staging.raw_batches (
    id bigserial PRIMARY KEY,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    filename text NOT NULL UNIQUE,
    source_url text NOT NULL,
    payload jsonb NOT NULL
);

GRANT USAGE ON SCHEMA staging TO admin;

GRANT SELECT
    ON ALL TABLES IN SCHEMA staging
    TO admin;

GRANT SELECT
    ON ALL SEQUENCES IN SCHEMA staging
    TO admin;

GRANT ALL
    ON SCHEMA staging
    TO loader;

GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA staging
    TO loader;

GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA staging
    TO loader;

ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA staging
    GRANT SELECT ON TABLES TO admin;

ALTER DEFAULT PRIVILEGES FOR ROLE loader IN SCHEMA staging
    GRANT SELECT ON SEQUENCES TO admin;
