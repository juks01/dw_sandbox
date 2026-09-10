CREATE ROLE admin LOGIN PASSWORD 'admin';
CREATE ROLE loader LOGIN PASSWORD 'loader';

GRANT pg_read_all_data TO admin;
ALTER ROLE admin BYPASSRLS;

CREATE SCHEMA staging;

CREATE TABLE staging.raw_batches (
    id bigserial PRIMARY KEY,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    filename text NOT NULL UNIQUE,
    payload jsonb NOT NULL
);

CREATE TABLE staging.stg_products (
    product_id integer PRIMARY KEY,
    attributes jsonb NOT NULL,
    batch_id bigint,
    updated_at timestamptz NOT NULL DEFAULT now()
);

GRANT USAGE ON SCHEMA staging TO loader;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA staging TO loader;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO loader;

GRANT ALL ON SCHEMA staging TO admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO admin;
