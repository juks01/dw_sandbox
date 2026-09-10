CREATE ROLE admin LOGIN PASSWORD 'admin';

GRANT pg_read_all_data TO admin;
ALTER ROLE admin BYPASSRLS;

CREATE SCHEMA core AUTHORIZATION admin;

CREATE TABLE core.dim_product (
    product_sk bigserial PRIMARY KEY,
    product_id integer NOT NULL,
    attributes jsonb NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz NOT NULL DEFAULT 'infinity',
    is_current boolean NOT NULL DEFAULT true,

    CONSTRAINT ck_dim_product_period
        CHECK (valid_to > valid_from),

    UNIQUE (product_id, valid_from)
);

CREATE UNIQUE INDEX ux_dim_product_current
ON core.dim_product(product_id)
WHERE is_current;

CREATE TABLE core.load_log (
    id bigserial PRIMARY KEY,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    source text NOT NULL,
    batch_id bigint,
    rows_affected integer
);

GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA core TO admin;

GRANT USAGE, SELECT
ON ALL SEQUENCES IN SCHEMA core TO admin;
