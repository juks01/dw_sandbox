CREATE ROLE admin LOGIN PASSWORD 'admin';

CREATE ROLE reporting LOGIN PASSWORD 'reporting';

GRANT pg_read_all_data TO admin;
ALTER ROLE admin BYPASSRLS;

CREATE SCHEMA mart AUTHORIZATION admin;

SET ROLE admin;

CREATE TABLE mart.product (
    product_id integer PRIMARY KEY
);

ALTER TABLE mart.product ENABLE ROW LEVEL SECURITY;

GRANT USAGE ON SCHEMA mart TO reporting;
GRANT SELECT ON mart.product TO reporting;

CREATE POLICY product_read
ON mart.product
FOR SELECT
TO reporting
USING (true);

RESET ROLE;
