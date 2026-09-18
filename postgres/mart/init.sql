-- Mart = easily reportable "current state" layer. Tables are (re)built by
-- mart.refresh() (see procedures.sql) from core_ext.dim_* (imported via FDW
-- in init.sh). Only is_current = true rows are published; SCD technical
-- columns are dropped.

CREATE SCHEMA IF NOT EXISTS mart AUTHORIZATION CURRENT_USER;

GRANT USAGE ON SCHEMA mart TO reporting;
-- SELECT grants on individual mart tables are (re)issued by mart.refresh()
-- every time it (re)creates a table, since the table list is dynamic.

GRANT USAGE ON SCHEMA mart TO mart_service;
GRANT ALL PRIVILEGES ON SCHEMA mart TO CURRENT_USER;
GRANT ALL PRIVILEGES ON SCHEMA core_ext TO CURRENT_USER, mart_service;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA core_ext TO CURRENT_USER, mart_service;

-- reporting must never reach core_ext, staging or users directly.
REVOKE ALL ON SCHEMA core_ext FROM reporting;
REVOKE ALL ON SCHEMA public FROM reporting;
