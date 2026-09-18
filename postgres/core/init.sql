-- Core = historized dimensional layer (SCD Type 2).
-- FDW servers, user mappings and the staging_ext/users_ext foreign-table
-- mirrors are created in init.sh (they need secret credentials from the
-- environment). This file only contains static structural SQL and uses
-- the fixed role-naming convention from .env (role *names* are not secret,
-- only their passwords are -- those live in init.sh).

CREATE SCHEMA IF NOT EXISTS core AUTHORIZATION CURRENT_USER;

-- ---------------------------------------------------------------------
-- Dimensional tables for the users master data (fixed, known shape).
-- Populated/merged by core.sync_users() -- see procedures.sql.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.dim_department (
    _sk             BIGSERIAL PRIMARY KEY,
    department_id   INTEGER NOT NULL,
    code            TEXT NOT NULL,
    name            TEXT NOT NULL,
    valid_from      TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to        TIMESTAMPTZ,
    is_current      BOOLEAN NOT NULL DEFAULT true,
    _content_hash   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dim_department_bk ON core.dim_department (department_id, is_current);

CREATE TABLE IF NOT EXISTS core.dim_user (
    _sk             BIGSERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    username        TEXT NOT NULL,
    full_name       TEXT NOT NULL,
    department_id   INTEGER,
    is_active       BOOLEAN NOT NULL,
    valid_from      TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to        TIMESTAMPTZ,
    is_current      BOOLEAN NOT NULL DEFAULT true,
    _content_hash   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dim_user_bk ON core.dim_user (user_id, is_current);

-- ---------------------------------------------------------------------
-- Deletion policy (explicit, per spec section 7):
-- when a business key present as is_current=true in a dimension no longer
-- appears in the source, the sync procedures CLOSE the current version
-- (valid_to = now(), is_current = false) and insert NO replacement row.
-- The row is preserved for history but no longer shows up as current, so
-- it disappears from mart.* (which only ever selects is_current = true).
-- ---------------------------------------------------------------------

-- ---------------------------------------------------------------------
-- Row Level Security demo: restrict core.dim_user to the caller's own
-- department, driven by a session GUC that is derived from users data.
-- Set the GUC with:  SELECT set_config('core.department_code', 'ENG', false);
-- '', NULL or 'ALL' (or bypassing via a non-RLS role) removes the filter.
-- ---------------------------------------------------------------------
ALTER TABLE core.dim_user ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.dim_user FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS dim_user_department_isolation ON core.dim_user;
CREATE POLICY dim_user_department_isolation ON core.dim_user
    USING (
        current_setting('core.department_code', true) IS NULL
        OR current_setting('core.department_code', true) = ''
        OR current_setting('core.department_code', true) = 'ALL'
        OR department_id = (
            SELECT department_id FROM core.dim_department d
            WHERE d.code = current_setting('core.department_code', true)
              AND d.is_current = true
            LIMIT 1
        )
    );

-- ---------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------
GRANT USAGE ON SCHEMA core TO core_reader, core_service;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO core_reader, core_service;
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO core_reader, core_service;

GRANT ALL PRIVILEGES ON SCHEMA core TO CURRENT_USER;
GRANT ALL PRIVILEGES ON SCHEMA staging_ext TO CURRENT_USER, core_service;
GRANT ALL PRIVILEGES ON SCHEMA users_ext TO CURRENT_USER, core_service;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging_ext TO CURRENT_USER, core_service;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA users_ext TO CURRENT_USER, core_service;
