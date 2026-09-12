CREATE ROLE admin LOGIN PASSWORD 'admin';

GRANT pg_read_all_data TO admin;
ALTER ROLE admin BYPASSRLS;

-- admin does not own the "core" database, so without this it cannot
-- CREATE SCHEMA staging_ext / users_ext at runtime inside the sync
-- procedures below.
GRANT CREATE ON DATABASE core TO admin;

CREATE SCHEMA core AUTHORIZATION admin;

CREATE TABLE core.load_log (
    id bigserial PRIMARY KEY,
    run_at timestamptz NOT NULL DEFAULT now(),
    source text NOT NULL,
    batch_count integer NOT NULL DEFAULT 0
);

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-------------------------------------------------------------------------------
-- STAGING FDW
-------------------------------------------------------------------------------

CREATE SERVER staging_srv
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host 'staging',
        dbname 'staging',
        port '5432'
    );

CREATE USER MAPPING FOR admin
    SERVER staging_srv
    OPTIONS (
        user 'admin',
        password 'admin'
    );

GRANT USAGE
    ON FOREIGN SERVER staging_srv
    TO admin;

-------------------------------------------------------------------------------
-- USERS FDW
-------------------------------------------------------------------------------

CREATE SERVER users_srv
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host 'users',
        dbname 'users',
        port '5432'
    );

CREATE USER MAPPING FOR admin
    SERVER users_srv
    OPTIONS (
        user 'core_reader',
        password 'core_reader'
    );

GRANT USAGE
    ON FOREIGN SERVER users_srv
    TO admin;

-------------------------------------------------------------------------------
-- GENERIC STAGING -> CORE SCD2
-------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE core.sync_from_staging()
LANGUAGE plpgsql
AS $proc$
DECLARE
    tbl record;
    col record;

    dim_table text;

    has_id boolean;
    has_parent boolean;

    key_expr text;
    col_list text;

    load_time timestamptz := now();
BEGIN
    DROP SCHEMA IF EXISTS staging_ext CASCADE;

    CREATE SCHEMA staging_ext;

    IMPORT FOREIGN SCHEMA staging
    FROM SERVER staging_srv
    INTO staging_ext;

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging_ext'
          AND table_name <> 'raw_batches'
    LOOP

        dim_table := 'dim_' || tbl.table_name;

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS core.%I (
                _sk bigserial PRIMARY KEY,
                _key text NOT NULL,
                _hash text NOT NULL,
                valid_from timestamptz NOT NULL,
                valid_to timestamptz NOT NULL DEFAULT ''infinity'',
                is_current boolean NOT NULL DEFAULT true
            )',
            dim_table
        );

        EXECUTE format(
            'CREATE UNIQUE INDEX IF NOT EXISTS %I
             ON core.%I(_key)
             WHERE is_current',
            'ux_' || dim_table || '_current',
            dim_table
        );

        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'staging_ext'
              AND table_name = tbl.table_name
              AND column_name = 'id'
        )
        INTO has_id;

        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'staging_ext'
              AND table_name = tbl.table_name
              AND column_name = '_parent_id'
        )
        INTO has_parent;

        IF has_parent THEN

            key_expr := format(
                '(_parent_id::text || '':'' || %s)',
                CASE
                    WHEN has_id
                    THEN 'COALESCE(id::text, _row_index::text)'
                    ELSE '_row_index::text'
                END
            );

        ELSE

            key_expr :=
                CASE
                    WHEN has_id THEN 'id::text'
                    ELSE '_row_id::text'
                END;

        END IF;

        ---------------------------------------------------------------------
        -- Add source columns dynamically.
        --
        -- Internal staging metadata is retained where useful, but _row_id is
        -- not copied into the business columns.
        ---------------------------------------------------------------------

        FOR col IN
            SELECT
                column_name,
                data_type
            FROM information_schema.columns
            WHERE table_schema = 'staging_ext'
              AND table_name = tbl.table_name
              AND column_name NOT IN ('_row_id')
        LOOP

            EXECUTE format(
                'ALTER TABLE core.%I
                 ADD COLUMN IF NOT EXISTS %I %s',
                dim_table,
                col.column_name,
                col.data_type
            );

        END LOOP;

        SELECT string_agg(
            quote_ident(column_name),
            ', '
            ORDER BY ordinal_position
        )
        INTO col_list
        FROM information_schema.columns
        WHERE table_schema = 'staging_ext'
          AND table_name = tbl.table_name
          AND column_name <> '_row_id';

        ---------------------------------------------------------------------
        -- Close changed current versions.
        ---------------------------------------------------------------------

        EXECUTE format(
            'UPDATE core.%I d
             SET
                 valid_to = %L,
                 is_current = false
             FROM (
                 SELECT
                     %s AS _key,
                     md5(row(%s)::text) AS _hash
                 FROM staging_ext.%I
             ) s
             WHERE d._key = s._key
               AND d.is_current
               AND d._hash IS DISTINCT FROM s._hash',
            dim_table,
            load_time,
            key_expr,
            col_list,
            tbl.table_name
        );

        ---------------------------------------------------------------------
        -- Insert new and changed versions.
        ---------------------------------------------------------------------

        EXECUTE format(
            'INSERT INTO core.%I (
                 _key,
                 _hash,
                 valid_from,
                 %s
             )
             SELECT
                 %s,
                 md5(row(%s)::text),
                 %L,
                 %s
             FROM staging_ext.%I s
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM core.%I d
                 WHERE d._key = %s
                   AND d.is_current
             )',
            dim_table,
            col_list,
            key_expr,
            col_list,
            load_time,
            col_list,
            tbl.table_name,
            dim_table,
            key_expr
        );

    END LOOP;

    INSERT INTO core.load_log (
        source,
        batch_count
    )
    SELECT
        'staging_ext',
        count(*)
    FROM staging_ext.raw_batches;

END;
$proc$;

-------------------------------------------------------------------------------
-- USERS -> CORE SCD2
-------------------------------------------------------------------------------

CREATE TABLE core.dim_user (
    _sk bigserial PRIMARY KEY,
    user_id integer NOT NULL,
    username text NOT NULL,
    full_name text,
    department_id integer,
    is_active boolean NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz NOT NULL DEFAULT 'infinity',
    is_current boolean NOT NULL DEFAULT true
);

CREATE UNIQUE INDEX ux_dim_user_current
    ON core.dim_user(user_id)
    WHERE is_current;

CREATE TABLE core.dim_department (
    _sk bigserial PRIMARY KEY,
    department_id integer NOT NULL,
    code text NOT NULL,
    name text NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz NOT NULL DEFAULT 'infinity',
    is_current boolean NOT NULL DEFAULT true
);

CREATE UNIQUE INDEX ux_dim_department_current
    ON core.dim_department(department_id)
    WHERE is_current;

-- load_log, dim_user and dim_department above are created by the bootstrap
-- superuser while this init script runs, so admin does not own them even
-- though it owns the "core" schema. Without these grants, core.sync_users()
-- and core.sync_from_staging() cannot INSERT/UPDATE into them at runtime.
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA core
    TO admin;

GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA core
    TO admin;

CREATE OR REPLACE PROCEDURE core.sync_users()
LANGUAGE plpgsql
AS $proc$
DECLARE
    load_time timestamptz := now();
BEGIN

    DROP SCHEMA IF EXISTS users_ext CASCADE;

    CREATE SCHEMA users_ext;

    IMPORT FOREIGN SCHEMA users
    FROM SERVER users_srv
    INTO users_ext;

    -----------------------------------------------------------------------
    -- Departments
    -----------------------------------------------------------------------

    UPDATE core.dim_department d
    SET
        valid_to = load_time,
        is_current = false
    FROM users_ext.department s
    WHERE d.department_id = s.department_id
      AND d.is_current
      AND (
          d.code IS DISTINCT FROM s.code
          OR d.name IS DISTINCT FROM s.name
      );

    INSERT INTO core.dim_department (
        department_id,
        code,
        name,
        valid_from
    )
    SELECT
        s.department_id,
        s.code,
        s.name,
        load_time
    FROM users_ext.department s
    WHERE NOT EXISTS (
        SELECT 1
        FROM core.dim_department d
        WHERE d.department_id = s.department_id
          AND d.is_current
    );

    -----------------------------------------------------------------------
    -- Users
    -----------------------------------------------------------------------

    UPDATE core.dim_user d
    SET
        valid_to = load_time,
        is_current = false
    FROM users_ext.end_user s
    WHERE d.user_id = s.user_id
      AND d.is_current
      AND (
          d.username IS DISTINCT FROM s.username
          OR d.full_name IS DISTINCT FROM s.full_name
          OR d.department_id IS DISTINCT FROM s.department_id
          OR d.is_active IS DISTINCT FROM s.is_active
      );

    INSERT INTO core.dim_user (
        user_id,
        username,
        full_name,
        department_id,
        is_active,
        valid_from
    )
    SELECT
        s.user_id,
        s.username,
        s.full_name,
        s.department_id,
        s.is_active,
        load_time
    FROM users_ext.end_user s
    WHERE NOT EXISTS (
        SELECT 1
        FROM core.dim_user d
        WHERE d.user_id = s.user_id
          AND d.is_current
    );

    -----------------------------------------------------------------------
    -- Handle deletes from users source.
    --
    -- If a source row disappears, close the current SCD2 version.
    -----------------------------------------------------------------------

    UPDATE core.dim_user d
    SET
        valid_to = load_time,
        is_current = false
    WHERE d.is_current
      AND NOT EXISTS (
          SELECT 1
          FROM users_ext.end_user s
          WHERE s.user_id = d.user_id
      );

    UPDATE core.dim_department d
    SET
        valid_to = load_time,
        is_current = false
    WHERE d.is_current
      AND NOT EXISTS (
          SELECT 1
          FROM users_ext.department s
          WHERE s.department_id = d.department_id
      );

    INSERT INTO core.load_log (
        source,
        batch_count
    )
    VALUES (
        'users',
        1
    );

END;
$proc$;
