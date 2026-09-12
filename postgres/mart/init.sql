CREATE ROLE admin LOGIN PASSWORD 'admin';
CREATE ROLE reporting LOGIN PASSWORD 'reporting';

GRANT pg_read_all_data TO admin;
ALTER ROLE admin BYPASSRLS;

-- admin does not own the "mart" database, so without this it cannot
-- CREATE SCHEMA core_ext at runtime inside mart.refresh() below.
GRANT CREATE ON DATABASE mart TO admin;

CREATE SCHEMA mart AUTHORIZATION admin;

GRANT USAGE
    ON SCHEMA mart
    TO reporting;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER core_srv
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host 'core',
        dbname 'core',
        port '5432'
    );

CREATE USER MAPPING FOR admin
    SERVER core_srv
    OPTIONS (
        user 'admin',
        password 'admin'
    );

GRANT USAGE
    ON FOREIGN SERVER core_srv
    TO admin;

CREATE OR REPLACE PROCEDURE mart.refresh()
LANGUAGE plpgsql
AS $proc$
DECLARE
    tbl record;
    col record;

    target text;
    col_list text;
BEGIN

    DROP SCHEMA IF EXISTS core_ext CASCADE;

    CREATE SCHEMA core_ext;

    IMPORT FOREIGN SCHEMA core
    FROM SERVER core_srv
    INTO core_ext;

    -----------------------------------------------------------------------
    -- Generic current-state projection of dim_* tables.
    -----------------------------------------------------------------------

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'core_ext'
          AND table_name LIKE 'dim\_%'
    LOOP

        target := regexp_replace(
            tbl.table_name,
            '^dim_',
            ''
        );

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS mart.%I ()',
            target
        );

        FOR col IN
            SELECT
                column_name,
                data_type
            FROM information_schema.columns
            WHERE table_schema = 'core_ext'
              AND table_name = tbl.table_name
              AND column_name NOT IN (
                  '_sk',
                  '_key',
                  '_hash',
                  'valid_from',
                  'valid_to',
                  'is_current'
              )
        LOOP

            EXECUTE format(
                'ALTER TABLE mart.%I
                 ADD COLUMN IF NOT EXISTS %I %s',
                target,
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
        WHERE table_schema = 'core_ext'
          AND table_name = tbl.table_name
          AND column_name NOT IN (
              '_sk',
              '_key',
              '_hash',
              'valid_from',
              'valid_to',
              'is_current'
          );

        ---------------------------------------------------------------------
        -- Reporting gets SELECT only.
        ---------------------------------------------------------------------

        EXECUTE format(
            'ALTER TABLE mart.%I ENABLE ROW LEVEL SECURITY',
            target
        );

        IF NOT EXISTS (
            SELECT 1
            FROM pg_policies
            WHERE schemaname = 'mart'
              AND tablename = target
              AND policyname = 'reporting_read'
        ) THEN

            EXECUTE format(
                'CREATE POLICY reporting_read
                 ON mart.%I
                 FOR SELECT
                 TO reporting
                 USING (true)',
                target
            );

        END IF;

        EXECUTE format(
            'GRANT SELECT ON mart.%I TO reporting',
            target
        );

        ---------------------------------------------------------------------
        -- admin owns the mart and refreshes it.
        ---------------------------------------------------------------------

        EXECUTE format(
            'GRANT SELECT, INSERT, UPDATE, DELETE
             ON mart.%I TO admin',
            target
        );

        EXECUTE format(
            'TRUNCATE mart.%I',
            target
        );

        EXECUTE format(
            'INSERT INTO mart.%I (%s)
             SELECT %s
             FROM core_ext.%I
             WHERE is_current',
            target,
            col_list,
            col_list,
            tbl.table_name
        );

    END LOOP;

END;
$proc$;
