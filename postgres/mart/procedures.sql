-- mart.refresh(): rebuilds mart.<name> for every core_ext.dim_<name> table,
-- keeping only is_current = true rows and dropping the SCD technical
-- columns before publishing to the reporting role.
--
-- All identifiers used in dynamic SQL come from information_schema (never
-- from raw user input) and are always passed through format()'s %I/%L.

CREATE OR REPLACE FUNCTION mart.refresh()
RETURNS TABLE(dim_table text, mart_table text, row_count bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = mart, core_ext, pg_temp
AS $fn$
DECLARE
    tbl RECORD;
    mart_name TEXT;
    col_list TEXT;
    v_count bigint;
BEGIN
    EXECUTE 'DROP SCHEMA IF EXISTS core_ext CASCADE';
    EXECUTE 'CREATE SCHEMA core_ext';
    EXECUTE 'IMPORT FOREIGN SCHEMA core FROM SERVER core_srv INTO core_ext';
    EXECUTE 'GRANT ALL PRIVILEGES ON SCHEMA core_ext TO mart_service';
    EXECUTE 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA core_ext TO mart_service';
    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'core_ext' AND table_name LIKE 'dim\_%' ESCAPE '\'
        ORDER BY table_name
    LOOP
        mart_name := regexp_replace(lower(substring(tbl.table_name from 5)), '[^a-z0-9_]', '_', 'g');
        mart_name := left(mart_name, 63);
        IF mart_name ~ '^[0-9]' THEN
            mart_name := 't_' || mart_name;
        END IF;
        IF mart_name = '' THEN
            CONTINUE;
        END IF;

        SELECT string_agg(format('%I', column_name), ', ' ORDER BY ordinal_position)
        INTO col_list
        FROM information_schema.columns
        WHERE table_schema = 'core_ext' AND table_name = tbl.table_name
          AND column_name NOT IN ('_sk', 'valid_from', 'valid_to', 'is_current', '_content_hash', '_business_key');

        IF col_list IS NULL THEN
            CONTINUE;
        END IF;

        EXECUTE format('DROP TABLE IF EXISTS mart.%I', mart_name);
        EXECUTE format(
            'CREATE TABLE mart.%I AS SELECT %s FROM core_ext.%I WHERE is_current = true',
            mart_name, col_list, tbl.table_name
        );
        EXECUTE format('ALTER TABLE mart.%I OWNER TO CURRENT_USER', mart_name);
        EXECUTE format('GRANT SELECT ON mart.%I TO reporting', mart_name);
        EXECUTE format('SELECT count(*) FROM mart.%I', mart_name) INTO v_count;

        dim_table := tbl.table_name;
        mart_table := mart_name;
        row_count := v_count;
        RETURN NEXT;
    END LOOP;
END;
$fn$;

REVOKE ALL ON FUNCTION mart.refresh() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION mart.refresh() TO mart_service;
