-- Generic staging -> core SCD2 sync mechanism, plus the fixed users sync.
-- SECURITY DEFINER with a locked-down search_path (per spec section 7);
-- EXECUTE is revoked from PUBLIC and reader roles, granted only to
-- core_service (used by the orchestrator) and the schema owner (admin).

-- =======================================================================
-- core.sync_users(): users_ext.department / users_ext.end_user -> dims
-- =======================================================================
CREATE OR REPLACE FUNCTION core.sync_users()
RETURNS TABLE(entity text, upserted_count bigint, closed_count bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = core, users_ext, pg_temp
AS $fn$
DECLARE
    rec RECORD;
    cur_hash TEXT;
    v_dept_upserts bigint := 0;
    v_dept_closed bigint := 0;
    v_user_upserts bigint := 0;
    v_user_closed bigint := 0;
    v_rowcount bigint;
BEGIN
    EXECUTE 'DROP SCHEMA IF EXISTS users_ext CASCADE';
    EXECUTE 'CREATE SCHEMA users_ext';
    EXECUTE 'IMPORT FOREIGN SCHEMA users FROM SERVER users_srv INTO users_ext';
    EXECUTE 'GRANT ALL PRIVILEGES ON SCHEMA users_ext TO core_service';
    EXECUTE 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA users_ext TO core_service';

    -- ---- departments ----
    UPDATE core.dim_department d
    SET valid_to = now(), is_current = false
    WHERE d.is_current
      AND NOT EXISTS (SELECT 1 FROM users_ext.department s WHERE s.department_id = d.department_id);
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_dept_closed := v_dept_closed + v_rowcount;

    FOR rec IN
        SELECT s.department_id, s.code, s.name,
               md5(row(s.code, s.name)::text) AS h
        FROM users_ext.department s
    LOOP
        SELECT _content_hash INTO cur_hash
        FROM core.dim_department
        WHERE department_id = rec.department_id AND is_current;

        IF cur_hash IS NULL THEN
            INSERT INTO core.dim_department
                (department_id, code, name, valid_from, valid_to, is_current, _content_hash)
            VALUES (rec.department_id, rec.code, rec.name, now(), NULL, true, rec.h);
            v_dept_upserts := v_dept_upserts + 1;
        ELSIF cur_hash <> rec.h THEN
            UPDATE core.dim_department
            SET valid_to = now(), is_current = false
            WHERE department_id = rec.department_id AND is_current;

            INSERT INTO core.dim_department
                (department_id, code, name, valid_from, valid_to, is_current, _content_hash)
            VALUES (rec.department_id, rec.code, rec.name, now(), NULL, true, rec.h);
            v_dept_upserts := v_dept_upserts + 1;
        END IF;
    END LOOP;

    entity := 'department'; upserted_count := v_dept_upserts; closed_count := v_dept_closed;
    RETURN NEXT;

    -- ---- end users ----
    UPDATE core.dim_user d
    SET valid_to = now(), is_current = false
    WHERE d.is_current
      AND NOT EXISTS (SELECT 1 FROM users_ext.end_user s WHERE s.user_id = d.user_id);
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_user_closed := v_user_closed + v_rowcount;

    FOR rec IN
        SELECT s.user_id, s.username, s.full_name, s.department_id, s.is_active,
               md5(row(s.username, s.full_name, s.department_id, s.is_active)::text) AS h
        FROM users_ext.end_user s
    LOOP
        SELECT _content_hash INTO cur_hash
        FROM core.dim_user
        WHERE user_id = rec.user_id AND is_current;

        IF cur_hash IS NULL THEN
            INSERT INTO core.dim_user
                (user_id, username, full_name, department_id, is_active, valid_from, valid_to, is_current, _content_hash)
            VALUES (rec.user_id, rec.username, rec.full_name, rec.department_id, rec.is_active, now(), NULL, true, rec.h);
            v_user_upserts := v_user_upserts + 1;
        ELSIF cur_hash <> rec.h THEN
            UPDATE core.dim_user
            SET valid_to = now(), is_current = false
            WHERE user_id = rec.user_id AND is_current;

            INSERT INTO core.dim_user
                (user_id, username, full_name, department_id, is_active, valid_from, valid_to, is_current, _content_hash)
            VALUES (rec.user_id, rec.username, rec.full_name, rec.department_id, rec.is_active, now(), NULL, true, rec.h);
            v_user_upserts := v_user_upserts + 1;
        END IF;
    END LOOP;
END;
$fn$;

REVOKE ALL ON FUNCTION core.sync_users() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION core.sync_users() TO core_service;

-- =======================================================================
-- core.sync_from_staging(): generic staging.<table> -> core.dim_<table>
-- Business key: the "id" column when present, otherwise an md5 hash of
-- every source column. Content hash: md5 of every source column (this is
-- what drives change detection; technical/SCD columns are never hashed).
-- Deletion policy: identical to sync_users() above -- close, don't reinsert.
-- Identifiers are only ever built with format(%I/%L) from information_schema,
-- never from raw user/source input, and are additionally normalized.
--
-- Returns one row per staging table it processed. Callers (the
-- orchestrator) compare this against the tables the loader just wrote to,
-- so a table that silently fails to make it into core turns into a real
-- pipeline error instead of a quiet no-op.
-- =======================================================================
CREATE OR REPLACE FUNCTION core.sync_from_staging()
RETURNS TABLE(source_table text, dim_table text, inserted_count bigint, closed_count bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = core, staging_ext, pg_temp
AS $fn$
DECLARE
    tbl RECORD;
    col RECORD;
    dim_name TEXT;
    col_list TEXT;
    bk_expr TEXT;
    hash_expr TEXT;
    has_id BOOLEAN;
    sql TEXT;
    v_closed1 bigint;
    v_closed2 bigint;
    v_inserted bigint;
BEGIN
    EXECUTE 'DROP SCHEMA IF EXISTS staging_ext CASCADE';
    EXECUTE 'CREATE SCHEMA staging_ext';
    EXECUTE 'IMPORT FOREIGN SCHEMA staging FROM SERVER staging_srv INTO staging_ext';
    EXECUTE 'GRANT ALL PRIVILEGES ON SCHEMA staging_ext TO core_service';
    EXECUTE 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging_ext TO core_service';

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging_ext'
          AND table_name <> 'raw_batches'
        ORDER BY table_name
    LOOP
        dim_name := 'dim_' || regexp_replace(lower(tbl.table_name), '[^a-z0-9_]', '_', 'g');
        dim_name := left(dim_name, 63);
        -- PostgreSQL identifiers can't start with a digit.
        IF dim_name ~ '^[0-9]' THEN
            dim_name := 't_' || dim_name;
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'core' AND table_name = dim_name
        ) THEN
            EXECUTE format('CREATE TABLE core.%I (LIKE staging_ext.%I INCLUDING DEFAULTS)', dim_name, tbl.table_name);
            EXECUTE format('ALTER TABLE core.%I ADD COLUMN _sk BIGSERIAL PRIMARY KEY', dim_name);
            EXECUTE format('ALTER TABLE core.%I ADD COLUMN _business_key TEXT', dim_name);
            EXECUTE format('ALTER TABLE core.%I ADD COLUMN valid_from TIMESTAMPTZ NOT NULL DEFAULT now()', dim_name);
            EXECUTE format('ALTER TABLE core.%I ADD COLUMN valid_to TIMESTAMPTZ', dim_name);
            EXECUTE format('ALTER TABLE core.%I ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT true', dim_name);
            EXECUTE format('ALTER TABLE core.%I ADD COLUMN _content_hash TEXT', dim_name);
            EXECUTE format('CREATE INDEX %I ON core.%I (_business_key, is_current)', dim_name || '_bk_idx', dim_name);
        ELSE
            -- Adapt to new source columns; existing dim columns are kept
            -- (they simply stay empty for rows loaded before the change).
            FOR col IN
                SELECT column_name, udt_name
                FROM information_schema.columns
                WHERE table_schema = 'staging_ext' AND table_name = tbl.table_name
            LOOP
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'core' AND table_name = dim_name AND column_name = col.column_name
                ) THEN
                    EXECUTE format('ALTER TABLE core.%I ADD COLUMN %I %s', dim_name, col.column_name, col.udt_name);
                END IF;
            END LOOP;
        END IF;

        SELECT string_agg(format('%I', column_name), ', ' ORDER BY ordinal_position)
        INTO col_list
        FROM information_schema.columns
        WHERE table_schema = 'staging_ext' AND table_name = tbl.table_name;

        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'staging_ext' AND table_name = tbl.table_name AND column_name = 'id'
        ) INTO has_id;

        hash_expr := format('md5((%s)::text)', col_list);
        bk_expr := CASE WHEN has_id THEN 'id::text' ELSE hash_expr END;

        -- 1) close current dim rows whose content changed in the source
        sql := format(
            'UPDATE core.%I d SET valid_to = now(), is_current = false
             WHERE d.is_current AND EXISTS (
               SELECT 1 FROM staging_ext.%I s
               WHERE (%s) = d._business_key AND (%s) <> d._content_hash
             )',
            dim_name, tbl.table_name, bk_expr, hash_expr
        );
        EXECUTE sql;
        GET DIAGNOSTICS v_closed1 = ROW_COUNT;

        -- 2) close current dim rows whose business key disappeared from the source
        sql := format(
            'UPDATE core.%I d SET valid_to = now(), is_current = false
             WHERE d.is_current AND NOT EXISTS (
               SELECT 1 FROM staging_ext.%I s WHERE (%s) = d._business_key
             )',
            dim_name, tbl.table_name, bk_expr
        );
        EXECUTE sql;
        GET DIAGNOSTICS v_closed2 = ROW_COUNT;

        -- 3) insert current versions for anything not already current
        --    (covers brand-new business keys and rows just closed in step 1)
        sql := format(
            'INSERT INTO core.%I (%s, _business_key, valid_from, valid_to, is_current, _content_hash)
             SELECT %s, (%s), now(), NULL, true, (%s)
             FROM staging_ext.%I s
             WHERE NOT EXISTS (
               SELECT 1 FROM core.%I d WHERE d.is_current AND d._business_key = (%s)
             )',
            dim_name, col_list, col_list, bk_expr, hash_expr, tbl.table_name, dim_name, bk_expr
        );
        EXECUTE sql;
        GET DIAGNOSTICS v_inserted = ROW_COUNT;

        source_table := tbl.table_name;
        dim_table := dim_name;
        inserted_count := v_inserted;
        closed_count := v_closed1 + v_closed2;
        RETURN NEXT;
    END LOOP;
END;
$fn$;

REVOKE ALL ON FUNCTION core.sync_from_staging() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION core.sync_from_staging() TO core_service;
