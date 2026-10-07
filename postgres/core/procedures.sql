-- Generic staging -> core SCD2 sync mechanism, plus the fixed users sync.
-- SECURITY DEFINER with a locked-down search_path (per spec section 7);
-- EXECUTE is revoked from PUBLIC and reader roles, granted only to
-- core_service (used by the orchestrator) and the schema owner (admin).

-- =======================================================================
-- core.sync_users(mode): users_ext.department / users_ext.end_user -> dims
-- =======================================================================
DROP FUNCTION IF EXISTS core.sync_users();
DROP FUNCTION IF EXISTS core.sync_users(text);
CREATE OR REPLACE FUNCTION core.sync_users(
    p_load_mode text DEFAULT 'full_snapshot',
    p_delete_policy text DEFAULT 'close_on_full_snapshot'
)
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
BEGIN
    IF p_load_mode IS NULL OR p_load_mode NOT IN ('full_snapshot', 'incremental_upsert') THEN
        RAISE EXCEPTION 'unsupported load mode: %', p_load_mode;
    END IF;
    IF p_delete_policy IS NULL OR p_delete_policy NOT IN ('close_on_full_snapshot', 'never_close') THEN
        RAISE EXCEPTION 'unsupported delete policy: %', p_delete_policy;
    END IF;

    EXECUTE 'DROP SCHEMA IF EXISTS users_ext CASCADE';
    EXECUTE 'CREATE SCHEMA users_ext';
    EXECUTE 'IMPORT FOREIGN SCHEMA users FROM SERVER users_srv INTO users_ext';
    EXECUTE 'GRANT ALL PRIVILEGES ON SCHEMA users_ext TO core_service';
    EXECUTE 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA users_ext TO core_service';

    -- ---- departments ----
    UPDATE core.dim_department d
    SET valid_to = now(), is_current = false
    WHERE p_load_mode = 'full_snapshot'
      AND p_delete_policy = 'close_on_full_snapshot' AND d.is_current
      AND NOT EXISTS (SELECT 1 FROM users_ext.department s WHERE s.department_id = d.department_id);
    GET DIAGNOSTICS v_dept_closed = ROW_COUNT;

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
    WHERE p_load_mode = 'full_snapshot'
      AND p_delete_policy = 'close_on_full_snapshot' AND d.is_current
      AND NOT EXISTS (SELECT 1 FROM users_ext.end_user s WHERE s.user_id = d.user_id);
    GET DIAGNOSTICS v_user_closed = ROW_COUNT;

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

REVOKE ALL ON FUNCTION core.sync_users(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION core.sync_users(text, text) TO core_service;

-- =======================================================================
-- core.sync_from_staging(): one explicit staging batch -> core.dim_<table>
-- Business key: configured keys when present on a table, otherwise "id" when present.
-- Content hashes detect changes but are not used as business keys. Full snapshots close missing keys.
-- Identifiers are only ever built with format(%I/%L) from information_schema,
-- never from raw user/source input, and are additionally normalized.
--
-- Returns one row per staging table it processed. Callers (the
-- orchestrator) compare this against the tables the loader just wrote to,
-- so a table that silently fails to make it into core turns into a real
-- pipeline error instead of a quiet no-op.
-- =======================================================================
DROP FUNCTION IF EXISTS core.sync_from_staging();
DROP FUNCTION IF EXISTS core.sync_from_staging(bigint, text, text, text[]);
CREATE OR REPLACE FUNCTION core.sync_from_staging(
    p_batch_id bigint, p_source text, p_load_mode text, p_delete_policy text,
    p_key_fields text[]
)
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
    data_col_list TEXT;
    bk_expr TEXT;
    hash_expr TEXT;
    has_id BOOLEAN;
    sql TEXT;
    v_batch_source TEXT;
    v_batch_mode TEXT;
    v_batch_delete_policy TEXT;
    v_pagination_complete BOOLEAN;
    v_batch_tables TEXT[];
    v_in_batch BOOLEAN;
    v_key_count INTEGER;
    normalized_keys TEXT[];
    v_key_fields_found BOOLEAN := false;
    v_has_batch_rows BOOLEAN := false;
    root_table TEXT;
    v_closed1 bigint := 0;
    v_closed2 bigint := 0;
    v_inserted bigint := 0;
BEGIN
    IF p_load_mode IS NULL OR p_load_mode NOT IN ('full_snapshot', 'incremental_upsert') THEN
        RAISE EXCEPTION 'unsupported load mode: %', p_load_mode;
    END IF;
    IF p_delete_policy IS NULL OR p_delete_policy NOT IN ('close_on_full_snapshot', 'never_close') THEN
        RAISE EXCEPTION 'unsupported delete policy: %', p_delete_policy;
    END IF;

    root_table := regexp_replace(lower(p_source), '[^a-z0-9_]', '_', 'g');
    root_table := regexp_replace(root_table, '_+', '_', 'g');
    root_table := trim(both '_' from root_table);
    IF root_table = '' THEN
        root_table := 'col';
    END IF;
    IF root_table ~ '^[0-9]' THEN
        root_table := 'c_' || root_table;
    END IF;
    root_table := left(root_table, 63);

    EXECUTE 'DROP SCHEMA IF EXISTS staging_ext CASCADE';
    EXECUTE 'CREATE SCHEMA staging_ext';
    EXECUTE 'IMPORT FOREIGN SCHEMA staging FROM SERVER staging_srv INTO staging_ext';
    EXECUTE 'GRANT ALL PRIVILEGES ON SCHEMA staging_ext TO core_service';
    EXECUTE 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging_ext TO core_service';

    SELECT b.source, b.load_mode, b.delete_policy, b.pagination_complete,
           ARRAY(SELECT jsonb_array_elements_text(b.table_names))
    INTO v_batch_source, v_batch_mode, v_batch_delete_policy, v_pagination_complete, v_batch_tables
    FROM staging_ext.raw_batches b
    WHERE b.id = p_batch_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'staging batch % does not exist', p_batch_id;
    END IF;
    IF v_batch_source IS DISTINCT FROM p_source OR v_batch_mode IS DISTINCT FROM p_load_mode
       OR v_batch_delete_policy IS DISTINCT FROM p_delete_policy THEN
        RAISE EXCEPTION 'staging batch % source/mode metadata does not match request', p_batch_id;
    END IF;
    IF NOT v_pagination_complete THEN
        RAISE EXCEPTION 'staging batch % is incomplete; refusing core synchronization', p_batch_id;
    END IF;

    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging_ext'
          AND table_name <> 'raw_batches'
          AND (table_name = ANY(v_batch_tables) OR table_name = root_table)
        ORDER BY table_name
    LOOP
        dim_name := 'dim_' || regexp_replace(lower(tbl.table_name), '[^a-z0-9_]', '_', 'g');
        dim_name := left(dim_name, 63);
        -- PostgreSQL identifiers can't start with a digit.
        IF dim_name ~ '^[0-9]' THEN
            dim_name := 't_' || dim_name;
        END IF;

        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM staging_ext.%I WHERE _source_batch_id = $1)',
            tbl.table_name
        ) INTO v_in_batch USING p_batch_id;
        v_has_batch_rows := v_has_batch_rows OR v_in_batch;
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

        SELECT COALESCE(
            string_agg(format('s.%I', column_name), ', ' ORDER BY ordinal_position),
            'NULL::text'
        )
        INTO data_col_list
        FROM information_schema.columns
        WHERE table_schema = 'staging_ext'
          AND table_name = tbl.table_name
          AND column_name NOT IN ('_row_id', '_source_batch_id', '_parent_id', '_row_index');

        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'staging_ext' AND table_name = tbl.table_name AND column_name = 'id'
        ) INTO has_id;
        hash_expr := format('md5(COALESCE((%s)::text, ''''))', data_col_list);
        normalized_keys := ARRAY(
            SELECT left(
                CASE WHEN normalized = '' THEN 'col'
                     WHEN normalized ~ '^[0-9]' THEN 'c_' || normalized
                     ELSE normalized END,
                63
            )
            FROM (
                SELECT regexp_replace(
                    trim(both '_' from regexp_replace(
                        lower(key_field), '[^a-z0-9_]', '_', 'g'
                    )),
                    '_+', '_', 'g'
                ) AS normalized
                FROM unnest(COALESCE(p_key_fields, ARRAY[]::text[]))
                     WITH ORDINALITY AS keys(key_field, ord)
                ORDER BY ord
            ) normalized_fields
        );
        v_key_count := cardinality(normalized_keys);
        IF v_key_count > 0 THEN
            SELECT count(*) INTO v_key_count
            FROM information_schema.columns
            WHERE table_schema = 'staging_ext'
              AND table_name = tbl.table_name
              AND column_name = ANY(normalized_keys);
            IF v_key_count = cardinality(normalized_keys) THEN
                v_key_fields_found := true;
                IF cardinality(normalized_keys) = 1 THEN
                    bk_expr := format('s.%I::text', normalized_keys[1]);
                ELSE
                    SELECT string_agg(format('s.%I', key_field), ', ' ORDER BY ord)
                    INTO col_list
                    FROM unnest(normalized_keys) WITH ORDINALITY AS keys(key_field, ord);
                    bk_expr := format('row(%s)::text', col_list);
                END IF;
            ELSIF NOT v_in_batch THEN
                bk_expr := 'NULL::text';
            ELSIF has_id THEN
                bk_expr := 's.id::text';
            ELSE
                RAISE EXCEPTION
                    'configured key field(s) are missing from source batch % for table %; no id column is available',
                    p_batch_id, tbl.table_name;
            END IF;
        ELSIF has_id THEN
            bk_expr := 's.id::text';
        ELSIF NOT v_in_batch THEN
            bk_expr := 'NULL::text';
        ELSE
            RAISE EXCEPTION
                'source table % has no stable business key; configure key fields or provide an id column',
                tbl.table_name;
        END IF;

        SELECT string_agg(format('%I', column_name), ', ' ORDER BY ordinal_position)
        INTO col_list
        FROM information_schema.columns
        WHERE table_schema = 'staging_ext' AND table_name = tbl.table_name;

        -- 1) close current dim rows whose content changed in the source
        sql := format(
            'WITH latest AS (
                    SELECT DISTINCT ON ((%s)) *
                    FROM staging_ext.%I s
                    WHERE s._source_batch_id = %L
                    ORDER BY (%s), s._row_index DESC, s._row_id DESC
                )
                UPDATE core.%I d SET valid_to = now(), is_current = false
                WHERE d.is_current AND EXISTS (
                    SELECT 1 FROM latest s
                    WHERE (%s) = d._business_key
                        AND (%s) IS DISTINCT FROM d._content_hash
                )',
            bk_expr, tbl.table_name, p_batch_id, bk_expr, dim_name, bk_expr, hash_expr
        );
        EXECUTE sql;
        GET DIAGNOSTICS v_closed1 = ROW_COUNT;

        -- 2) close current dim rows whose business key disappeared from the source
        IF p_delete_policy = 'close_on_full_snapshot'
           AND p_load_mode = 'full_snapshot' THEN
            sql := format(
                'WITH latest AS (
                    SELECT DISTINCT ON ((%s)) *
                    FROM staging_ext.%I s
                    WHERE s._source_batch_id = %L
                    ORDER BY (%s), s._row_index DESC, s._row_id DESC
                )
                UPDATE core.%I d SET valid_to = now(), is_current = false
                WHERE d.is_current AND NOT EXISTS (
                    SELECT 1 FROM latest s WHERE (%s) = d._business_key
                )',
                bk_expr, tbl.table_name, p_batch_id, bk_expr, dim_name, bk_expr
            );
            EXECUTE sql;
            GET DIAGNOSTICS v_closed2 = ROW_COUNT;
        END IF;

        -- 3) insert current versions for anything not already current
        --    (covers brand-new business keys and rows just closed in step 1)
        sql := format(
            'WITH latest AS (
                    SELECT DISTINCT ON ((%s)) *
                    FROM staging_ext.%I s
                    WHERE s._source_batch_id = %L
                    ORDER BY (%s), s._row_index DESC, s._row_id DESC
                )
                INSERT INTO core.%I (%s, _business_key, valid_from, valid_to, is_current, _content_hash)
                SELECT %s, (%s), now(), NULL, true, (%s)
                FROM latest s
                WHERE NOT EXISTS (
                    SELECT 1 FROM core.%I d WHERE d.is_current AND d._business_key = (%s)
                )',
            bk_expr, tbl.table_name, p_batch_id, bk_expr, dim_name, col_list, col_list,
            bk_expr, hash_expr, dim_name, bk_expr
        );
        EXECUTE sql;
        GET DIAGNOSTICS v_inserted = ROW_COUNT;

        source_table := tbl.table_name;
        dim_table := dim_name;
        inserted_count := v_inserted;
        closed_count := v_closed1 + v_closed2;
        RETURN NEXT;
    END LOOP;
    IF cardinality(normalized_keys) > 0 AND v_has_batch_rows AND NOT v_key_fields_found THEN
        RAISE EXCEPTION 'configured key field(s) are missing from source batch %', p_batch_id;
    END IF;
END;
$fn$;

REVOKE ALL ON FUNCTION core.sync_from_staging(bigint, text, text, text, text[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION core.sync_from_staging(bigint, text, text, text, text[]) TO core_service;
