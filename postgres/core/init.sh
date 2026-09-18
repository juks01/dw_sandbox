#!/usr/bin/env bash
# Creates roles, the postgres_fdw extension, foreign servers and user
# mappings for the core database. Anything that needs a secret value from
# the environment lives here (bash heredocs interpolate env vars); init.sql
# only contains static structural SQL.
set -Eeuo pipefail

: "${ADMIN_USER:?ADMIN_USER is required}"
: "${ADMIN_PASSWORD:?ADMIN_PASSWORD is required}"
: "${CORE_READER_USER:?CORE_READER_USER is required}"
: "${CORE_READER_PASSWORD:?CORE_READER_PASSWORD is required}"
: "${CORE_SERVICE_USER:?CORE_SERVICE_USER is required}"
: "${CORE_SERVICE_PASSWORD:?CORE_SERVICE_PASSWORD is required}"
: "${STAGING_READER_USER:?STAGING_READER_USER is required}"
: "${STAGING_READER_PASSWORD:?STAGING_READER_PASSWORD is required}"
: "${USERS_READER_USER:?USERS_READER_USER is required}"
: "${USERS_READER_PASSWORD:?USERS_READER_PASSWORD is required}"
: "${STAGING_DB:?STAGING_DB is required}"
: "${USERS_DB:?USERS_DB is required}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE EXTENSION IF NOT EXISTS postgres_fdw;

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${ADMIN_USER}') THEN
            CREATE ROLE ${ADMIN_USER} WITH LOGIN PASSWORD '${ADMIN_PASSWORD}' CREATEROLE CREATEDB;
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${CORE_READER_USER}') THEN
            CREATE ROLE ${CORE_READER_USER} WITH LOGIN PASSWORD '${CORE_READER_PASSWORD}';
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${CORE_SERVICE_USER}') THEN
            -- core_service is used by the orchestrator to invoke the sync procedures.
            CREATE ROLE ${CORE_SERVICE_USER} WITH LOGIN PASSWORD '${CORE_SERVICE_PASSWORD}';
        END IF;
    END
    \$\$;

    -- ---- Foreign servers pointing at the staging and users containers ----
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_foreign_server WHERE srvname = 'staging_srv') THEN
            CREATE SERVER staging_srv FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (host 'staging', port '5432', dbname '${STAGING_DB}');
        END IF;
        IF NOT EXISTS (SELECT FROM pg_foreign_server WHERE srvname = 'users_srv') THEN
            CREATE SERVER users_srv FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (host 'users', port '5432', dbname '${USERS_DB}');
        END IF;
    END
    \$\$;

    -- ---- User mappings: local role -> remote read-only credentials ----
    DO \$\$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_user_mappings
            WHERE srvname = 'staging_srv' AND usename = '${ADMIN_USER}'
        ) THEN
            EXECUTE format(
                'CREATE USER MAPPING FOR %I SERVER staging_srv OPTIONS (user %L, password %L)',
                '${ADMIN_USER}', '${STAGING_READER_USER}', '${STAGING_READER_PASSWORD}'
            );
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_user_mappings
            WHERE srvname = 'users_srv' AND usename = '${ADMIN_USER}'
        ) THEN
            EXECUTE format(
                'CREATE USER MAPPING FOR %I SERVER users_srv OPTIONS (user %L, password %L)',
                '${ADMIN_USER}', '${USERS_READER_USER}', '${USERS_READER_PASSWORD}'
            );
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_user_mappings
            WHERE srvname = 'staging_srv' AND usename = '${CORE_SERVICE_USER}'
        ) THEN
            EXECUTE format(
                'CREATE USER MAPPING FOR %I SERVER staging_srv OPTIONS (user %L, password %L)',
                '${CORE_SERVICE_USER}', '${STAGING_READER_USER}', '${STAGING_READER_PASSWORD}'
            );
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_user_mappings
            WHERE srvname = 'users_srv' AND usename = '${CORE_SERVICE_USER}'
        ) THEN
            EXECUTE format(
                'CREATE USER MAPPING FOR %I SERVER users_srv OPTIONS (user %L, password %L)',
                '${CORE_SERVICE_USER}', '${USERS_READER_USER}', '${USERS_READER_PASSWORD}'
            );
        END IF;
    END
    \$\$;

    -- ---- Local mirror schemas for the remote tables (foreign tables) ----
    DROP SCHEMA IF EXISTS staging_ext CASCADE;
    CREATE SCHEMA staging_ext;
    IMPORT FOREIGN SCHEMA staging FROM SERVER staging_srv INTO staging_ext;

    DROP SCHEMA IF EXISTS users_ext CASCADE;
    CREATE SCHEMA users_ext;
    IMPORT FOREIGN SCHEMA users FROM SERVER users_srv INTO users_ext;
SQL

echo "[core/init.sh] extension, roles, FDW servers, user mappings and foreign schemas ensured"
