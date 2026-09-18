#!/usr/bin/env bash
set -Eeuo pipefail

: "${ADMIN_USER:?ADMIN_USER is required}"
: "${ADMIN_PASSWORD:?ADMIN_PASSWORD is required}"
: "${MART_SERVICE_USER:?MART_SERVICE_USER is required}"
: "${MART_SERVICE_PASSWORD:?MART_SERVICE_PASSWORD is required}"
: "${REPORTING_USER:?REPORTING_USER is required}"
: "${REPORTING_PASSWORD:?REPORTING_PASSWORD is required}"
: "${CORE_READER_USER:?CORE_READER_USER is required}"
: "${CORE_READER_PASSWORD:?CORE_READER_PASSWORD is required}"
: "${CORE_DB:?CORE_DB is required}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE EXTENSION IF NOT EXISTS postgres_fdw;

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${ADMIN_USER}') THEN
            CREATE ROLE ${ADMIN_USER} WITH LOGIN PASSWORD '${ADMIN_PASSWORD}' CREATEROLE CREATEDB;
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${MART_SERVICE_USER}') THEN
            CREATE ROLE ${MART_SERVICE_USER} WITH LOGIN PASSWORD '${MART_SERVICE_PASSWORD}';
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${REPORTING_USER}') THEN
            CREATE ROLE ${REPORTING_USER} WITH LOGIN PASSWORD '${REPORTING_PASSWORD}';
        END IF;
    END
    \$\$;

    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_foreign_server WHERE srvname = 'core_srv') THEN
            CREATE SERVER core_srv FOREIGN DATA WRAPPER postgres_fdw
                OPTIONS (host 'core', port '5432', dbname '${CORE_DB}');
        END IF;
    END
    \$\$;

    DO \$\$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_user_mappings WHERE srvname = 'core_srv' AND usename = '${ADMIN_USER}'
        ) THEN
            EXECUTE format(
                'CREATE USER MAPPING FOR %I SERVER core_srv OPTIONS (user %L, password %L)',
                '${ADMIN_USER}', '${CORE_READER_USER}', '${CORE_READER_PASSWORD}'
            );
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_user_mappings WHERE srvname = 'core_srv' AND usename = '${MART_SERVICE_USER}'
        ) THEN
            EXECUTE format(
                'CREATE USER MAPPING FOR %I SERVER core_srv OPTIONS (user %L, password %L)',
                '${MART_SERVICE_USER}', '${CORE_READER_USER}', '${CORE_READER_PASSWORD}'
            );
        END IF;
    END
    \$\$;

    DROP SCHEMA IF EXISTS core_ext CASCADE;
    CREATE SCHEMA core_ext;
    IMPORT FOREIGN SCHEMA core FROM SERVER core_srv INTO core_ext;
SQL

echo "[mart/init.sh] extension, roles, FDW server, user mappings and foreign schema ensured"
