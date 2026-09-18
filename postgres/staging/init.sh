#!/usr/bin/env bash
# Creates roles for the staging database.
# Runs once, on first container initialization, as part of the official
# postgres entrypoint (docker-entrypoint-initdb.d), before init.sql.
set -Eeuo pipefail

: "${ADMIN_USER:?ADMIN_USER is required}"
: "${ADMIN_PASSWORD:?ADMIN_PASSWORD is required}"
: "${LOADER_WRITER_USER:?LOADER_WRITER_USER is required}"
: "${LOADER_WRITER_PASSWORD:?LOADER_WRITER_PASSWORD is required}"
: "${STAGING_READER_USER:?STAGING_READER_USER is required}"
: "${STAGING_READER_PASSWORD:?STAGING_READER_PASSWORD is required}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${ADMIN_USER}') THEN
            CREATE ROLE ${ADMIN_USER} WITH LOGIN PASSWORD '${ADMIN_PASSWORD}' CREATEROLE CREATEDB;
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${LOADER_WRITER_USER}') THEN
            CREATE ROLE ${LOADER_WRITER_USER} WITH LOGIN PASSWORD '${LOADER_WRITER_PASSWORD}';
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${STAGING_READER_USER}') THEN
            CREATE ROLE ${STAGING_READER_USER} WITH LOGIN PASSWORD '${STAGING_READER_PASSWORD}';
        END IF;
    END
    \$\$;
SQL

echo "[staging/init.sh] roles ensured"
