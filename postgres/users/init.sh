#!/usr/bin/env bash
set -Eeuo pipefail

: "${ADMIN_USER:?ADMIN_USER is required}"
: "${ADMIN_PASSWORD:?ADMIN_PASSWORD is required}"
: "${USERS_READER_USER:?USERS_READER_USER is required}"
: "${USERS_READER_PASSWORD:?USERS_READER_PASSWORD is required}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${ADMIN_USER}') THEN
            CREATE ROLE ${ADMIN_USER} WITH LOGIN PASSWORD '${ADMIN_PASSWORD}' CREATEROLE CREATEDB;
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${USERS_READER_USER}') THEN
            CREATE ROLE ${USERS_READER_USER} WITH LOGIN PASSWORD '${USERS_READER_PASSWORD}';
        END IF;
    END
    \$\$;
SQL

echo "[users/init.sh] roles ensured"
