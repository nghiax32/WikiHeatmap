#!/bin/sh
# Create the databases that are not the warehouse itself (Airflow metadata).
# POSTGRES_DB (the warehouse) is created by the postgres image on first start.
# Idempotent: CREATE DATABASE has no IF NOT EXISTS, so check first.
set -eu

for db in airflow; do
    if psql -tAc "SELECT 1 FROM pg_database WHERE datname = '$db'" | grep -q 1; then
        echo "database $db exists"
    else
        psql -c "CREATE DATABASE $db"
        echo "database $db created"
    fi
done
