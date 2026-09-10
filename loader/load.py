import glob
import json
import os

import psycopg


dsn = (
    f"host={os.environ['PGHOST']} "
    f"dbname={os.environ['PGDATABASE']} "
    f"user={os.environ['PGUSER']} "
    f"password={os.environ['PGPASSWORD']}"
)

files = sorted(glob.glob("/landing/*.json"))

if not files:
    print("No raw files found")
    raise SystemExit(0)

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        for filename in files:
            with open(filename) as handle:
                payload = json.load(handle)

            cur.execute(
                """
                INSERT INTO staging.raw_batches(filename, payload)
                VALUES (%s, %s)
                ON CONFLICT (filename) DO NOTHING
                RETURNING id
                """,
                (
                    os.path.basename(filename),
                    json.dumps(payload),
                ),
            )

            row = cur.fetchone()

            if row:
                print("LOAD OK:", os.path.basename(filename))
            else:
                print("LOAD SKIP:", os.path.basename(filename))

print("LOADER OK")
