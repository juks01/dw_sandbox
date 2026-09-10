import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg import sql


MAX_DEPTH = 3
ROOT_TABLE = "product"


def wait(dsn):
    for _ in range(30):
        try:
            with psycopg.connect(dsn):
                return
        except Exception:
            time.sleep(1)

    raise RuntimeError("database unavailable")


def ident(value):
    value = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value))
    value = re.sub(r"_+", "_", value).strip("_").lower()

    if not value:
        value = "field"

    if value[0].isdigit():
        value = "_" + value

    return value[:63]


def json_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value)

    return value


def pg_type(value):
    if value is None:
        return "text"

    if isinstance(value, bool):
        return "boolean"

    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"

    if isinstance(value, float):
        return "numeric"

    if isinstance(value, str):
        return "text"

    return "jsonb"


def merge_type(values):
    types = {
        pg_type(value)
        for value in values
        if value is not None
    }

    if not types:
        return "text"

    if len(types) == 1:
        return types.pop()

    if types <= {"integer", "numeric"}:
        return "numeric"

    return "text"


def add_column(conn, table, column, data_type):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}"
            ).format(
                sql.Identifier("mart", table),
                sql.Identifier(column),
                sql.SQL(data_type),
            )
        )


def ensure_product_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mart.product (
                product_id integer PRIMARY KEY
            )
            """
        )


def ensure_child_table(conn, table):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    _row_id bigserial PRIMARY KEY,
                    _parent_id bigint NOT NULL,
                    _row_index integer NOT NULL
                )
                """
            ).format(
                sql.Identifier("mart", table)
            )
        )


def flatten_object(
    obj: dict[str, Any],
    prefix: tuple[str, ...],
    depth: int,
    scalars: dict[str, list[Any]],
    arrays: list[tuple[tuple[str, ...], list[Any], int]],
):
    """
    Flatten JSON objects through level 3.

    Levels 1-3:
        scalar -> column
        object -> continue flattening
        array  -> child table

    At level 3:
        object/array -> JSONB

    Therefore level 4+ remains nested JSONB.
    """

    for key, value in obj.items():
        path = prefix + (str(key),)
        column = ident("_".join(path))

        if value is None:
            scalars[column].append(None)

        elif isinstance(value, dict):
            if depth >= MAX_DEPTH:
                scalars[column].append(value)
            else:
                flatten_object(
                    value,
                    path,
                    depth + 1,
                    scalars,
                    arrays,
                )

        elif isinstance(value, list):
            if depth >= MAX_DEPTH:
                scalars[column].append(value)
            else:
                arrays.append(
                    (path, value, depth + 1)
                )

        else:
            scalars[column].append(value)


def write_product(conn, record):
    ensure_product_table(conn)

    scalars = defaultdict(list)
    arrays = []

    flatten_object(
        {
            key: value
            for key, value in record.items()
            if key != "id"
        },
        (),
        1,
        scalars,
        arrays,
    )

    for column, values in scalars.items():
        add_column(
            conn,
            ROOT_TABLE,
            column,
            merge_type(values),
        )

    row = {
        "product_id": record["id"]
    }

    for column, observed in scalars.items():
        row[column] = observed[0] if observed else None

    columns = list(row)

    assignments = [
        sql.SQL("{} = EXCLUDED.{}").format(
            sql.Identifier(column),
            sql.Identifier(column),
        )
        for column in columns
        if column != "product_id"
    ]

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                INSERT INTO mart.product ({})
                VALUES ({})
                ON CONFLICT (product_id)
                DO UPDATE SET {}
                """
            ).format(
                sql.SQL(", ").join(
                    sql.Identifier(column)
                    for column in columns
                ),
                sql.SQL(", ").join(
                    sql.Placeholder()
                    for _ in columns
                ),
                sql.SQL(", ").join(assignments),
            ),
            [
                json_value(row[column])
                for column in columns
            ],
        )

    return arrays


def write_array(
    conn,
    table,
    parent_id,
    values,
    depth,
):
    ensure_child_table(conn, table)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "DELETE FROM {} WHERE _parent_id = %s"
            ).format(
                sql.Identifier("mart", table)
            ),
            (parent_id,),
        )

    for index, value in enumerate(values):
        scalars = defaultdict(list)
        arrays = []

        if isinstance(value, dict):
            flatten_object(
                value,
                (),
                depth,
                scalars,
                arrays,
            )
        else:
            scalars["value"].append(value)

        for column, observed in scalars.items():
            add_column(
                conn,
                table,
                column,
                merge_type(observed),
            )

        row = {
            "_parent_id": parent_id,
            "_row_index": index,
        }

        for column, observed in scalars.items():
            row[column] = observed[0] if observed else None

        columns = list(row)

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {} ({})
                    VALUES ({})
                    RETURNING _row_id
                    """
                ).format(
                    sql.Identifier("mart", table),
                    sql.SQL(", ").join(
                        sql.Identifier(column)
                        for column in columns
                    ),
                    sql.SQL(", ").join(
                        sql.Placeholder()
                        for _ in columns
                    ),
                ),
                [
                    json_value(row[column])
                    for column in columns
                ],
            )

            row_id = cur.fetchone()[0]

        for path, child_values, child_depth in arrays:
            if child_depth >= MAX_DEPTH:
                continue

            child_table = ident(
                table + "_" + "_".join(path)
            )

            write_array(
                conn,
                child_table,
                row_id,
                child_values,
                child_depth,
            )


def get_latest_batch(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, payload
            FROM staging.raw_batches
            ORDER BY loaded_at DESC, id DESC
            LIMIT 1
            """
        )

        row = cur.fetchone()

    if not row:
        return None, []

    batch_id, payload = row
    products = payload.get("products")

    if not isinstance(products, list):
        raise ValueError(
            "source payload.products must be an array"
        )

    records = [
        product
        for product in products
        if isinstance(product, dict)
        and "id" in product
    ]

    return batch_id, records


def update_staging(conn, records, batch_id):
    with conn.cursor() as cur:
        for record in records:
            attributes = {
                key: value
                for key, value in record.items()
                if key != "id"
            }

            cur.execute(
                """
                INSERT INTO staging.stg_products (
                    product_id,
                    attributes,
                    batch_id
                )
                VALUES (%s, %s::jsonb, %s)
                ON CONFLICT (product_id)
                DO UPDATE SET
                    attributes = EXCLUDED.attributes,
                    batch_id = EXCLUDED.batch_id,
                    updated_at = now()
                """,
                (
                    record["id"],
                    json.dumps(attributes),
                    batch_id,
                ),
            )


def update_core(
    staging_dsn,
    core_dsn,
    batch_id,
    load_time,
):
    with psycopg.connect(staging_dsn) as source:
        with source.cursor() as src:
            src.execute(
                """
                SELECT product_id, attributes
                FROM staging.stg_products
                """
            )

            rows = src.fetchall()

    with psycopg.connect(core_dsn) as conn:
        with conn.cursor() as cur:

            for product_id, attributes in rows:
                attributes_json = json.dumps(attributes)

                cur.execute(
                    """
                    UPDATE core.dim_product
                    SET
                        valid_to = %s,
                        is_current = false
                    WHERE product_id = %s
                      AND is_current
                      AND attributes IS DISTINCT FROM %s::jsonb
                    """,
                    (
                        load_time,
                        product_id,
                        attributes_json,
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO core.dim_product (
                        product_id,
                        attributes,
                        valid_from,
                        valid_to,
                        is_current
                    )
                    SELECT
                        %s,
                        %s::jsonb,
                        %s,
                        'infinity',
                        true
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM core.dim_product
                        WHERE product_id = %s
                          AND is_current
                    )
                    """,
                    (
                        product_id,
                        attributes_json,
                        load_time,
                        product_id,
                    ),
                )

            cur.execute(
                """
                INSERT INTO core.load_log(
                    source,
                    batch_id,
                    rows_affected
                )
                VALUES (%s, %s, %s)
                """,
                (
                    "orchestrator",
                    batch_id,
                    len(rows),
                ),
            )

    return rows


def remove_products_not_in_snapshot(conn, product_ids):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM mart.product
            WHERE NOT (product_id = ANY(%s))
            """,
            (product_ids,),
        )


def main():
    staging_dsn = os.environ["STAGING_DSN"]
    core_dsn = os.environ["CORE_DSN"]
    mart_dsn = os.environ["MART_DSN"]

    # Wait for databases to be ready
    wait(staging_dsn)
    wait(core_dsn)
    wait(mart_dsn)

    load_time = datetime.now(timezone.utc)

    # --------------------------------------------------------------
    # Staging
    # --------------------------------------------------------------

    with psycopg.connect(staging_dsn) as conn:
        batch_id, records = get_latest_batch(conn)

        if not records:
            print("No product records found")
            return

        update_staging(
            conn,
            records,
            batch_id,
        )

    # --------------------------------------------------------------
    # Core SCD2
    # --------------------------------------------------------------

    update_core(
        staging_dsn,
        core_dsn,
        batch_id,
        load_time,
    )

    # --------------------------------------------------------------
    # Mart
    # --------------------------------------------------------------

    with psycopg.connect(mart_dsn) as conn:
        product_ids = [
            record["id"]
            for record in records
        ]

        remove_products_not_in_snapshot(
            conn,
            product_ids,
        )

        for record in records:
            arrays = write_product(
                conn,
                record,
            )

            for path, values, depth in arrays:
                if depth >= MAX_DEPTH:
                    continue

                child_table = ident(
                    "product_" + "_".join(path)
                )

                write_array(
                    conn,
                    child_table,
                    record["id"],
                    values,
                    depth,
                )

        print(
            f"MART OK: {len(records)} products, "
            f"flatten depth {MAX_DEPTH}"
        )

    print("PIPELINE OK")


if __name__ == "__main__":
    main()
