import glob
import json
import os
import re
from collections import defaultdict

import psycopg
from psycopg import sql


FLATTEN_MAX_DEPTH = 6
SOURCE_URL = "https://dummyjson.com/products"


def ident(value):
    value = re.sub(
        r"[^a-zA-Z0-9_]+",
        "_",
        str(value),
    )

    value = re.sub(
        r"_+",
        "_",
        value,
    ).strip("_").lower()

    if not value:
        value = "field"

    if value[0].isdigit():
        value = "_" + value

    return value[:63]


def pg_type(value):
    if value is None:
        return "text"

    if isinstance(value, bool):
        return "boolean"

    if isinstance(value, int):
        return "bigint"

    if isinstance(value, float):
        return "numeric"

    if isinstance(value, str):
        return "text"

    if isinstance(value, (dict, list)):
        return "jsonb"

    return "text"


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

    if types <= {"bigint", "numeric"}:
        return "numeric"

    return "text"


def json_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(
            value,
            ensure_ascii=False,
        )

    return value


def flatten_object(
    obj,
    prefix,
    depth,
    scalars,
    arrays,
):
    for key, value in obj.items():

        path = prefix + (str(key),)

        column = ident(
            "_".join(path)
        )

        if value is None:
            scalars[column].append(None)

        elif isinstance(value, dict):

            if depth >= FLATTEN_MAX_DEPTH:
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

            if depth >= FLATTEN_MAX_DEPTH:
                scalars[column].append(value)

            else:
                arrays.append(
                    (
                        path,
                        value,
                    )
                )

        else:
            scalars[column].append(value)


def ensure_table(
    conn,
    table,
    extra_columns,
):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    _row_id bigserial PRIMARY KEY,
                    _source_batch_id bigint NOT NULL{}
                )
                """
            ).format(
                sql.Identifier(
                    "staging",
                    table,
                ),
                sql.SQL(extra_columns),
            )
        )


def ensure_columns(
    conn,
    table,
    columns_types,
):
    with conn.cursor() as cur:

        for column, data_type in columns_types.items():

            cur.execute(
                sql.SQL(
                    """
                    ALTER TABLE {}
                    ADD COLUMN IF NOT EXISTS {} {}
                    """
                ).format(
                    sql.Identifier(
                        "staging",
                        table,
                    ),
                    sql.Identifier(column),
                    sql.SQL(data_type),
                )
            )


def load_records(
    conn,
    table,
    records,
    source_batch_id,
    parent_id=None,
):
    is_child = parent_id is not None

    if is_child:

        ensure_table(
            conn,
            table,
            ", _parent_id bigint NOT NULL, _row_index integer NOT NULL",
        )

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    DELETE FROM {}
                    WHERE _parent_id = %s
                    """
                ).format(
                    sql.Identifier(
                        "staging",
                        table,
                    )
                ),
                (parent_id,),
            )

    else:

        ensure_table(
            conn,
            table,
            ", id bigint UNIQUE",
        )

    for index, record in enumerate(records):

        scalars = defaultdict(list)
        arrays = []

        if isinstance(record, dict):

            flatten_object(
                record,
                (),
                1,
                scalars,
                arrays,
            )

        else:
            scalars["value"].append(record)

        ensure_columns(
            conn,
            table,
            {
                column: merge_type(values)
                for column, values in scalars.items()
            },
        )

        row = {
            "_source_batch_id": source_batch_id,
        }

        if is_child:
            row["_parent_id"] = parent_id
            row["_row_index"] = index

        for column, values in scalars.items():
            row[column] = (
                values[0]
                if values
                else None
            )

        columns = list(row)

        with conn.cursor() as cur:

            if not is_child and "id" in row:

                assignments = [
                    sql.SQL(
                        "id = EXCLUDED.id"
                    )
                ]

                assignments.extend(
                    sql.SQL(
                        "{0} = EXCLUDED.{0}"
                    ).format(
                        sql.Identifier(column)
                    )
                    for column in columns
                    if column not in {
                        "id",
                        "_source_batch_id",
                    }
                )

                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {} ({})
                        VALUES ({})
                        ON CONFLICT (id)
                        DO UPDATE SET {}
                        RETURNING _row_id
                        """
                    ).format(
                        sql.Identifier(
                            "staging",
                            table,
                        ),
                        sql.SQL(", ").join(
                            sql.Identifier(column)
                            for column in columns
                        ),
                        sql.SQL(", ").join(
                            sql.Placeholder()
                            for _ in columns
                        ),
                        sql.SQL(", ").join(
                            assignments
                        ),
                    ),
                    [
                        json_value(row[column])
                        for column in columns
                    ],
                )

            else:

                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {} ({})
                        VALUES ({})
                        RETURNING _row_id
                        """
                    ).format(
                        sql.Identifier(
                            "staging",
                            table,
                        ),
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

        #######################################################################
        # Arrays become child tables.
        #######################################################################

        for path, values in arrays:

            child_table = ident(
                table
                + "_"
                + "_".join(path)
            )

            child_records = [
                value
                for value in values
                if isinstance(
                    value,
                    dict,
                )
            ]

            if child_records:

                load_records(
                    conn,
                    child_table,
                    child_records,
                    source_batch_id,
                    parent_id=row_id,
                )


def main():

    dsn = (
        f"host={os.environ['PGHOST']} "
        f"dbname={os.environ['PGDATABASE']} "
        f"user={os.environ['PGUSER']} "
        f"password={os.environ['PGPASSWORD']}"
    )

    files = sorted(
        glob.glob(
            "/landing/*.json"
        )
    )

    if not files:
        print(
            "No raw files found",
            flush=True,
        )
        raise SystemExit(0)

    with psycopg.connect(dsn) as conn:

        for filename in files:

            basename = os.path.basename(
                filename
            )

            with open(
                filename,
                encoding="utf-8",
            ) as handle:

                payload = json.load(handle)

            ###################################################################
            # Register raw batch first.
            ###################################################################

            with conn.cursor() as cur:

                cur.execute(
                    """
                    INSERT INTO staging.raw_batches (
                        filename,
                        source_url,
                        payload
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (filename)
                    DO NOTHING
                    RETURNING id
                    """,
                    (
                        basename,
                        SOURCE_URL,
                        json.dumps(
                            payload,
                            ensure_ascii=False,
                        ),
                    ),
                )

                row = cur.fetchone()

            if not row:

                print(
                    "LOAD SKIP:",
                    basename,
                    flush=True,
                )

                continue

            batch_id = row[0]

            ###################################################################
            # Root JSON array.
            ###################################################################

            if isinstance(payload, list):

                load_records(
                    conn,
                    "records",
                    payload,
                    batch_id,
                )

            ###################################################################
            # Root JSON object containing one or more arrays.
            ###################################################################

            elif isinstance(payload, dict):

                found = False

                for key, value in payload.items():

                    if (
                        isinstance(value, list)
                        and value
                        and all(
                            isinstance(
                                item,
                                dict,
                            )
                            for item in value
                        )
                    ):

                        load_records(
                            conn,
                            ident(key),
                            value,
                            batch_id,
                        )

                        found = True

                #################################################################
                # Plain object with no array collection.
                #################################################################

                if not found:

                    load_records(
                        conn,
                        "records",
                        [payload],
                        batch_id,
                    )

            else:

                load_records(
                    conn,
                    "records",
                    [payload],
                    batch_id,
                )

            print(
                "LOAD OK:",
                basename,
                "batch_id=",
                batch_id,
                flush=True,
            )

    print(
        "LOADER OK",
        flush=True,
    )


if __name__ == "__main__":
    main()
