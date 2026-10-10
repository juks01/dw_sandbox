from __future__ import annotations

import argparse
import json
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from psycopg import sql
from psycopg.types.json import Jsonb

from . import db

DEFAULT_MEDIUM_BYTES = 100_000_000
DEFAULT_LARGE_BYTES = 1_000_000_000
MIN_CREATED_AGE = timedelta(days=365)
MAX_CREATED_AGE = timedelta(days=3 * 365)
MAX_UPDATED_AGE = timedelta(days=365)


def _bulk_row_bytes(row_id: int, payload_size: int) -> int:
    row = {
        "id": row_id,
        "created_at": "2024-01-01T00:00:00.000000Z",
        "updated_at": "2026-01-01T00:00:00.000000Z",
        "record_type": "snapshot",
        "score": row_id % 1000,
        "payload": "x" * payload_size,
        "attributes": {
            "source": "mock",
            "labels": ["synthetic", "bulk"],
            "active": True,
        },
        "metadata": {
            "version": 1,
            "context": {
                "batch": row_id // 1000,
                "generation": "fixture",
                "identity": {"id": row_id},
            },
        },
    }
    return len(json.dumps(row, separators=(",", ":")).encode("utf-8"))


def _random_timestamps(
    rng: random.Random,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    current_time = now or datetime.now(timezone.utc)
    created_age = MIN_CREATED_AGE + timedelta(
        seconds=rng.uniform(0, (MAX_CREATED_AGE - MIN_CREATED_AGE).total_seconds()),
    )
    updated_age = timedelta(
        seconds=rng.uniform(0, min(MAX_UPDATED_AGE, created_age).total_seconds()),
    )
    return current_time - created_age, current_time - updated_age


def _bulk_row(
    row_id: int,
    payload_size: int,
    rng: random.Random,
    updated_at: datetime | None = None,
) -> tuple[Any, ...]:
    created_at, generated_updated_at = _random_timestamps(rng)
    return (
        row_id,
        created_at,
        updated_at if updated_at is not None else generated_updated_at,
        ("event", "metric", "snapshot")[row_id % 3],
        row_id % 1000,
        "x" * payload_size,
        Jsonb({
            "source": "mock",
            "labels": ["synthetic", "bulk"],
            "active": row_id % 2 == 0,
        }),
        Jsonb({
            "version": 1,
            "context": {
                "batch": row_id // 1000,
                "generation": "fixture",
                "identity": {"id": row_id},
            },
        }),
    )


def records_for_target(target_bytes: int, payload_size: int = 1900) -> int:
    """Estimate row count for a target compact JSON response byte size."""
    if target_bytes <= 0 or payload_size <= 0:
        raise ValueError("target_bytes and payload_size must be positive")
    row_bytes = _bulk_row_bytes(100_000, payload_size)
    envelope_bytes = len(b'{"items":[],"total":0,"skip":0,"limit":1000,"has_more":false}')
    return max(1, math.floor((target_bytes - envelope_bytes) / (row_bytes + 1)))


def initialize(
    seed: int = 1,
    reset: bool = False,
    if_empty: bool = False,
    small_rows: int = 1000,
    medium_bytes: int = DEFAULT_MEDIUM_BYTES,
    large_bytes: int = DEFAULT_LARGE_BYTES,
    payload_size: int = 1900,
    batch_size: int = 5000,
) -> dict[str, int]:
    if small_rows < 1 or batch_size < 1:
        raise ValueError("small_rows and batch_size must be positive")
    rng = random.Random(seed)
    counts = {
        "departments": min(1000, small_rows),
        "customers": small_rows,
        "medium_records": records_for_target(medium_bytes, payload_size),
        "large_records": records_for_target(large_bytes, payload_size),
    }

    with db.connect() as conn:
        db.ensure_schema(conn)
        current_counts = {
            table: conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))).fetchone()[0]
            for table in db.TABLES
        }
        if any(current_counts.values()) and not reset:
            if if_empty and all(current_counts.values()):
                print("Mock data already exists; skipping initialization.", flush=True)
                return current_counts
            raise ValueError("mock data already exists; use --reset to replace it")
        if reset:
            conn.execute("TRUNCATE large_records, medium_records, customers, departments")

        print("Initializing departments and customers...", flush=True)
        departments = [
            (idx, f"Department {idx:04d}", *_random_timestamps(rng))
            for idx in range(1, counts["departments"] + 1)
        ]
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO departments (id, name, created_at, updated_at) VALUES (%s, %s, %s, %s)",
                departments,
            )
            cursor.executemany(
                """
                INSERT INTO customers (id, department_id, name, email, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    (
                        idx,
                        rng.randint(1, counts["departments"]),
                        f"Customer {idx:06d}",
                        f"customer{idx}@example.test",
                        *_random_timestamps(rng),
                    )
                    for idx in range(1, counts["customers"] + 1)
                ),
            )
        print(f"departments: inserted {counts['departments']:,} rows", flush=True)
        print(f"customers: inserted {counts['customers']:,} rows", flush=True)

        for table in db.BULK_TABLES:
            count = counts[table]
            print(
                f"{table}: inserting {count:,} rows in batches of {batch_size:,}",
                flush=True,
            )
            for start in range(1, count + 1, batch_size):
                end = min(count + 1, start + batch_size)
                with conn.cursor() as cursor:
                    cursor.executemany(
                        sql.SQL(
                            "INSERT INTO {} "
                            "(id, created_at, updated_at, record_type, score, payload, attributes, metadata) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                        ).format(
                            sql.Identifier(table),
                        ),
                        (_bulk_row(idx, payload_size, rng) for idx in range(start, end)),
                    )
                print(f"{table}: inserted {end - 1:,}/{count:,} rows", flush=True)
        conn.commit()
        print("Initialization committed.", flush=True)
    return counts


def mutate(
    table: str = "customers",
    add_rows: int = 20,
    update_rows: int = 20,
    seed: int = 1,
) -> dict[str, int]:
    if table not in db.TABLES:
        raise ValueError(f"unknown mock table: {table}")
    if add_rows < 0 or update_rows < 0 or add_rows + update_rows == 0:
        raise ValueError("request at least one non-negative addition or update")

    rng = random.Random(seed)
    changed_at = datetime.now(timezone.utc)
    with db.connect() as conn:
        db.ensure_schema(conn)
        max_id = conn.execute(
            sql.SQL("SELECT COALESCE(max(id), 0) FROM {}").format(sql.Identifier(table))
        ).fetchone()[0]
        existing_ids = [
            row[0] for row in conn.execute(
                sql.SQL("SELECT id FROM {} ORDER BY id LIMIT %s").format(sql.Identifier(table)),
                (update_rows,),
            ).fetchall()
        ]
        if len(existing_ids) < update_rows:
            raise ValueError(f"{table} has only {len(existing_ids)} rows to update")

        print(
            f"Mutating {table}: updating {update_rows:,} rows and adding {add_rows:,} rows...",
            flush=True,
        )
        if table == "departments":
            for row_id in existing_ids:
                conn.execute(
                    "UPDATE departments SET name = name || ' updated', updated_at = %s WHERE id = %s",
                    (changed_at, row_id),
                )
            with conn.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO departments (id, name, created_at, updated_at) VALUES (%s, %s, %s, %s)",
                    (
                        (
                            max_id + idx,
                            f"New department {max_id + idx}",
                            _random_timestamps(rng)[0],
                            changed_at,
                        )
                        for idx in range(1, add_rows + 1)
                    ),
                )
        elif table == "customers":
            departments = conn.execute("SELECT id FROM departments ORDER BY id").fetchall()
            if (add_rows or update_rows) and not departments:
                raise ValueError("initialize departments before mutating customers")
            department_ids = [row[0] for row in departments]
            for row_id in existing_ids:
                conn.execute(
                    "UPDATE customers SET name = name || ' updated', updated_at = %s WHERE id = %s",
                    (changed_at, row_id),
                )
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO customers (id, department_id, name, email, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        (
                            max_id + idx,
                            rng.choice(department_ids),
                            f"New customer {max_id + idx}",
                            f"new{max_id + idx}@example.test",
                            _random_timestamps(rng)[0],
                            changed_at,
                        )
                        for idx in range(1, add_rows + 1)
                    ),
                )
        else:
            for row_id in existing_ids:
                conn.execute(
                    sql.SQL(
                        "UPDATE {} SET payload = payload || ' updated', score = score + 1, "
                        "updated_at = %s WHERE id = %s"
                    ).format(
                        sql.Identifier(table),
                    ),
                    (changed_at, row_id),
                )
            with conn.cursor() as cursor:
                cursor.executemany(
                    sql.SQL(
                        "INSERT INTO {} "
                        "(id, created_at, updated_at, record_type, score, payload, attributes, metadata) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    ).format(
                        sql.Identifier(table),
                    ),
                    (
                        _bulk_row(max_id + idx, 1900, rng, changed_at)
                        for idx in range(1, add_rows + 1)
                    ),
                )
        conn.commit()
    return {"updated": update_rows, "added": add_rows}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate and mutate standalone mock-source data.")
    commands = parser.add_subparsers(dest="command", required=True)
    initialize_cmd = commands.add_parser("init", help="create initial mock datasets")
    initialize_cmd.add_argument("--reset", action="store_true", help="replace all existing mock data")
    initialize_cmd.add_argument(
        "--if-empty",
        action="store_true",
        help="skip initialization when all mock tables already contain data",
    )
    initialize_cmd.add_argument("--seed", type=int, default=1)
    initialize_cmd.add_argument("--small-rows", type=int, default=1000)
    initialize_cmd.add_argument("--medium-bytes", type=int, default=DEFAULT_MEDIUM_BYTES)
    initialize_cmd.add_argument("--large-bytes", type=int, default=DEFAULT_LARGE_BYTES)
    initialize_cmd.add_argument("--payload-size", type=int, default=1900)
    initialize_cmd.add_argument("--batch-size", type=int, default=5000)
    mutate_cmd = commands.add_parser("mutate", help="add and update rows for incremental tests")
    mutate_cmd.add_argument("--table", choices=tuple(db.TABLES), default="customers")
    mutate_cmd.add_argument("--add", type=int, default=20)
    mutate_cmd.add_argument("--update", type=int, default=20)
    mutate_cmd.add_argument("--seed", type=int, default=1)
    commands.add_parser("stats", help="report row counts and generated dataset sizes")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    if args.command == "init":
        result = initialize(
            seed=args.seed,
            reset=args.reset,
            if_empty=args.if_empty,
            small_rows=args.small_rows,
            medium_bytes=args.medium_bytes,
            large_bytes=args.large_bytes,
            payload_size=args.payload_size,
            batch_size=args.batch_size,
        )
    elif args.command == "mutate":
        result = mutate(args.table, args.add, args.update, args.seed)
    else:
        for name, values in db.dataset_stats().items():
            details = ", ".join(
                f"{key}={value:,}" for key, value in values.items()
            )
            print(f"{name}: {details}")
        return
    print(", ".join(f"{name}={count:,}" for name, count in result.items()))


if __name__ == "__main__":
    main()
