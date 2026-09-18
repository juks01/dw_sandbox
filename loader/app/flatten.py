"""Generic JSON -> relational flattening.

Turns an arbitrary JSON object/array (object, nested object, nested array,
scalar) into a dict of {table_name: [row, ...]}, following the rules from
the spec:
  - the top-level object gets its own table (named after the source)
  - nested objects are flattened into the parent row (prefixed columns)
  - nested arrays become a child table
  - child rows carry _parent_id (the parent row's _row_id)
  - every row gets _row_id, _source_batch_id, _row_index
  - array order is preserved in _row_index
"""
from __future__ import annotations

import re
import uuid
from typing import Any, Optional

MAX_IDENTIFIER_LEN = 63
TECHNICAL_COLUMNS = {"_row_id", "_source_batch_id", "_parent_id", "_row_index"}


def normalize_identifier(name: str) -> str:
    """Make an arbitrary string safe to use as a PostgreSQL identifier."""
    slug = re.sub(r"[^a-zA-Z0-9_]", "_", str(name).strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        slug = "col"
    if slug[0].isdigit():
        slug = f"c_{slug}"
    return slug[:MAX_IDENTIFIER_LEN]


def _new_row_shell(source_batch_id: int, parent_id: Optional[str], row_index: int) -> dict:
    return {
        "_row_id": str(uuid.uuid4()),
        "_source_batch_id": source_batch_id,
        "_parent_id": parent_id,
        "_row_index": row_index,
    }


def _flatten_object(
    table_name: str,
    obj: dict,
    source_batch_id: int,
    parent_id: Optional[str],
    row_index: int,
    tables: dict[str, list[dict]],
) -> str:
    """Flatten one JSON object into a row of `table_name`, recursing into
    nested objects (same row, prefixed columns) and nested arrays (child
    tables). Returns the new row's _row_id."""
    row = _new_row_shell(source_batch_id, parent_id, row_index)
    row_id = row["_row_id"]

    def walk(o: dict, prefix: str) -> None:
        for raw_key, value in o.items():
            col = normalize_identifier(f"{prefix}_{raw_key}" if prefix else raw_key)
            if col in TECHNICAL_COLUMNS:
                col = f"src_{col}"

            if isinstance(value, dict):
                walk(value, col)
            elif isinstance(value, list):
                child_table = normalize_identifier(f"{table_name}_{col}")
                for idx, item in enumerate(value):
                    if isinstance(item, dict):
                        _flatten_object(child_table, item, source_batch_id, row_id, idx, tables)
                    else:
                        child_row = _new_row_shell(source_batch_id, row_id, idx)
                        child_row["value"] = item
                        tables.setdefault(child_table, []).append(child_row)
            else:
                row[col] = value

    walk(obj, "")
    tables.setdefault(table_name, []).append(row)
    return row_id


def flatten_payload(source: str, payload: Any, source_batch_id: int) -> dict[str, list[dict]]:
    """Entry point: flatten a whole extractor payload for `source`."""
    table_name = normalize_identifier(source)
    tables: dict[str, list[dict]] = {}

    if isinstance(payload, list):
        for idx, item in enumerate(payload):
            if isinstance(item, dict):
                _flatten_object(table_name, item, source_batch_id, None, idx, tables)
            else:
                row = _new_row_shell(source_batch_id, None, idx)
                row["value"] = item
                tables.setdefault(table_name, []).append(row)
    elif isinstance(payload, dict):
        _flatten_object(table_name, payload, source_batch_id, None, 0, tables)
    else:
        row = _new_row_shell(source_batch_id, None, 0)
        row["value"] = payload
        tables.setdefault(table_name, []).append(row)

    return tables
