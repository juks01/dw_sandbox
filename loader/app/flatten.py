"""Generic JSON -> relational flattening.

Turns an arbitrary JSON object/array (object, nested object, nested array,
scalar) into a dict of {table_name: [row, ...]}, following the rules from
the spec:
  - the top-level object gets its own table (named after the source)
  - nested objects are flattened into the parent row (prefixed columns)
  - nested arrays become a child table
  - child rows carry _parent_id (the parent row's _row_id)
  - child rows carry a stable _parent_key when the parent has a stable id
  - every row gets _row_id, _source_batch_id, _row_index
  - array order is preserved in _row_index
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any, BinaryIO, NamedTuple, Optional

import ijson
from ijson.common import JSONError

MAX_IDENTIFIER_LEN = 63
TECHNICAL_COLUMNS = {
    "_row_id", "_source_batch_id", "_parent_id", "_parent_key", "_row_index",
}
PAGINATED_COLLECTION_KEYS = ("results", "items", "records", "products", "data")
PAGINATION_METADATA_KEYS = {
    "total", "skip", "offset", "limit", "has_more",
    "next", "next_cursor", "nextCursor",
}
FLATTEN_BATCH_SIZE = 500


class PayloadInfo(NamedTuple):
    root_type: str
    collection_key: Optional[str]
    byte_size: int
    sha256: str


class _HashingReader:
    def __init__(self, stream: BinaryIO) -> None:
        self.stream = stream
        self.digest = hashlib.sha256()
        self.byte_size = 0

    def read(self, size: int = -1) -> bytes:
        chunk = self.stream.read(size)
        self.digest.update(chunk)
        self.byte_size += len(chunk)
        return chunk


def normalize_identifier(name: str) -> str:
    """Make an arbitrary string safe to use as a PostgreSQL identifier."""
    slug = re.sub(r"[^a-zA-Z0-9_]", "_", str(name).strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        slug = "col"
    if slug[0].isdigit():
        slug = f"c_{slug}"
    return slug[:MAX_IDENTIFIER_LEN]


def _new_row_shell(
    source_batch_id: int,
    parent_id: Optional[str],
    parent_key: Optional[str],
    row_index: int,
) -> dict:
    return {
        "_row_id": str(uuid.uuid4()),
        "_source_batch_id": source_batch_id,
        "_parent_id": parent_id,
        "_parent_key": parent_key,
        "_row_index": row_index,
    }


def _flatten_object(
    table_name: str,
    obj: dict,
    source_batch_id: int,
    parent_id: Optional[str],
    parent_key: Optional[str],
    row_index: int,
    tables: dict[str, list[dict]],
) -> str:
    """Flatten one JSON object into a row of `table_name`, recursing into
    nested objects (same row, prefixed columns) and nested arrays (child
    tables). Returns the new row's _row_id."""
    row = _new_row_shell(source_batch_id, parent_id, parent_key, row_index)
    row_id = row["_row_id"]
    object_id = obj.get("id")
    if parent_key is None:
        row_key = (
            json.dumps([table_name, object_id], separators=(",", ":"))
            if object_id is not None and not isinstance(object_id, (dict, list))
            else None
        )
    else:
        identity = (
            object_id
            if object_id is not None and not isinstance(object_id, (dict, list))
            else row_index
        )
        row_key = json.dumps(
            [parent_key, table_name, identity],
            separators=(",", ":"),
        )

    def walk(o: dict, prefix: str) -> None:
        for raw_key, value in o.items():
            col = normalize_identifier(f"{prefix}_{raw_key}" if prefix else raw_key)
            if col in TECHNICAL_COLUMNS:
                col = f"src_{col}"

            if isinstance(value, dict):
                walk(value, col)
            elif isinstance(value, list):
                child_table = normalize_identifier(f"{table_name}_{col}")
                tables.setdefault(child_table, [])
                for idx, item in enumerate(value):
                    if isinstance(item, dict):
                        _flatten_object(
                            child_table, item, source_batch_id, row_id,
                            row_key, idx, tables,
                        )
                    else:
                        child_row = _new_row_shell(
                            source_batch_id, row_id, row_key, idx,
                        )
                        child_row["value"] = item
                        tables.setdefault(child_table, []).append(child_row)
            else:
                row[col] = value

    walk(obj, "")
    tables.setdefault(table_name, []).append(row)
    return row_id


def flatten_payload(source: str, payload: Any, source_batch_id: int) -> dict[str, list[dict]]:
    """Flatten a payload, unwrapping recognized paginated collection envelopes."""
    table_name = normalize_identifier(source)
    if isinstance(payload, dict) and PAGINATION_METADATA_KEYS.intersection(payload):
        payload = next(
            (
                payload[key]
                for key in PAGINATED_COLLECTION_KEYS
                if isinstance(payload.get(key), list)
            ),
            payload,
        )
    items = payload if isinstance(payload, list) else [payload]
    tables: dict[str, list[dict]] = {}
    for idx, item in enumerate(items):
        _merge_tables(tables, flatten_record(source, item, source_batch_id, idx))
    tables.setdefault(table_name, [])
    return tables


def flatten_record(
    source: str, item: Any, source_batch_id: int, row_index: int,
) -> dict[str, list[dict]]:
    """Flatten one top-level record, bounding memory to that record."""
    table_name = normalize_identifier(source)
    tables: dict[str, list[dict]] = {}
    if isinstance(item, dict):
        _flatten_object(table_name, item, source_batch_id, None, None, row_index, tables)
    else:
        row = _new_row_shell(source_batch_id, None, None, row_index)
        row["value"] = item
        tables.setdefault(table_name, []).append(row)
    tables.setdefault(table_name, [])
    return tables


def inspect_payload(path: Path) -> PayloadInfo:
    """Stream-validate a JSON document and discover its top-level row array."""
    root_type = "scalar"
    root_keys: set[str] = set()
    root_arrays: set[str] = set()
    reader: _HashingReader
    with path.open("rb") as stream:
        reader = _HashingReader(stream)
        first_event = True
        try:
            for prefix, event, value in ijson.parse(reader):
                if first_event:
                    if prefix == "" and event == "start_map":
                        root_type = "object"
                    elif prefix == "" and event == "start_array":
                        root_type = "array"
                    first_event = False
                if root_type == "object" and prefix == "" and event == "map_key":
                    root_keys.add(value)
                elif (
                    root_type == "object"
                    and event == "start_array"
                    and "." not in prefix
                ):
                    root_arrays.add(prefix)
        except JSONError as exc:
            raise ValueError("malformed JSON document") from exc
    if first_event:
        raise ValueError("empty JSON document")

    collection_key = None
    if PAGINATION_METADATA_KEYS.intersection(root_keys):
        collection_key = next(
            (key for key in PAGINATED_COLLECTION_KEYS if key in root_arrays),
            None,
        )
    return PayloadInfo(
        root_type,
        collection_key,
        reader.byte_size,
        reader.digest.hexdigest(),
    )


def iter_flattened_batches(
    source: str,
    path: Path,
    source_batch_id: int,
    root_type: str,
    collection_key: Optional[str],
    batch_size: int = FLATTEN_BATCH_SIZE,
) -> Iterator[dict[str, list[dict]]]:
    """Yield bounded groups of flattened rows from a landing JSON file."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    if root_type == "array":
        prefix = "item"
    elif collection_key is not None:
        prefix = f"{collection_key}.item"
    else:
        prefix = ""

    flattened_batch: dict[str, list[dict]] = {}
    row_index = 0
    with path.open("rb") as stream:
        for item in ijson.items(stream, prefix, use_float=True):
            _merge_tables(
                flattened_batch,
                flatten_record(source, item, source_batch_id, row_index),
            )
            row_index += 1
            if row_index % batch_size == 0:
                yield flattened_batch
                flattened_batch = {}

    if flattened_batch:
        yield flattened_batch
    elif row_index == 0:
        yield {normalize_identifier(source): []}


def _merge_tables(
    destination: dict[str, list[dict]], source: dict[str, list[dict]],
) -> None:
    for table_name, rows in source.items():
        destination.setdefault(table_name, []).extend(rows)
