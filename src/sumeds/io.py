"""Small table readers and bounded-memory writers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import meds
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

#: Map entries: a list of ``key``/``value`` structs. Polars 1, which has no map
#: type, reads an Arrow map as these, and JSON output stores a map as these on
#: every Polars version: JSON objects would have different keys in every row,
#: which Polars' JSON schema inference cannot read back.
MAP_ENTRIES = pl.List(pl.Struct({"key": pl.String, "value": pl.String}))
#: A string-to-string map column: ``pl.Map`` on Polars 2, map entries on Polars 1.
#: Parquet output stores it as ``map<string, string>``, CSV as a JSON object and
#: JSON as map entries.
MAP_DTYPE = pl.Map(pl.String, pl.String) if hasattr(pl, "Map") else MAP_ENTRIES
#: The names lists of earlier suMEDS versions, aligned with their codes list. CSV
#: stores them as JSON arrays, because names can contain ``|`` and can be missing.
JSON_LIST_COLUMNS = frozenset({"parent_names", "child_names", "sibling_names"})
OUTPUT_FORMATS = {
    ".parquet": "parquet",
    ".csv": "csv",
    ".json": "json",
    ".jsonl": "ndjson",
    ".ndjson": "ndjson",
}


def output_format(path: str | Path) -> str:
    """Return the table format selected by a path suffix."""

    path = Path(path)
    try:
        return OUTPUT_FORMATS[path.suffix.lower()]
    except KeyError:
        raise ValueError(
            f"unsupported output suffix {path.suffix!r}; use Parquet (.parquet), CSV (.csv), JSON (.json), or NDJSON (.jsonl/.ndjson)"
        ) from None


def scan_table(path: str | Path) -> pl.LazyFrame:
    """Read a supported standalone metadata or summary table.

    Parquet, CSV, JSONL, and NDJSON inputs are scanned lazily. Polars has no
    lazy reader for standard JSON arrays, so ``.json`` is loaded eagerly before
    being converted to a :class:`LazyFrame`. Prefer Parquet or newline-delimited
    JSON for large inputs.
    """

    path = Path(path).expanduser().resolve()
    format_ = output_format(path)
    if not path.is_file():
        raise FileNotFoundError(f"table not found: {path}")
    if format_ == "parquet":
        return pl.scan_parquet(path)
    if format_ == "csv":
        return pl.scan_csv(path)
    if format_ == "ndjson":
        return pl.scan_ndjson(path)
    # Polars intentionally exposes scan_ndjson, but no scan for JSON arrays.
    return pl.read_json(path).lazy()


def sink_table(
    frame: pl.LazyFrame, path: str | Path, format_: str | None = None
) -> None:
    """Write a lazy table in a supported format."""

    path = Path(path)
    format_ = format_ or output_format(path)
    if format_ == "parquet":
        _sink_meds_parquet(frame, path)
    elif format_ == "csv":
        schema = frame.collect_schema()
        nested = []
        for name, dtype in schema.items():
            column = pl.col(name)
            if isinstance(dtype, pl.Array):
                column = column.arr.to_list()
                dtype = pl.List(dtype.inner)
            if dtype in (MAP_DTYPE, MAP_ENTRIES):
                nested.append(
                    column.map_elements(_json_object, return_dtype=pl.String).alias(
                        name
                    )
                )
            elif isinstance(dtype, pl.List) and name in JSON_LIST_COLUMNS:
                nested.append(
                    column.map_elements(
                        lambda values: json.dumps(list(values), ensure_ascii=False),
                        return_dtype=pl.String,
                    ).alias(name)
                )
            elif isinstance(dtype, pl.List):
                nested.append(
                    column.list.eval(pl.element().cast(pl.String))
                    .list.join("|")
                    .alias(name)
                )
            elif isinstance(dtype, pl.Struct):
                nested.append(column.struct.json_encode().alias(name))
        frame.with_columns(nested).sink_csv(path)
    elif format_ == "ndjson":
        _with_map_entries(frame).sink_ndjson(path)
    else:
        lines = path.with_suffix(path.suffix + ".ndjson")
        try:
            _with_map_entries(frame).sink_ndjson(lines)
            with lines.open("rb") as source, path.open("wb") as target:
                target.write(b"[")
                separator = b""
                for line in source:
                    line = line.strip()
                    if line:
                        target.write(separator + line)
                        separator = b",\n"
                target.write(b"]\n")
        finally:
            lines.unlink(missing_ok=True)


def _json_object(value: Any) -> str:
    """A map value (a dict, or map entries on Polars 1) as a JSON object."""

    if not isinstance(value, dict):
        value = {entry["key"]: entry["value"] for entry in value}
    return json.dumps(value, ensure_ascii=False)


def _with_map_entries(frame: pl.LazyFrame) -> pl.LazyFrame:
    """``frame`` with its ``pl.Map`` columns as map entries, for JSON output."""

    if MAP_DTYPE == MAP_ENTRIES:
        return frame
    maps = [
        name for name, dtype in frame.collect_schema().items() if dtype == MAP_DTYPE
    ]
    return frame.with_columns(pl.col(maps).cast(MAP_ENTRIES)) if maps else frame


_MAP_TYPE = pa.map_(pa.string(), pa.string())


def _is_string_map(type_: pa.DataType) -> bool:
    """Whether ``type_`` is a string-to-string map, or the map entries of one."""

    if pa.types.is_map(type_):
        fields = [type_.key_field, type_.item_field]
    elif pa.types.is_list(type_) or pa.types.is_large_list(type_):
        entry = type_.value_type
        if not pa.types.is_struct(entry) or [f.name for f in entry] != ["key", "value"]:
            return False
        fields = list(entry)
    else:
        return False
    return all(
        pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
        for field in fields
    )


def _meds_output_schema(schema: pa.Schema) -> pa.Schema:
    """Give the MEDS code-metadata columns of ``schema`` their MEDS Arrow types.

    Polars writes every string as ``large_string`` and every list as ``large_list``,
    but the MEDS schema requires ``string`` and ``list<string>``, and
    ``meds.CodeMetadataSchema.validate`` rejects the large variants. Map columns
    (:data:`MAP_DTYPE`, which Polars 2 writes with ``large_string`` keys and values)
    become ``map<string, string>``. Other columns keep their type.
    """

    expected = {field.name: field.type for field in meds.CodeMetadataSchema.schema()}
    return pa.schema(
        [
            field.with_type(
                expected.get(
                    field.name,
                    _MAP_TYPE if _is_string_map(field.type) else field.type,
                )
            )
            for field in schema
        ]
    )


def _as_map(array: pa.Array, type_: pa.MapType) -> pa.MapArray:
    """A list of ``key``/``value`` structs as a map array."""

    keys, items = array.values.flatten()
    offsets = array.offsets.cast(pa.int32())
    if array.null_count:
        # A null offset makes its slot null (``MapArray.from_arrays`` has no mask
        # before pyarrow 17); the closing offset is never null.
        nulls = pa.concat_arrays([array.is_null(), pa.array([False])])
        offsets = pc.if_else(nulls, pa.scalar(None, pa.int32()), offsets)
    return pa.MapArray.from_arrays(
        offsets, keys.cast(type_.key_type), items.cast(type_.item_type)
    )


def _with_schema(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    """``batch`` in ``schema``; a cast cannot make a map from map entries."""

    columns = [
        _as_map(column, field.type)
        if pa.types.is_map(field.type) and not pa.types.is_map(column.type)
        else column.cast(field.type)
        for column, field in zip(batch.columns, schema)
    ]
    return pa.RecordBatch.from_arrays(columns, schema=schema)


def _sink_meds_parquet(frame: pl.LazyFrame, path: Path) -> None:
    """Stream ``frame`` to Parquet with the MEDS types of :func:`_meds_output_schema`.

    Polars cannot write the narrower types, so the frame is streamed to a staged
    file and re-encoded one record batch at a time, keeping memory bounded.
    """

    staged = path.with_name(f".{path.name}.{uuid4().hex}.polars.parquet")
    try:
        frame.sink_parquet(staged)
        with pq.ParquetFile(staged) as source:
            schema = _meds_output_schema(source.schema_arrow)
            if not schema.equals(source.schema_arrow):
                with pq.ParquetWriter(path, schema, compression="zstd") as writer:
                    for batch in source.iter_batches():
                        writer.write_batch(_with_schema(batch, schema))
                return
        os.replace(staged, path)
    finally:
        staged.unlink(missing_ok=True)
