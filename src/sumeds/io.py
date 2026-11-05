"""Small table readers and bounded-memory writers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

import meds
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

#: A string-to-string map column. Polars has no map type and reads an Arrow map
#: as a list of ``key``/``value`` structs, so that is the in-memory form; Parquet
#: output stores it as ``map<string, string>`` and CSV as a JSON object.
MAP_DTYPE = pl.List(pl.Struct({"key": pl.String, "value": pl.String}))
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
            if dtype == MAP_DTYPE:
                nested.append(
                    column.map_elements(
                        lambda entries: json.dumps(
                            {entry["key"]: entry["value"] for entry in entries},
                            ensure_ascii=False,
                        ),
                        return_dtype=pl.String,
                    ).alias(name)
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
        frame.sink_ndjson(path)
    else:
        lines = path.with_suffix(path.suffix + ".ndjson")
        try:
            frame.sink_ndjson(lines)
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


_MAP_TYPE = pa.map_(pa.string(), pa.string())


def _is_map_entries(type_: pa.DataType) -> bool:
    """Whether ``type_`` is the list of string ``key``/``value`` structs of a map."""

    if not (pa.types.is_list(type_) or pa.types.is_large_list(type_)):
        return False
    entry = type_.value_type
    return (
        pa.types.is_struct(entry)
        and [field.name for field in entry] == ["key", "value"]
        and all(
            pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
            for field in entry
        )
    )


def _meds_output_schema(schema: pa.Schema) -> pa.Schema:
    """Give the MEDS code-metadata columns of ``schema`` their MEDS Arrow types.

    Polars writes every string as ``large_string`` and every list as ``large_list``,
    but the MEDS schema requires ``string`` and ``list<string>``, and
    ``meds.CodeMetadataSchema.validate`` rejects the large variants. Map columns
    (:data:`MAP_DTYPE`) become ``map<string, string>``. Other columns keep their
    type.
    """

    expected = {field.name: field.type for field in meds.CodeMetadataSchema.schema()}
    return pa.schema(
        [
            field.with_type(
                expected.get(
                    field.name,
                    _MAP_TYPE if _is_map_entries(field.type) else field.type,
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
    """``batch`` in ``schema``; a cast cannot make a map from a list of structs."""

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
