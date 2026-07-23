"""Validated, projection-friendly MEDS scans."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


def _meds_compatible_schema(actual: pa.Schema, expected: pa.Schema) -> pa.Schema:
    """Normalize equivalent Arrow offset widths for strict MEDS validation."""

    def compatible_type(actual: pa.DataType, expected: pa.DataType) -> pa.DataType:
        strings = (pa.types.is_string, pa.types.is_large_string)
        lists = (pa.types.is_list, pa.types.is_large_list)
        if any(check(actual) for check in strings) and any(
            check(expected) for check in strings
        ):
            return expected
        if any(check(actual) for check in lists) and any(
            check(expected) for check in lists
        ):
            item = compatible_type(actual.value_type, expected.value_type)
            if item == expected.value_type:
                return expected
        return actual

    expected_types = {field.name: field.type for field in expected}
    return pa.schema(
        [
            field.with_type(compatible_type(field.type, expected_types[field.name]))
            if field.name in expected_types
            else field
            for field in actual
        ],
        metadata=actual.metadata,
    )


def _validate_meds_schema(path: Path, schema: pa.Schema, expected: Any) -> None:
    try:
        expected.validate(_meds_compatible_schema(schema, expected.schema()))
    except Exception as error:
        raise type(error)(f"Invalid MEDS schema in {path}: {error}") from error


def dataset_root(path: str | Path) -> Path:
    """Return a validated MEDS root containing ``data`` and ``metadata``."""

    root = Path(path).expanduser().resolve()
    for child in (meds.data_subdirectory, Path(meds.dataset_metadata_filepath).parent):
        if not (root / child).is_dir():
            raise FileNotFoundError(f"MEDS directory not found: {root / child}")
    return root


def read_dataset_metadata(root: str | Path) -> dict[str, Any]:
    """Read and validate MEDS ``metadata/dataset.json``."""

    path = dataset_root(root) / meds.dataset_metadata_filepath
    if not path.is_file():
        raise FileNotFoundError(f"MEDS dataset metadata not found: {path}")
    value = json.loads(path.read_text())
    meds.DatasetMetadataSchema.validate(value)
    return value


def code_modifier_columns(metadata: dict[str, Any]) -> list[str]:
    """Return declared code modifiers, rejecting duplicates and invalid names."""

    modifiers = metadata.get("code_modifier_columns", [])
    if not isinstance(modifiers, list) or not all(
        isinstance(item, str) and item for item in modifiers
    ):
        raise ValueError("code_modifier_columns must be a list of non-empty strings")
    if len(modifiers) != len(set(modifiers)):
        raise ValueError("code_modifier_columns contains duplicates")
    if set(modifiers) & {"subject_id", "time", "code"}:
        raise ValueError("code_modifier_columns contains a core MEDS column")
    return modifiers


def event_files(root: str | Path) -> list[Path]:
    """List event shards beneath the standard MEDS data directory."""

    data = dataset_root(root) / meds.data_subdirectory
    files = sorted(data.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No MEDS Parquet shards found below {data}")
    return files


def scan_events(root: str | Path, modifiers: Sequence[str] = ()) -> pl.LazyFrame:
    """Validate shard footers and lazily project fields needed for counting."""

    files = event_files(root)
    for path in files:
        schema = pq.read_schema(path)
        _validate_meds_schema(path, schema, meds.DataSchema)
        for column in modifiers:
            if column not in schema.names:
                raise ValueError(
                    f"Declared code modifier '{column}' is missing from {path}"
                )
            if not (
                pa.types.is_string(schema.field(column).type)
                or pa.types.is_large_string(schema.field(column).type)
            ):
                raise ValueError(f"Code modifier '{column}' must be a string in {path}")
    columns = ["subject_id", "code", *modifiers]
    return pl.scan_parquet([str(path) for path in files]).select(columns)


def scan_code_metadata(root: str | Path) -> pl.LazyFrame:
    """Validate and lazily scan the canonical code metadata table.

    Optional standard columns are added as typed nulls so the summary output
    always includes descriptions and parent-code relationships.
    """

    path = dataset_root(root) / meds.code_metadata_filepath
    if not path.is_file():
        raise FileNotFoundError(f"MEDS code metadata not found: {path}")
    schema = pq.read_schema(path)
    _validate_meds_schema(path, schema, meds.CodeMetadataSchema)
    frame = pl.scan_parquet(path)
    names = set(frame.collect_schema().names())
    missing = []
    if "description" not in names:
        missing.append(pl.lit(None, dtype=pl.String).alias("description"))
    if "parent_codes" not in names:
        missing.append(pl.lit(None, dtype=pl.List(pl.String)).alias("parent_codes"))
    return frame.with_columns(missing) if missing else frame


def scan_subject_splits(root: str | Path) -> pl.LazyFrame:
    """Validate and lazily scan canonical subject-to-split assignments."""

    path = dataset_root(root) / meds.subject_splits_filepath
    if not path.is_file():
        raise FileNotFoundError(f"MEDS subject splits not found: {path}")
    schema = pq.read_schema(path)
    required = meds.SubjectSplitSchema.schema().names
    schema = pa.schema(
        [field for field in schema if field.name in required], metadata=schema.metadata
    )
    _validate_meds_schema(path, schema, meds.SubjectSplitSchema)
    return pl.scan_parquet(path).select("subject_id", "split")
