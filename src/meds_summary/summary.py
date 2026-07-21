"""Lazy code occurrence summaries with rare-code masking."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence
from uuid import uuid4

import polars as pl

from .config import SummaryConfig
from .scan import (
    code_modifier_columns,
    dataset_root,
    read_dataset_metadata,
    scan_code_metadata,
    scan_events,
    scan_subject_splits,
)

_COUNT_COLUMNS = ("event_count", "subject_count")
_RESERVED_COLUMNS = {*_COUNT_COLUMNS, "is_masked", "split"}
_OUTPUT_FORMATS = {
    ".parquet": "parquet",
    ".csv": "csv",
    ".json": "json",
    ".jsonl": "ndjson",
    ".ndjson": "ndjson",
}


def code_occurrences(events: pl.LazyFrame, keys: Sequence[str]) -> pl.LazyFrame:
    """Count event rows and unique subjects for each code key lazily."""

    return events.group_by(list(keys)).agg(
        pl.len().cast(pl.UInt64).alias("event_count"),
        pl.col("subject_id").n_unique().cast(pl.UInt64).alias("subject_count"),
    )


def summarize(
    root: str | Path,
    output: str | Path,
    config: SummaryConfig | None = None,
) -> Path:
    """Write a privacy-aware MEDS code catalog and return its path.

    Event rows remain in Polars lazy plans and are streamed to bounded-memory outputs. Bucket
    mode performs a second projected event scan so the masked bucket's subject
    count is the exact union of subjects across rare codes.
    """

    config = config or SummaryConfig()
    root = dataset_root(root)
    output = Path(output).expanduser().resolve()
    output_format = _OUTPUT_FORMATS.get(output.suffix.lower())
    if output_format is None:
        raise ValueError(
            f"unsupported output suffix {output.suffix!r}; use Parquet, CSV, or JSON"
        )
    if output.is_relative_to(root / "data") or output.is_relative_to(root / "metadata"):
        raise ValueError("output must not overwrite MEDS data or metadata")
    output.parent.mkdir(parents=True, exist_ok=True)

    metadata_json = read_dataset_metadata(root)
    modifiers = code_modifier_columns(metadata_json)
    metadata = scan_code_metadata(root)
    _validate_metadata(metadata, modifiers, config)

    events = scan_events(root, modifiers)
    scope: list[str] = []
    if config.per_split:
        splits = scan_subject_splits(root)
        duplicate = (
            splits.group_by("subject_id").len().filter(pl.col("len") > 1).limit(1)
        )
        _raise_if_any(duplicate, "subject_splits contains duplicate subject IDs")
        events = events.join(splits, on="subject_id", how="left")
        scope.append("split")

    base_keys = ["code", *modifiers]
    count_keys = [*scope, *base_keys]
    token = uuid4().hex
    counts_path = output.with_name(f".{output.name}.{token}.counts.parquet")
    staged_path = output.with_name(f".{output.name}.{token}.tmp{output.suffix}")

    try:
        code_occurrences(events, count_keys).sink_parquet(counts_path)
        counts = pl.scan_parquet(counts_path)
        if config.per_split:
            _raise_if_any(
                counts.filter(pl.col("split").is_null()).limit(1),
                "subject_splits does not cover every event subject",
            )
        safe = counts.filter(pl.col("subject_count") >= config.min_subjects)
        common = safe.join(
            metadata, on=base_keys, how="left", nulls_equal=True
        ).with_columns(pl.lit(False).alias("is_masked"))
        columns = [
            *scope,
            *metadata.collect_schema().names(),
            *_COUNT_COLUMNS,
            "is_masked",
        ]
        common = _round_counts(common, config.round_counts_to).select(columns)

        frames = [common]
        if config.rare_code_action == "bucket":
            rare_keys = counts.filter(
                pl.col("subject_count") < config.min_subjects
            ).select(count_keys)
            rare_events = events.join(
                rare_keys, on=count_keys, how="semi", nulls_equal=True
            )
            bucket = _rare_bucket(rare_events, metadata, scope, config).select(columns)
            frames.append(bucket)

        result = pl.concat(frames, how="vertical_relaxed").sort(
            [*scope, "code", *modifiers]
        )
        _sink_output(result, staged_path, output_format)
        staged_path.replace(output)
    finally:
        counts_path.unlink(missing_ok=True)
        staged_path.unlink(missing_ok=True)

    return output


def _sink_output(frame: pl.LazyFrame, path: Path, output_format: str) -> None:
    if output_format == "parquet":
        frame.sink_parquet(path)
    elif output_format == "csv":
        schema = frame.collect_schema()
        nested = []
        for name, dtype in schema.items():
            column = pl.col(name)
            if isinstance(dtype, pl.Array):
                column = column.arr.to_list()
                dtype = pl.List(dtype.inner)
            if isinstance(dtype, pl.List):
                nested.append(
                    column.list.eval(pl.element().cast(pl.String))
                    .list.join("|")
                    .alias(name)
                )
            elif isinstance(dtype, pl.Struct):
                nested.append(column.struct.json_encode().alias(name))
        frame.with_columns(nested).sink_csv(path)
    elif output_format == "ndjson":
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


def _validate_metadata(
    metadata: pl.LazyFrame, modifiers: Sequence[str], config: SummaryConfig
) -> None:
    names = set(metadata.collect_schema().names())
    missing = set(modifiers) - names
    if missing:
        raise ValueError(
            f"Code metadata is missing declared modifiers: {sorted(missing)}"
        )
    collisions = names & _RESERVED_COLUMNS
    if collisions:
        raise ValueError(
            f"Code metadata uses reserved output columns: {sorted(collisions)}"
        )
    keys = ["code", *modifiers]
    duplicate = metadata.group_by(keys).len().filter(pl.col("len") > 1).limit(1)
    _raise_if_any(duplicate, f"Code metadata keys are not unique: {keys}")
    if config.rare_code_action == "bucket":
        collision = metadata.filter(pl.col("code") == config.rare_code_label).limit(1)
        _raise_if_any(
            collision, f"rare_code_label already exists: {config.rare_code_label!r}"
        )


def _rare_bucket(
    events: pl.LazyFrame,
    metadata: pl.LazyFrame,
    scope: Sequence[str],
    config: SummaryConfig,
) -> pl.LazyFrame:
    if scope:
        result = events.group_by(list(scope)).agg(
            pl.len().cast(pl.UInt64).alias("event_count"),
            pl.col("subject_id").n_unique().cast(pl.UInt64).alias("subject_count"),
        )
    else:
        result = events.select(
            pl.len().cast(pl.UInt64).alias("event_count"),
            pl.col("subject_id").n_unique().cast(pl.UInt64).alias("subject_count"),
        ).filter(pl.col("event_count") > 0)

    schema = metadata.collect_schema()
    values = [
        pl.lit(config.rare_code_label, dtype=dtype).alias(name)
        if name == "code"
        else pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in schema.items()
    ]
    return _round_counts(
        result.with_columns(*values, pl.lit(True).alias("is_masked")),
        config.round_counts_to,
    )


def _round_counts(frame: pl.LazyFrame, multiple: int | None) -> pl.LazyFrame:
    if not multiple or multiple == 1:
        return frame
    return frame.with_columns(
        (((pl.col(name) + multiple // 2) // multiple) * multiple)
        .cast(pl.UInt64)
        .alias(name)
        for name in _COUNT_COLUMNS
    )


def _raise_if_any(frame: pl.LazyFrame, message: str) -> None:
    if frame.collect(engine="streaming").height:
        raise ValueError(message)
