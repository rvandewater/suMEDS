"""Lazy code occurrence summaries with rare-code masking."""

from __future__ import annotations

import re
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


def code_occurrences(
    events: pl.LazyFrame,
    keys: Sequence[str],
    splits: Sequence[str] = (),
) -> pl.LazyFrame:
    """Count total and optional named-split events and unique subjects lazily."""

    return _code_occurrences(events, keys, _split_columns(splits))


def _code_occurrences(
    events: pl.LazyFrame,
    keys: Sequence[str],
    split_columns: Sequence[tuple[str, str, str]],
) -> pl.LazyFrame:
    expressions = _count_expressions(split_columns)
    if split_columns:
        expressions.append(
            pl.col("split")
            .is_null()
            .sum()
            .cast(pl.UInt64)
            .alias("_missing_split_count")
        )
    return events.group_by(list(keys)).agg(expressions)


def summarize(
    root: str | Path,
    output: str | Path,
    config: SummaryConfig | None = None,
) -> Path:
    """Write a privacy-aware MEDS code catalog and return its path.

    Event rows remain in Polars lazy plans and are streamed to bounded-memory
    outputs. Bucket mode performs a second projected event scan so its subject
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
    events = scan_events(root, modifiers)
    scope: list[str] = []
    split_columns: list[tuple[str, str, str]] = []
    if config.per_split or config.split_columns:
        splits = scan_subject_splits(root)
        duplicate = (
            splits.group_by("subject_id").len().filter(pl.col("len") > 1).limit(1)
        )
        _raise_if_any(duplicate, "subject_splits contains duplicate subject IDs")
        if config.split_columns:
            split_values = (
                splits.select("split")
                .unique()
                .sort("split")
                .collect(engine="streaming")
                .get_column("split")
                .to_list()
            )
            if not split_values:
                raise ValueError("subject_splits contains no splits")
            split_columns = _split_columns(split_values)
        events = events.join(splits, on="subject_id", how="left")
        if config.per_split:
            scope.append("split")

    base_keys = ["code", *modifiers]
    count_keys = [*scope, *base_keys]
    split_count_names = _split_count_names(split_columns)
    _validate_metadata(metadata, modifiers, config, split_count_names)
    token = uuid4().hex
    counts_path = output.with_name(f".{output.name}.{token}.counts.parquet")
    staged_path = output.with_name(f".{output.name}.{token}.tmp{output.suffix}")

    try:
        _code_occurrences(events, count_keys, split_columns).sink_parquet(counts_path)
        counts = pl.scan_parquet(counts_path)
        if config.per_split:
            missing_split = counts.filter(pl.col("split").is_null()).limit(1)
        elif config.split_columns:
            missing_split = counts.filter(pl.col("_missing_split_count") > 0).limit(1)
            counts = counts.drop("_missing_split_count")
        else:
            missing_split = None
        if missing_split is not None:
            _raise_if_any(
                missing_split,
                "subject_splits does not cover every event subject",
            )
        safe = counts.filter(pl.col("subject_count") >= config.min_subjects)
        if split_columns:
            safe = _mask_split_counts(safe, split_columns, config.min_split_subjects)
        common = safe.join(
            metadata, on=base_keys, how="left", nulls_equal=True
        ).with_columns(pl.lit(False).alias("is_masked"))
        count_columns = [*_COUNT_COLUMNS, *split_count_names]
        columns = [
            *scope,
            *metadata.collect_schema().names(),
            *count_columns,
            "is_masked",
        ]
        common = _round_counts(common, config.round_counts_to, count_columns).select(
            columns
        )

        frames = [common]
        if config.rare_code_action == "bucket":
            rare_keys = counts.filter(
                pl.col("subject_count") < config.min_subjects
            ).select(count_keys)
            rare_events = events.join(
                rare_keys, on=count_keys, how="semi", nulls_equal=True
            )
            bucket = _rare_bucket(
                rare_events, metadata, scope, split_columns, config
            ).select(columns)
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


def _split_columns(values: Sequence[str]) -> list[tuple[str, str, str]]:
    result = []
    used = set()
    for value in values:
        suffix = re.sub(r"\W+", "_", value).strip("_").lower() or "unnamed"
        names = (f"event_count_{suffix}", f"subject_count_{suffix}")
        if used & set(names):
            raise ValueError("split names produce duplicate output columns")
        used.update(names)
        result.append((value, *names))
    return result


def _split_count_names(split_columns: Sequence[tuple[str, str, str]]) -> list[str]:
    return [name for columns in split_columns for name in columns[1:]]


def _count_expressions(
    split_columns: Sequence[tuple[str, str, str]],
) -> list[pl.Expr]:
    expressions = [
        pl.len().cast(pl.UInt64).alias("event_count"),
        pl.col("subject_id").n_unique().cast(pl.UInt64).alias("subject_count"),
    ]
    for value, event_name, subject_name in split_columns:
        in_split = pl.col("split") == value
        expressions.extend(
            [
                in_split.sum().cast(pl.UInt64).alias(event_name),
                pl.col("subject_id")
                .filter(in_split)
                .n_unique()
                .cast(pl.UInt64)
                .alias(subject_name),
            ]
        )
    return expressions


def _mask_split_counts(
    frame: pl.LazyFrame,
    split_columns: Sequence[tuple[str, str, str]],
    min_subjects: int,
) -> pl.LazyFrame:
    if min_subjects == 1:
        return frame
    return frame.with_columns(
        expression
        for _, event_name, subject_name in split_columns
        for expression in (
            pl.when(pl.col(subject_name) >= min_subjects)
            .then(pl.col(event_name))
            .otherwise(None)
            .alias(event_name),
            pl.when(pl.col(subject_name) >= min_subjects)
            .then(pl.col(subject_name))
            .otherwise(None)
            .alias(subject_name),
        )
    )


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
    metadata: pl.LazyFrame,
    modifiers: Sequence[str],
    config: SummaryConfig,
    extra_reserved: Sequence[str] = (),
) -> None:
    names = set(metadata.collect_schema().names())
    missing = set(modifiers) - names
    if missing:
        raise ValueError(
            f"Code metadata is missing declared modifiers: {sorted(missing)}"
        )
    collisions = names & (_RESERVED_COLUMNS | set(extra_reserved))
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
    split_columns: Sequence[tuple[str, str, str]],
    config: SummaryConfig,
) -> pl.LazyFrame:
    expressions = _count_expressions(split_columns)
    if scope:
        result = events.group_by(list(scope)).agg(expressions)
    else:
        result = events.select(expressions)
    result = result.filter(pl.col("subject_count") >= config.min_subjects)
    if split_columns:
        result = _mask_split_counts(result, split_columns, config.min_split_subjects)

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
        [*_COUNT_COLUMNS, *_split_count_names(split_columns)],
    )


def _round_counts(
    frame: pl.LazyFrame,
    multiple: int | None,
    columns: Sequence[str] = _COUNT_COLUMNS,
) -> pl.LazyFrame:
    if not multiple or multiple == 1:
        return frame
    return frame.with_columns(
        (((pl.col(name) + multiple // 2) // multiple) * multiple)
        .cast(pl.UInt64)
        .alias(name)
        for name in columns
    )


def _raise_if_any(frame: pl.LazyFrame, message: str) -> None:
    if frame.collect(engine="streaming").height:
        raise ValueError(message)
