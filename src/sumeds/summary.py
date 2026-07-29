"""Lazy code occurrence summaries with rare-code masking."""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Sequence
from uuid import uuid4

import polars as pl

from .config import SummaryConfig
from .enrich import enrich_metadata
from .io import output_format, sink_table
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


def _partition_by_subject(
    frame: pl.LazyFrame, path: Path, partitions: int
) -> dict[str, Path]:
    # ponytail: fixed-count disk partitions trade I/O for bounded group state.
    key = (pl.col("subject_id").hash(seed=0) % partitions).alias("_partition")
    # PartitionBy replaced PartitionByKey in Polars 1.37.
    partition = (
        pl.PartitionBy(path, key=key, include_key=False)
        if hasattr(pl, "PartitionBy")
        else pl.PartitionByKey(path, by=key, include_key=False)
    )
    frame.sink_parquet(
        partition,
        compression="lz4",
        row_group_size=10_000,
        maintain_order=False,
        mkdir=True,
    )
    return {
        child.name: child
        for child in path.iterdir()
        if child.is_dir() and any(child.glob("*.parquet"))
    }


def _scan_partition(path: Path) -> pl.LazyFrame:
    return pl.scan_parquet([str(file) for file in sorted(path.glob("*.parquet"))])


def _partition_events(
    event_path: Path,
    split_parts: dict[str, Path] | None,
    validate_splits: bool,
) -> pl.LazyFrame:
    events = _scan_partition(event_path)
    if split_parts is None:
        return events
    split_path = split_parts.get(event_path.name)
    if split_path is None:
        return events.with_columns(pl.lit(None, dtype=pl.String).alias("split"))
    splits = _scan_partition(split_path)
    if validate_splits:
        duplicate = (
            splits.group_by("subject_id").len().filter(pl.col("len") > 1).limit(1)
        )
        _raise_if_any(duplicate, "subject_splits contains duplicate subject IDs")
    return events.join(splits, on="subject_id", how="left")


def _partial_count_names(
    split_columns: Sequence[tuple[str, str, str]],
) -> list[str]:
    names = [*_COUNT_COLUMNS, *_split_count_names(split_columns)]
    if split_columns:
        names.append("_missing_split_count")
    return names


def _sum_counts(
    frame: pl.LazyFrame, keys: Sequence[str], columns: Sequence[str]
) -> pl.LazyFrame:
    expressions = [pl.col(name).sum().cast(pl.UInt64).alias(name) for name in columns]
    return (
        frame.group_by(list(keys)).agg(expressions)
        if keys
        else frame.select(expressions)
    )


def _sink_partitioned_counts(
    event_parts: dict[str, Path],
    split_parts: dict[str, Path] | None,
    keys: Sequence[str],
    split_columns: Sequence[tuple[str, str, str]],
    partial_dir: Path,
    output: Path,
) -> None:
    if not event_parts:
        raise ValueError("MEDS data contains no events")
    partial_dir.mkdir(parents=True)
    partial_paths = []
    for name, event_path in sorted(event_parts.items()):
        path = partial_dir / f"{name}.parquet"
        _code_occurrences(
            _partition_events(event_path, split_parts, validate_splits=True),
            keys,
            split_columns,
        ).sink_parquet(path, compression="lz4", maintain_order=False)
        partial_paths.append(str(path))
    # Subjects occur in exactly one hash partition, so exact subject counts add.
    _sum_counts(
        pl.scan_parquet(partial_paths),
        keys,
        _partial_count_names(split_columns),
    ).sink_parquet(output, maintain_order=False)


def summarize(
    root: str | Path,
    output: str | Path,
    config: SummaryConfig | None = None,
) -> Path:
    """Write a privacy-aware MEDS code catalog and return its path.

    Projected event rows are partitioned by subject on disk so exact distinct
    counts remain additive with bounded aggregation state. Bucket mode reuses
    those partitions to count the exact subject union across rare codes.
    """

    config = config or SummaryConfig()
    root = dataset_root(root)
    output = Path(output).expanduser().resolve()
    format_ = output_format(output)
    if output.is_relative_to(root / "data") or output.is_relative_to(root / "metadata"):
        raise ValueError("output must not overwrite MEDS data or metadata")
    output.parent.mkdir(parents=True, exist_ok=True)

    metadata_json = read_dataset_metadata(root)
    modifiers = code_modifier_columns(metadata_json)
    metadata = scan_code_metadata(root)
    events = scan_events(root, modifiers)
    splits = None
    scope: list[str] = []
    split_columns: list[tuple[str, str, str]] = []
    if config.per_split or config.split_columns:
        splits = scan_subject_splits(root)
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
        if config.per_split:
            scope.append("split")

    base_keys = ["code", *modifiers]
    count_keys = [*scope, *base_keys]
    split_count_names = _split_count_names(split_columns)
    token = uuid4().hex
    counts_path = output.with_name(f".{output.name}.{token}.counts.parquet")
    staged_path = output.with_name(f".{output.name}.{token}.tmp{output.suffix}")
    work_path = output.with_name(f".{output.name}.{token}.work")

    try:
        event_parts = _partition_by_subject(
            events, work_path / "events", config.partitions
        )
        split_parts = (
            _partition_by_subject(splits, work_path / "splits", config.partitions)
            if splits is not None
            else None
        )
        _sink_partitioned_counts(
            event_parts,
            split_parts,
            count_keys,
            split_columns,
            work_path / "counts",
            counts_path,
        )
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
        metadata = metadata.join(
            safe.select(base_keys),
            on=base_keys,
            how="semi",
            nulls_equal=True,
        )
        _validate_metadata(metadata, modifiers, config, split_count_names)
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
            bucket = _partitioned_rare_bucket(
                event_parts,
                split_parts,
                rare_keys,
                metadata,
                scope,
                count_keys,
                split_columns,
                config,
                work_path / "rare",
            ).select(columns)
            frames.append(bucket)

        result = pl.concat(frames, how="vertical_relaxed").sort(
            [*scope, "code", *modifiers]
        )
        if config.enrichment is not None:
            result = enrich_metadata(result, config.enrichment)
        sink_table(result, staged_path, format_)
        staged_path.replace(output)
    finally:
        counts_path.unlink(missing_ok=True)
        staged_path.unlink(missing_ok=True)
        shutil.rmtree(work_path, ignore_errors=True)

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


def _partitioned_rare_bucket(
    event_parts: dict[str, Path],
    split_parts: dict[str, Path] | None,
    rare_keys: pl.LazyFrame,
    metadata: pl.LazyFrame,
    scope: Sequence[str],
    count_keys: Sequence[str],
    split_columns: Sequence[tuple[str, str, str]],
    config: SummaryConfig,
    partial_dir: Path,
) -> pl.LazyFrame:
    partial_dir.mkdir(parents=True)
    partial_paths = []
    expressions = _count_expressions(split_columns)
    for name, event_path in sorted(event_parts.items()):
        events = _partition_events(event_path, split_parts, validate_splits=False)
        rare_events = events.join(
            rare_keys, on=list(count_keys), how="semi", nulls_equal=True
        )
        partial = (
            rare_events.group_by(list(scope)).agg(expressions)
            if scope
            else rare_events.select(expressions)
        )
        path = partial_dir / f"{name}.parquet"
        partial.sink_parquet(path, compression="lz4", maintain_order=False)
        partial_paths.append(str(path))

    count_columns = [*_COUNT_COLUMNS, *_split_count_names(split_columns)]
    result = _sum_counts(pl.scan_parquet(partial_paths), scope, count_columns)
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
        count_columns,
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
