"""Lazy code occurrence summaries with rare-code masking."""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from statistics import NormalDist
from typing import Callable, Sequence
from uuid import uuid4

import polars as pl

from .config import SummaryConfig
from .enrich import _coverage, _enrich, _print_enrichment_report
from .io import output_format, scan_table, sink_table
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
_NUMERIC_PARTIALS = (
    "_numeric_count",
    "_numeric_subjects",
    "_numeric_min",
    "_numeric_max",
    "_numeric_mean",
    "_numeric_m2",
)


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
    numeric: bool = False,
) -> pl.LazyFrame:
    expressions = _count_expressions(split_columns)
    if numeric:
        expressions.extend(_numeric_partial_expressions())
    if split_columns:
        expressions.append(
            pl.col("split")
            .is_null()
            .sum()
            .cast(pl.UInt64)
            .alias("_missing_split_count")
        )
    return _aggregate(events, keys, expressions)


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


def _aggregate(
    frame: pl.LazyFrame, keys: Sequence[str], expressions: Sequence[pl.Expr]
) -> pl.LazyFrame:
    return (
        frame.group_by(list(keys)).agg(expressions)
        if keys
        else frame.select(expressions)
    )


def _sum_counts(
    frame: pl.LazyFrame,
    keys: Sequence[str],
    columns: Sequence[str],
    extra: Sequence[pl.Expr] = (),
) -> pl.LazyFrame:
    expressions = [pl.col(name).sum().cast(pl.UInt64).alias(name) for name in columns]
    return _aggregate(frame, keys, [*expressions, *extra])


def _sink_partials(
    event_parts: dict[str, Path],
    partial_dir: Path,
    partial: Callable[[Path], pl.LazyFrame],
) -> pl.LazyFrame:
    partial_dir.mkdir(parents=True)
    paths = []
    for name, event_path in sorted(event_parts.items()):
        path = partial_dir / f"{name}.parquet"
        partial(event_path).sink_parquet(path, compression="lz4", maintain_order=False)
        paths.append(str(path))
    return pl.scan_parquet(paths)


def _sink_partitioned_counts(
    event_parts: dict[str, Path],
    split_parts: dict[str, Path] | None,
    keys: Sequence[str],
    split_columns: Sequence[tuple[str, str, str]],
    partial_dir: Path,
    output: Path,
    numeric: bool = False,
) -> None:
    if not event_parts:
        raise ValueError("MEDS data contains no events")
    partials = _sink_partials(
        event_parts,
        partial_dir,
        lambda path: _code_occurrences(
            _partition_events(path, split_parts, validate_splits=True),
            keys,
            split_columns,
            numeric,
        ),
    )
    names = [*_COUNT_COLUMNS, *_split_count_names(split_columns)]
    if split_columns:
        names.append("_missing_split_count")
    # Subjects occur in exactly one hash partition, so exact subject counts add.
    _sum_counts(
        partials, keys, names, _merge_numeric_partials() if numeric else ()
    ).sink_parquet(output, maintain_order=False)


def summarize(
    root: str | Path,
    output: str | Path,
    config: SummaryConfig | None = None,
    *,
    verbose: bool = False,
) -> Path:
    """Write a privacy-aware MEDS code catalog and return its path.

    Projected event rows are partitioned by subject on disk so exact distinct
    counts remain additive with bounded aggregation state. Bucket mode reuses
    those partitions to count the exact subject union across rare codes.
    ``verbose=True`` prints the enrichment coverage and rule report.
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
    numeric_stats = config.numeric_stats
    events = scan_events(root, modifiers, numeric=bool(numeric_stats))
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
    numeric_columns = _numeric_columns(numeric_stats)
    token = uuid4().hex
    counts_path = output.with_name(f".{output.name}.{token}.counts.parquet")
    staged_path = output.with_name(f".{output.name}.{token}.tmp{output.suffix}")
    work_path = output.with_name(f".{output.name}.{token}.work")
    report = None
    stats: dict = {}

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
            numeric=bool(numeric_stats),
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
        if config.rare_code_action == "bucket":
            # Released codes need not have metadata, so check counts, not metadata.
            _raise_if_any(
                safe.filter(pl.col("code") == config.rare_code_label).limit(1),
                f"rare_code_label already exists: {config.rare_code_label!r}",
            )
        if numeric_stats:
            medians = None
            if "median" in numeric_stats:
                medians = _sink_medians(
                    event_parts,
                    split_parts if config.per_split else None,
                    safe.select(count_keys),
                    count_keys,
                    work_path / "median.parquet",
                )
            safe = _numeric_statistics(safe, medians, count_keys, config)
        metadata = metadata.join(
            safe.select(base_keys),
            on=base_keys,
            how="semi",
            nulls_equal=True,
        )
        _validate_metadata(metadata, modifiers, [*split_count_names, *numeric_columns])
        common = safe.join(
            metadata, on=base_keys, how="left", nulls_equal=True
        ).with_columns(pl.lit(False).alias("is_masked"))
        count_columns = [*_COUNT_COLUMNS, *split_count_names]
        columns = [
            *scope,
            *metadata.collect_schema().names(),
            *count_columns,
            *numeric_columns,
            "is_masked",
        ]
        common = _round_counts(
            common,
            config.round_counts_to,
            [*count_columns, *numeric_columns[:1]],
        ).select(columns)

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
            )
            # Mixed-code values have no meaning, so the bucket carries none.
            bucket = bucket.with_columns(
                pl.lit(
                    None, dtype=pl.UInt64 if name == "numeric_count" else pl.Float64
                ).alias(name)
                for name in numeric_columns
            ).select(columns)
            frames.append(bucket)

        result = pl.concat(frames, how="vertical_relaxed").sort(
            [*scope, "code", *modifiers]
        )
        if config.enrichment is not None:
            unenriched = result
            result = _enrich(result, config.enrichment, stats)
        sink_table(result, staged_path, format_)
        staged_path.replace(output)
        if verbose and config.enrichment is not None:
            report = _coverage(unenriched), _coverage(scan_table(output))
    finally:
        counts_path.unlink(missing_ok=True)
        staged_path.unlink(missing_ok=True)
        shutil.rmtree(work_path, ignore_errors=True)

    if report is not None:
        _print_enrichment_report(report, stats)
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


def _numeric_columns(stats: Sequence[str]) -> list[str]:
    if not stats:
        return []
    names = ["numeric_count"]
    for stat in stats:
        if stat == "ci":
            names.extend(("numeric_ci_lower", "numeric_ci_upper"))
        else:
            names.append(f"numeric_{stat}")
    return names


def _numeric_partial_expressions() -> list[pl.Expr]:
    finite = pl.col("numeric_value").is_finite()
    value = pl.col("numeric_value").filter(finite)
    return [
        value.count().cast(pl.UInt64).alias("_numeric_count"),
        pl.col("subject_id")
        .filter(finite)
        .n_unique()
        .cast(pl.UInt64)
        .alias("_numeric_subjects"),
        value.min().alias("_numeric_min"),
        value.max().alias("_numeric_max"),
        value.mean().alias("_numeric_mean"),
        ((value - value.mean()) ** 2).sum().alias("_numeric_m2"),
    ]


def _merge_numeric_partials() -> list[pl.Expr]:
    count = pl.col("_numeric_count")
    mean = (count * pl.col("_numeric_mean")).sum() / count.sum()
    return [
        count.sum().cast(pl.UInt64).alias("_numeric_count"),
        # Subjects occur in one partition, so value-bearing subject counts add.
        pl.col("_numeric_subjects").sum().cast(pl.UInt64).alias("_numeric_subjects"),
        pl.col("_numeric_min").min().alias("_numeric_min"),
        pl.col("_numeric_max").max().alias("_numeric_max"),
        mean.alias("_numeric_mean"),
        # Chan et al. pairwise merge avoids the cancellation of sum-of-squares.
        (pl.col("_numeric_m2") + count * (pl.col("_numeric_mean") - mean) ** 2)
        .sum()
        .alias("_numeric_m2"),
    ]


def _sink_medians(
    event_parts: dict[str, Path],
    split_parts: dict[str, Path] | None,
    keys: pl.LazyFrame,
    count_keys: Sequence[str],
    output: Path,
) -> pl.LazyFrame:
    # ponytail: exact medians are not additive across subject partitions, so
    # this pass holds every released code's values; opt-in via numeric_stats.
    events = pl.concat(
        [
            _partition_events(path, split_parts, validate_splits=False)
            for _, path in sorted(event_parts.items())
        ]
    )
    events.join(keys, on=list(count_keys), how="semi", nulls_equal=True).filter(
        pl.col("numeric_value").is_finite()
    ).group_by(list(count_keys)).agg(
        pl.col("numeric_value").median().alias("numeric_median")
    ).sink_parquet(output, maintain_order=False)
    return pl.scan_parquet(output)


def _numeric_statistics(
    frame: pl.LazyFrame,
    medians: pl.LazyFrame | None,
    count_keys: Sequence[str],
    config: SummaryConfig,
) -> pl.LazyFrame:
    if medians is not None:
        frame = frame.join(medians, on=list(count_keys), how="left", nulls_equal=True)
    count = pl.col("_numeric_count").cast(pl.Float64)
    mean = pl.col("_numeric_mean")
    std = pl.when(count > 1).then((pl.col("_numeric_m2") / (count - 1)).sqrt())
    z = NormalDist().inv_cdf((1 + config.numeric_ci_level) / 2)
    margin = z * std / count.sqrt()
    values = {
        "numeric_count": pl.col("_numeric_count"),
        "numeric_min": pl.col("_numeric_min"),
        "numeric_max": pl.col("_numeric_max"),
        "numeric_mean": mean,
        "numeric_median": pl.col("numeric_median"),
        "numeric_std": std,
        "numeric_ci_lower": mean - margin,
        "numeric_ci_upper": mean + margin,
    }
    # Values from fewer than min_subjects people are suppressed like rare codes.
    released = pl.col("_numeric_subjects") >= config.min_subjects
    return frame.with_columns(
        pl.when(released).then(values[name]).alias(name)
        for name in _numeric_columns(config.numeric_stats)
    ).drop(_NUMERIC_PARTIALS)


def _mask_split_counts(
    frame: pl.LazyFrame,
    split_columns: Sequence[tuple[str, str, str]],
    min_subjects: int,
) -> pl.LazyFrame:
    if min_subjects == 1:
        return frame
    return frame.with_columns(
        pl.when(pl.col(subject_name) >= min_subjects).then(name).alias(name)
        for _, event_name, subject_name in split_columns
        for name in (event_name, subject_name)
    )


def _validate_metadata(
    metadata: pl.LazyFrame,
    modifiers: Sequence[str],
    extra_reserved: Sequence[str],
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
    partials = _sink_partials(
        event_parts,
        partial_dir,
        lambda path: _code_occurrences(
            _partition_events(path, split_parts, validate_splits=False).join(
                rare_keys, on=list(count_keys), how="semi", nulls_equal=True
            ),
            scope,
            split_columns,
        ),
    )
    count_columns = [*_COUNT_COLUMNS, *_split_count_names(split_columns)]
    result = _sum_counts(partials, scope, count_columns)
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
    columns: Sequence[str],
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
