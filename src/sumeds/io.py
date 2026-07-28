"""Small table readers and bounded-memory writers."""

from __future__ import annotations

from pathlib import Path

import polars as pl

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
    """Scan a supported standalone metadata or summary table."""

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
    return pl.read_json(path).lazy()


def sink_table(
    frame: pl.LazyFrame, path: str | Path, format_: str | None = None
) -> None:
    """Write a lazy table in a supported format."""

    path = Path(path)
    format_ = format_ or output_format(path)
    if format_ == "parquet":
        frame.sink_parquet(path)
    elif format_ == "csv":
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
