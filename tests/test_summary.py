from __future__ import annotations

from pathlib import Path

import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sumeds import SummaryConfig, summarize
from sumeds.scan import _meds_compatible_schema, scan_subject_splits


def test_bucket_masks_metadata_and_counts_subject_union(
    meds_dataset: Path, tmp_path: Path
) -> None:
    output = summarize(
        meds_dataset, tmp_path / "summary.parquet", SummaryConfig(min_subjects=3)
    )
    rows = {row["code"]: row for row in pl.read_parquet(output).to_dicts()}

    assert rows["A"] == {
        "code": "A",
        "modifier": "main",
        "description": "Common code",
        "parent_codes": ["ROOT"],
        "event_count": 3,
        "subject_count": 3,
        "is_masked": False,
    }
    assert rows["__RARE__"]["event_count"] == 4
    assert rows["__RARE__"]["subject_count"] == 3  # union, not 2 + 2
    assert rows["__RARE__"]["description"] is None
    assert rows["__RARE__"]["modifier"] is None
    assert rows["__RARE__"]["is_masked"] is True


def test_drop_and_rounding(meds_dataset: Path, tmp_path: Path) -> None:
    output = summarize(
        meds_dataset,
        tmp_path / "summary.parquet",
        SummaryConfig(min_subjects=3, rare_code_action="drop", round_counts_to=5),
    )
    result = pl.read_parquet(output)
    assert result["code"].to_list() == ["A"]
    assert result["event_count"].to_list() == [5]
    assert result["subject_count"].to_list() == [5]


def test_per_split_uses_subject_metadata(meds_dataset: Path, tmp_path: Path) -> None:
    output = summarize(
        meds_dataset,
        tmp_path / "summary.parquet",
        SummaryConfig(per_split=True, min_subjects=2),
    )
    result = pl.read_parquet(output)

    assert set(
        result.filter(~pl.col("is_masked")).select("split", "code").iter_rows()
    ) == {
        ("train", "A"),
        ("train", "B"),
    }
    assert result.filter(pl.col("is_masked")).is_empty()


def test_split_columns_include_private_cells_and_totals(
    meds_dataset: Path, tmp_path: Path
) -> None:
    output = summarize(
        meds_dataset,
        tmp_path / "summary.parquet",
        SummaryConfig(split_columns=True, min_subjects=2, min_split_subjects=2),
    )
    rows = {row["code"]: row for row in pl.read_parquet(output).to_dicts()}

    assert rows["A"]["event_count"] == 3
    assert rows["A"]["subject_count"] == 3
    assert rows["A"]["event_count_train"] == 2
    assert rows["A"]["subject_count_train"] == 2
    assert rows["A"]["event_count_held_out"] is None
    assert rows["A"]["subject_count_held_out"] is None
    assert rows["B"]["event_count_train"] == 2
    assert rows["C"]["event_count_train"] is None
    assert rows["C"]["event_count_held_out"] is None


@pytest.mark.parametrize(
    ("suffix", "reader"),
    [
        (".csv", pl.read_csv),
        (".json", pl.read_json),
        (".jsonl", pl.read_ndjson),
    ],
)
def test_optional_text_outputs(
    meds_dataset: Path, tmp_path: Path, suffix: str, reader
) -> None:
    output = summarize(
        meds_dataset, tmp_path / f"summary{suffix}", SummaryConfig(min_subjects=3)
    )
    result = reader(output)
    assert result["code"].to_list() == ["A", "__RARE__"]
    assert result["event_count"].sum() == 7
    if suffix == ".csv":
        assert result["parent_codes"][0] == "ROOT"
    else:
        assert result["parent_codes"].to_list()[0] == ["ROOT"]


def test_subject_splits_allows_unused_extra_columns(meds_dataset: Path) -> None:
    path = meds_dataset / "metadata" / "subject_splits.parquet"
    table = pq.read_table(path).append_column("has_visit", [[True, True, False]])
    pq.write_table(table, path)

    result = scan_subject_splits(meds_dataset).collect()

    assert result.columns == ["subject_id", "split"]


def test_subject_splits_schema_error_names_file(meds_dataset: Path) -> None:
    path = meds_dataset / "metadata" / "subject_splits.parquet"
    table = pq.read_table(path)
    table = table.set_column(1, "split", pa.array([1, 1, 2]))
    pq.write_table(table, path)

    with pytest.raises(
        Exception, match=r"Invalid MEDS schema in .*subject_splits.parquet"
    ):
        scan_subject_splits(meds_dataset)


def test_large_arrow_offsets_are_meds_compatible(
    meds_dataset: Path, tmp_path: Path
) -> None:
    def rewrite(path: Path, types: dict[str, pa.DataType]) -> None:
        table = pq.read_table(path)
        schema = pa.schema(
            [
                field.with_type(types.get(field.name, field.type))
                for field in table.schema
            ],
            metadata=table.schema.metadata,
        )
        pq.write_table(table.cast(schema), path)

    for path in (meds_dataset / "data").glob("*.parquet"):
        rewrite(path, {"code": pa.large_string()})
    rewrite(
        meds_dataset / "metadata" / "codes.parquet",
        {
            "code": pa.large_string(),
            "description": pa.large_string(),
            "parent_codes": pa.large_list(pa.field("element", pa.large_string())),
        },
    )
    large_metadata_schema = pq.read_schema(meds_dataset / "metadata" / "codes.parquet")
    assert large_metadata_schema.field("code").type == pa.large_string()
    assert pa.types.is_large_list(large_metadata_schema.field("parent_codes").type)
    rewrite(
        meds_dataset / "metadata" / "subject_splits.parquet",
        {"split": pa.large_string()},
    )

    output = summarize(
        meds_dataset,
        tmp_path / "summary.parquet",
        SummaryConfig(split_columns=True, min_subjects=2),
    )
    rows = {row["code"]: row for row in pl.read_parquet(output).to_dicts()}

    assert set(rows) == {"A", "B", "C"}
    assert rows["A"]["description"] == "Common code"
    assert rows["A"]["parent_codes"] == ["ROOT"]
    assert rows["A"]["event_count_train"] == 2

    bad = pa.schema([pa.field("code", pa.int64(), nullable=False)])
    with pytest.raises(Exception, match="incorrect types"):
        meds.CodeMetadataSchema.validate(
            _meds_compatible_schema(bad, meds.CodeMetadataSchema.schema())
        )


def test_unused_duplicate_metadata_is_ignored(
    meds_dataset: Path, tmp_path: Path
) -> None:
    codes_path = meds_dataset / "metadata" / "codes.parquet"
    codes = pq.read_table(codes_path)
    unused = pa.Table.from_pylist(
        [
            {
                "code": "UNUSED",
                "modifier": "x",
                "description": description,
                "parent_codes": ["ROOT"],
            }
            for description in ("First", "Second")
        ],
        schema=codes.schema,
    )
    pq.write_table(pa.concat_tables([codes, unused]), codes_path)

    output = summarize(
        meds_dataset, tmp_path / "summary.parquet", SummaryConfig(min_subjects=2)
    )

    assert "UNUSED" not in pl.read_parquet(output)["code"].to_list()


def test_output_cannot_overwrite_source_dataset(meds_dataset: Path) -> None:
    with pytest.raises(ValueError, match="must not overwrite"):
        summarize(meds_dataset, meds_dataset / "metadata" / "codes.parquet")


def test_failure_preserves_existing_output(meds_dataset: Path, tmp_path: Path) -> None:
    output = tmp_path / "summary.parquet"
    output.write_bytes(b"existing")
    codes_path = meds_dataset / "metadata" / "codes.parquet"
    codes = pq.read_table(codes_path)
    pq.write_table(pa.concat_tables([codes, codes.slice(0, 1)]), codes_path)

    with pytest.raises(ValueError, match="not unique"):
        summarize(meds_dataset, output, SummaryConfig(min_subjects=2))
    assert output.read_bytes() == b"existing"
    assert not list(tmp_path.glob(".*.tmp.parquet"))
    assert not list(tmp_path.glob(".*.counts.parquet"))
    assert not list(tmp_path.glob(".*.work"))
