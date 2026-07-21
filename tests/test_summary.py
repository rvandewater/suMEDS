from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sumeds import SummaryConfig, summarize


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
