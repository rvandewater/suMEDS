from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sumeds import SummaryConfig, summarize


DEMO = Path(__file__).parents[1] / "MIMICIV_DEMO" / "MEDS_cohort"


@pytest.mark.skipif(not DEMO.exists(), reason="MIMIC-IV demo is not available")
def test_mimic_demo_summary(tmp_path: Path) -> None:
    output = summarize(
        DEMO, tmp_path / "mimic-summary.parquet", SummaryConfig(min_subjects=20)
    )
    result = pl.read_parquet(output)
    shards = [str(path) for path in (DEMO / "data").rglob("*.parquet")]
    event_count = pl.scan_parquet(shards).select(pl.len()).collect().item()

    assert {
        "code",
        "description",
        "parent_codes",
        "event_count",
        "subject_count",
        "is_masked",
    } <= set(result.columns)
    assert result["event_count"].sum() == event_count
    assert result.filter(pl.col("is_masked")).height == 1
    assert result.filter(pl.col("is_masked"))["description"][0] is None
    assert result.filter(~pl.col("is_masked"))["subject_count"].min() >= 20


@pytest.mark.skipif(not DEMO.exists(), reason="MIMIC-IV demo is not available")
def test_mimic_demo_split_columns(tmp_path: Path) -> None:
    output = summarize(
        DEMO,
        tmp_path / "mimic-wide.parquet",
        SummaryConfig(split_columns=True),
    )
    result = pl.read_parquet(output)
    event_columns = [
        "event_count_held_out",
        "event_count_train",
        "event_count_tuning",
    ]

    assert set(event_columns) <= set(result.columns)
    assert all(result[column].null_count() == 0 for column in event_columns)
    assert (
        result.select(pl.sum_horizontal(event_columns).sum()).item()
        == result["event_count"].sum()
    )
