from __future__ import annotations

import json
import shutil
from pathlib import Path

import polars as pl
import pytest

from sumeds import SummaryConfig, summarize


DEMO = Path(__file__).parent / "resources" / "MIMICIV_DEMO" / "MEDS_cohort"


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


@pytest.mark.skipif(not DEMO.exists(), reason="MIMIC-IV demo is not available")
def test_mimic_demo_declared_unit_modifier(tmp_path: Path) -> None:
    # MEDS-Extract 0.8.1 declares MIMIC-IV's ``unit`` modifier, but writes
    # codes.parquet keyed by code alone.
    root = tmp_path / "MEDS_cohort"
    shutil.copytree(DEMO, root)
    dataset_json = root / "metadata" / "dataset.json"
    dataset = json.loads(dataset_json.read_text())
    dataset_json.write_text(json.dumps({**dataset, "code_modifier_columns": ["unit"]}))

    output = summarize(
        root, tmp_path / "mimic-units.parquet", SummaryConfig(min_subjects=20)
    )
    result = pl.read_parquet(output)
    shards = [str(path) for path in (root / "data").rglob("*.parquet")]
    event_count = pl.scan_parquet(shards).select(pl.len()).collect().item()

    assert result.columns[:3] == ["code", "unit", "description"]
    assert result["event_count"].sum() == event_count
    released = result.filter(~pl.col("is_masked"))
    assert released.select("code", "unit").is_duplicated().sum() == 0
    # Some codes are released with more than one unit, with the same metadata.
    variants = released.group_by("code").agg(
        pl.len(), pl.col("description").n_unique().alias("descriptions")
    )
    assert variants.filter(pl.col("len") > 1).height > 0
    assert variants["descriptions"].max() == 1
    assert released["subject_count"].min() >= 20


@pytest.mark.skipif(not DEMO.exists(), reason="MIMIC-IV demo is not available")
def test_mimic_demo_unit_metadata_with_null_data_units(tmp_path: Path) -> None:
    # MIMIC-IV ETLs now write one String ``unit`` per code into codes.parquet,
    # while only some events fill the declared ``unit`` modifier
    # (rvandewater/alignEHR#47). A code's row still describes its null-unit events.
    root = tmp_path / "MEDS_cohort"
    shutil.copytree(DEMO, root)
    dataset_json = root / "metadata" / "dataset.json"
    dataset = json.loads(dataset_json.read_text())
    dataset_json.write_text(json.dumps({**dataset, "code_modifier_columns": ["unit"]}))
    shards = [str(path) for path in (root / "data").rglob("*.parquet")]
    units = (
        pl.scan_parquet(shards)
        .drop_nulls("unit")
        .group_by("code")
        .agg(pl.col("unit").mode().sort().first())
    )
    codes_path = root / "metadata" / "codes.parquet"
    codes = pl.read_parquet(codes_path).lazy().join(units, on="code", how="left")
    codes.collect().write_parquet(codes_path)

    output = summarize(
        root, tmp_path / "mimic-units.parquet", SummaryConfig(min_subjects=20)
    )
    released = pl.read_parquet(output).filter(~pl.col("is_masked"))
    described = codes.filter(pl.col("description").is_not_null()).select("code")
    rows = released.join(described.collect(), on="code", how="semi")

    # Rows whose unit differs from the code's row, null ones among them.
    assert rows.filter(pl.col("unit").is_null()).height > 0
    assert rows["description"].null_count() == 0
