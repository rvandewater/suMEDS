from __future__ import annotations

import json
from datetime import datetime
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


def _add_numeric_values(root: Path) -> None:
    # Only shard 0 gets the optional column; shard 1 must contribute nulls.
    path = root / "data" / "0.parquet"
    table = pq.read_table(path)
    values = pa.array([1.0, 5.0, 3.0, float("nan")], type=pa.float32())
    pq.write_table(table.append_column("numeric_value", values), path)


def test_numeric_stats_are_exact_and_privacy_masked(
    meds_dataset: Path, tmp_path: Path
) -> None:
    _add_numeric_values(meds_dataset)
    output = summarize(
        meds_dataset,
        tmp_path / "summary.parquet",
        SummaryConfig(min_subjects=2, numeric_stats=True),
    )
    rows = {row["code"]: row for row in pl.read_parquet(output).to_dicts()}

    a = rows["A"]
    assert a["numeric_count"] == 2
    assert (a["numeric_min"], a["numeric_max"]) == (1.0, 3.0)
    assert a["numeric_mean"] == a["numeric_median"] == 2.0
    assert a["numeric_std"] == pytest.approx(2**0.5)
    margin = 1.959964 * 2**0.5 / 2**0.5
    assert a["numeric_ci_lower"] == pytest.approx(2.0 - margin, abs=1e-5)
    assert a["numeric_ci_upper"] == pytest.approx(2.0 + margin, abs=1e-5)
    # B has one finite value (NaN is ignored), below min_subjects=2.
    assert rows["B"]["subject_count"] == 2
    assert rows["B"]["numeric_count"] is None
    assert rows["B"]["numeric_mean"] is None


def test_numeric_stats_selection_and_value_threshold(
    meds_dataset: Path, tmp_path: Path
) -> None:
    _add_numeric_values(meds_dataset)
    output = summarize(
        meds_dataset,
        tmp_path / "summary.parquet",
        SummaryConfig(min_subjects=3, numeric_stats=["avg", "min"]),
    )
    result = pl.read_parquet(output)

    assert [name for name in result.columns if name.startswith("numeric_")] == [
        "numeric_count",
        "numeric_min",
        "numeric_mean",
    ]
    # A is released (3 subjects) but only 2 subjects carry values.
    row = result.filter(pl.col("code") == "A").row(0, named=True)
    assert row["numeric_mean"] is None


def test_numeric_stats_without_numeric_column(
    meds_dataset: Path, tmp_path: Path
) -> None:
    output = summarize(
        meds_dataset,
        tmp_path / "summary.parquet",
        SummaryConfig(min_subjects=1, numeric_stats=True, per_split=True),
    )
    result = pl.read_parquet(output)
    assert result["numeric_mean"].null_count() == result.height


def test_rare_label_rejects_released_code_without_metadata(
    meds_dataset: Path, tmp_path: Path
) -> None:
    codes_path = meds_dataset / "metadata" / "codes.parquet"
    pq.write_table(pq.read_table(codes_path).slice(1), codes_path)

    with pytest.raises(ValueError, match="rare_code_label already exists"):
        summarize(
            meds_dataset,
            tmp_path / "summary.parquet",
            SummaryConfig(min_subjects=3, rare_code_label="A"),
        )


# Events with two declared modifiers. ``A`` has three modifier variants; the
# variants (A, main, g) and (C, z, null) have one subject each and are masked.
_MODIFIER_EVENTS = [
    (1, "A", "main", "mg"),
    (2, "A", "main", "mg"),
    (3, "A", "main", "g"),
    (1, "A", "alt", "mg"),
    (2, "A", "alt", "mg"),
    (1, "B", "x", None),
    (2, "B", "x", None),
    (2, "C", "z", None),
]
# Code metadata keyed by no, some, and all of the declared modifiers.
_MODIFIER_METADATA = {
    "none": [
        {"code": "A", "description": "A"},
        {"code": "B", "description": "B"},
        {"code": "C", "description": "C"},
    ],
    "subset": [
        {"code": "A", "modifier": "main", "description": "A main"},
        {"code": "A", "modifier": "alt", "description": "A alt"},
        {"code": "B", "modifier": "x", "description": "B x"},
        {"code": "C", "modifier": "z", "description": "C z"},
    ],
    "all": [
        {"code": "A", "modifier": "main", "unit": "mg", "description": "A main mg"},
        {"code": "A", "modifier": "main", "unit": "g", "description": "A main g"},
        {"code": "A", "modifier": "alt", "unit": "mg", "description": "A alt mg"},
        {"code": "B", "modifier": "x", "unit": None, "description": "B x"},
        {"code": "C", "modifier": "z", "unit": None, "description": "C z"},
    ],
}


def _write_modifier_dataset(
    root: Path, metadata: list[dict], modifiers: list[str]
) -> None:
    """Rewrite the fixture with ``_MODIFIER_EVENTS``, ``metadata`` and ``modifiers``."""

    event_schema = pa.schema(
        [
            pa.field("subject_id", pa.int64(), nullable=False),
            pa.field("time", pa.timestamp("us")),
            pa.field("code", pa.string(), nullable=False),
            pa.field("modifier", pa.string()),
            pa.field("unit", pa.string()),
        ]
    )
    for path in (root / "data").glob("*.parquet"):
        path.unlink()
    for index, rows in enumerate((_MODIFIER_EVENTS[:4], _MODIFIER_EVENTS[4:])):
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "subject_id": subject,
                        "time": datetime(2020, 1, 1),
                        "code": code,
                        "modifier": modifier,
                        "unit": unit,
                    }
                    for subject, code, modifier, unit in rows
                ],
                schema=event_schema,
            ),
            root / "data" / f"{index}.parquet",
        )
    names = list(metadata[0])
    code_schema = pa.schema(
        [
            pa.field(name, pa.string(), nullable=name != "code")
            for name in names
            if name != "description"
        ]
        + [
            pa.field("description", pa.string()),
            pa.field("parent_codes", pa.list_(pa.string())),
        ]
    )
    pq.write_table(
        pa.Table.from_pylist(
            [{**row, "parent_codes": [f"P/{row['code']}"]} for row in metadata],
            schema=code_schema,
        ),
        root / "metadata" / "codes.parquet",
    )
    dataset = json.loads((root / "metadata" / "dataset.json").read_text())
    dataset["code_modifier_columns"] = modifiers
    (root / "metadata" / "dataset.json").write_text(json.dumps(dataset))


@pytest.mark.parametrize(
    ("keys", "descriptions"),
    [
        # One metadata row applies to every modifier variant of its code.
        ("none", ("A", "A", "B")),
        # Only the modifier the metadata has distinguishes metadata rows.
        ("subset", ("A alt", "A main", "B x")),
        # Keyed by all modifiers, as before: each variant has its own row.
        ("all", ("A alt mg", "A main mg", "B x")),
    ],
)
def test_code_metadata_keyed_by_any_declared_modifiers(
    meds_dataset: Path, tmp_path: Path, keys: str, descriptions: tuple[str, ...]
) -> None:
    _write_modifier_dataset(
        meds_dataset, _MODIFIER_METADATA[keys], ["modifier", "unit"]
    )

    output = summarize(
        meds_dataset, tmp_path / "summary.parquet", SummaryConfig(min_subjects=2)
    )
    result = pl.read_parquet(output)

    # The output schema does not depend on the metadata keys.
    assert result.columns == [
        "code",
        "modifier",
        "unit",
        "description",
        "parent_codes",
        "event_count",
        "subject_count",
        "is_masked",
    ]
    assert result.schema["modifier"] == result.schema["unit"] == pl.String
    # Modifier values come from the counts, which keep every modifier: a released
    # variant is never merged with another, nor with a masked one.
    assert result.select(
        "code", "modifier", "unit", "event_count", "subject_count", "is_masked"
    ).rows() == [
        ("A", "alt", "mg", 2, 2, False),
        ("A", "main", "mg", 2, 2, False),
        ("B", "x", None, 2, 2, False),
        # (A, main, g) and (C, z, null): subjects {3} and {2}.
        ("__RARE__", None, None, 2, 2, True),
    ]
    assert tuple(result["description"].head(3)) == descriptions
    assert result["parent_codes"].to_list() == [["P/A"], ["P/A"], ["P/B"], None]

    csv = pl.read_csv(
        summarize(meds_dataset, tmp_path / "summary.csv", SummaryConfig(min_subjects=2))
    )
    assert csv.columns == result.columns
    assert csv["unit"].to_list() == ["mg", "mg", None, None]


@pytest.mark.parametrize(
    ("keys", "message"),
    [("none", r"\['code'\]"), ("subset", r"\['code', 'modifier'\]")],
)
def test_code_metadata_keys_must_be_unique(
    meds_dataset: Path, tmp_path: Path, keys: str, message: str
) -> None:
    metadata = _MODIFIER_METADATA[keys]
    _write_modifier_dataset(
        meds_dataset,
        [*metadata, {**metadata[0], "description": "Again"}],
        ["modifier", "unit"],
    )

    with pytest.raises(ValueError, match=f"keys are not unique: {message}"):
        summarize(
            meds_dataset, tmp_path / "summary.parquet", SummaryConfig(min_subjects=2)
        )


def test_undeclared_metadata_modifier_is_an_ordinary_column(
    meds_dataset: Path, tmp_path: Path
) -> None:
    # ``unit`` is in the events and the metadata, but not declared.
    metadata = [{**row, "unit": "label"} for row in _MODIFIER_METADATA["subset"]]
    _write_modifier_dataset(meds_dataset, metadata, ["modifier"])

    output = summarize(
        meds_dataset, tmp_path / "summary.parquet", SummaryConfig(min_subjects=2)
    )
    result = pl.read_parquet(output)

    assert result.columns[:4] == ["code", "modifier", "unit", "description"]
    # Counted by code and modifier only; C (one subject) is dropped.
    assert result.select("code", "modifier", "unit", "subject_count").rows() == [
        ("A", "alt", "label", 2),
        ("A", "main", "label", 3),
        ("B", "x", "label", 2),
    ]
