from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def meds_dataset(tmp_path: Path) -> Path:
    root = tmp_path / "meds"
    (root / "data").mkdir(parents=True)
    (root / "metadata").mkdir()

    rows = [
        (1, "A", "main"),
        (1, "B", "x"),
        (2, "A", "main"),
        (2, "B", "x"),
        (3, "A", "main"),
        (2, "C", "z"),
        (3, "C", "z"),
    ]
    event_schema = pa.schema(
        [
            pa.field("subject_id", pa.int64(), nullable=False),
            pa.field("time", pa.timestamp("us")),
            pa.field("code", pa.string(), nullable=False),
            pa.field("modifier", pa.string()),
        ]
    )
    for index, shard_rows in enumerate((rows[:4], rows[4:])):
        table = pa.Table.from_pylist(
            [
                {
                    "subject_id": subject,
                    "time": datetime(2020, 1, 1),
                    "code": code,
                    "modifier": modifier,
                }
                for subject, code, modifier in shard_rows
            ],
            schema=event_schema,
        )
        pq.write_table(table, root / "data" / f"{index}.parquet")

    code_schema = pa.schema(
        [
            pa.field("code", pa.string(), nullable=False),
            pa.field("modifier", pa.string()),
            pa.field("description", pa.string()),
            pa.field("parent_codes", pa.list_(pa.string())),
        ]
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "code": "A",
                    "modifier": "main",
                    "description": "Common code",
                    "parent_codes": ["ROOT"],
                },
                {
                    "code": "B",
                    "modifier": "x",
                    "description": "Rare B",
                    "parent_codes": ["ROOT"],
                },
                {
                    "code": "C",
                    "modifier": "z",
                    "description": "Rare C",
                    "parent_codes": ["ROOT"],
                },
            ],
            schema=code_schema,
        ),
        root / "metadata" / "codes.parquet",
    )
    split_schema = pa.schema(
        [
            pa.field("subject_id", pa.int64(), nullable=False),
            pa.field("split", pa.string(), nullable=False),
        ]
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"subject_id": 1, "split": "train"},
                {"subject_id": 2, "split": "train"},
                {"subject_id": 3, "split": "held_out"},
            ],
            schema=split_schema,
        ),
        root / "metadata" / "subject_splits.parquet",
    )
    (root / "metadata" / "dataset.json").write_text(
        json.dumps(
            {
                "dataset_name": "synthetic",
                "meds_version": "0.4.1",
                "code_modifier_columns": ["modifier"],
            }
        )
    )
    return root
