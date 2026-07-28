from __future__ import annotations

import subprocess
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sumeds import (
    EnrichmentConfig,
    SummaryConfig,
    enrich_file,
    enrich_metadata,
    summarize,
)
from sumeds.cli import enrich_main


@pytest.fixture
def athena_csv(tmp_path: Path) -> Path:
    directory = tmp_path / "athena"
    directory.mkdir()
    pl.DataFrame(
        {
            "concept_id": [5083, 9000, 100, 200, 300, 999],
            "concept_name": [
                'Telehealth Provided Other than in Patient’s "Home',
                "Duplicate valid concept",
                "Visit parent",
                "Versioned diagnosis",
                "Urine pH",
                "Invalid concept",
            ],
            "domain_id": [
                "Visit",
                "Visit",
                "Visit",
                "Condition",
                "Measurement",
                "Condition",
            ],
            "vocabulary_id": [
                "CMS Place of Service",
                "CMS Place of Service",
                "SNOMED",
                "ICD10CM",
                "LOINC",
                "TEST",
            ],
            "concept_code": ["02", "02", "PARENT", "A01", "5803-2", "BAD"],
            "standard_concept": [None, None, "S", "S", "S", None],
            "invalid_reason": [None, None, None, None, None, "D"],
        }
    ).write_csv(directory / "CONCEPT.csv", separator="\t", quote_style="never")
    pl.DataFrame(
        {
            "ancestor_concept_id": [100, 100],
            "descendant_concept_id": [5083, 200],
            "min_levels_of_separation": [1, 2],
        }
    ).write_csv(directory / "CONCEPT_ANCESTOR.csv", separator="\t")
    return directory


def test_csv_enrichment_resolves_common_and_versioned_codes(
    athena_csv: Path,
) -> None:
    frame = pl.DataFrame(
        {
            "code": [
                "CMS Place of Service//02//end",
                "ICD10CM//2024//A01",
                "TEST//BAD//end",
                "malformed",
                "LAB//51491//units",
            ],
            "description": [None, "Keep this", None, None, "Existing lab name"],
            "parent_codes": [None, ["EXISTING"], None, None, ["LOINC/5803-2"]],
        },
        schema_overrides={"parent_codes": pl.List(pl.String)},
    ).lazy()

    rows = {
        row["code"]: row
        for row in enrich_metadata(frame, EnrichmentConfig(csv_dir=athena_csv))
        .collect()
        .to_dicts()
    }

    cms = rows["CMS Place of Service//02//end"]
    assert cms["concept_id"] == 5083
    assert cms["concept_code"] == "02"
    assert cms["description"].startswith("Telehealth")
    assert cms["parent_codes"] == ["SNOMED//PARENT"]
    versioned = rows["ICD10CM//2024//A01"]
    assert versioned["concept_id"] == 200
    assert versioned["concept_code"] == "A01"
    assert versioned["description"] == "Keep this"
    assert versioned["parent_codes"] == ["EXISTING"]
    assert rows["TEST//BAD//end"]["concept_id"] is None
    assert rows["malformed"]["concept_id"] is None
    lab = rows["LAB//51491//units"]
    assert lab["concept_id"] == 300
    assert lab["description"] == "Existing lab name"
    assert lab["parent_codes"] == ["LOINC/5803-2"]


def test_postgres_uses_batched_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(command: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
        assert command[:2] == ["psql", "postgresql://example/omop"]
        assert "COPY requested FROM STDIN" in kwargs["input"]
        assert "CMS Place of Service,02" in kwargs["input"]
        stdout = (
            "_target_code,_rank,description,vocabulary_id,concept_id,concept_code,"
            "domain_id,standard_concept,_parent_vocabulary_id,_parent_concept_code\n"
            "CMS Place of Service//02//end,0,Telehealth,CMS Place of Service,5083,02,"
            "Visit,,,\n"
        )
        return subprocess.CompletedProcess(command, 0, stdout, "")

    monkeypatch.setattr("sumeds.enrich.subprocess.run", fake_run)
    result = enrich_metadata(
        pl.DataFrame({"code": ["CMS Place of Service//02//end"]}).lazy(),
        EnrichmentConfig(postgres="postgresql://example/omop"),
    ).collect()

    assert result["concept_id"].to_list() == [5083]
    assert result["concept_code"].to_list() == ["02"]


def test_postgres_rejects_copy_terminators(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected_run(*args, **kwargs):
        pytest.fail("psql must not run for newline-containing candidates")

    monkeypatch.setattr("sumeds.enrich.subprocess.run", unexpected_run)
    with pytest.raises(ValueError, match="newlines"):
        enrich_metadata(
            pl.DataFrame({"code": ["VOCAB//code\n\\.\nSELECT 1;"]}).lazy(),
            EnrichmentConfig(postgres="postgresql://example/omop"),
        )


def test_standalone_handles_csv_and_null_columns(
    athena_csv: Path, tmp_path: Path
) -> None:
    csv_input = tmp_path / "codes.csv"
    csv_output = tmp_path / "enriched.csv"
    parquet_output = tmp_path / "enriched.parquet"
    csv_input.write_text(
        "code,description,parent_codes,concept_id\n"
        "CMS Place of Service//02//end,,,\n"
        "LAB//51491//units,Existing lab name,LOINC/5803-2,\n"
    )
    enrich_file(csv_input, csv_output, EnrichmentConfig(csv_dir=athena_csv))
    csv_rows = {row["code"]: row for row in pl.read_csv(csv_output).to_dicts()}
    cms = csv_rows["CMS Place of Service//02//end"]
    assert cms["concept_id"] == 5083
    assert cms["parent_codes"] == "SNOMED//PARENT"
    lab = csv_rows["LAB//51491//units"]
    assert lab["concept_id"] == 300
    assert lab["parent_codes"] == "LOINC/5803-2"

    enrich_file(csv_input, parquet_output, EnrichmentConfig(csv_dir=athena_csv))
    assert pl.read_parquet(parquet_output).schema["concept_id"] == pl.Int64

    json_input = tmp_path / "codes.json"
    json_output = tmp_path / "enriched.json"
    json_input.write_text(
        '[{"code":"CMS Place of Service//02//end",'
        '"description":null,"parent_codes":null}]'
    )
    enrich_file(json_input, json_output, EnrichmentConfig(csv_dir=athena_csv))
    json_row = pl.read_json(json_output).row(0, named=True)
    assert json_row["concept_id"] == 5083
    assert json_row["parent_codes"] == ["SNOMED//PARENT"]


def test_standalone_cli(athena_csv: Path, tmp_path: Path, capsys) -> None:
    input_path = tmp_path / "codes.parquet"
    output_path = tmp_path / "enriched.parquet"
    pl.DataFrame({"code": ["ICD10CM//2024//A01"]}).write_parquet(input_path)

    assert (
        enrich_main(
            [
                str(input_path),
                "-o",
                str(output_path),
                "--athena-csv",
                str(athena_csv),
            ]
        )
        == 0
    )
    assert f"Wrote {output_path}" in capsys.readouterr().out
    assert pl.read_parquet(output_path)["concept_id"].to_list() == [200]


def test_summary_enriches_only_released_rows(
    meds_dataset: Path, athena_csv: Path, tmp_path: Path
) -> None:
    replacement = "CMS Place of Service//02//end"
    paths = [
        *sorted((meds_dataset / "data").glob("*.parquet")),
        meds_dataset / "metadata" / "codes.parquet",
    ]
    for path in paths:
        table = pq.read_table(path)
        values = [
            replacement if value.as_py() == "A" else value.as_py()
            for value in table["code"]
        ]
        index = table.schema.get_field_index("code")
        pq.write_table(
            table.set_column(index, table.schema.field(index), pa.array(values)), path
        )

    output = summarize(
        meds_dataset,
        tmp_path / "summary.parquet",
        SummaryConfig(
            min_subjects=3,
            rare_code_label="ICD10CM//2024//A01",
            enrichment=EnrichmentConfig(csv_dir=athena_csv),
        ),
    )
    rows = {row["code"]: row for row in pl.read_parquet(output).to_dicts()}

    assert rows[replacement]["concept_id"] == 5083
    assert rows[replacement]["description"] == "Common code"
    masked = rows["ICD10CM//2024//A01"]
    assert masked["is_masked"] is True
    assert masked["concept_id"] is None


def test_enrichment_config_is_strict(tmp_path: Path) -> None:
    path = tmp_path / "summary.yaml"
    path.write_text("enrichment:\n  csv_dir: athena\n")
    assert SummaryConfig.from_yaml(path).enrichment == EnrichmentConfig(
        csv_dir=tmp_path / "athena"
    )

    path.write_text("enrichment:\n  csv_dir: athena\n  postgres: dbname=omop\n")
    with pytest.raises(ValueError, match="exactly one"):
        SummaryConfig.from_yaml(path)
    path.write_text("enrichment:\n  csv_directory: athena\n")
    with pytest.raises(ValueError, match="Unknown configuration"):
        SummaryConfig.from_yaml(path)
