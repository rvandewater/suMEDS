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
    concepts = [
        (10, "Root", "Visit", "SNOMED", "ROOT", "S", None),
        (20, "Branch", "Visit", "SNOMED", "BRANCH", "S", None),
        (100, "Visit parent", "Visit", "SNOMED", "PARENT", "S", None),
        (200, "Versioned diagnosis", "Condition", "ICD10CM", "A01", "S", None),
        (300, "Urine pH", "Measurement", "LOINC", "5803-2", "S", None),
        (400, "Sibling", "Visit", "SNOMED", "SIB", "S", None),
        (500, "Child one", "Visit", "SNOMED", "CHILD1", "S", None),
        (600, "Child two", "Visit", "SNOMED", "CHILD2", "S", None),
        (700, "Child three", "Visit", "SNOMED", "CHILD3", "S", None),
        (800, "Child four", "Visit", "SNOMED", "CHILD4", "S", None),
        (900, "Invalid relative", "Visit", "SNOMED", "INVALID-RELATIVE", "S", "D"),
        (999, "Invalid concept", "Condition", "TEST", "BAD", None, "D"),
        (1100, "Cyclic relative", "Visit", "SNOMED", "CYCLE", "S", None),
        (
            5083,
            'Telehealth Provided Other than in Patient’s "Home',
            "Visit",
            "CMS Place of Service",
            "02",
            None,
            None,
        ),
        (
            9000,
            "Duplicate valid concept",
            "Visit",
            "CMS Place of Service",
            "02",
            None,
            None,
        ),
    ]
    pl.DataFrame(
        concepts,
        schema=[
            "concept_id",
            "concept_name",
            "domain_id",
            "vocabulary_id",
            "concept_code",
            "standard_concept",
            "invalid_reason",
        ],
        orient="row",
    ).write_csv(directory / "CONCEPT.csv", separator="\t", quote_style="never")
    hierarchy = [
        (10, 10, 0),
        (10, 20, 1),
        (10, 100, 2),
        (10, 5083, 3),
        (10, 200, 3),
        (10, 400, 3),
        (20, 100, 1),
        (20, 5083, 2),
        (20, 200, 2),
        (20, 400, 2),
        (100, 5083, 1),
        (100, 5083, 1),
        (100, 200, 1),
        (100, 400, 1),
        (100, 900, 1),
        (5083, 500, 1),
        (5083, 600, 2),
        (5083, 700, 3),
        (5083, 800, 4),
        (5083, 900, 1),
        (5083, 1100, 1),
        (5083, 12346, 1),
        (5083, 5083, 1),
        (500, 600, 1),
        (500, 700, 2),
        (500, 800, 3),
        (600, 700, 1),
        (600, 800, 2),
        (700, 800, 1),
        (900, 5083, 1),
        (1100, 5083, 1),
        (12345, 5083, 1),
    ]
    pl.DataFrame(
        hierarchy,
        schema=[
            "ancestor_concept_id",
            "descendant_concept_id",
            "min_levels_of_separation",
        ],
        orient="row",
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
    assert cms["parent_codes"] == [
        "SNOMED//BRANCH",
        "SNOMED//PARENT",
        "SNOMED//ROOT",
    ]
    versioned = rows["ICD10CM//2024//A01"]
    assert versioned["concept_id"] == 200
    assert versioned["concept_code"] == "A01"
    assert versioned["description"] == "Keep this"
    assert versioned["parent_codes"] == [
        "EXISTING",
        "SNOMED//BRANCH",
        "SNOMED//PARENT",
        "SNOMED//ROOT",
    ]
    assert rows["TEST//BAD//end"]["concept_id"] is None
    assert rows["malformed"]["concept_id"] is None
    lab = rows["LAB//51491//units"]
    assert lab["concept_id"] == 300
    assert lab["description"] == "Existing lab name"
    assert lab["parent_codes"] is None
    assert rows["malformed"]["parent_codes"] is None

    retained = enrich_metadata(
        frame,
        EnrichmentConfig(csv_dir=athena_csv, exclude_self_parent_code=False),
    ).collect()
    assert retained.filter(pl.col("code") == "LAB//51491//units").to_dicts()[0][
        "parent_codes"
    ] == ["LOINC//5803-2"]


def test_unmatched_relationship_arrays_pass_through(athena_csv: Path) -> None:
    frame = pl.DataFrame(
        {
            "code": ["not-in-athena"],
            "parent_codes": [[]],
            "child_codes": [[None]],
            "sibling_codes": [["KEEP"]],
        },
        schema_overrides={
            "parent_codes": pl.List(pl.String),
            "child_codes": pl.List(pl.String),
        },
    ).lazy()
    row = (
        enrich_metadata(
            frame,
            EnrichmentConfig(
                csv_dir=athena_csv,
                child_codes=True,
                sibling_codes=True,
            ),
        )
        .collect()
        .row(0, named=True)
    )

    assert row["parent_codes"] == []
    assert row["child_codes"] == [None]
    assert row["sibling_codes"] == ["KEEP"]


def test_csv_hierarchy_options_merge_depth_and_validate_codes(
    athena_csv: Path,
) -> None:
    frame = pl.DataFrame(
        {
            "code": ["CMS Place of Service//02//end"],
            "parent_codes": [["KEEP-PARENT", "SNOMED//ROOT"]],
            "child_codes": [["KEEP-CHILD", "SNOMED//CHILD1"]],
            "sibling_codes": [["KEEP-SIBLING"]],
        }
    ).lazy()
    row = (
        enrich_metadata(
            frame,
            EnrichmentConfig(
                csv_dir=athena_csv,
                child_codes=True,
                sibling_codes=True,
            ),
        )
        .collect()
        .row(0, named=True)
    )

    assert row["parent_codes"] == [
        "KEEP-PARENT",
        "SNOMED//ROOT",
        "SNOMED//BRANCH",
        "SNOMED//PARENT",
    ]
    assert row["child_codes"] == [
        "KEEP-CHILD",
        "SNOMED//CHILD1",
        "SNOMED//CHILD2",
        "SNOMED//CHILD3",
    ]
    assert row["sibling_codes"] == [
        "KEEP-SIBLING",
        "ICD10CM//A01",
        "SNOMED//BRANCH",
        "SNOMED//PARENT",
        "SNOMED//SIB",
    ]
    added = row["parent_codes"] + row["child_codes"] + row["sibling_codes"]
    assert not any(
        value in "|".join(added)
        for value in ("INVALID-RELATIVE", "CYCLE", "12345", "12346", "CHILD4")
    )


def test_csv_parent_toggle_and_custom_child_depth(athena_csv: Path) -> None:
    row = (
        enrich_metadata(
            pl.DataFrame(
                {
                    "code": ["CMS Place of Service//02//end"],
                    "parent_codes": [["KEEP"]],
                }
            ).lazy(),
            EnrichmentConfig(
                csv_dir=athena_csv,
                parent_codes=False,
                child_codes=True,
                child_depth=1,
            ),
        )
        .collect()
        .row(0, named=True)
    )

    assert row["parent_codes"] == ["KEEP"]
    assert row["child_codes"] == ["SNOMED//CHILD1"]
    assert "sibling_codes" not in row


def test_csv_root_has_no_parent(athena_csv: Path) -> None:
    row = (
        enrich_metadata(
            pl.DataFrame({"code": ["SNOMED//ROOT"]}).lazy(),
            EnrichmentConfig(csv_dir=athena_csv),
        )
        .collect()
        .row(0, named=True)
    )

    assert row["concept_id"] == 10
    assert row["parent_codes"] is None


def test_postgres_uses_batched_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(command: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
        assert command[:2] == ["psql", "postgresql://example/omop"]
        assert "COPY requested FROM STDIN" in kwargs["input"]
        assert "CMS Place of Service,02" in kwargs["input"]
        assert "ca.min_levels_of_separation > 0" in kwargs["input"]
        assert "NOT EXISTS" in kwargs["input"]
        assert "related.invalid_reason IS NULL" in kwargs["input"]
        assert "h.min_levels_of_separation <= 2" in kwargs["input"]
        assert "child_h.min_levels_of_separation = 1" in kwargs["input"]
        assert "'sibling'::text" in kwargs["input"]
        stdout = (
            "_target_code,_rank,description,vocabulary_id,concept_id,concept_code,"
            "domain_id,standard_concept,_relationship,_related_vocabulary_id,"
            "_related_concept_code\n"
            "CMS Place of Service//02//end,0,Telehealth,CMS Place of Service,5083,02,"
            "Visit,,parent,SNOMED,PARENT\n"
            "CMS Place of Service//02//end,0,Telehealth,CMS Place of Service,5083,02,"
            "Visit,,parent,SNOMED,PARENT\n"
            "CMS Place of Service//02//end,0,Telehealth,CMS Place of Service,5083,02,"
            "Visit,,child,SNOMED,CHILD1\n"
            "CMS Place of Service//02//end,0,Telehealth,CMS Place of Service,5083,02,"
            "Visit,,sibling,SNOMED,SIB\n"
        )
        return subprocess.CompletedProcess(command, 0, stdout, "")

    monkeypatch.setattr("sumeds.enrich.subprocess.run", fake_run)
    result = enrich_metadata(
        pl.DataFrame({"code": ["CMS Place of Service//02//end"]}).lazy(),
        EnrichmentConfig(
            postgres="postgresql://example/omop",
            child_codes=True,
            sibling_codes=True,
            child_depth=2,
        ),
    ).collect()

    assert result["concept_id"].to_list() == [5083]
    assert result["concept_code"].to_list() == ["02"]
    assert result["parent_codes"].to_list() == [["SNOMED//PARENT"]]
    assert result["child_codes"].to_list() == [["SNOMED//CHILD1"]]
    assert result["sibling_codes"].to_list() == [["SNOMED//SIB"]]


def test_postgres_parent_toggle_preserves_unmatched_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(command: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
        assert "'parent'::text" not in kwargs["input"]
        assert "'child'::text" not in kwargs["input"]
        assert "'sibling'::text" not in kwargs["input"]
        stdout = (
            "_target_code,_rank,description,vocabulary_id,concept_id,concept_code,"
            "domain_id,standard_concept,_relationship,_related_vocabulary_id,"
            "_related_concept_code\n"
            "SNOMED//ROOT,0,Root,SNOMED,10,ROOT,Visit,S,,,\n"
        )
        return subprocess.CompletedProcess(command, 0, stdout, "")

    monkeypatch.setattr("sumeds.enrich.subprocess.run", fake_run)
    rows = (
        enrich_metadata(
            pl.DataFrame(
                {
                    "code": ["SNOMED//ROOT", "not-in-athena"],
                    "parent_codes": [["KEEP"], []],
                }
            ).lazy(),
            EnrichmentConfig(
                postgres="postgresql://example/omop",
                parent_codes=False,
            ),
        )
        .collect()
        .to_dicts()
    )

    assert rows[0]["concept_id"] == 10
    assert rows[0]["parent_codes"] == ["KEEP"]
    assert rows[1]["concept_id"] is None
    assert rows[1]["parent_codes"] == []


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
    assert cms["parent_codes"] == "SNOMED//BRANCH|SNOMED//PARENT|SNOMED//ROOT"
    lab = csv_rows["LAB//51491//units"]
    assert lab["concept_id"] == 300
    assert lab["parent_codes"] is None

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
    assert json_row["parent_codes"] == [
        "SNOMED//BRANCH",
        "SNOMED//PARENT",
        "SNOMED//ROOT",
    ]


def test_standalone_cli(athena_csv: Path, tmp_path: Path, capsys) -> None:
    input_path = tmp_path / "codes.parquet"
    output_path = tmp_path / "enriched.parquet"
    pl.DataFrame({"code": ["CMS Place of Service//02//end"]}).write_parquet(input_path)

    assert (
        enrich_main(
            [
                str(input_path),
                "-o",
                str(output_path),
                "--athena-csv",
                str(athena_csv),
                "--no-parent-codes",
                "--child-codes",
                "--sibling-codes",
                "--child-depth",
                "1",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert f"Wrote {output_path}" in captured.out
    assert "Enrichment report" in captured.out
    assert "Athena matches added:      1 rows, 1 unique codes" in captured.out
    assert "Descriptions" in captured.out
    assert "Parent-code lists" in captured.out
    assert "4/4" in captured.err
    result = pl.read_parquet(output_path)
    assert result["concept_id"].to_list() == [5083]
    assert "parent_codes" not in result
    assert result["child_codes"].to_list() == [["SNOMED//CHILD1"]]
    assert result["sibling_codes"].to_list() == [
        [
            "ICD10CM//A01",
            "SNOMED//BRANCH",
            "SNOMED//PARENT",
            "SNOMED//SIB",
        ]
    ]


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
    path.write_text(
        "enrichment:\n"
        "  csv_dir: athena\n"
        "  parent_codes: false\n"
        "  child_codes: true\n"
        "  sibling_codes: true\n"
        "  exclude_self_parent_code: false\n"
        "  child_depth: 2\n"
    )
    assert SummaryConfig.from_yaml(path).enrichment == EnrichmentConfig(
        csv_dir=tmp_path / "athena",
        parent_codes=False,
        child_codes=True,
        sibling_codes=True,
        exclude_self_parent_code=False,
        child_depth=2,
    )

    path.write_text("enrichment:\n  csv_dir: athena\n  postgres: dbname=omop\n")
    with pytest.raises(ValueError, match="exactly one"):
        SummaryConfig.from_yaml(path)
    path.write_text("enrichment:\n  csv_directory: athena\n")
    with pytest.raises(ValueError, match="Unknown configuration"):
        SummaryConfig.from_yaml(path)
    path.write_text("enrichment:\n  csv_dir: athena\n  child_depth: 0\n")
    with pytest.raises(ValueError, match="child_depth"):
        SummaryConfig.from_yaml(path)
    with pytest.raises(ValueError, match="parent_codes"):
        EnrichmentConfig(csv_dir=tmp_path, parent_codes="yes")  # type: ignore[arg-type]
