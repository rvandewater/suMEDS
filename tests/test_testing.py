"""``sumeds.testing``: the local ontology source, the helpers, and the ETL contract tests.

The contracts run against ``contract_etl``, a throwaway ETL package written into a site directory
that is put on ``sys.path`` (and ``PYTHONPATH``, for the ``meds-extract-run`` it spawns). It
registers its spec and its rules under ``CONTRACT``, as an ETL package does. The rules contract
needs only suMEDS; the enrichment contract runs ``meds-extract-run`` and is skipped without
MEDS-Extract (>= 0.9), like the end-to-end tests of ``test_stage.py``.
"""

from __future__ import annotations

import gzip
import os
import sys
from collections.abc import Iterator
from importlib.metadata import entry_points
from pathlib import Path

import polars as pl
import pytest

from sumeds import EnrichmentConfig, OntologyPacks, enrich_metadata, load_rules
from sumeds.testing import (
    Pack,
    load_spec,
    names_map,
    registered_resource,
    stage_name,
    stage_overrides,
    synthetic_title,
    write_ontology_source,
)
from sumeds.testing.contracts import EnrichmentContract, RulesContract

# ------------------------------------------------------------------ the local ontology source


def test_packs_are_read_by_suMEDS(tmp_path: Path) -> None:
    source = write_ontology_source(
        tmp_path / "ontologies",
        {
            "ICD10CM": (
                "fy2016-fy2027",
                {"I50": None, "I50.2": "I50", "I50.23": "I50.2"},
            ),
            "ICD9CM": {"401": None, "401.9": "401"},
            "ICD10PCS": ["5A1955Z"],
            "LOINC": Pack(
                ["2160-0", "8867-4"],
                release="2.83",
                parents=False,
                fields={"code": "LOINC_NUM", "description": "LONG_COMMON_NAME"},
                shard_key_length=2,
                compress=True,
                titles={"8867-4": "Heart rate"},
            ),
        },
    )
    client = OntologyPacks(str(source))
    assert sorted(client.vocabularies()) == ["ICD10CM", "ICD10PCS", "ICD9CM", "LOINC"]
    assert client.manifest("ICD10CM")["release"] == "fy2016-fy2027"
    assert client.manifest("ICD9CM")["release"] == "test"
    entry = client.lookup("ICD10CM", {"I50.23"})["I50.23"]
    assert entry.description == synthetic_title("ICD10CM", "I50.23")
    assert entry.ancestors == ("I50.2", "I50")
    assert entry.ancestor_names == (
        synthetic_title("ICD10CM", "I50.2"),
        synthetic_title("ICD10CM", "I50"),
    )
    entry = client.lookup("ICD10PCS", {"5A1955Z"})["5A1955Z"]
    assert (entry.description, entry.ancestors) == (
        "Synthetic ICD10PCS title 5A1955Z",
        (),
    )
    loinc = client.manifest("LOINC")
    assert loinc["fields"] == {
        "code": "LOINC_NUM",
        "description": "LONG_COMMON_NAME",
        "parent": None,
    }
    assert sorted(loinc["shards"]) == ["21", "88"]
    shard = source / "LOINC" / "2.83" / loinc["shards"]["88"]["file"]
    assert (
        gzip.decompress(shard.read_bytes())
        .decode()
        .startswith("LOINC_NUM,LONG_COMMON_NAME\n")
    )
    found = client.lookup("LOINC", {"8867-4", "2160-0"})
    assert {code: (e.description, e.ancestors) for code, e in found.items()} == {
        "8867-4": ("Heart rate", ()),
        "2160-0": ("Synthetic LOINC title 2160-0", ()),
    }


def test_crosswalks_take_row_mappings_or_a_header_row(tmp_path: Path) -> None:
    source = write_ontology_source(
        tmp_path,
        crosswalks={
            "demo": {
                "lab_loinc": [
                    {
                        "itemid": "1",
                        "source_table": "labevents",
                        "loinc_code": "2160-0",
                    },
                    {"itemid": "2", "loinc_code": "8867-4"},
                ],
                "vitals": [["code", "loinc_code"], ["NURSING//HR", "8867-4"]],
            }
        },
    )
    client = OntologyPacks(str(source))
    assert client.vocabularies() == []
    assert client.crosswalk("demo/lab_loinc") == [
        {"itemid": "1", "source_table": "labevents", "loinc_code": "2160-0"},
        {"itemid": "2", "source_table": "", "loinc_code": "8867-4"},
    ]
    assert client.crosswalk("demo/vitals") == [
        {"code": "NURSING//HR", "loinc_code": "8867-4"}
    ]


def test_an_empty_source_runs_string_hierarchy_rules_offline(tmp_path: Path) -> None:
    source = str(write_ontology_source(tmp_path))
    rules = {
        "schema": "sumeds-rules/0.1",
        "hierarchy": [
            {
                "id": "p",
                "pattern": "^DX//(?P<c>.+)$",
                "group": "c",
                "mode": "prefix",
                "ignore": ".",
            }
        ],
    }
    enriched = enrich_metadata(
        pl.LazyFrame({"code": ["DX//I50.23", "DX//I50"]}),
        EnrichmentConfig(rules=rules, ontology_source=source, offline=True),
    ).collect()
    parents = dict(
        zip(enriched["code"], enriched["parent_codes"].to_list(), strict=True)
    )
    assert parents["DX//I50.23"] == ["DX//I50"]


def test_a_pack_without_a_parent_column_takes_no_parents(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cannot give parents"):
        write_ontology_source(tmp_path, {"LOINC": Pack({"2160-0": "X"}, parents=False)})


def test_small_helpers() -> None:
    assert names_map({"A": "a"}) == {"A": "a"}
    assert names_map([{"key": "A", "value": "a"}]) == {"A": "a"}
    assert names_map([("A", "a")]) == {"A": "a"}
    assert names_map(None) == {}
    assert (
        stage_overrides(ontology_source=Path("/p"))
        == "overrides=['++stage_cfg.ontology_source=/p']"
    )
    assert stage_name("sumeds_normalize_units") == "sumeds_normalize_units"
    assert stage_name({"sumeds_enrich": {"rules": "X"}}) == "sumeds_enrich"


# ------------------------------------------------------------------ a synthetic ETL package

SPEC = """\
etl:
  dataset_name: CONTRACT
  raw_dataset_version: "1.0"
  n_subjects_per_shard: 10
  split_fracs: {train: 0.5, tuning: 0.25, held_out: 0.25}
  post_stages:
    - sumeds_link_unit_siblings
    - sumeds_normalize_units
    - sumeds_enrich: {rules: CONTRACT}

_defaults:
  subject_id: $patient_id

patients:
  dob:
    code: MEDS_BIRTH
    time: '$dob::"%Y-%m-%dT%H:%M:%S"'

diagnoses:
  dx:
    code: 'f"DX//{$diagnosis_code}"'
    time: '$timestamp::"%Y-%m-%dT%H:%M:%S"'

procedures:
  proc:
    code: 'f"PROC//{$path}"'
    time: '$timestamp::"%Y-%m-%dT%H:%M:%S"'

labs:
  lab:
    code: 'f"LAB//{$name}"'
    time: '$timestamp::"%Y-%m-%dT%H:%M:%S"'
    numeric_value: $value
"""

RULES = """\
schema: sumeds-rules/0.1
ontology:
  - {id: synth-icd10cm, pattern: '^DX//(?P<code>.+)$', vocabulary: ICD10CM}
crosswalk:
  - id: synth-lab-loinc
    pattern: '^LAB//(?P<key>[^/]+)$'
    table: synth/lab_loinc
    key_column: name
    code_column: loinc_code
    vocabulary: LOINC
hierarchy:
  - {id: synth-procedure-paths, pattern: '^PROC//(?P<path>.+)$', group: path, separator: '|'}
  - {id: synth-diagnosis-parents, from_ontology: true}
"""

RAW = {
    "patients.csv": "patient_id,dob\n1,1954-01-24T00:00:00\n2,2009-02-19T00:00:00\n"
    "3,1980-05-01T00:00:00\n4,1970-07-07T00:00:00\n",
    "diagnoses.csv": "patient_id,diagnosis_code,timestamp\n1,I50.23,2024-04-03T00:00:00\n"
    "1,I50,2024-08-30T00:00:00\n2,I10,2024-01-01T00:00:00\n3,Z99.9,2024-02-01T00:00:00\n",
    "procedures.csv": "patient_id,path,timestamp\n1,surgery|cardiac,2024-04-04T00:00:00\n"
    "4,surgery,2024-03-01T00:00:00\n",
    "labs.csv": "patient_id,name,value,timestamp\n2,creatinine,1.1,2024-01-01T01:00:00\n"
    "3,sodium,140,2024-02-01T01:00:00\n",
}


@pytest.fixture(scope="module", autouse=True)
def contract_etl(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Path]:
    """Install ``contract_etl``: its spec and rules, registered under ``CONTRACT``."""
    site = tmp_path_factory.mktemp("site")
    package = site / "contract_etl"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "messy.yaml").write_text(SPEC)
    (package / "sumeds_rules.yaml").write_text(RULES)
    dist_info = site / "contract_etl-0.0.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: contract-etl\nVersion: 0.0.0\n"
    )
    (dist_info / "entry_points.txt").write_text(
        "[MEDS_extract.pipelines]\nCONTRACT = contract_etl:messy.yaml\n\n[sumeds.rules]\nCONTRACT = contract_etl:sumeds_rules.yaml\n"
    )
    old_path, old_env = list(sys.path), os.environ.get("PYTHONPATH")
    sys.path.insert(0, str(site))
    os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [str(site), old_env]))
    try:
        yield site
    finally:
        sys.path[:] = old_path
        sys.modules.pop("contract_etl", None)
        if old_env is None:
            os.environ.pop("PYTHONPATH", None)
        else:
            os.environ["PYTHONPATH"] = old_env


def test_registered_files_are_found(contract_etl: Path) -> None:
    assert (
        registered_resource("sumeds.rules", "CONTRACT")
        == contract_etl / "contract_etl" / "sumeds_rules.yaml"
    )
    assert load_spec("CONTRACT")["etl"]["dataset_name"] == "CONTRACT"
    with pytest.raises(LookupError, match="no installed package registers 'NOPE'"):
        registered_resource("sumeds.rules", "NOPE")


class TestRulesContract(RulesContract):
    rules_name = "CONTRACT"
    expected_ontology = {
        "DX//I50.23": ("synth-icd10cm", "I50.23"),
        "DX//I50": ("synth-icd10cm", "I50"),
    }
    expected_crosswalk_keys = {"LAB//creatinine": ("synth-lab-loinc", "creatinine")}
    expected_hierarchy = {
        "synth-procedure-paths": {"PROC//surgery|cardiac": ["PROC//surgery"]}
    }

    @pytest.fixture(scope="class")
    @classmethod
    def example_codes(cls) -> list[str]:
        """The default examples, and codes that no rule may match (as an ETL's own codes would be)."""
        return [
            "DX//I50.23",
            "DX//I50",
            "LAB//creatinine",
            "PROC//surgery|cardiac",
            "PROC//surgery",
        ] + [
            "MEDS_BIRTH",
            "LAB//creatinine//mg/dL",
            "PROC//other",
        ]


def test_the_rules_contract_catches_a_wrong_expectation() -> None:
    contract = TestRulesContract()
    rules = load_rules("CONTRACT")
    contract.expected_ontology = {"DX//I50.23": ("synth-icd10cm", "I50.2")}
    with pytest.raises(AssertionError):
        contract.test_ontology_rules_resolve_the_examples(rules, ["DX//I50.23"])
    contract.expected_ontology = {"DX//I50.23": ("synth-icd10cm", "I50.23")}
    contract.test_ontology_rules_resolve_the_examples(
        rules, ["DX//I50.23", "MEDS_BIRTH"]
    )
    contract.own_post_stages = ("synth_own_stage",)
    with pytest.raises(AssertionError):
        contract.test_spec_runs_the_sumeds_post_stages_in_order()


def _meds_extract_installed() -> bool:
    try:
        import MEDS_extract.config
    except ImportError:
        return False
    stages = {ep.name for ep in entry_points(group="MEDS_transforms.stages")}
    return hasattr(MEDS_extract.config, "ExtensionStage") and "sumeds_enrich" in stages


@pytest.mark.skipif(
    not _meds_extract_installed(),
    reason="needs MEDS-Extract >= 0.9 and an installed suMEDS",
)
class TestEnrichmentContract(EnrichmentContract):
    rules_name = "CONTRACT"
    packs = {
        "ICD10CM": (
            "fy2016-fy2027",
            {"I50": None, "I50.2": "I50", "I50.23": "I50.2", "I10": None},
        ),
        "LOINC": Pack(["2160-0"], release="2.83", parents=False),
    }
    crosswalks = {
        "synth": {"lab_loinc": [{"name": "creatinine", "loinc_code": "2160-0"}]}
    }
    expected_parents = {
        "DX//I50.23": ["ICD10CM//I50.23", "ICD10CM//I50.2", "ICD10CM//I50", "DX//I50"],
        "LAB//creatinine": ["LOINC//2160-0"],
        "PROC//surgery|cardiac": ["PROC//surgery"],
    }
    expected_descriptions = {
        "DX//I50.23": synthetic_title("ICD10CM", "I50.23"),
        "DX//I10": synthetic_title("ICD10CM", "I10"),
    }
    unenriched_codes = ["DX//Z99.9", "LAB//sodium", "MEDS_BIRTH"]

    @pytest.fixture(scope="class")
    @classmethod
    def raw_dir(cls, tmp_path_factory: pytest.TempPathFactory) -> Path:
        raw = tmp_path_factory.mktemp("raw")
        for name, text in RAW.items():
            (raw / name).write_text(text)
        return raw
