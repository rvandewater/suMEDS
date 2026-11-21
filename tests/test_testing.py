"""``sumeds.testing``: the local ontology source, the helpers, and the ETL contract tests.

The contracts run against ``contract_etl``, a throwaway ETL package written into a site directory
that is put on ``sys.path`` (and ``PYTHONPATH``, for the ``meds-extract-run`` it spawns). It
registers its spec and its rules under ``CONTRACT``, as an ETL package does. The rules contract
needs only suMEDS; the enrichment contract runs ``meds-extract-run`` and is skipped without
MEDS-Extract (>= 0.9), like the end-to-end tests of ``test_stage.py``.
"""

from __future__ import annotations

import copy
import gzip
import os
import re
import sys
from collections.abc import Iterator
from importlib.metadata import entry_points
from importlib.util import find_spec
from pathlib import Path

import polars as pl
import pytest
import yaml

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
from sumeds.testing.contracts import (
    EnrichmentContract,
    RulesContract,
    UnitComponentContract,
    check_units_in_code,
    find_unit_components,
)

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


# ------------------------------------------------------------------ the unit component contract

_needs_dftly = pytest.mark.skipif(
    find_spec("dftly") is None,
    reason="UnitComponentContract needs dftly (Python >= 3.11)",
)

STRIP_OR_UNK = '(extract /(?s)\\S(?:.*\\S)?/ from ${column}) ?? "UNK"'

#: A spec with each shape discovery has to decide: units through a helper (with and without
#: `_self.unit`, read from `_self` or from an external metadata table, under a code condition),
#: a unit extracted from inside a label, constant units, and codes without a unit.
UNIT_SPEC = {
    "etl": {"dataset_name": "CONTRACT"},
    "_defaults": {"subject_id": "$patient_id"},
    "labs": {
        "_table": {"cols": {"_value_unit": STRIP_OR_UNK.format(column="unit")}},
        "lab_drawn": {
            "code": "f\"LAB//DRAWN//{$name ?? 'UNK'}//{$_value_unit}\"",
            "time": "$t",
        },
        "lab_result": {
            "code": "f\"LAB//{$name ?? 'UNK'}//{$_value_unit}\" if /\\S/ in $value",
            "time": "$t",
            "numeric_value": "$value::?float64",
            "_metadata": {
                "_self": {"description": "$name", "unit": '$unit if $unit != ""'}
            },
        },
        "lab_priority": {
            "code": "f\"LAB//PRIORITY//{$priority ?? 'UNK'}\"",
            "time": "$t",
        },
    },
    "drugs": {
        "_table": {
            "cols": {
                "drug_name": {"column": "Drug Name"},
                "_dose_unit": STRIP_OR_UNK.format(column="`Dose Unit`"),
            }
        },
        "dose": {
            "code": 'f"DRUG//{$drug_name ?? $drug_id::str}//{$_dose_unit}"',
            "time": "$t",
            "_metadata": {
                "drug_reference": {
                    "drug_id": "$id",
                    "unit": "$`Dose Unit` if $kind == 'drug'",
                }
            },
        },
    },
    "flowsheet": {
        "row": {
            "code": 'f"FLOWSHEET//{$label}"',
            "time": "$t",
            "_metadata": {
                "_self": {"unit": "extract group 1 of /\\(([^)]+)\\)$/ from $label"}
            },
        },
    },
    "patients": {
        "weight": {
            "code": '"WEIGHT" if $weight == $weight',
            "time": None,
            "_metadata": {"_self": {"unit": '"kg"'}},
        },
        "gender": {"code": "f\"GENDER//{$gender ?? 'UNK'}\"", "time": None},
    },
}

UNIT_EVENTS = ["drugs/dose", "labs/lab_drawn", "labs/lab_result"]


def _with(spec: dict, table: str, event: str | None = None, **changes) -> dict:
    """A copy of ``spec`` with ``changes`` to one event (or, without ``event``, to ``_table.cols``)."""
    import copy

    spec = copy.deepcopy(spec)
    if event is None:
        spec[table]["_table"]["cols"].update(changes)
    else:
        spec[table][event].update(changes)
    return spec


def _contract(spec, **attributes) -> type[UnitComponentContract]:
    return type("Contract", (UnitComponentContract,), {"spec": spec, **attributes})


def _run(contract: type[UnitComponentContract], test: str) -> None:
    """Run one contract test over every event it is parametrized with."""
    instance = contract()
    if test in {
        "test_unit_component_is_stripped_and_blank_is_unk",
        "test_unit_metadata_stays_the_raw_column",
    }:
        components = contract.discover()[0]
        if test == "test_unit_metadata_stays_the_raw_column":
            components = [c for c in components if c.unit is not None]
        for component in components:
            getattr(instance, test)(component)
    else:
        getattr(instance, test)()


@_needs_dftly
class TestUnitComponentContract(UnitComponentContract):
    spec = UNIT_SPEC
    expected_unit_events = UNIT_EVENTS


@_needs_dftly
def test_discovery_decides_each_shape() -> None:
    found, undecided = find_unit_components(UNIT_SPEC)
    assert undecided == {}
    assert {c.id: (c.column, c.helpers) for c in found} == {
        "labs/lab_drawn": ("unit", ("_value_unit",)),
        "labs/lab_result": ("unit", ("_value_unit",)),
        "drugs/dose": ("Dose Unit", ("_dose_unit",)),
    }
    units = {c.id: c.unit for c in found}
    # Only `_self.unit` is evaluated over the event's own rows; an external block is not.
    assert units == {
        "labs/lab_drawn": None,
        "labs/lab_result": '$unit if $unit != ""',
        "drugs/dose": None,
    }


@_needs_dftly
def test_the_contract_reads_a_spec_file_or_a_registered_name(tmp_path: Path) -> None:
    path = tmp_path / "messy.yaml"
    path.write_text(yaml.safe_dump(UNIT_SPEC))
    for spec in (path, str(path)):
        assert _contract(spec).load_spec() == UNIT_SPEC
    assert _contract("CONTRACT").load_spec() == load_spec("CONTRACT")


@_needs_dftly
def test_a_raw_unit_column_at_the_end_of_a_code_fails() -> None:
    """What a new ETL gets for free: a code that ends in the unit as recorded is named."""
    spec = _with(UNIT_SPEC, "labs", "lab_result", code='f"LAB//{$name}//{$unit}"')
    contract = _contract(spec)
    assert {c.id for c in contract.discover()[0]} == set(UNIT_EVENTS)
    with pytest.raises(
        AssertionError, match=r"end in their raw unit column: \['labs/lab_result'\]"
    ):
        _run(contract, "test_no_code_ends_in_a_raw_unit_column")
    with pytest.raises(AssertionError, match="labs/lab_result"):
        _run(contract, "test_unit_component_is_stripped_and_blank_is_unk")


@pytest.mark.parametrize(
    "helper",
    [
        '$unit ?? "UNK"',  # not stripped
        '(extract /\\S+/ from $unit) ?? "UNK"',  # loses the inner spaces of `mL / h`
        "extract /(?s)\\S(?:.*\\S)?/ from $unit",  # a blank unit stays null, not `UNK`
    ],
)
@_needs_dftly
def test_a_helper_that_does_not_strip_or_fill_fails(helper: str) -> None:
    contract = _contract(_with(UNIT_SPEC, "labs", _value_unit=helper))
    with pytest.raises(AssertionError, match="labs/lab_drawn"):
        _run(contract, "test_unit_component_is_stripped_and_blank_is_unk")


@_needs_dftly
def test_a_stripped_unit_metadata_fails() -> None:
    """``_self.unit`` must stay the raw column; MEDS-Extract strips it when it writes ``codes.parquet``."""
    for unit in ("$_value_unit", "extract /(?s)\\S(?:.*\\S)?/ from $unit"):
        spec = _with(UNIT_SPEC, "labs", "lab_result")
        spec["labs"]["lab_result"]["_metadata"]["_self"]["unit"] = unit
        contract = _contract(spec)
        with pytest.raises(
            AssertionError, match="labs/lab_result: unit ' mg' became 'mg'"
        ):
            _run(contract, "test_unit_metadata_stays_the_raw_column")


@_needs_dftly
def test_overrides_and_undecided_events() -> None:
    # A code condition on a number cannot be evaluated on the dummy string, until a dummy is given.
    spec = _with(
        UNIT_SPEC,
        "labs",
        "lab_drawn",
        code='f"LAB//DRAWN//{$name}//{$_value_unit}" if $count > 0',
    )
    _, undecided = find_unit_components(spec)
    assert list(undecided) == ["labs/lab_drawn"]
    contract = _contract(spec)
    with pytest.raises(
        AssertionError, match="cannot tell whether these codes end in a unit"
    ):
        _run(contract, "test_unit_components_are_found")
    contract = _contract(
        spec, dummy_values={"count": 1}, expected_unit_events=UNIT_EVENTS
    )
    _run(contract, "test_unit_components_are_found")
    _run(contract, "test_unit_component_is_stripped_and_blank_is_unk")

    # `ignored_events` leaves an event out; `unit_events` adds one discovery does not find.
    contract = _contract(
        UNIT_SPEC, ignored_events=["drugs/dose"], expected_unit_events=UNIT_EVENTS
    )
    with pytest.raises(AssertionError):
        _run(contract, "test_unit_components_are_found")
    spec = _with(UNIT_SPEC, "drugs", "dose", _metadata={})
    assert "drugs/dose" not in {c.id for c in find_unit_components(spec)[0]}
    contract = _contract(
        spec, unit_events={"drugs/dose": "Dose Unit"}, expected_unit_events=UNIT_EVENTS
    )
    _run(contract, "test_unit_components_are_found")
    _run(contract, "test_unit_component_is_stripped_and_blank_is_unk")
    _, undecided = find_unit_components(UNIT_SPEC, unit_events={"labs/nope": "unit"})
    assert undecided == {
        "labs/nope": "unit_events names an event the spec does not have"
    }

    # A spec without unit components fails, unless it says so.
    spec = {"patients": UNIT_SPEC["patients"]}
    with pytest.raises(
        AssertionError, match="no code in the spec ends in a unit column"
    ):
        _run(_contract(spec), "test_unit_components_are_found")
    _run(_contract(spec, expected_unit_events=()), "test_unit_components_are_found")


# ------------------------------------------------------------- require_unit_in_code

#: The verdict on each event of UNIT_SPEC with a `_self.unit`; `drugs/dose` reads its unit from an
#: external metadata table and is not checked.
UNIT_IN_CODE_VERDICTS = {
    "labs/lab_result": "in code",
    "flowsheet/row": "derived",
    "patients/weight": "fixed",
}


def _verdicts(spec: dict, **kwargs) -> dict[str, str]:
    return {
        k: verdict for k, (verdict, _) in check_units_in_code(spec, **kwargs).items()
    }


@_needs_dftly
class TestRequireUnitInCode(UnitComponentContract):
    spec = UNIT_SPEC
    expected_unit_events = UNIT_EVENTS
    require_unit_in_code = True


@_needs_dftly
def test_require_unit_in_code_is_off_by_default() -> None:
    spec = _with(UNIT_SPEC, "labs", "lab_priority")
    spec["labs"]["lab_priority"]["_metadata"] = {"_self": {"unit": "$unit"}}
    with pytest.raises(pytest.skip.Exception, match="require_unit_in_code is off"):
        _run(_contract(spec), "test_row_units_are_in_the_code")


@_needs_dftly
def test_units_in_code_skip_fixed_derived_and_external_units() -> None:
    """A constant unit, a unit extracted from a label the code carries (in its last component, as
    MIMIC-IV's `omr`) or chosen by a column the code carries elsewhere, and a unit from an external
    metadata table all have one unit per code without ending the code in it."""
    assert _verdicts(UNIT_SPEC) == UNIT_IN_CODE_VERDICTS
    spec = copy.deepcopy(UNIT_SPEC)
    spec["flowsheet"]["vital"] = {
        "code": 'f"VITAL//{$measure}//{$source}"',
        "time": "$t",
        "_metadata": {
            "_self": {
                "unit": '"%" if $measure == "SpO2" else "mm[Hg]" if $measure == "SBP"'
            }
        },
    }
    # MIMIC-IV's `omr`: blood pressures (`120/80`) are other events, so a helper is the condition.
    spec["omr"] = {
        "_table": {
            "cols": {"systolic": "extract group 1 of /^([0-9]+)\\/[0-9]+$/ from $text"}
        }
    }
    spec["omr"]["omr"] = {
        "code": '$name if ($systolic ?? "") == ""',
        "time": "$t",
        "_metadata": {
            "_self": {
                "unit": "extract group 1 of /^(?:Weight|Height) \\(([^)]+)\\)$/ from $name"
            }
        },
    }
    assert _verdicts(spec) == {
        **UNIT_IN_CODE_VERDICTS,
        "flowsheet/vital": "derived",
        "omr/omr": "derived",
    }
    _run(_contract(spec, require_unit_in_code=True), "test_row_units_are_in_the_code")


def _missing(code: str) -> dict:
    spec = _with(UNIT_SPEC, "labs", "lab_priority", code=code)
    spec["labs"]["lab_priority"]["_metadata"] = {"_self": {"unit": "$unit"}}
    return spec


@pytest.mark.parametrize(
    ("code", "detail"),
    [
        # The code does not read the unit column at all.
        (
            "f\"LAB//PRIORITY//{$priority ?? 'UNK'}\"",
            r"reads \$unit, which the code does not",
        ),
        # It reads it only in a condition: `mg` and `kg` give one code.
        (
            "f\"LAB//PRIORITY//{$priority}\" if $unit != ''",
            r"the code 'LAB//PRIORITY//1' gets the units \[.*'kg'.*'mg'.*\] from \$unit",
        ),
    ],
)
@_needs_dftly
def test_a_row_unit_missing_from_the_code_fails(code: str, detail: str) -> None:
    spec = _missing(code)
    assert _verdicts(spec)["labs/lab_priority"] == "not in code"
    contract = _contract(spec, require_unit_in_code=True)
    with pytest.raises(AssertionError) as failure:
        _run(contract, "test_row_units_are_in_the_code")
    message = str(failure.value)
    assert re.search(r"labs/lab_priority: .*" + detail, message), message
    assert "_table.cols" in message and "ignored_events" in message

    # Ending the code in the unit through a stripping helper passes, as does ignoring the event.
    fixed = _missing('f"LAB//PRIORITY//{$priority}//{$_value_unit}"')
    assert _verdicts(fixed)["labs/lab_priority"] == "in code"
    _run(_contract(fixed, require_unit_in_code=True), "test_row_units_are_in_the_code")
    ignored = {"labs/lab_priority": "one unit per priority in this source"}
    assert _verdicts(spec, ignored_events=ignored)["labs/lab_priority"] == "ignored"
    _run(
        _contract(spec, require_unit_in_code=True, ignored_events=ignored),
        "test_row_units_are_in_the_code",
    )


@_needs_dftly
def test_a_code_no_probe_gives_is_undecided() -> None:
    """A code condition the dummy values fail leaves no code to compare: set `dummy_values`."""
    spec = _missing("f\"LAB//PRIORITY//{$unit}//{$priority}\" if $priority == 'STAT'")
    verdict, detail = check_units_in_code(spec)["labs/lab_priority"]
    assert verdict == "undecided" and "set dummy_values" in detail
    with pytest.raises(AssertionError, match="labs/lab_priority: no probe of"):
        _run(
            _contract(spec, require_unit_in_code=True), "test_row_units_are_in_the_code"
        )
    assert (
        _verdicts(spec, dummy_values={"priority": "STAT"})["labs/lab_priority"]
        == "derived"
    )
