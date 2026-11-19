"""Unit spellings as UCUM codes, and the ``sumeds_normalize_units`` stage."""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from pathlib import Path
from types import SimpleNamespace

import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sumeds.cli import normalize_units_main
from sumeds.units import (
    PLACEHOLDERS,
    SOURCE_COLUMN,
    known_spellings,
    normalize_units,
    normalize_units_file,
    to_ucum,
)
from sumeds.units_stage import STAGE_NAME, _main

# The canonical choices: spelling -> UCUM code.
CANONICAL = {
    # Temperature.
    "°C": "Cel",
    "degC": "Cel",
    "celsius": "Cel",
    "Celsius": "Cel",
    "C": "Cel",
    "&#176;C": "Cel",
    "Cel": "Cel",
    "°F": "[degF]",
    "degF": "[degF]",
    "F": "[degF]",
    "fahrenheit": "[degF]",
    # Mass, length, volume.
    "kg": "kg",
    "kilogram": "kg",
    "kilograms": "kg",
    "Kg": "kg",
    "lb": "[lb_av]",
    "lbs": "[lb_av]",
    "lbs.": "[lb_av]",
    "Lbs": "[lb_av]",
    "pound": "[lb_av]",
    "pounds": "[lb_av]",
    "inch": "[in_i]",
    "Inch": "[in_i]",
    "Inches": "[in_i]",
    "in": "[in_i]",
    "oz": "[oz_av]",
    "ml": "mL",
    "mL": "mL",
    "cc": "mL",
    "mls": "mL",
    "l": "L",
    "L": "L",
    "liter": "L",
    "liters": "L",
    "Liters": "L",
    "fl": "fL",
    "fL": "fL",
    "FL": "fL",
    "mcg": "ug",
    "µg": "ug",
    "μg": "ug",
    "grams": "g",
    # Pressure.
    "mmHg": "mm[Hg]",
    "mm Hg": "mm[Hg]",
    "mm[Hg]": "mm[Hg]",
    "cmH2O": "cm[H2O]",
    "cm H2O": "cm[H2O]",
    # Ratios and rates.
    "%": "%",
    "percent": "%",
    "bpm": "{beats}/min",
    "beats/min": "{beats}/min",
    "breaths/min": "{breaths}/min",
    "insp/min": "{breaths}/min",
    "/min": "/min",
    "1/min": "/min",
    "LPM": "L/min",
    # Concentrations.
    "mg/dl": "mg/dL",
    "g/dl": "g/dL",
    "mmol/l": "mmol/L",
    "mmoles/L": "mmol/L",
    "umol/l": "umol/L",
    "µmol/L": "umol/L",
    "μmol/L": "umol/L",
    "U/l": "U/L",
    "U": "U",
    "IU": "[iU]",
    "iu": "[iU]",
    "IU/L": "[iU]/L",
    "IE/l": "[iU]/L",
    "mIU/mL": "m[iU]/mL",
    "mEq": "meq",
    "mEq/L": "meq/L",
    "mEq.": "meq",
    "mOsm/kg": "mosm/kg",
    "mOsm/kg H2O": "mosm/kg{H2O}",
    "ng/mL FEU": "ng/mL{FEU}",
    "U/g/Hb": "U/g{Hb}",
    # Rates and doses.
    "mL/hr": "mL/h",
    "ml/hr": "mL/h",
    "mL/hour": "mL/h",
    "mEq./hour": "meq/h",
    "mcg/kg/min": "ug/kg/min",
    "mcg/kg/hour": "ug/kg/h",
    "mg/24hr": "mg/(24.h)",
    "mm/1h": "mm/h",
    # Time.
    "hr": "h",
    "hrs": "h",
    "hour": "h",
    "hours": "h",
    "sec": "s",
    "secs": "s",
    "second": "s",
    "seconds": "s",
    "msec": "ms",
    "min": "min",
    "minute": "min",
    "minutes": "min",
    "day": "d",
    "days": "d",
    "year": "a",
    "years": "a",
    # Cell counts.
    "K/uL": "10*9/L",
    "K/µL": "10*9/L",
    "K/mcL": "10*9/L",
    "10^9/L": "10*9/L",
    "x10^9/L": "10*9/L",
    "×10^9/L": "10*9/L",
    "10^3/uL": "10*9/L",
    "/nL": "10*9/L",
    "G/l": "10*9/L",
    "G/L": "10*9/L",
    "Gpt/l": "10*9/L",
    "M/uL": "10*12/L",
    "M/mcL": "10*12/L",
    "10^12/L": "10*12/L",
    "T/l": "10*12/L",
    "T/L": "10*12/L",
    "Tpt/l": "10*12/L",
    "Million/uL": "10*12/L",
    "#/uL": "/uL",
    "/hpf": "/[HPF]",
    "#/lpf": "/[LPF]",
    # Exponents.
    "m^2": "m2",
    "mL/m^2": "mL/m2",
    "kg/m2": "kg/m2",
    "dynes*sec/cm5": "dyn.s/cm5",
    "dynes.sec.cm-5/m2": "dyn.s/cm5/m2",
    # Words that do not say which unit they mean: annotations.
    "units": "{units}",
    "Units": "{units}",
    "unit": "{units}",
    "Unit": "{units}",
    "Units/L": "{units}/L",
    "units/hour": "{units}/h",
    "dose": "{dose}",
    "Dose": "{dose}",
    "bag": "{bag}",
    "tube": "{tube}",
    "each": "{each}",
    "Each": "{each}",
    "ratio": "{ratio}",
    "Ratio": "{ratio}",
    "RATIO": "{ratio}",
    "index": "{index}",
    "Index": "{index}",
    "mL/beat": "mL/{beat}",
    "INR": "{INR}",
    "tablet": "{tablet}",
    "drops": "{drop}",
}

# Case carries meaning: these differ only in case, and map differently (or not at all).
CASE_TRAPS = [
    ("G/l", "10*9/L"),
    ("g/l", "g/L"),
    ("G/L", "10*9/L"),
    ("g/L", "g/L"),
    ("mL", "mL"),
    ("ML", None),
    ("Cel", "Cel"),
    ("CEL", None),
    ("T/l", "10*12/L"),
    ("t/l", None),
    ("U", "U"),
    ("u", None),
    ("mU/L", "mU/L"),
    ("MU/L", None),
]

UNKNOWN = [
    "furlongs",
    "mg/xyz",  # every atom must be known
    "m/uL",  # metre per microlitre, though labs mean millions
    "G/24HR",  # grams, or giga?
    "log10 IU/mL",
    "+/-",
    "1/2/3/4",
    "C/s",  # C is a temperature only on its own
    "mg//dL",
    "pm",  # p.m., not picometre
    "cs",
    "mg/",
]


@pytest.mark.parametrize(("spelling", "ucum"), CANONICAL.items())
def test_canonical_choices(spelling: str, ucum: str) -> None:
    assert to_ucum(spelling) == ucum
    assert to_ucum(f"  {spelling} ") == ucum  # surrounding whitespace


@pytest.mark.parametrize(("spelling", "ucum"), CASE_TRAPS)
def test_case_carries_meaning(spelling: str, ucum: str | None) -> None:
    assert to_ucum(spelling) == ucum


@pytest.mark.parametrize("spelling", UNKNOWN)
def test_unknown_spellings_are_not_guessed(spelling: str) -> None:
    assert to_ucum(spelling) is None


@pytest.mark.parametrize(
    "spelling",
    [
        None,
        "",
        "  ",
        "unknown",
        "UNKNOWN",
        "UNK",
        "n/a",
        "N/A",
        "none",
        "-",
        "?",
        "Geen",
    ],
)
def test_placeholders_are_not_units(spelling: str | None) -> None:
    assert to_ucum(spelling) is None


def _outputs() -> set[str]:
    spellings = [*CANONICAL, *known_spellings()]
    return {ucum for s in spellings if (ucum := to_ucum(s)) is not None}


def test_canonical_spellings_are_fixed_points() -> None:
    outputs = _outputs()
    assert len(outputs) > 100
    assert {u: to_ucum(u) for u in outputs if to_ucum(u) != u} == {}


def test_placeholders_are_lower_case() -> None:
    assert all(p == p.casefold() for p in PLACEHOLDERS)


def test_canonical_spellings_are_valid_ucum() -> None:
    ucumvert = pytest.importorskip("ucumvert")
    parser = ucumvert.get_ucum_parser()
    invalid = []
    for ucum in sorted(_outputs()):
        try:
            parser.parse(ucum)
        except Exception:
            invalid.append(ucum)
    assert invalid == []
    # The validator knows the atoms: the case traps are rejected.
    for wrong in ["CEL", "degC", "mcg"]:
        with pytest.raises(Exception):
            parser.parse(wrong)


# ------------------------------------------------------------------ normalize_units


def test_normalize_units_keeps_the_recorded_spelling() -> None:
    df = pl.DataFrame(
        {
            "code": ["T", "W", "X", "Y", "Z"],
            "unit": ["°C", "kg", None, "furlongs", "N/A"],
        }
    )
    out, counts = normalize_units(df)
    assert out.columns == ["code", "unit", SOURCE_COLUMN]
    assert out["unit"].to_list() == ["Cel", "kg", None, "furlongs", "N/A"]
    assert out[SOURCE_COLUMN].to_list() == ["°C", "kg", None, "furlongs", "N/A"]
    assert counts == {
        "rows_with_unit": 4,
        "already_canonical": 1,
        "rewritten": 1,
        "unknown_kept": 1,
        "placeholder_kept": 1,
        "unknown_spellings": {"furlongs": 1},
    }

    again, counts = normalize_units(out)
    assert again.equals(out)
    assert counts["rewritten"] == 0
    assert counts["already_canonical"] == 2


def test_normalize_units_fills_only_missing_source_values() -> None:
    df = pl.DataFrame(
        {
            "unit": ["mg/dl", "mmol/l"],
            SOURCE_COLUMN: ["MG/DL (from the ETL)", None],
            "after": [1, 2],
        }
    )
    out, _ = normalize_units(df)
    assert out.columns == ["unit", SOURCE_COLUMN, "after"]
    assert out[SOURCE_COLUMN].to_list() == ["MG/DL (from the ETL)", "mmol/l"]
    assert out["unit"].to_list() == ["mg/dL", "mmol/L"]


def test_normalize_units_handles_list_columns() -> None:
    df = pl.DataFrame({"unit": [["mg/dl", "mmol/l"], None, ["xyz"]]})
    out, counts = normalize_units(df)
    assert out["unit"].to_list() == [["mg/dL", "mmol/L"], None, ["xyz"]]
    assert out[SOURCE_COLUMN].to_list() == [["mg/dl", "mmol/l"], None, ["xyz"]]
    assert counts["rows_with_unit"] == 3
    assert counts["unknown_spellings"] == {"xyz": 1}


def test_normalize_units_without_a_unit_column() -> None:
    df = pl.DataFrame({"code": ["A"]})
    out, counts = normalize_units(df)
    assert out.equals(df)
    assert counts["rows_with_unit"] == 0


# ------------------------------------------------------------------ file and stage

_MAP = pa.map_(pa.string(), pa.string())


def _codes_table(unit_type: pa.DataType = pa.large_string()) -> pa.Table:
    units = ["mg/dl", None, "°C", "furlongs", "mg/dL"]
    return pa.table(
        {
            "code": pa.array(
                ["LAB//Z//mg/dl", "DX//A", "VITAL//T", "LAB//Q", "LAB//B"]
            ),
            "description": pa.array(["z", None, "temperature", "q", "b"]),
            "parent_codes": pa.array(
                [["LAB//Z"], None, [], None, None], pa.list_(pa.string())
            ),
            "unit": pa.array(units, unit_type),
            "parent_names": pa.array([[("LAB//Z", "Z")], None, None, None, None], _MAP),
            "n": pa.array([1, 2, 3, 4, 5], pa.int32()),
        }
    )


def _meds_root(root: Path, table: pa.Table) -> Path:
    (root / "metadata").mkdir(parents=True)
    pq.write_table(table, root / meds.code_metadata_filepath)
    return root


def test_normalize_units_file_preserves_other_columns(tmp_path: Path) -> None:
    for unit_type in [pa.string(), pa.large_string()]:
        path = tmp_path / f"{unit_type}.parquet"
        before = _codes_table(unit_type)
        pq.write_table(before, path)
        counts = normalize_units_file(path)
        after = pq.read_table(path)
        meds.CodeMetadataSchema.validate(after)
        assert after.schema.names == [*before.schema.names, SOURCE_COLUMN]
        for field in before.schema:
            assert after.schema.field(field.name).type == field.type
            if field.name != "unit":
                assert after.column(field.name).equals(before.column(field.name))
        assert after.schema.field(SOURCE_COLUMN).type == pa.large_string()
        assert after.column("unit").to_pylist() == [
            "mg/dL",
            None,
            "Cel",
            "furlongs",
            "mg/dL",
        ]
        assert (
            after.column(SOURCE_COLUMN).to_pylist() == before.column("unit").to_pylist()
        )
        assert counts["rewritten"] == 2
        assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".")] == []

        # Idempotent: the recorded spellings survive a second run.
        normalize_units_file(path)
        assert pq.read_table(path).equals(after)


def test_normalize_units_file_without_units(tmp_path: Path) -> None:
    path = tmp_path / "codes.parquet"
    pq.write_table(_codes_table().drop_columns(["unit"]), path)
    before = path.read_bytes()
    normalize_units_file(path)
    assert path.read_bytes() == before
    normalize_units_file(path, tmp_path / "out" / "codes.parquet")
    assert (tmp_path / "out" / "codes.parquet").read_bytes() == before


def test_normalize_units_file_rejects_invalid_metadata(tmp_path: Path) -> None:
    path = tmp_path / "codes.parquet"
    pq.write_table(pa.table({"unit": ["mg/dl"]}), path)  # no code column
    before = path.read_bytes()
    with pytest.raises(Exception):
        normalize_units_file(path)
    assert path.read_bytes() == before


def _cfg(root: Path, worker: int = 0) -> SimpleNamespace:
    metadata = str(root / "metadata")
    return SimpleNamespace(
        worker=worker,
        stage_cfg=SimpleNamespace(
            metadata_input_dir=metadata, reducer_output_dir=metadata
        ),
    )


def test_the_stage_normalises_in_place_and_logs(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    root = _meds_root(tmp_path / "meds", _codes_table())
    codes = root / meds.code_metadata_filepath
    before = codes.read_bytes()

    _main(_cfg(root, worker=1))  # only worker 0 works
    assert codes.read_bytes() == before

    with caplog.at_level(logging.INFO, logger="sumeds.units"):
        _main(_cfg(root))
    assert pq.read_table(codes).column("unit").to_pylist()[2] == "Cel"
    info = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert any(
        "4 with a unit, 1 already canonical UCUM, 2 rewritten, 1 unknown" in m
        for m in info
    )
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "'furlongs': 1" in warnings[0]

    normalized = codes.read_bytes()
    _main(_cfg(root))
    assert pq.read_table(codes).equals(pq.read_table(pa.BufferReader(normalized)))


def test_the_stage_is_registered() -> None:
    stages = {ep.name: ep.value for ep in entry_points(group="MEDS_transforms.stages")}
    assert stages[STAGE_NAME] == "sumeds.units_stage:main"


def test_the_cli(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    root = _meds_root(tmp_path / "meds", _codes_table())
    assert normalize_units_main([str(root)]) == 0
    assert "2 of 4 units rewritten, 1 unknown kept" in capsys.readouterr().out
    assert (
        pq.read_table(root / meds.code_metadata_filepath).column("unit")[0].as_py()
        == "mg/dL"
    )

    out = tmp_path / "out.parquet"
    assert (
        normalize_units_main([str(root / meds.code_metadata_filepath), "-o", str(out)])
        == 0
    )
    assert out.is_file()
    with pytest.raises(SystemExit):
        normalize_units_main([str(root), "-o", str(out)])
    with pytest.raises(SystemExit):
        normalize_units_main([str(tmp_path / "missing")])


# ------------------------------------------------------------------ end to end


def _extension_stages_installed() -> bool:
    try:
        import MEDS_extract.config
    except ImportError:
        return False
    registered = {ep.name for ep in entry_points(group="MEDS_transforms.stages")}
    return hasattr(MEDS_extract.config, "ExtensionStage") and STAGE_NAME in registered


SPEC = """\
etl:
  dataset_name: SYNTH
  raw_dataset_version: "1.0"
  n_subjects_per_shard: 10
  split_fracs: {train: 0.5, tuning: 0.25, held_out: 0.25}
  post_stages:
    - sumeds_normalize_units

_defaults:
  subject_id: $patient_id

patients:
  dob:
    code: MEDS_BIRTH
    time: '$dob::"%Y-%m-%dT%H:%M:%S"'

labs:
  glucose:
    code: '"LAB//GLUCOSE//mg/dl"'
    time: '$timestamp::"%Y-%m-%dT%H:%M:%S"'
    numeric_value: $glucose::?float64
    _metadata:
      _self:
        unit: '"mg/dl"'
  temperature:
    code: '"VITAL//TEMP"'
    time: '$timestamp::"%Y-%m-%dT%H:%M:%S"'
    numeric_value: $temp::?float64
    _metadata:
      _self:
        unit: '"celsius"'
"""


@pytest.mark.skipif(
    not _extension_stages_installed(),
    reason="needs MEDS-Extract with extension stages and an installed suMEDS",
)
def test_meds_extract_runs_the_stage(tmp_path: Path) -> None:
    import os
    import subprocess
    import sys

    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "patients.csv").write_text(
        "patient_id,dob\n1,1954-01-24T00:00:00\n2,2009-02-19T00:00:00\n"
        "3,1980-05-01T00:00:00\n4,1970-07-07T00:00:00\n"
    )
    (raw / "labs.csv").write_text(
        "patient_id,timestamp,glucose,temp\n1,2024-04-03T00:00:00,101,37.1\n"
        "2,2024-01-01T00:00:00,95,36.8\n3,2024-02-01T00:00:00,120,38.0\n"
        "4,2024-03-01T00:00:00,88,37.4\n"
    )
    spec = tmp_path / "spec.yaml"
    spec.write_text(SPEC)
    out = tmp_path / "meds"
    env = {
        **os.environ,
        "PATH": os.pathsep.join([str(Path(sys.executable).parent), os.environ["PATH"]]),
    }
    result = subprocess.run(
        [
            "meds-extract-run",
            f"spec={spec}",
            f"output_dir={out}",
            "do_download=false",
            f"input_dir={raw}",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=600,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    table = pq.read_table(out / meds.code_metadata_filepath)
    meds.CodeMetadataSchema.validate(table)
    rows = {r["code"]: r for r in table.to_pylist()}
    assert rows["LAB//GLUCOSE//mg/dl"]["unit"] == "mg/dL"  # the code keeps its name
    assert rows["LAB//GLUCOSE//mg/dl"][SOURCE_COLUMN] == "mg/dl"
    assert rows["VITAL//TEMP"]["unit"] == "Cel"
    assert rows["VITAL//TEMP"][SOURCE_COLUMN] == "celsius"
