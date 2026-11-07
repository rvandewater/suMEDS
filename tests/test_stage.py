"""The ``sumeds_enrich`` MEDS-Extract post-stage.

The unit tests call the stage function directly and need neither MEDS-Transforms nor
MEDS-Extract. The end-to-end tests run ``meds-extract-run`` on a synthetic spec whose
``etl.post_stages`` declares the stage; they need MEDS-Extract with extension stages (0.8.1,
mmcdermott/MEDS_extract#320) and suMEDS installed, so that its ``MEDS_transforms.stages`` entry
point is registered, and are skipped otherwise. The rules are registered by a throwaway package
on ``PYTHONPATH`` and the ontology pack source is a local directory, so nothing is read over the
network.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from types import SimpleNamespace

import meds
import polars as pl
import pyarrow.parquet as pq
import pytest

import sumeds.ontology
from sumeds.cli import STAGE_OPTIONS, enrich_main, stage_argv
from sumeds.ontology import INDEX_SCHEMA, PACK_SCHEMA, RULES_ENTRY_POINT_GROUP
from sumeds.stage import DEFAULT_CONFIG, STAGE_NAME, _main

RULES = (
    "schema: sumeds-rules/0.1\n"
    "ontology:\n"
    "  - {id: icd10cm, pattern: '^DX//(?P<code>.+)$', vocabulary: ICD10CM}\n"
)
PACK = {
    "E": [
        ("E11", "Type 2 diabetes mellitus", ""),
        ("E11.9", "Type 2 diabetes mellitus without complications", "E11"),
    ],
    "I": [
        ("I10", "Essential (primary) hypertension", ""),
        ("I50", "Heart failure", ""),
        ("I50.2", "Systolic (congestive) heart failure", "I50"),
        ("I50.23", "Acute on chronic systolic (congestive) heart failure", "I50.2"),
    ],
}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def packs(tmp_path: Path) -> Path:
    """A local ontology source with a two-shard ICD10CM pack."""

    root = tmp_path / "ontologies"
    (root / "ICD10CM" / "test").mkdir(parents=True)
    shards = {}
    for key, rows in PACK.items():
        data = "code,description,parent\n" + "".join(
            f"{c},{d},{p}\n" for c, d, p in rows
        )
        (root / "ICD10CM" / "test" / f"{key}.csv").write_text(data)
        shards[key] = {"file": f"{key}.csv", "sha256": _sha(data.encode())}
    manifest = json.dumps(
        {
            "schema": PACK_SCHEMA,
            "vocabulary": "ICD10CM",
            "release": "test",
            "shard_key_length": 1,
            "shards": shards,
        }
    ).encode()
    (root / "ICD10CM" / "test" / "manifest.json").write_bytes(manifest)
    (root / "index.json").write_text(
        json.dumps(
            {
                "schema": INDEX_SCHEMA,
                "vocabularies": {
                    "ICD10CM": {
                        "manifest": "ICD10CM/test/manifest.json",
                        "sha256": _sha(manifest),
                    }
                },
            }
        )
    )
    return root


# ------------------------------------------------------------------ unit tests


def test_stage_options_become_suMEDS_enrich_arguments() -> None:
    assert set(DEFAULT_CONFIG) == set(STAGE_OPTIONS)
    assert DEFAULT_CONFIG["strict"] is False
    assert {v for k, v in DEFAULT_CONFIG.items() if k != "strict"} == {None}

    assert stage_argv("in.parquet", "out.parquet", DEFAULT_CONFIG) == [
        "in.parquet",
        "-o",
        "out.parquet",
        "--soft-fail",
    ]
    options = {
        "rules": "eICU",
        "athena_csv": "/athena",
        "offline": True,
        "parent_codes": False,
        "child_depth": 2,
        "loinc_hierarchy": True,
        "strict": True,
    }
    assert stage_argv("c", "c", options) == [
        "c",
        "-o",
        "c",
        "--rules",
        "eICU",
        "--athena-csv",
        "/athena",
        "--offline",
        "--loinc-hierarchy",
        "--no-parent-codes",
        "--child-depth",
        "2",
        "--strict",
    ]
    for name, value, message in [
        ("offline", "yes", "must be true or false"),
        ("child_depth", True, "must be an integer"),
        ("rules", "", "must be a non-empty string"),
    ]:
        with pytest.raises(ValueError, match=f"option '{name}' {message}"):
            stage_argv("c", "c", {name: value})


def _meds_root(root: Path) -> Path:
    (root / "metadata").mkdir(parents=True)
    pl.DataFrame({"code": ["DX//I50.23", "DX//Z99.9"]}).write_parquet(
        root / "metadata" / "codes.parquet"
    )
    (root / "metadata" / "dataset.json").write_text('{"dataset_name": "SYNTH"}')
    return root


def _cfg(root: Path, worker: int = 0, **options) -> SimpleNamespace:
    metadata = str(root / "metadata")
    stage_cfg = {**DEFAULT_CONFIG, "verbose": False, **options}
    return SimpleNamespace(
        worker=worker,
        etl_metadata=SimpleNamespace(dataset_name="SYNTH"),
        stage_cfg=SimpleNamespace(
            **stage_cfg, metadata_input_dir=metadata, reducer_output_dir=metadata
        ),
    )


def _record(root: Path) -> dict | None:
    dataset = json.loads((root / "metadata" / "dataset.json").read_text())
    meds.DatasetMetadataSchema.validate(dataset)
    return dataset.get("sumeds_enrichment")


@pytest.fixture
def synth_rules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Register a rules file under the dataset name ``SYNTH``, in this process."""

    package = tmp_path / "site" / "synth_stage_rules"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "rules.yaml").write_text(RULES)
    monkeypatch.syspath_prepend(str(tmp_path / "site"))
    eps = [EntryPoint("SYNTH", "synth_stage_rules:rules.yaml", RULES_ENTRY_POINT_GROUP)]
    monkeypatch.setattr(
        sumeds.ontology,
        "entry_points",
        lambda group: [ep for ep in eps if ep.group == group],
    )
    return package / "rules.yaml"


def test_the_stage_function_enriches_in_place_with_the_dataset_rules(
    synth_rules: Path, packs: Path, tmp_path: Path
) -> None:
    root = _meds_root(tmp_path / "meds")
    before = (root / "metadata" / "codes.parquet").read_bytes()

    # Only worker 0 works.
    _main(_cfg(root, worker=1, ontology_source=str(packs)))
    assert (root / "metadata" / "codes.parquet").read_bytes() == before
    assert _record(root) is None

    _main(_cfg(root, ontology_source=str(packs)))
    record = _record(root)
    assert record["enriched"] is True
    assert record["rules"] == "SYNTH"  # the rules registered under the dataset name
    assert record["rules_sha256"] == _sha(synth_rules.read_bytes())
    codes = pl.read_parquet(root / "metadata" / "codes.parquet")
    assert codes.filter(pl.col("code") == "DX//I50.23")["concept_code"].to_list() == [
        "I50.23"
    ]


def test_the_stage_function_soft_fails_unless_strict(
    synth_rules: Path, tmp_path: Path, capsys
) -> None:
    root = _meds_root(tmp_path / "meds")
    before = (root / "metadata" / "codes.parquet").read_bytes()
    missing = str(tmp_path / "no-packs")

    with pytest.raises(SystemExit) as exc:
        _main(_cfg(root, ontology_source=missing, strict=True))
    assert exc.value.code == 1
    assert _record(root) is None

    _main(_cfg(root, ontology_source=missing))
    assert (root / "metadata" / "codes.parquet").read_bytes() == before
    record = _record(root)
    assert record["enriched"] is False
    assert record["error"] == (
        "OntologyPackError: ontology file not found: <PACKS_DIR>/index.json"
    )
    assert record["retry"] == "suMEDS-enrich <MEDS_ROOT> --ontology-source <PACKS_DIR>"
    assert (
        f"suMEDS-enrich {root} --ontology-source {missing}" in capsys.readouterr().err
    )


def test_without_rules_or_athena_the_dataset_name_is_tried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sumeds.ontology, "entry_points", lambda group: [])
    root = _meds_root(tmp_path / "meds")
    _main(_cfg(root))
    record = _record(root)
    assert record["enriched"] is False
    assert record["rules"] == "SYNTH"
    assert "no installed package registers rules under 'SYNTH'" in record["error"]


# ------------------------------------------------------------------ end to end


def _extension_stages_installed() -> bool:
    try:
        import MEDS_extract.config
    except ImportError:
        return False
    registered = {ep.name for ep in entry_points(group="MEDS_transforms.stages")}
    return hasattr(MEDS_extract.config, "ExtensionStage") and STAGE_NAME in registered


end_to_end = pytest.mark.skipif(
    not _extension_stages_installed(),
    reason="needs MEDS-Extract with extension stages (#320) and an installed suMEDS",
)

SPEC = """\
etl:
  dataset_name: SYNTH
  raw_dataset_version: "1.0"
  n_subjects_per_shard: 10
  split_fracs: {{train: 0.5, tuning: 0.25, held_out: 0.25}}
  post_stages:
    - {post_stage}

_defaults:
  subject_id: $patient_id

patients:
  dob:
    code: MEDS_BIRTH
    time: '$dob::"%Y-%m-%dT%H:%M:%S"'

diagnoses:
  dx:
    code: 'f"DX//{{$diagnosis_code}}"'
    time: '$timestamp::"%Y-%m-%dT%H:%M:%S"'
"""


class _Etl:
    """A synthetic raw dataset and spec, and the environment to run ``meds-extract-run`` in."""

    def __init__(self, root: Path, post_stage: str) -> None:
        self.raw = root / "raw"
        self.raw.mkdir()
        (self.raw / "patients.csv").write_text(
            "patient_id,dob\n1,1954-01-24T00:00:00\n2,2009-02-19T00:00:00\n"
            "3,1980-05-01T00:00:00\n4,1970-07-07T00:00:00\n"
        )
        (self.raw / "diagnoses.csv").write_text(
            "patient_id,diagnosis_code,timestamp\n1,E11.9,2024-04-03T00:00:00\n"
            "1,I10,2024-08-30T00:00:00\n2,I50.23,2024-01-01T00:00:00\n"
            "3,I50.23,2024-02-01T00:00:00\n4,Z99.9,2024-03-01T00:00:00\n"
        )
        self.spec = root / "messy.yaml"
        self.spec.write_text(SPEC.format(post_stage=post_stage))
        self.out = root / "meds"
        # The rules, registered under the dataset name by an installed package.
        site = root / "site"
        (site / "synth_etl").mkdir(parents=True)
        (site / "synth_etl" / "__init__.py").write_text("")
        (site / "synth_etl" / "sumeds_rules.yaml").write_text(RULES)
        dist_info = site / "synth_etl-0.0.0.dist-info"
        dist_info.mkdir()
        (dist_info / "METADATA").write_text(
            "Metadata-Version: 2.1\nName: synth-etl\nVersion: 0.0.0\n"
        )
        (dist_info / "entry_points.txt").write_text(
            f"[{RULES_ENTRY_POINT_GROUP}]\nSYNTH = synth_etl:sumeds_rules.yaml\n"
        )
        self.site = site
        self.env = {
            **os.environ,
            "PATH": os.pathsep.join(
                [str(Path(sys.executable).parent), os.environ["PATH"]]
            ),
            "PYTHONPATH": os.pathsep.join(
                filter(None, [str(site), os.getenv("PYTHONPATH")])
            ),
            "XDG_CACHE_HOME": str(root / "cache"),
        }

    def run(self, *args: str, ontology_source: Path) -> None:
        env = {**self.env, "SUMEDS_ONTOLOGY_SOURCE": str(ontology_source)}
        result = subprocess.run(
            [
                "meds-extract-run",
                f"spec={self.spec}",
                f"output_dir={self.out}",
                "do_download=false",
                f"input_dir={self.raw}",
                *args,
            ],
            capture_output=True,
            text=True,
            env=env,
            check=False,
            timeout=600,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    def stages(self) -> list[str]:
        import yaml

        pipeline = yaml.safe_load(
            (self.out / ".meds_extract_run" / "pipeline.yaml").read_text()
        )
        return [s if isinstance(s, str) else next(iter(s)) for s in pipeline["stages"]]

    def codes(self) -> pl.DataFrame:
        table = pq.read_table(self.out / "metadata" / "codes.parquet")
        meds.CodeMetadataSchema.validate(table)
        return pl.from_arrow(table).sort("code")


@end_to_end
def test_meds_extract_runs_the_declared_stage_once(packs: Path, tmp_path: Path) -> None:
    etl = _Etl(tmp_path, "sumeds_enrich: {rules: SYNTH}")
    etl.run(ontology_source=packs)
    assert etl.stages()[-2:] == ["finalize_MEDS_data", "sumeds_enrich"]

    record = _record(etl.out)
    assert record["enriched"] is True
    assert record["rules"] == "SYNTH"
    assert record["ontology_source"] == "local"
    assert record["coverage"] == {
        "codes": 5,
        "described": [0, 3],
        "with_parent_codes": [0, 3],
    }
    enriched = etl.codes()
    rows = {row["code"]: row for row in enriched.to_dicts()}
    assert rows["DX//I50.23"]["parent_codes"] == [
        "ICD10CM//I50.23",
        "ICD10CM//I50.2",
        "ICD10CM//I50",
    ]
    assert rows["DX//Z99.9"]["description"] is None
    dataset_json = (etl.out / "metadata" / "dataset.json").read_bytes()

    # A finished run runs nothing again.
    etl.run(ontology_source=packs)
    assert (etl.out / "metadata" / "dataset.json").read_bytes() == dataset_json

    # Re-running the stage on its own output changes nothing but the record's time.
    (etl.out / ".logs" / "sumeds_enrich.done").unlink()
    etl.run(ontology_source=packs)
    assert etl.codes().equals(enriched)
    record = _record(etl.out)
    assert record["enriched"] is True
    assert record["coverage"]["described"] == [3, 3]


@end_to_end
def test_a_skipped_or_failed_stage_leaves_the_cohort_for_suMEDS_enrich(
    packs: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    etl = _Etl(tmp_path, "sumeds_enrich")  # no options: the dataset name's rules
    missing = tmp_path / "no-packs"

    # skip_stages leaves the cohort as MEDS-Extract wrote it, with no record.
    etl.run("skip_stages=[sumeds_enrich]", ontology_source=packs)
    assert STAGE_NAME not in etl.stages()
    assert _record(etl.out) is None
    plain = (etl.out / "metadata" / "codes.parquet").read_bytes()
    assert "vocabulary_id" not in etl.codes().columns

    # Without packs, the same command without skip_stages runs only the stage, which
    # soft-fails: the run succeeds, the cohort is unchanged, and the record says why.
    etl.run(ontology_source=missing)
    assert (etl.out / ".logs" / "sumeds_enrich.done").exists()
    assert (etl.out / "metadata" / "codes.parquet").read_bytes() == plain
    record = _record(etl.out)
    assert record["enriched"] is False
    assert record["rules"] == "SYNTH"
    assert record["error"] == (
        "OntologyPackError: ontology file not found: <PACKS_DIR>/index.json"
    )
    assert record["retry"] == "suMEDS-enrich <MEDS_ROOT>"
    # The pipeline log has the stage's warning, with the command to finish it.
    log = (etl.out / ".logs" / "pipeline.log").read_text()
    assert (
        f"once the cause is fixed, enrich it with:\n    suMEDS-enrich {etl.out}\n"
        in log
    )

    # Once the packs are there, `suMEDS-enrich OUT` finishes it with the recorded options.
    monkeypatch.syspath_prepend(str(etl.site))
    monkeypatch.setenv("SUMEDS_ONTOLOGY_SOURCE", str(packs))
    assert enrich_main([str(etl.out), "--no-verbose"]) == 0
    record = _record(etl.out)
    assert record["enriched"] is True
    assert record["rules"] == "SYNTH"
    assert record["coverage"]["with_parent_codes"] == [0, 3]
    assert "vocabulary_id" in etl.codes().columns
