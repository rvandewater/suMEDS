"""Registered rules (the ``sumeds.rules`` entry-point group) and ``suMEDS-extract``.

A throwaway package on ``sys.path`` holds a rules file and the entry-point lookup is patched to
register it, so no distribution is installed. ``meds-extract-run`` is a fake on ``PATH`` that
records its arguments and writes a two-code ``metadata/codes.parquet`` and a
``metadata/dataset.json``. The rules are hierarchy rules over an empty ontology source, so
nothing is read over the network.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import sys
import urllib.error
from importlib.metadata import EntryPoint
from pathlib import Path

import meds
import polars as pl
import pytest

import sumeds.ontology
from sumeds import SummaryConfig, load_rules, registered_rules
from sumeds.cli import enrich_main
from sumeds.extract import main as extract_main
from sumeds.ontology import INDEX_SCHEMA, RULES_ENTRY_POINT_GROUP

RULES = (
    "schema: sumeds-rules/0.1\n"
    "hierarchy:\n"
    "  - {id: paths, pattern: '^(?P<prefix>DX//)(?P<path>.+)$', group: path, separator: '|'}\n"
)
# Validates DX//A against a pack, so enrichment needs the ontology packs.
PACKED_RULES = (
    "schema: sumeds-rules/0.1\n"
    "ontology:\n"
    "  - {id: icd, pattern: '^DX//(?P<code>A)$', vocabulary: ICD10CM}\n" + RULES[25:]
)

FAKE_MEDS_EXTRACT_RUN = """\
import json, os, sys
from pathlib import Path
import polars as pl

args = sys.argv[1:]
# Like Hydra, read output_dir=... with or without quotes.
out = Path(next(a.split("=", 1)[1] for a in args if a.startswith("output_dir=")).strip("'\\""))
out.mkdir(parents=True, exist_ok=True)
(out / "argv.json").write_text(json.dumps(args))
returncode = int(os.environ.get("FAKE_RETURNCODE", "0"))
if returncode == 0:
    (out / "metadata").mkdir(exist_ok=True)
    pl.DataFrame({"code": ["DX//A|B", "DX//A"], "description": [None, "A"]}).write_parquet(
        out / "metadata" / "codes.parquet"
    )
    # As MEDS-Extract's finalize stage writes it: compact JSON.
    (out / "metadata" / "dataset.json").write_text(json.dumps(
        {"dataset_name": "DEMO", "etl_name": "MEDS_extract", "meds_version": "0.4.1"}
    ))
sys.exit(returncode)
"""


@pytest.fixture
def registered(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Register ``demo_etl:sumeds_rules.yaml`` as ``DEMO``; return the rules file."""

    package = tmp_path / "site" / "demo_etl"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    rules = package / "sumeds_rules.yaml"
    rules.write_text(RULES)
    (package / "packed.yaml").write_text(PACKED_RULES)
    (package / "broken.yaml").write_text("schema: nope\n")
    monkeypatch.syspath_prepend(str(tmp_path / "site"))
    eps = [
        EntryPoint("DEMO", "demo_etl:sumeds_rules.yaml", RULES_ENTRY_POINT_GROUP),
        EntryPoint("NO-FILE", "demo_etl:missing.yaml", RULES_ENTRY_POINT_GROUP),
        EntryPoint("BAD-VALUE", "demo_etl", RULES_ENTRY_POINT_GROUP),
        EntryPoint("BROKEN", "demo_etl:broken.yaml", RULES_ENTRY_POINT_GROUP),
        EntryPoint("PACKED", "demo_etl:packed.yaml", RULES_ENTRY_POINT_GROUP),
    ]
    monkeypatch.setattr(
        sumeds.ontology,
        "entry_points",
        lambda group: [ep for ep in eps if ep.group == group],
    )
    return rules


@pytest.fixture
def empty_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An ontology source with no vocabularies, used for every enrichment here."""

    source = tmp_path / "ontologies"
    source.mkdir()
    (source / "index.json").write_text(
        json.dumps({"schema": INDEX_SCHEMA, "vocabularies": {}})
    )
    monkeypatch.setenv("SUMEDS_ONTOLOGY_SOURCE", str(source))
    return source


@pytest.fixture
def fake_meds_extract_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    script = bin_dir / "meds-extract-run"
    script.write_text(f"#!{sys.executable}\n{FAKE_MEDS_EXTRACT_RUN}")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")


def _codes(output_dir: Path) -> dict[str, dict]:
    table = pl.read_parquet(output_dir / "metadata" / "codes.parquet")
    return {row["code"]: row for row in table.to_dicts()}


def test_registered_rules_load_by_name(registered: Path) -> None:
    assert registered_rules()["DEMO"] == "demo_etl:sumeds_rules.yaml"
    assert load_rules("DEMO") == load_rules(registered)
    assert load_rules(Path("DEMO")) == load_rules(registered)  # the CLI passes a Path

    with pytest.raises(FileNotFoundError, match=r"rules file not found: .*UNKNOWN"):
        load_rules("UNKNOWN")
    with pytest.raises(FileNotFoundError, match="has no file 'missing.yaml'"):
        load_rules("NO-FILE")
    with pytest.raises(ValueError, match="<package.module>:<filename.yaml>"):
        load_rules("BAD-VALUE")


def test_yaml_config_keeps_a_registered_name(
    registered: Path, empty_source: Path, tmp_path: Path
) -> None:
    config_dir = tmp_path / "conf"
    config_dir.mkdir()
    path = config_dir / "summary.yaml"
    path.write_text("enrichment:\n  rules: DEMO\n")
    assert SummaryConfig.from_yaml(path).enrichment.rules == load_rules(registered)

    # A path is still resolved against the YAML file.
    (config_dir / "local.yaml").write_text(RULES)
    path.write_text("enrichment:\n  rules: local.yaml\n")
    assert SummaryConfig.from_yaml(path).enrichment.rules == load_rules(registered)


def test_enrich_cli_takes_a_registered_name(
    registered: Path, empty_source: Path, tmp_path: Path
) -> None:
    source = tmp_path / "codes.csv"
    source.write_text("code,description\nDX//A|B,\nDX//A,A\n")
    output = tmp_path / "out.parquet"
    assert enrich_main([str(source), "-o", str(output), "--rules", "DEMO"]) == 0
    rows = {row["code"]: row for row in pl.read_parquet(output).to_dicts()}
    assert rows["DX//A|B"]["parent_codes"] == ["DX//A"]


def test_extract_runs_the_pipeline_then_enriches_in_place(
    registered: Path, empty_source: Path, fake_meds_extract_run: None, tmp_path: Path
) -> None:
    out = tmp_path / "meds"
    overrides = ["spec=DEMO", f"output_dir={out}", "do_download=false"]
    assert extract_main([*overrides, "--", "--no-verbose"]) == 0

    # Everything before the `--` reaches meds-extract-run unchanged.
    assert json.loads((out / "argv.json").read_text()) == overrides
    # The rules registered under the spec's name enriched the table in place.
    codes = _codes(out)
    assert codes["DX//A|B"]["parent_codes"] == ["DX//A"]
    assert codes["DX//A"]["description"] == "A"
    assert sorted(p.name for p in (out / "metadata").iterdir()) == [
        "codes.parquet",
        "dataset.json",
    ]

    # dataset.json records the enrichment and still validates.
    dataset = json.loads((out / "metadata" / "dataset.json").read_text())
    meds.DatasetMetadataSchema.validate(dataset)
    assert list(dataset) == [
        "dataset_name",
        "etl_name",
        "meds_version",
        "sumeds_enrichment",
    ]
    record = dataset["sumeds_enrichment"]
    assert record["enriched"] is True
    assert record["rules"] == "DEMO"
    assert record["rules_sha256"] == hashlib.sha256(registered.read_bytes()).hexdigest()
    assert record["ontology_source"] == "local"
    assert record["athena"] is False
    assert record["coverage"] == {
        "codes": 2,
        "described": [1, 1],
        "with_parent_codes": [0, 1],
    }

    # Running again on the same output replaces the record.
    rules = tmp_path / "rules.yaml"
    rules.write_text(
        RULES
        + "  - {id: again, pattern: '^(?P<path>.+)$', group: path, separator: '|'}\n"
    )
    assert extract_main([*overrides, "--", "--no-verbose", "--rules", str(rules)]) == 0
    dataset = json.loads((out / "metadata" / "dataset.json").read_text())
    assert list(dataset)[-1] == "sumeds_enrichment"
    assert dataset["sumeds_enrichment"]["rules"] == "rules.yaml"
    assert dataset["sumeds_enrichment"]["rules_sha256"] == (
        hashlib.sha256(rules.read_bytes()).hexdigest()
    )


def test_extract_takes_explicit_rules_for_any_spec(
    registered: Path, empty_source: Path, fake_meds_extract_run: None, tmp_path: Path
) -> None:
    rules = tmp_path / "rules.yaml"
    rules.write_text(RULES)
    out = tmp_path / "meds"
    argv = [
        "spec=/path/to/messy.yaml",
        f"output_dir='{out}'",
        "--",
        "--rules",
        str(rules),
    ]
    assert extract_main(argv) == 0
    assert _codes(out)["DX//A|B"]["parent_codes"] == ["DX//A"]


@pytest.mark.parametrize(
    "argv, message",
    [
        (["output_dir=OUT"], "spec=... and output_dir=... are required"),
        (["spec=DEMO"], "spec=... and output_dir=... are required"),
        (["spec=DEMO", "spec=X", "output_dir=OUT"], "spec= is given 2 times"),
        (["spec=UNREGISTERED", "output_dir=OUT"], "no suMEDS rules are registered"),
        (["spec=BROKEN", "output_dir=OUT"], "'schema' must be"),
        (["spec=DEMO", "output_dir=OUT", "--offline"], "go after a `--`"),
        (["spec=DEMO", "output_dir=OUT", "--", "--no-such-option"], "unrecognized"),
    ],
)
def test_extract_rejects_bad_arguments_before_running_the_pipeline(
    registered: Path,
    fake_meds_extract_run: None,
    tmp_path: Path,
    capsys,
    argv: list[str],
    message: str,
) -> None:
    out = tmp_path / "meds"
    argv = [a.replace("OUT", str(out)) for a in argv]
    with pytest.raises(SystemExit) as exc:
        extract_main(argv)
    assert exc.value.code == 2
    assert message in capsys.readouterr().err
    assert not out.exists(), "meds-extract-run must not run"


def test_extract_returns_the_pipeline_failure_without_enriching(
    registered: Path,
    fake_meds_extract_run: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    monkeypatch.setenv("FAKE_RETURNCODE", "3")
    out = tmp_path / "meds"
    assert extract_main(["spec=DEMO", f"output_dir={out}"]) == 3
    assert "nothing was enriched" in capsys.readouterr().err
    assert not (out / "metadata").exists()


def test_extract_needs_meds_extract_run_on_path(
    registered: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setenv("PATH", str(tmp_path / "empty-bin"))
    with pytest.raises(SystemExit) as exc:
        extract_main(["spec=DEMO", f"output_dir={tmp_path / 'meds'}"])
    assert exc.value.code == 1
    assert "meds-extract-run is not on PATH" in capsys.readouterr().err


class _Network:
    """``urlopen`` for the default pack repository: down, or serving a one-code pack."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.up = False
        shard = b"code,description,parent\nA,Alpha,\n"
        manifest = json.dumps(
            {
                "schema": "sumeds-ontology-pack/0.1",
                "vocabulary": "ICD10CM",
                "release": "test",
                "shard_key_length": 1,
                "shards": {"A": {"file": "A.csv", "sha256": _sha(shard)}},
            }
        ).encode()
        index = json.dumps(
            {
                "schema": INDEX_SCHEMA,
                "vocabularies": {
                    "ICD10CM": {
                        "manifest": "ICD10CM/test/manifest.json",
                        "sha256": _sha(manifest),
                    }
                },
            }
        ).encode()
        self.files = {
            "index.json": index,
            "ICD10CM/test/manifest.json": manifest,
            "ICD10CM/test/A.csv": shard,
        }

    def __call__(self, request, timeout=None):
        if not self.up:
            raise urllib.error.URLError("no network")
        return io.BytesIO(self.files[request.full_url.split("/ontologies/", 1)[1]])


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def network(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Network:
    """The default pack source behind a switchable network, with an empty cache."""

    network = _Network(tmp_path)
    monkeypatch.delenv("SUMEDS_ONTOLOGY_SOURCE", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    monkeypatch.setattr(sumeds.ontology.urllib.request, "urlopen", network)
    monkeypatch.setattr(
        sumeds.ontology, "DEFAULT_INDEX_SHA256", _sha(network.files["index.json"])
    )
    return network


def _record(output_dir: Path) -> dict:
    dataset = json.loads((output_dir / "metadata" / "dataset.json").read_text())
    meds.DatasetMetadataSchema.validate(dataset)
    return dataset["sumeds_enrichment"]


DEFAULT_OPTIONS = {
    "parent_codes": True,
    "child_codes": False,
    "sibling_codes": False,
    "child_depth": 3,
    "exclude_self_parent_code": False,
    "packs": True,
    "loinc_hierarchy": False,
}


def test_a_failed_enrichment_keeps_the_etl_and_suMEDS_enrich_root_completes_it(
    registered: Path,
    network: _Network,
    fake_meds_extract_run: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    out = tmp_path / "meds"
    # Without network the ETL still succeeds ...
    assert (
        extract_main(["spec=PACKED", f"output_dir={out}", "--", "--child-codes"]) == 0
    )
    # ... with codes.parquet as the pipeline wrote it ...
    assert pl.read_parquet(out / "metadata" / "codes.parquet").to_dicts() == [
        {"code": "DX//A|B", "description": None},
        {"code": "DX//A", "description": "A"},
    ]
    assert sorted(p.name for p in (out / "metadata").iterdir()) == [
        "codes.parquet",
        "dataset.json",
    ]
    # ... a warning that says how to enrich it later ...
    err = capsys.readouterr().err
    assert "enrichment failed" in err and "no network" in err
    assert f"    suMEDS-enrich {out}\n" in err
    # ... and a record of what failed, with everything needed to enrich it again.
    record = _record(out)
    assert list(record) == [
        "enriched",
        "sumeds_version",
        "rules",
        "rules_sha256",
        "ontology_source",
        "athena",
        "options",
        "attempted_at",
        "error",
        "retry",
    ]
    packed = registered.with_name("packed.yaml")
    assert record["enriched"] is False
    assert record["rules"] == "PACKED"
    assert record["rules_sha256"] == _sha(packed.read_bytes())
    assert record["ontology_source"] == "rvandewater/sumeds-ontologies@v0.2.0"
    assert record["athena"] is False
    assert record["options"] == {**DEFAULT_OPTIONS, "child_codes": True}
    assert record["error"].startswith("OntologyPackError: could not download")
    assert record["retry"] == "suMEDS-enrich <MEDS_ROOT>"

    # Once the network is back, the dataset root is all suMEDS-enrich needs.
    network.up = True
    assert enrich_main([str(out), "--no-verbose"]) == 0
    record = _record(out)
    assert record["enriched"] is True
    assert {
        key: record[key] for key in ("rules", "rules_sha256", "ontology_source")
    } == {
        "rules": "PACKED",
        "rules_sha256": _sha(packed.read_bytes()),
        "ontology_source": "rvandewater/sumeds-ontologies@v0.2.0",
    }
    assert record["options"] == {**DEFAULT_OPTIONS, "child_codes": True}
    assert record["coverage"] == {
        "codes": 2,
        "described": [1, 1],
        "with_parent_codes": [0, 2],
    }
    codes = _codes(out)
    assert codes["DX//A"]["vocabulary_id"] == "ICD10CM"
    assert codes["DX//A"]["parent_codes"] == ["ICD10CM//A"]
    assert codes["DX//A|B"]["parent_codes"] == ["DX//A"]

    # A later failure keeps the record of the enrichment codes.parquet still holds.
    network.up = False
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "empty-cache"))
    capsys.readouterr()
    codes = out / "metadata" / "codes.parquet"
    before = codes.read_bytes()
    argv = [str(codes), "-o", str(codes), "--rules", "PACKED", "--soft-fail"]
    assert enrich_main(argv) == 0
    assert "recorded as not enriched" not in capsys.readouterr().err
    assert _record(out)["enriched"] is True
    assert codes.read_bytes() == before


def test_strict_restores_a_hard_failure(
    registered: Path, network: _Network, fake_meds_extract_run: None, tmp_path: Path
) -> None:
    out = tmp_path / "meds"
    with pytest.raises(SystemExit) as exc:
        extract_main(["spec=PACKED", f"output_dir={out}", "--", "--strict"])
    assert exc.value.code == 1
    assert "sumeds_enrichment" not in (out / "metadata" / "dataset.json").read_text()
    assert pl.read_parquet(out / "metadata" / "codes.parquet").columns == [
        "code",
        "description",
    ]

    # A plain suMEDS-enrich fails loudly too, and records nothing.
    codes = str(out / "metadata" / "codes.parquet")
    with pytest.raises(SystemExit) as exc:
        enrich_main([codes, "-o", codes, "--rules", "PACKED"])
    assert exc.value.code == 1
    assert "sumeds_enrichment" not in (out / "metadata" / "dataset.json").read_text()


def test_custom_runners_soft_fail_on_broken_rules(
    registered: Path, tmp_path: Path, capsys
) -> None:
    out = tmp_path / "meds"
    (out / "metadata").mkdir(parents=True)
    codes = out / "metadata" / "codes.parquet"
    pl.DataFrame({"code": ["DX//A|B", "DX//A"]}).write_parquet(codes)
    (out / "metadata" / "dataset.json").write_text('{"dataset_name": "DEMO"}')
    before = codes.read_bytes()
    argv = [str(codes), "-o", str(codes), "--rules", "BROKEN", "--soft-fail"]
    assert enrich_main(argv) == 0
    assert codes.read_bytes() == before
    record = _record(out)
    assert record["enriched"] is False
    assert record["rules"] == "BROKEN"
    assert record["rules_sha256"] == _sha(
        registered.with_name("broken.yaml").read_bytes()
    )
    assert record["error"].startswith("ValueError: ")
    assert "'schema' must be" in record["error"]
    assert f"suMEDS-enrich {out}" in capsys.readouterr().err

    # A rules file that is not registered must be given again, and the record says so.
    rules = tmp_path / "local.yaml"
    rules.write_text("schema: nope\n")
    argv = [str(codes), "-o", str(codes), "--rules", str(rules), "--soft-fail"]
    assert enrich_main(argv) == 0
    record = _record(out)
    assert record["retry"] == "suMEDS-enrich <MEDS_ROOT> --rules <RULES_FILE>"
    assert record["error"].startswith("ValueError: <RULES_FILE>: 'schema' must be")
    assert f"suMEDS-enrich {out} --rules {rules}" in capsys.readouterr().err

    # The record never holds the location of an Athena download.
    athena = tmp_path / "athena-download"
    argv = [str(codes), "-o", str(codes), "--athena-csv", str(athena), "--soft-fail"]
    assert enrich_main(argv) == 0
    record = _record(out)
    assert record["athena"] is True and record["rules"] is None
    assert record["error"] == (
        "FileNotFoundError: Athena file not found: <ATHENA_DIR>/CONCEPT.csv"
    )
    assert record["retry"] == "suMEDS-enrich <MEDS_ROOT> --athena-csv <ATHENA_DIR>"
    assert str(athena) not in (out / "metadata" / "dataset.json").read_text()
    assert codes.read_bytes() == before


def _write_record(out: Path, **record) -> None:
    path = out / "metadata" / "dataset.json"
    dataset = json.loads(path.read_text())
    dataset["sumeds_enrichment"] = {"enriched": False, **record}
    path.write_text(json.dumps(dataset))


@pytest.mark.parametrize(
    "record, message",
    [
        ({}, "no sumeds_enrichment record to reuse"),
        ({"rules": "my_rules.yaml"}, "pass the rules file with --rules FILE"),
        ({"rules": "DEMO", "athena": True}, "pass --athena-csv DIR"),
        ({"rules": "DEMO", "ontology_source": "local"}, "pass --ontology-source DIR"),
    ],
)
def test_suMEDS_enrich_root_asks_for_what_the_record_cannot_hold(
    registered: Path,
    empty_source: Path,
    fake_meds_extract_run: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
    record: dict,
    message: str,
) -> None:
    out = tmp_path / "meds"
    assert extract_main(["spec=DEMO", f"output_dir={out}"]) == 0
    if record:
        _write_record(out, **record)
    else:
        (out / "metadata" / "dataset.json").write_text("{}")
    monkeypatch.delenv("SUMEDS_ONTOLOGY_SOURCE")
    capsys.readouterr()
    with pytest.raises(SystemExit) as exc:
        enrich_main([str(out)])
    assert exc.value.code == 2
    assert message in capsys.readouterr().err


def test_suMEDS_enrich_root_flags_override_the_record(
    registered: Path, empty_source: Path, fake_meds_extract_run: None, tmp_path: Path
) -> None:
    out = tmp_path / "meds"
    assert extract_main(["spec=DEMO", f"output_dir={out}", "--", "--no-verbose"]) == 0
    _write_record(
        out, rules="DEMO", ontology_source="local", options={"child_depth": 7}
    )
    rules = tmp_path / "rules.yaml"
    rules.write_text(RULES)
    argv = [str(out), "--rules", str(rules), "--ontology-source", str(empty_source)]
    assert enrich_main([*argv, "--no-parent-codes", "--no-verbose"]) == 0
    record = _record(out)
    assert record["enriched"] is True
    assert record["rules"] == "rules.yaml"
    assert record["ontology_source"] == "local"
    assert record["options"] == {
        **DEFAULT_OPTIONS,
        "parent_codes": False,
        "child_depth": 7,
    }


@pytest.mark.parametrize(
    "argv, message",
    [
        (["ROOT", "-o", "out.parquet"], "a MEDS dataset root is enriched in place"),
        (["ROOT/metadata", "--rules", "DEMO"], "not a MEDS dataset root"),
        (["ROOT/metadata/codes.parquet"], "-o/--output"),
    ],
)
def test_suMEDS_enrich_root_arguments(
    registered: Path,
    fake_meds_extract_run: None,
    tmp_path: Path,
    capsys,
    argv: list[str],
    message: str,
) -> None:
    out = tmp_path / "meds"
    assert extract_main(["spec=DEMO", f"output_dir={out}", "--", "--strict"]) == 0
    with pytest.raises(SystemExit) as exc:
        enrich_main([a.replace("ROOT", str(out)) for a in argv])
    assert exc.value.code == 2
    assert message in capsys.readouterr().err
