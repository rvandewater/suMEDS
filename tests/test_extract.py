"""Registered rules (the ``sumeds.rules`` entry-point group) and ``suMEDS-extract``.

A throwaway package on ``sys.path`` holds a rules file and the entry-point lookup is patched to
register it, so no distribution is installed. ``meds-extract-run`` is a fake on ``PATH`` that
records its arguments and writes a two-code ``metadata/codes.parquet``. The rules are hierarchy
rules over an empty ontology source, so nothing is read over the network.
"""

from __future__ import annotations

import json
import os
import sys
from importlib.metadata import EntryPoint
from pathlib import Path

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
    (package / "broken.yaml").write_text("schema: nope\n")
    monkeypatch.syspath_prepend(str(tmp_path / "site"))
    eps = [
        EntryPoint("DEMO", "demo_etl:sumeds_rules.yaml", RULES_ENTRY_POINT_GROUP),
        EntryPoint("NO-FILE", "demo_etl:missing.yaml", RULES_ENTRY_POINT_GROUP),
        EntryPoint("BAD-VALUE", "demo_etl", RULES_ENTRY_POINT_GROUP),
        EntryPoint("BROKEN", "demo_etl:broken.yaml", RULES_ENTRY_POINT_GROUP),
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
    assert sorted(p.name for p in (out / "metadata").iterdir()) == ["codes.parquet"]


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
