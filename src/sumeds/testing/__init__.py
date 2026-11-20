"""Test helpers for packages that use suMEDS: a local ontology source and output readers.

ETL packages that declare suMEDS's post-stages test their enrichment offline. They need a tiny
ontology source in the ``sumeds-ontologies/0.1`` layout of the published packs, a way to run the
installed console scripts, and readers for the ``codes.parquet`` and ``dataset.json`` the stages
write. This module provides them, so each ETL does not keep its own copy:

>>> import tempfile
>>> from pathlib import Path
>>> from sumeds import OntologyPacks
>>> root = Path(tempfile.mkdtemp())
>>> source = write_ontology_source(
...     root / "ontologies",
...     {"ICD10CM": {"I50": None, "I50.2": "I50"}, "LOINC": Pack(["2160-0"], parents=False)},
...     crosswalks={"demo": {"lab_loinc": [{"itemid": "1", "loinc_code": "2160-0"}]}},
... )
>>> client = OntologyPacks(str(source))
>>> entry = client.lookup("ICD10CM", {"I50.2"})["I50.2"]
>>> entry.description, entry.ancestors
('Synthetic ICD10CM title I50.2', ('I50',))
>>> client.crosswalk("demo/lab_loinc")
[{'itemid': '1', 'loinc_code': '2160-0'}]

The pytest contract tests that build on these helpers are in :mod:`sumeds.testing.contracts`,
which needs pytest. This module needs nothing beyond suMEDS's own dependencies.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import subprocess
import sys
import sysconfig
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from importlib.resources import files
from pathlib import Path
from typing import Any

import meds
import polars as pl
import pyarrow.parquet as pq
import yaml

from ..enrich import DATASET_RECORD_KEY
from ..ontology import (
    CROSSWALK_SCHEMA,
    INDEX_SCHEMA,
    PACK_SCHEMA,
)

__all__ = [
    "PIPELINE_ENTRY_POINT_GROUP",
    "POST_STAGE_ORDER",
    "Pack",
    "dataset_metadata",
    "enrichment_record",
    "load_spec",
    "names_map",
    "read_codes",
    "read_events",
    "registered_resource",
    "run_cli",
    "stage_name",
    "stage_overrides",
    "synthetic_title",
    "write_ontology_source",
]

#: Entry-point group in which an ETL package registers its MEDS-Extract spec.
PIPELINE_ENTRY_POINT_GROUP = "MEDS_extract.pipelines"
#: The suMEDS post-stages in the order a spec declares them: an ETL's own post-stages go between
#: ``sumeds_normalize_units`` and ``sumeds_enrich``.
POST_STAGE_ORDER = (
    "sumeds_link_unit_siblings",
    "sumeds_normalize_units",
    "sumeds_enrich",
)


def synthetic_title(vocabulary: str, code: str) -> str:
    """The made-up description a test pack gives ``code``: ``Synthetic {vocabulary} title {code}``."""

    return f"Synthetic {vocabulary} title {code}"


@dataclass(frozen=True)
class Pack:
    """One synthetic ontology pack.

    ``codes`` maps each code to its parent (``None`` for a top-level code), or lists the codes of a
    pack without parents. ``titles`` gives a code's description; any other code gets
    ``title(vocabulary, code)`` (see :func:`write_ontology_source`). ``parents=False`` writes no parent
    column, as the published LOINC pack, whose terms are leaves. ``fields`` renames the code,
    description and parent columns (the published LOINC pack keeps the LOINC Table's
    ``LOINC_NUM`` and ``LONG_COMMON_NAME``). ``extra`` adds manifest keys, such as
    ``description_path``.
    """

    codes: Mapping[str, str | None] | Iterable[str]
    release: str = "test"
    titles: Mapping[str, str] = field(default_factory=dict)
    parents: bool = True
    fields: Mapping[str, str] = field(default_factory=dict)
    shard_key_length: int = 1
    compress: bool = False
    extra: Mapping[str, Any] = field(default_factory=dict)

    def rows(self) -> dict[str, str | None]:
        """The pack's codes, each with its parent."""

        if isinstance(self.codes, Mapping):
            return dict(self.codes)
        return dict.fromkeys(self.codes)


PackLike = Pack | Mapping[str, str | None] | Iterable[str] | tuple[str, Any]
CrosswalkRows = Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]]


def _pack(value: PackLike) -> Pack:
    """A :class:`Pack`, from a pack, a ``(release, codes)`` pair, or the codes alone."""

    if isinstance(value, Pack):
        return value
    if (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[0], str)
        and not isinstance(value[1], str)
    ):
        return Pack(value[1], release=value[0])
    return Pack(value)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _csv(rows: Iterable[Sequence[Any]]) -> bytes:
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerows(
        ["" if value is None else value for value in row] for row in rows
    )
    return buffer.getvalue().encode("utf-8")


def _write_pack(
    root: Path, vocabulary: str, pack: Pack, title: Callable[[str, str], str]
) -> dict:
    fields = {
        "code": "code",
        "description": "description",
        "parent": "parent",
        **pack.fields,
    }
    columns = [fields["code"], fields["description"]] + (
        [fields["parent"]] if pack.parents else []
    )
    rows = pack.rows()
    if not pack.parents and any(rows.values()):
        raise ValueError(
            f"{vocabulary}: a pack without a parent column cannot give parents"
        )
    directory = root / vocabulary / pack.release
    directory.mkdir(parents=True, exist_ok=True)
    by_shard: dict[str, list[list[Any]]] = {}
    for code, parent in rows.items():
        description = pack.titles.get(code, title(vocabulary, code))
        row = [code, description] + ([parent] if pack.parents else [])
        by_shard.setdefault(code[: pack.shard_key_length], []).append(row)
    shards = {}
    for key, shard_rows in sorted(by_shard.items()):
        data = _csv([columns, *shard_rows])
        name = f"{key}.csv"
        if pack.compress:
            data, name = gzip.compress(data, mtime=0), f"{name}.gz"
        (directory / name).write_bytes(data)
        shards[key] = {
            "file": name,
            "rows": len(shard_rows),
            "bytes": len(data),
            "sha256": _sha256(data),
        }
    manifest = {
        "schema": PACK_SCHEMA,
        "vocabulary": vocabulary,
        "release": pack.release,
        "columns": columns,
        "fields": {**fields, "parent": fields["parent"] if pack.parents else None},
        "shard_key_length": pack.shard_key_length,
        "rows": len(rows),
        "bytes": sum(shard["bytes"] for shard in shards.values()),
        **pack.extra,
        "shards": shards,
    }
    text = json.dumps(manifest).encode()
    (directory / "manifest.json").write_bytes(text)
    return {
        "manifest": f"{vocabulary}/{pack.release}/manifest.json",
        "sha256": _sha256(text),
        "release": pack.release,
        "rows": manifest["rows"],
        "bytes": manifest["bytes"],
    }


def _table_rows(name: str, rows: CrosswalkRows) -> list[Sequence[Any]]:
    """A crosswalk table as a header and rows, from mappings or from a header row and rows."""

    rows = list(rows)
    if not rows:
        raise ValueError(f"crosswalk table {name!r} needs at least a header")
    if isinstance(rows[0], Mapping):
        header = list(dict.fromkeys(key for row in rows for key in row))
        return [header, *([row.get(column) for column in header] for row in rows)]
    return rows


def _write_crosswalk(
    root: Path, dataset: str, tables: Mapping[str, CrosswalkRows]
) -> dict:
    directory = root / "crosswalks" / dataset
    directory.mkdir(parents=True, exist_ok=True)
    entries = {}
    for name, rows in tables.items():
        table = _table_rows(name, rows)
        data = _csv(table)
        (directory / f"{name}.csv").write_bytes(data)
        entries[name] = {
            "file": f"{name}.csv",
            "rows": len(table) - 1,
            "sha256": _sha256(data),
        }
    text = json.dumps(
        {"schema": CROSSWALK_SCHEMA, "dataset": dataset, "tables": entries}
    ).encode()
    (directory / "manifest.json").write_bytes(text)
    return {"manifest": f"crosswalks/{dataset}/manifest.json", "sha256": _sha256(text)}


def write_ontology_source(
    root: str | Path,
    packs: Mapping[str, PackLike] | None = None,
    *,
    crosswalks: Mapping[str, Mapping[str, CrosswalkRows]] | None = None,
    title: Callable[[str, str], str] = synthetic_title,
) -> Path:
    """Write a local ontology source in the ``sumeds-ontologies/0.1`` layout; return ``root``.

    ``packs`` maps a vocabulary to a :class:`Pack`, to a ``(release, codes)`` pair, or to the codes
    alone (release ``test``), where codes map each code to its parent or list codes without one.
    ``crosswalks`` maps a dataset to its tables, each a list of row mappings or a header row
    followed by rows; a rule reads one as ``table: <dataset>/<name>``. Every file is listed with
    its sha256 checksum, as in the published packs, so suMEDS verifies them as it does those.

    Pass the result to the ``sumeds_enrich`` stage as ``ontology_source`` (see
    :func:`stage_overrides`), to ``suMEDS-enrich --ontology-source``, or to
    ``EnrichmentConfig(ontology_source=...)``. Without packs and crosswalks it writes an empty
    source: string hierarchy rules still run, and nothing is read over the network.
    """

    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    index: dict[str, Any] = {
        "schema": INDEX_SCHEMA,
        "vocabularies": {
            vocabulary: _write_pack(root, vocabulary, _pack(pack), title)
            for vocabulary, pack in (packs or {}).items()
        },
    }
    if crosswalks:
        index["crosswalks"] = {
            dataset: _write_crosswalk(root, dataset, tables)
            for dataset, tables in crosswalks.items()
        }
    (root / "index.json").write_text(json.dumps(index))
    return root


# ------------------------------------------------------------------ running and reading


def stage_overrides(**options: object) -> str:
    """A ``meds-extract-run`` argument that sets stage options for one run.

    ``meds-extract-run`` passes ``overrides`` to every stage of the pipeline, so each option is
    added with ``++`` (set where a stage declares it, added where it does not):

    >>> stage_overrides(ontology_source="/packs", strict="true")
    "overrides=['++stage_cfg.ontology_source=/packs','++stage_cfg.strict=true']"
    """

    values = ",".join(
        f"'++stage_cfg.{name}={value}'" for name, value in options.items()
    )
    return f"overrides=[{values}]"


def run_cli(
    command: str,
    *args: str | Path,
    env: Mapping[str, str] | None = None,
    ok: bool = True,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run an installed console script of this interpreter's environment and return the result.

    The script is taken from the environment pytest runs in, not from the first match on
    ``PATH``, and that environment's script directory leads the child's ``PATH``, so
    ``meds-extract-run`` also spawns its stages from it. ``env`` adds variables. The command must
    succeed, or with ``ok=False`` fail; otherwise an ``AssertionError`` shows its output.
    """

    scripts = Path(sysconfig.get_path("scripts"))
    executable = scripts / command
    if not executable.exists() and not executable.with_suffix(".exe").exists():
        executable = Path(sys.executable).parent / command
    child_env = {
        **os.environ,
        "PATH": os.pathsep.join([str(scripts), os.environ.get("PATH", "")]),
        **(env or {}),
    }
    argv = [str(executable), *map(str, args)]
    result = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        env=child_env,
        timeout=timeout,
        check=False,
    )
    assert (result.returncode == 0) is ok, (
        f"{' '.join(argv)} exited with code {result.returncode} "
        f"(expected {'success' if ok else 'failure'}).\n"
        f"stdout:\n{result.stdout[-4000:]}\nstderr:\n{result.stderr[-4000:]}"
    )
    return result


def names_map(value: Any) -> dict[str, str]:
    """A names column value (``parent_names``) as a dict, however it was read.

    suMEDS writes it as a Parquet ``map<string, string>``: Polars 2 reads that as a dict, Polars 1
    as a list of key/value structs, and pyarrow as a list of pairs.

    >>> names_map([("ICD10CM//I50", "Heart failure")])
    {'ICD10CM//I50': 'Heart failure'}
    >>> names_map(None)
    {}
    """

    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    return dict(
        (entry["key"], entry["value"]) if isinstance(entry, Mapping) else tuple(entry)
        for entry in value
    )


def read_codes(output_dir: str | Path) -> dict[str, dict[str, Any]]:
    """``metadata/codes.parquet`` of a MEDS root as rows by code; names maps become dicts."""

    table = pq.read_table(Path(output_dir) / "metadata" / "codes.parquet")
    rows = {}
    for row in table.to_pylist():
        for column in [c for c in row if c.endswith("_names")]:
            row[column] = names_map(row[column])
        rows[row["code"]] = row
    return rows


def read_events(output_dir: str | Path) -> pl.DataFrame:
    """Every data shard of a MEDS root, concatenated."""

    shards = sorted((Path(output_dir) / "data").rglob("*.parquet"))
    return pl.concat([pl.read_parquet(fp) for fp in shards], how="diagonal_relaxed")


def dataset_metadata(output_dir: str | Path) -> dict[str, Any]:
    """``metadata/dataset.json`` of a MEDS root, checked to be valid MEDS dataset metadata."""

    metadata = json.loads((Path(output_dir) / "metadata" / "dataset.json").read_text())
    meds.DatasetMetadataSchema.validate(metadata)
    return metadata


def enrichment_record(output_dir: str | Path) -> dict[str, Any] | None:
    """The record ``sumeds_enrich`` and ``suMEDS-enrich`` keep in ``dataset.json``, or ``None``."""

    return dataset_metadata(output_dir).get(DATASET_RECORD_KEY)


def stage_name(stage: str | Mapping[str, Any]) -> str:
    """The name of a spec's stage entry: ``name`` or ``{name: {options}}``."""

    return stage if isinstance(stage, str) else next(iter(stage))


def registered_resource(group: str, name: str) -> Path:
    """The file an installed package registers under ``name`` in entry-point ``group``.

    Both ``MEDS_extract.pipelines`` (a spec) and ``sumeds.rules`` (a rules file) register a file as
    ``package.module:relative/path``.
    """

    for entry_point in entry_points(group=group):
        if entry_point.name == name:
            module, _, resource = entry_point.value.partition(":")
            return Path(str(files(module).joinpath(resource)))
    raise LookupError(
        f"no installed package registers {name!r} in entry-point group {group!r}"
    )


def load_spec(name: str) -> dict[str, Any]:
    """The MEDS-Extract spec an installed package registers under ``name``."""

    return yaml.safe_load(
        registered_resource(PIPELINE_ENTRY_POINT_GROUP, name).read_text()
    )
