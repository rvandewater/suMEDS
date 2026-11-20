"""Contract tests for an ETL package that enriches its output with suMEDS.

Every ETL that declares suMEDS's post-stages makes the same promises: its rules file is packaged,
registered under the spec's name and loads in suMEDS; its spec runs the suMEDS post-stages in the
agreed order; and the enrichment only adds metadata, records itself in ``dataset.json``,
soft-fails without packs, can be retried with ``suMEDS-enrich``, and fails the run under
``strict``. These classes state those promises once. An ETL subclasses them in its test suite and
sets class attributes for what differs: the rules name, its synthetic packs, and the codes and
parents it expects.

A third promise is about the codes themselves: a code whose last component is a unit carries that
unit stripped of surrounding whitespace, and ``UNK`` where the row records none or a blank one,
while the ``unit`` metadata stays the raw column. ``UnitComponentContract`` finds those codes in
the spec and evaluates them with dftly, as MEDS-Extract does; the subclass names only the spec.

.. code-block:: python

    from sumeds.testing.contracts import EnrichmentContract, RulesContract, UnitComponentContract

    class TestRules(RulesContract):
        rules_name = "ALOTT"
        expected_ontology = {"DIAGNOSIS//ICD//10//I50.23": ("alott-icd10cm-diagnoses", "I50.23")}

    class TestEnrichment(EnrichmentContract):
        rules_name = "ALOTT"
        packs = {"ICD10CM": {"I50": None, "I50.2": "I50", "I50.23": "I50.2"}}
        expected_parents = {
            "DIAGNOSIS//ICD//10//I50.23": ["ICD10CM//I50.23", "ICD10CM//I50.2", "ICD10CM//I50"]
        }

    class TestUnitComponents(UnitComponentContract):
        spec = "ALOTT"

``EnrichmentContract`` needs a ``raw_dir`` fixture (scope ``class`` or wider) with the synthetic
raw input of the ETL, and MEDS-Extract installed. Every subclass test can be extended or
overridden like any pytest test, and the fixtures (``plain_output``, ``enriched_output``,
``ontology_source``, ``example_codes``) can be overridden too, for instance to reuse a plain
build the suite already makes.

This module imports pytest, so it is only for test suites; the helpers it uses are in
:mod:`sumeds.testing`. ``UnitComponentContract`` also needs dftly (``sumeds[testing]``, or
MEDS-Extract, which depends on it), imported when its tests run.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from meds import CodeMetadataSchema

from ..config import EnrichmentConfig
from ..enrich import DATASET_RECORD_KEY, enrich_metadata
from ..ontology import (
    RULES_ENTRY_POINT_GROUP,
    OntologyRules,
    load_rules,
    registered_rules,
)
from . import (
    PIPELINE_ENTRY_POINT_GROUP,
    _pack,
    POST_STAGE_ORDER,
    dataset_metadata,
    load_spec,
    read_codes,
    read_events,
    registered_resource,
    run_cli,
    stage_name,
    stage_overrides,
    synthetic_title,
    write_ontology_source,
)

__all__ = [
    "EnrichmentContract",
    "RulesContract",
    "UNIT_PROBES",
    "UnitComponent",
    "UnitComponentContract",
    "find_unit_components",
]


class RulesContract:
    """The packaged suMEDS rules of an ETL load in suMEDS and resolve the ETL's codes as expected.

    Set ``rules_name``, the name under which the package registers both its spec
    (``MEDS_extract.pipelines``) and its rules (``sumeds.rules``). Optional expectations, each
    tested only when set:

    - ``own_post_stages``: the ETL's own post-stages, which run between ``sumeds_normalize_units``
      and ``sumeds_enrich``;
    - ``expected_ontology``: ``{code: (rule id, vocabulary code)}`` for every example code that
      an ``ontology`` rule resolves; every other example must match none, and every ontology rule
      must be exercised;
    - ``expected_crosswalk_keys``: ``{code: (rule id, key)}``, the same for ``crosswalk`` rules;
    - ``expected_hierarchy``: ``{rule id: {code: [parents]}}`` for every string (``separator`` or
      ``prefix``) hierarchy rule: run alone over the example codes, offline, each rule must give
      exactly these parents.

    The example codes are the codes named in those expectations (parents included). Override the
    ``example_codes`` fixture to test the codes the ETL writes, or codes rendered from its spec.
    """

    rules_name: ClassVar[str]
    own_post_stages: ClassVar[Sequence[str]] = ()
    expected_ontology: ClassVar[Mapping[str, tuple[str, str]] | None] = None
    expected_crosswalk_keys: ClassVar[Mapping[str, tuple[str, str]] | None] = None
    expected_hierarchy: ClassVar[Mapping[str, Mapping[str, Sequence[str]]] | None] = (
        None
    )

    # ------------------------------------------------------------------ fixtures

    @pytest.fixture(scope="class")
    @classmethod
    def rules_path(cls) -> Path:
        return registered_resource(RULES_ENTRY_POINT_GROUP, cls.rules_name)

    @pytest.fixture(scope="class")
    @classmethod
    def rules(cls, rules_path: Path) -> OntologyRules:
        return load_rules(rules_path)

    @pytest.fixture(scope="class")
    @classmethod
    def example_codes(cls) -> Iterable[str]:
        codes = set(cls.expected_ontology or ()) | set(
            cls.expected_crosswalk_keys or ()
        )
        for parents_by_code in (cls.expected_hierarchy or {}).values():
            for code, parents in parents_by_code.items():
                codes |= {code, *parents}
        return sorted(codes)

    # ------------------------------------------------------------------ tests

    def test_rules_file_is_packaged(self, rules_path: Path):
        assert rules_path.is_file(), f"{rules_path} is not in the installed package"

    def test_rules_are_registered_under_the_spec_name(self, rules_path: Path):
        """``sumeds_enrich: {rules: NAME}`` and ``--rules NAME`` find the rules by this entry point,
        and the spec is registered under the same name."""
        assert self.rules_name in registered_rules()
        assert load_rules(self.rules_name) == load_rules(rules_path)
        assert registered_resource(
            PIPELINE_ENTRY_POINT_GROUP, self.rules_name
        ).is_file()

    def test_spec_runs_the_sumeds_post_stages_in_order(self):
        """``sumeds_link_unit_siblings``, ``sumeds_normalize_units``, the ETL's own post-stages, then
        ``sumeds_enrich`` with these rules (or none, so it takes the dataset name)."""
        etl = load_spec(self.rules_name)["etl"]
        stages = etl.get("post_stages") or []
        link, normalize, enrich = POST_STAGE_ORDER
        assert [stage_name(s) for s in stages] == [
            link,
            normalize,
            *self.own_post_stages,
            enrich,
        ]
        options = stages[-1][enrich] if isinstance(stages[-1], Mapping) else None
        rules = (options or {}).get("rules") or etl.get("dataset_name")
        assert rules == self.rules_name

    def test_rules_load_in_sumeds(self, rules: OntologyRules):
        """``load_rules`` rejects any malformed rule; each pattern must also span a whole code."""
        for rule in (*rules.vocabulary_rules, *rules.hierarchy):
            if rule.pattern is not None:
                assert rule.pattern.pattern.startswith("^"), rule.id
                assert rule.pattern.pattern.endswith("$"), rule.id

    def test_ontology_rules_resolve_the_examples(
        self, rules: OntologyRules, example_codes
    ):
        if self.expected_ontology is None:
            pytest.skip("no expected_ontology")
        resolved = {}
        for code in example_codes:
            hits = [
                (rule.id, value)
                for rule in rules.ontology
                if (value := rule.resolve(code)) is not None
            ]
            assert len(hits) <= 1, f"{code} matches several ontology rules: {hits}"
            if hits:
                resolved[code] = hits[0]
        assert resolved == dict(self.expected_ontology)
        assert {rule_id for rule_id, _ in resolved.values()} == {
            rule.id for rule in rules.ontology
        }

    def test_crosswalk_rules_key_the_examples(
        self, rules: OntologyRules, example_codes
    ):
        if self.expected_crosswalk_keys is None:
            pytest.skip("no expected_crosswalk_keys")
        keyed = {}
        for code in example_codes:
            hits = [
                (rule.id, key)
                for rule in rules.crosswalk
                if (key := rule.key(code)) is not None
            ]
            assert len(hits) <= 1, f"{code} matches several crosswalk rules: {hits}"
            if hits:
                keyed[code] = hits[0]
        assert keyed == dict(self.expected_crosswalk_keys)
        assert {rule_id for rule_id, _ in keyed.values()} == {
            rule.id for rule in rules.crosswalk
        }

    def test_hierarchy_rules_add_the_expected_parents(
        self, rules: OntologyRules, example_codes, tmp_path
    ):
        """Each string hierarchy rule runs alone in suMEDS's enrichment, offline, over the examples."""
        if self.expected_hierarchy is None:
            pytest.skip("no expected_hierarchy")
        hierarchy = [rule for rule in rules.hierarchy if rule.mode != "ontology"]
        assert {rule.id for rule in hierarchy} == set(self.expected_hierarchy)
        source = str(write_ontology_source(tmp_path / "no_packs"))
        codes = sorted(set(example_codes))
        for rule in hierarchy:
            enriched = enrich_metadata(
                pl.LazyFrame({"code": codes}),
                EnrichmentConfig(
                    rules=OntologyRules(hierarchy=(rule,)), ontology_source=source
                ),
            ).collect()
            parents = {
                row["code"]: row["parent_codes"]
                for row in enriched.select("code", "parent_codes").to_dicts()
                if row["parent_codes"]
            }
            expected = {
                code: list(values)
                for code, values in self.expected_hierarchy[rule.id].items()
            }
            assert parents == expected, rule.id


def _filled(rows: Mapping[str, Mapping[str, Any]], column: str) -> int:
    return sum(row.get(column) is not None for row in rows.values())


class EnrichmentContract:
    """``meds-extract-run spec=NAME`` runs the ETL and suMEDS's enrichment, offline.

    The subclass provides a ``raw_dir`` fixture (scope ``class`` or wider) with synthetic raw
    input, and sets:

    - ``rules_name``: the name the spec and the rules are registered under;
    - ``spec``: the ``spec=`` argument, if not ``rules_name`` (a path, for instance);
    - ``etl_args``: further ``meds-extract-run`` arguments for every run;
    - ``packs`` and ``crosswalks``: the synthetic ontology source, as for
      :func:`~sumeds.testing.write_ontology_source`;
    - ``expected_parents``: ``{code: [parent codes]}``, the whole ``parent_codes`` of a code
      after enrichment; each parent from a pack must be named with its pack title;
    - ``expected_descriptions``: ``{code: description}`` after enrichment;
    - ``unenriched_codes``: codes that must keep the plain ETL's description and parents.

    The plain ETL (``skip_stages=[sumeds_enrich]``) and the enriched one each run once per class.
    Further runs test the soft failure and its retry, ``strict`` and ``offline``.
    """

    rules_name: ClassVar[str]
    spec: ClassVar[str | None] = None
    etl_args: ClassVar[Sequence[str]] = ()
    packs: ClassVar[Mapping[str, Any]] = {}
    crosswalks: ClassVar[Mapping[str, Any]] = {}
    expected_parents: ClassVar[Mapping[str, Sequence[str]]] = {}
    expected_descriptions: ClassVar[Mapping[str, str | None]] = {}
    unenriched_codes: ClassVar[Sequence[str]] = ()

    def pytest_generate_tests(self, metafunc: pytest.Metafunc) -> None:
        """One test per expected code, so a failure names the code."""
        cases = {
            "expected_parents_code": self.expected_parents,
            "expected_description_code": self.expected_descriptions,
            "unenriched_code": self.unenriched_codes,
        }
        for argument, codes in cases.items():
            if argument in metafunc.fixturenames:
                codes = list(codes)
                metafunc.parametrize(
                    argument,
                    codes
                    or [
                        pytest.param(
                            None, marks=pytest.mark.skip(reason="nothing expected")
                        )
                    ],
                )

    # ------------------------------------------------------------------ running

    @classmethod
    def run_etl(
        cls,
        output_dir: Path,
        raw_dir: Path,
        *args: str,
        env: Mapping[str, str] | None = None,
        ok: bool = True,
    ) -> Path:
        """``meds-extract-run`` over ``raw_dir`` into ``output_dir``, with ``args`` appended."""
        run_cli(
            "meds-extract-run",
            f"spec={cls.spec or cls.rules_name}",
            f"output_dir={output_dir}",
            "do_download=false",
            f"input_dir={raw_dir}",
            *cls.etl_args,
            *args,
            env=env,
            ok=ok,
        )
        return output_dir

    # ------------------------------------------------------------------ fixtures

    @pytest.fixture(scope="class")
    @classmethod
    def ontology_source(cls, tmp_path_factory: pytest.TempPathFactory) -> Path:
        root = tmp_path_factory.mktemp("ontologies")
        return write_ontology_source(root, cls.packs, crosswalks=cls.crosswalks)

    @pytest.fixture(scope="class")
    @classmethod
    def plain_output(
        cls, tmp_path_factory: pytest.TempPathFactory, raw_dir: Path
    ) -> Path:
        output_dir = tmp_path_factory.mktemp("plain") / "MEDS_cohort"
        return cls.run_etl(output_dir, raw_dir, "skip_stages=[sumeds_enrich]")

    @pytest.fixture(scope="class")
    @classmethod
    def enriched_output(
        cls,
        tmp_path_factory: pytest.TempPathFactory,
        raw_dir: Path,
        ontology_source: Path,
    ) -> Path:
        output_dir = tmp_path_factory.mktemp("enriched") / "MEDS_cohort"
        return cls.run_etl(
            output_dir, raw_dir, stage_overrides(ontology_source=ontology_source)
        )

    @pytest.fixture(scope="class")
    @classmethod
    def plain_codes(cls, plain_output: Path) -> dict[str, dict]:
        return read_codes(plain_output)

    @pytest.fixture(scope="class")
    @classmethod
    def enriched_codes(cls, enriched_output: Path) -> dict[str, dict]:
        return read_codes(enriched_output)

    @pytest.fixture
    def empty_source(self, tmp_path: Path) -> Path:
        """A pack source without an index, as on a site that cannot reach the packs."""
        empty = tmp_path / "no_packs"
        empty.mkdir()
        return empty

    # ------------------------------------------------------------------ tests

    def test_codes_parquet_is_replaced_by_valid_meds_metadata(
        self, enriched_output: Path
    ):
        metadata = enriched_output / "metadata"
        codes = metadata / "codes.parquet"
        CodeMetadataSchema.validate(pq.read_table(codes))
        schema = pq.read_schema(codes)
        if "parent_names" in schema.names:
            assert schema.field("parent_names").type == pa.map_(
                pa.string(), pa.string()
            )
        assert not list(metadata.glob(".*enriched*")), (
            "the temporary enrichment output was left behind"
        )

    def test_data_matches_the_plain_etl(
        self, enriched_output: Path, plain_output: Path
    ):
        key = ["subject_id", "time", "code"]
        enriched, plain = read_events(enriched_output), read_events(plain_output)
        assert enriched.select(key).sort(key).equals(plain.select(key).sort(key))

    def test_enrichment_only_adds(self, plain_codes: dict, enriched_codes: dict):
        """Every code keeps its description and unit, and any parents it had lead the new list."""
        assert set(enriched_codes) == set(plain_codes)
        for code, row in plain_codes.items():
            enriched = enriched_codes[code]
            if row.get("description") is not None:
                assert enriched["description"] == row["description"], code
            parents = row.get("parent_codes") or []
            assert (enriched.get("parent_codes") or [])[: len(parents)] == parents, code
            if "unit" in row:
                assert enriched["unit"] == row["unit"], code

    def test_expected_parents(self, enriched_codes: dict, expected_parents_code: str):
        code = expected_parents_code
        assert code in enriched_codes, f"the ETL writes no {code}"
        row = enriched_codes[code]
        assert (row.get("parent_codes") or []) == list(self.expected_parents[code])
        names = row.get("parent_names") or {}
        for parent in self.expected_parents[code]:
            vocabulary, _, value = parent.partition("//")
            if vocabulary in self.packs:
                pack = _pack(self.packs[vocabulary])
                if value in pack.rows():
                    title = pack.titles.get(value, synthetic_title(vocabulary, value))
                    assert names.get(parent) == title, parent

    def test_expected_descriptions(
        self, enriched_codes: dict, expected_description_code: str
    ):
        code = expected_description_code
        assert code in enriched_codes, f"the ETL writes no {code}"
        assert enriched_codes[code]["description"] == self.expected_descriptions[code]

    def test_unenriched_codes_are_left_alone(
        self, plain_codes: dict, enriched_codes: dict, unenriched_code: str
    ):
        code = unenriched_code
        assert code in enriched_codes, f"the ETL writes no {code}"
        plain, enriched = plain_codes[code], enriched_codes[code]
        assert enriched["description"] == plain.get("description")
        assert (enriched.get("parent_codes") or []) == (plain.get("parent_codes") or [])

    def test_dataset_json_records_the_enrichment(
        self,
        enriched_output: Path,
        plain_output: Path,
        plain_codes: dict,
        enriched_codes: dict,
    ):
        """``dataset.json`` says the build is enriched, with which rules, and what it added."""
        dataset, plain = (
            dataset_metadata(enriched_output),
            dataset_metadata(plain_output),
        )
        assert DATASET_RECORD_KEY not in plain
        assert set(dataset) == {*plain, DATASET_RECORD_KEY}
        record = dataset[DATASET_RECORD_KEY]
        assert record["enriched"] is True
        assert record["rules"] == self.rules_name
        assert record["ontology_source"] == "local"
        assert record["athena"] is False
        assert record["coverage"] == {
            "codes": len(enriched_codes),
            "described": [
                _filled(plain_codes, "description"),
                _filled(enriched_codes, "description"),
            ],
            "with_parent_codes": [
                _filled(plain_codes, "parent_codes"),
                _filled(enriched_codes, "parent_codes"),
            ],
        }

    def test_skip_stages_runs_the_plain_etl(self, plain_output: Path):
        """``skip_stages=[sumeds_enrich]`` leaves the stage out: it never runs and records nothing."""
        assert DATASET_RECORD_KEY not in dataset_metadata(plain_output)
        assert not (plain_output / ".logs" / "sumeds_enrich.done").exists()
        pipeline = plain_output / ".meds_extract_run" / "pipeline.yaml"
        if pipeline.exists():
            stages = yaml.safe_load(pipeline.read_text())["stages"]
            assert "sumeds_enrich" not in [stage_name(s) for s in stages]

    def test_a_failed_enrichment_keeps_the_plain_etl_and_can_be_retried(
        self,
        tmp_path: Path,
        raw_dir: Path,
        empty_source: Path,
        ontology_source: Path,
        plain_output: Path,
        enriched_output: Path,
    ):
        """Without packs the run still succeeds, with the plain ``codes.parquet`` and a record of the
        failure; running the ETL again does not retry (the stage is done), ``suMEDS-enrich`` does."""
        output_dir = tmp_path / "MEDS_cohort"
        self.run_etl(
            output_dir, raw_dir, env={"SUMEDS_ONTOLOGY_SOURCE": str(empty_source)}
        )

        codes = output_dir / "metadata" / "codes.parquet"
        assert (
            codes.read_bytes()
            == (plain_output / "metadata" / "codes.parquet").read_bytes()
        )
        record = dataset_metadata(output_dir)[DATASET_RECORD_KEY]
        assert record["enriched"] is False
        assert record["rules"] == self.rules_name
        assert record["error"].startswith("OntologyPackError")
        assert record["retry"].startswith("suMEDS-enrich <MEDS_ROOT>")
        assert (output_dir / ".logs" / "sumeds_enrich.done").exists()
        log = output_dir / ".logs" / "pipeline.log"
        if (
            log.exists()
        ):  # the stage's warning, with the command to retry, goes to the pipeline log
            assert f"suMEDS-enrich {output_dir}" in log.read_text()

        self.run_etl(
            output_dir, raw_dir, stage_overrides(ontology_source=ontology_source)
        )
        assert dataset_metadata(output_dir)[DATASET_RECORD_KEY] == record

        run_cli("suMEDS-enrich", output_dir, "--ontology-source", ontology_source)
        retried = dataset_metadata(output_dir)[DATASET_RECORD_KEY]
        direct = dataset_metadata(enriched_output)[DATASET_RECORD_KEY]
        assert retried["enriched"] is True
        assert {k: v for k, v in retried.items() if k != "enriched_at"} == {
            k: v for k, v in direct.items() if k != "enriched_at"
        }
        assert pq.read_table(codes).equals(
            pq.read_table(enriched_output / "metadata" / "codes.parquet")
        )

    def test_strict_makes_a_failed_enrichment_fail_the_run(
        self, tmp_path: Path, raw_dir: Path, empty_source: Path
    ):
        output_dir = tmp_path / "MEDS_cohort"
        options = stage_overrides(ontology_source=empty_source, strict="true")
        self.run_etl(output_dir, raw_dir, options, ok=False)
        assert not (output_dir / ".logs" / "sumeds_enrich.done").exists()
        assert DATASET_RECORD_KEY not in dataset_metadata(output_dir)

    def test_offline_reads_only_the_cache(self, tmp_path: Path, raw_dir: Path):
        """With ``offline`` and an empty cache, the default pack source fails at once, without the network."""
        output_dir = tmp_path / "MEDS_cohort"
        env = {"XDG_CACHE_HOME": str(tmp_path / "cache"), "SUMEDS_ONTOLOGY_SOURCE": ""}
        self.run_etl(output_dir, raw_dir, stage_overrides(offline="true"), env=env)
        record = dataset_metadata(output_dir)[DATASET_RECORD_KEY]
        assert record["enriched"] is False
        assert "not in the ontology cache" in record["error"]


# ---------------------------------------------------------------------------------- unit components

#: The units each code is evaluated on: a clean one, the same one with surrounding whitespace, one
#: with inner spaces, blank ones and a missing one.
UNIT_PROBES: tuple[str | None, ...] = (
    "mg",
    " mg",
    "mg ",
    "\tmg\n",
    "mL / h",
    "  ",
    "",
    None,
)

#: The value a unit column takes while discovery looks for it at the end of a code.
_SENTINEL = "UNITPROBE"
#: The value every other column a code reads takes, unless ``dummy_values`` names one.
_DUMMY = "1"
#: Top-level spec keys that are not tables (MEDS-Extract's reserved sections and global defaults).
_NOT_TABLES = frozenset({"etl", "sources", "_defaults"})


def _expected_component(unit: str | None) -> str:
    """A unit as the last component of a code: stripped, and ``UNK`` when missing or blank."""
    return (unit or "").strip() or "UNK"


@dataclass(frozen=True)
class UnitComponent:
    """An event whose code ends in a unit: ``table/event`` reads it from the raw ``column``.

    ``helpers`` are the ``_table.cols`` entries the code reads the column through (empty when the
    code reads the column itself). ``unit`` is the event's ``_metadata._self.unit`` expression when
    it reads that column, else ``None``.
    """

    table: str
    event: str
    column: str
    helpers: tuple[str, ...] = ()
    unit: Any = None
    code: Any = field(default=None, compare=False)
    scope: _Table | None = field(default=None, compare=False, repr=False)

    @property
    def id(self) -> str:
        """``table/event``, as MEDS-Extract names a source block."""
        return f"{self.table}/{self.event}"


def _parser() -> Any:
    try:
        from dftly import Parser
    except ImportError as error:  # pragma: no cover - dftly comes with MEDS-Extract
        raise ImportError(
            "UnitComponentContract evaluates the spec with dftly: install sumeds[testing] or "
            "MEDS-Extract"
        ) from error
    return Parser()


def _tables(spec: Mapping[str, Any]) -> Iterable[tuple[str, Mapping[str, Any]]]:
    for name, block in spec.items():
        if (
            name not in _NOT_TABLES
            and not str(name).startswith("_")
            and isinstance(block, Mapping)
        ):
            yield str(name), block


def _events(block: Mapping[str, Any]) -> Iterable[tuple[str, Mapping[str, Any]]]:
    for name, event in block.items():
        if (
            not str(name).startswith("_")
            and isinstance(event, Mapping)
            and "code" in event
        ):
            yield str(name), event


class _Table:
    """One table's ``_table.cols`` helpers, parsed, to evaluate an event's expressions."""

    def __init__(self, block: Mapping[str, Any], parser: Any):
        self.parser = parser
        raw = ((block.get("_table") or {}).get("cols")) or {}
        self.cols = {name: parser(expr) for name, expr in raw.items()}

    def closure(self, node: Any) -> tuple[tuple[str, ...], frozenset[str]]:
        """The helpers ``node`` reads (in ``_table.cols`` order) and the raw columns under them."""
        helpers, raw, todo = set(), set(), list(node.referenced_columns)
        while todo:
            name = todo.pop()
            if name in self.cols:
                if name not in helpers:
                    helpers.add(name)
                    todo.extend(self.cols[name].referenced_columns)
            else:
                raw.add(name)
        return tuple(h for h in self.cols if h in helpers), frozenset(raw)

    def helpers_reading(self, node: Any, column: str) -> tuple[str, ...]:
        """The helpers of ``node`` whose value depends on the raw ``column``."""
        helpers, _ = self.closure(node)
        return tuple(h for h in helpers if column in self.closure(self.cols[h])[1])

    def evaluate(
        self,
        nodes: Mapping[str, Any],
        column: str,
        units: Sequence[str | None],
        dummy_values: Mapping[str, Any],
    ) -> pl.DataFrame:
        """``nodes`` over rows whose ``column`` takes ``units`` and every other column a dummy.

        The helpers the nodes read are added first, one ``with_columns`` each in ``_table.cols``
        order, as MEDS-Extract prepares a table.
        """
        helpers, raw = set(), set()
        for node in nodes.values():
            h, r = self.closure(node)
            helpers |= set(h)
            raw |= r
        n = len(units)
        frame = pl.DataFrame(
            {
                column: pl.Series(column, list(units), dtype=pl.String),
                **{
                    name: [dummy_values.get(name, _DUMMY)] * n
                    for name in sorted(raw - {column})
                },
            }
        )
        for name, node in self.cols.items():
            if name in helpers:
                frame = frame.with_columns(node.polars_expr.alias(name))
        return frame.select(**{name: node.polars_expr for name, node in nodes.items()})


def _unit_exprs(event: Mapping[str, Any]) -> dict[str, Any]:
    """The ``unit`` expression of each ``_metadata`` block of an event, by metadata prefix."""
    metadata = event.get("_metadata") or {}
    return {
        prefix: block["unit"]
        for prefix, block in metadata.items()
        if isinstance(block, Mapping) and block.get("unit") is not None
    }


def find_unit_components(
    spec: Mapping[str, Any],
    *,
    dummy_values: Mapping[str, Any] | None = None,
    unit_events: Mapping[str, str] | None = None,
    ignored_events: Collection[str] = (),
) -> tuple[list[UnitComponent], dict[str, str]]:
    """The events of a MEDS-Extract spec whose code ends in a unit, and the events it cannot decide.

    A table's unit columns are the raw columns its events' ``_metadata`` ``unit`` expressions read
    (``_self`` or another metadata prefix). An event's code ends in a unit when, with a unit column
    set to a probe value and every other column to a dummy, the code ends in ``//`` and that
    value, and the code reads the column through a ``_table.cols`` helper, or reads it itself while
    a ``_self.unit`` expression of the table passes it through unchanged. So
    ``f"LAB//{$item}//{$_value_unit}"`` counts, while ``f"INTAKE_OUTPUT//{$cellpath}"`` with a
    unit extracted from inside ``cellpath`` does not.

    ``unit_events`` adds ``{"table/event": raw unit column}`` that discovery misses, and
    ``ignored_events`` drops ``table/event`` ids. ``dummy_values`` sets the value of other columns
    a code reads (default ``"1"``). Events whose code fails to evaluate are returned, with the
    error, as undecided.
    """

    parser = _parser()
    dummy_values = dict(dummy_values or {})
    extra = dict(unit_events or {})
    forced = set(extra)
    found: list[UnitComponent] = []
    undecided: dict[str, str] = {}
    for table_name, block in _tables(spec):
        table = _Table(block, parser)
        events = dict(_events(block))
        units = {
            name: {prefix: parser(expr) for prefix, expr in _unit_exprs(event).items()}
            for name, event in events.items()
        }
        unit_columns: set[str] = set()
        for exprs in units.values():
            for node in exprs.values():
                unit_columns |= table.closure(node)[1]
        # Columns that some `_self.unit` of the table passes through unchanged.
        passed: set[str] = set()
        for exprs in units.values():
            node = exprs.get("_self")
            for column in sorted(table.closure(node)[1]) if node is not None else ():
                try:
                    value = table.evaluate(
                        {"u": node}, column, [_SENTINEL], dummy_values
                    )["u"][0]
                except Exception:  # noqa: BLE001 - not passed through, as far as discovery can tell
                    continue
                if value == _SENTINEL:
                    passed.add(column)
        for name, event in events.items():
            event_id = f"{table_name}/{name}"
            if event_id in ignored_events:
                continue
            code = parser(event["code"])
            _, reads = table.closure(code)
            columns = sorted(reads & unit_columns)
            if event_id in extra:
                columns = [extra.pop(event_id)]
            for column in columns:
                helpers = table.helpers_reading(code, column)
                if event_id not in forced and not (helpers or column in passed):
                    continue
                try:
                    value = table.evaluate(
                        {"code": code}, column, [_SENTINEL], dummy_values
                    )["code"][0]
                except Exception as error:  # noqa: BLE001 - reported, with the event, as undecided
                    undecided[event_id] = f"{type(error).__name__}: {error}"
                    continue
                if event_id not in forced and not (
                    isinstance(value, str) and value.endswith(f"//{_SENTINEL}")
                ):
                    continue
                self_unit = units[name].get("_self")
                reads_column = (
                    self_unit is not None and column in table.closure(self_unit)[1]
                )
                found.append(
                    UnitComponent(
                        table=table_name,
                        event=name,
                        column=column,
                        helpers=helpers,
                        unit=event["_metadata"]["_self"]["unit"]
                        if reads_column
                        else None,
                        code=event["code"],
                        scope=table,
                    )
                )
                break
    for event_id in extra:
        undecided[event_id] = "unit_events names an event the spec does not have"
    return found, undecided


class UnitComponentContract:
    """A code ending in a unit carries it stripped, and ``UNK`` where it is missing or blank.

    MEDS-Extract compares units stripped and drops blank ones when it writes ``codes.parquet``. A
    code that kept the whitespace would split one unit over several codes (`` mg``, ``mg``,
    ``mg ``), and a blank unit would give a code ending in whitespace (or in ``//``) with no unit.
    So an ETL builds the unit component with a ``_table.cols`` helper such as
    ``_value_unit: '(extract /(?s)\\S(?:.*\\S)?/ from $valueuom) ?? "UNK"'`` and keeps
    ``_metadata._self.unit`` the raw column.

    Set ``spec``: the name the ETL registers its spec under (``MEDS_extract.pipelines``), a path to
    the spec, or the parsed spec. :func:`find_unit_components` finds the events whose code ends in
    a unit; each is evaluated with dftly on :data:`UNIT_PROBES`, every other column a code reads
    taking a dummy value. Optional:

    - ``expected_unit_events``: the ``table/event`` ids discovery must find, to pin it;
    - ``unit_events``: ``{"table/event": raw unit column}`` for events discovery cannot decide;
    - ``ignored_events``: ``table/event`` ids to leave out;
    - ``dummy_values``: ``{column: value}`` for columns a code needs a particular value of (a
      condition such as ``$count == 1``); every other column is ``"1"``.
    """

    spec: ClassVar[str | Path | Mapping[str, Any]]
    expected_unit_events: ClassVar[Collection[str] | None] = None
    unit_events: ClassVar[Mapping[str, str]] = {}
    ignored_events: ClassVar[Collection[str]] = ()
    dummy_values: ClassVar[Mapping[str, Any]] = {}

    @classmethod
    def load_spec(cls) -> Mapping[str, Any]:
        """The spec: parsed already, a path to a YAML file, or a registered name."""
        spec = cls.spec
        if isinstance(spec, Mapping):
            return spec
        if isinstance(spec, Path) or str(spec).endswith((".yaml", ".yml")):
            return yaml.safe_load(Path(spec).read_text())
        return load_spec(str(spec))

    @classmethod
    def discover(cls) -> tuple[list[UnitComponent], dict[str, str]]:
        """:func:`find_unit_components` over the spec, once per class."""
        cached = cls.__dict__.get("_discovered")
        if cached is None:
            cached = find_unit_components(
                cls.load_spec(),
                dummy_values=cls.dummy_values,
                unit_events=cls.unit_events,
                ignored_events=cls.ignored_events,
            )
            cls._discovered = cached
        return cached

    def pytest_generate_tests(self, metafunc: pytest.Metafunc) -> None:
        """One test per event, named ``table/event``, so a failure names the event."""
        if "unit_component" not in metafunc.fixturenames:
            return
        try:
            components, _ = self.discover()
        except (
            ImportError
        ) as error:  # no dftly: the contract's other tests fail with the reason
            metafunc.parametrize(
                "unit_component",
                [pytest.param(None, marks=pytest.mark.skip(reason=str(error)))],
            )
            return
        if metafunc.function.__name__ == "test_unit_metadata_stays_the_raw_column":
            components = [c for c in components if c.unit is not None]
        metafunc.parametrize(
            "unit_component",
            [pytest.param(c, id=c.id) for c in components]
            or [pytest.param(None, marks=pytest.mark.skip(reason="no such event"))],
        )

    # ------------------------------------------------------------------ tests

    def test_unit_components_are_found(self):
        """Every event discovery cannot evaluate is reported; ``expected_unit_events`` pins the rest."""
        components, undecided = self.discover()
        assert not undecided, (
            "cannot tell whether these codes end in a unit; set dummy_values, unit_events or "
            f"ignored_events: {undecided}"
        )
        found = sorted(c.id for c in components)
        if self.expected_unit_events is not None:
            assert found == sorted(self.expected_unit_events)
        else:
            assert found, "no code in the spec ends in a unit column"

    def test_no_code_ends_in_a_raw_unit_column(self):
        """A code must not end in the unit column as recorded: `` mg`` and ``mg`` would be two codes."""
        raw = []
        for component in self.discover()[0]:
            table = component.scope
            probe = f" {_SENTINEL} "
            code = table.evaluate(
                {"code": table.parser(component.code)},
                component.column,
                [probe],
                self.dummy_values,
            )["code"][0]
            if isinstance(code, str) and code.endswith(f"//{probe}"):
                raw.append(component.id)
        assert not raw, (
            f"these codes end in their raw unit column: {raw}. Build the component with a "
            "`_table.cols` helper such as "
            "`_value_unit: '(extract /(?s)\\S(?:.*\\S)?/ from $unit) ?? \"UNK\"'`"
        )

    def test_unit_component_is_stripped_and_blank_is_unk(
        self, unit_component: UnitComponent
    ):
        component = unit_component
        table = component.scope
        code = table.parser(component.code)
        units = list(UNIT_PROBES)
        stem = table.evaluate(
            {"code": code}, component.column, [_SENTINEL], self.dummy_values
        )["code"][0]
        assert isinstance(stem, str) and stem.endswith(_SENTINEL), (component.id, stem)
        stem = stem[: -len(_SENTINEL)]
        codes = table.evaluate(
            {"code": code}, component.column, units, self.dummy_values
        )["code"].to_list()
        expected = [stem + _expected_component(unit) for unit in units]
        assert codes == expected, (
            f"{component.id}: {dict(zip(units, codes, strict=True))}"
        )

    def test_unit_metadata_stays_the_raw_column(self, unit_component: UnitComponent):
        """``_self.unit`` keeps the unit as recorded (a blank one may be left out): MEDS-Extract
        strips it and drops a blank one when it writes ``codes.parquet``."""
        component = unit_component
        table = component.scope
        units = list(UNIT_PROBES)
        values = table.evaluate(
            {"unit": table.parser(component.unit)},
            component.column,
            units,
            self.dummy_values,
        )["unit"].to_list()
        for unit, value in zip(units, values, strict=True):
            if unit is not None and unit.strip():
                assert value == unit, f"{component.id}: unit {unit!r} became {value!r}"
            else:
                assert value in (unit, None), (
                    f"{component.id}: unit {unit!r} became {value!r}"
                )
