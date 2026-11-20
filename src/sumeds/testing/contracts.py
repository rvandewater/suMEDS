"""Contract tests for an ETL package that enriches its output with suMEDS.

Every ETL that declares suMEDS's post-stages makes the same promises: its rules file is packaged,
registered under the spec's name and loads in suMEDS; its spec runs the suMEDS post-stages in the
agreed order; and the enrichment only adds metadata, records itself in ``dataset.json``,
soft-fails without packs, can be retried with ``suMEDS-enrich``, and fails the run under
``strict``. These classes state those promises once. An ETL subclasses them in its test suite and
sets class attributes for what differs: the rules name, its synthetic packs, and the codes and
parents it expects.

.. code-block:: python

    from sumeds.testing.contracts import EnrichmentContract, RulesContract

    class TestRules(RulesContract):
        rules_name = "ALOTT"
        expected_ontology = {"DIAGNOSIS//ICD//10//I50.23": ("alott-icd10cm-diagnoses", "I50.23")}

    class TestEnrichment(EnrichmentContract):
        rules_name = "ALOTT"
        packs = {"ICD10CM": {"I50": None, "I50.2": "I50", "I50.23": "I50.2"}}
        expected_parents = {
            "DIAGNOSIS//ICD//10//I50.23": ["ICD10CM//I50.23", "ICD10CM//I50.2", "ICD10CM//I50"]
        }

``EnrichmentContract`` needs a ``raw_dir`` fixture (scope ``class`` or wider) with the synthetic
raw input of the ETL, and MEDS-Extract installed. Every subclass test can be extended or
overridden like any pytest test, and the fixtures (``plain_output``, ``enriched_output``,
``ontology_source``, ``example_codes``) can be overridden too, for instance to reuse a plain
build the suite already makes.

This module imports pytest, so it is only for test suites; the helpers it uses are in
:mod:`sumeds.testing`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
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

__all__ = ["EnrichmentContract", "RulesContract"]


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
