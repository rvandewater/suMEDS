from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import re
import urllib.request
from pathlib import Path

import polars as pl
import pytest

from sumeds import EnrichmentConfig, OntologyPacks, enrich_file, enrich_metadata
from sumeds.cli import enrich_main
from sumeds.ontology import OntologyPackError, load_rules

ICD10CM = [
    ("A04", "Other bacterial intestinal infections", ""),
    ("A04.7", "Enterocolitis due to Clostridium difficile", "A04"),
    (
        "A04.72",
        "Enterocolitis due to C. difficile, not specified as recurrent",
        "A04.7",
    ),
    ("I10", "Essential (primary) hypertension", ""),
    ("I50", "Heart failure", ""),
    ("I50.2", "Systolic (congestive) heart failure", "I50"),
    ("I50.23", "Acute on chronic systolic (congestive) heart failure", "I50.2"),
]
PCS = [
    ("0", "Medical and Surgical", ""),
    ("0D", "Gastrointestinal System", "0"),
    ("0DT", "Resection", "0D"),
    ("0DTJ", "Appendix", "0DT"),
    ("0DTJ4", "Percutaneous Endoscopic", "0DTJ"),
    ("0DTJ4Z", "No Device", "0DTJ4"),
    ("0DTJ4ZZ", "Resection of Appendix, Percutaneous Endoscopic Approach", "0DTJ4Z"),
    ("0F", "Hepatobiliary System and Pancreas", "0"),
    ("B", "Imaging", ""),
]
LOINC = [
    ("2160-0", "Creatinine [Mass/volume] in Serum or Plasma", "ACTIVE"),
    ("8867-4", "Heart rate", "ACTIVE"),
]


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _csv(header: list[str], rows: list[tuple]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def _write_pack(
    root: Path,
    vocabulary: str,
    rows: list[tuple],
    *,
    key_length: int = 1,
    header: tuple[str, ...] = ("code", "description", "parent"),
    extra: dict | None = None,
    compress: bool = False,
) -> None:
    directory = root / vocabulary / "test"
    directory.mkdir(parents=True)
    groups: dict[str, list[tuple]] = {}
    for row in rows:
        groups.setdefault(row[0][:key_length], []).append(row)
    shards = {}
    for key, items in groups.items():
        data = _csv(list(header), items)
        if compress:
            data = gzip.compress(data, mtime=0)
        name = f"{key}.csv{'.gz' if compress else ''}"
        (directory / name).write_bytes(data)
        shards[key] = {"file": name, "rows": len(items), "sha256": _sha(data)}
    manifest = {
        "schema": "sumeds-ontology-pack/0.1",
        "vocabulary": vocabulary,
        "release": "test",
        "shard_key_length": key_length,
        **(extra or {}),
        "shards": shards,
    }
    text = json.dumps(manifest).encode()
    (directory / "manifest.json").write_bytes(text)
    _index(root)["vocabularies"][vocabulary] = {
        "manifest": f"{vocabulary}/test/manifest.json",
        "sha256": _sha(text),
    }


def _write_crosswalk(root: Path, dataset: str, name: str, data: bytes) -> None:
    directory = root / "crosswalks" / dataset
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{name}.csv").write_bytes(data)
    manifest = {
        "schema": "sumeds-crosswalk/0.1",
        "dataset": dataset,
        "tables": {name: {"file": f"{name}.csv", "sha256": _sha(data)}},
    }
    text = json.dumps(manifest).encode()
    (directory / "manifest.json").write_bytes(text)
    _index(root)["crosswalks"][dataset] = {
        "manifest": f"crosswalks/{dataset}/manifest.json",
        "sha256": _sha(text),
    }


_INDEXES: dict[Path, dict] = {}


def _index(root: Path) -> dict:
    index = _INDEXES.setdefault(
        root,
        {"schema": "sumeds-ontologies/0.1", "vocabularies": {}, "crosswalks": {}},
    )
    (root / "index.json").write_text(json.dumps(index))
    return index


def _finish(root: Path) -> None:
    (root / "index.json").write_text(json.dumps(_INDEXES.pop(root)))


@pytest.fixture
def packs(tmp_path: Path) -> Path:
    root = tmp_path / "ontologies"
    root.mkdir()
    _write_pack(root, "ICD10CM", ICD10CM)
    _write_pack(
        root,
        "ICD10PCS",
        PCS,
        key_length=2,
        extra={"description_path": {"max_code_length": 6, "separator": " @ "}},
    )
    _write_pack(
        root,
        "LOINC",
        LOINC,
        key_length=2,
        header=("LOINC_NUM", "LONG_COMMON_NAME", "STATUS"),
        extra={
            "fields": {
                "code": "LOINC_NUM",
                "description": "LONG_COMMON_NAME",
                "parent": None,
            }
        },
        compress=True,
    )
    _write_crosswalk(
        root,
        "demo",
        "lab_loinc",
        _csv(
            ["itemid", "source_table", "loinc_code", "loinc_long_common_name"],
            [
                ("100", "labevents", "2160-0", ""),
                ("100", "chartevents", "9999-9", ""),
                ("200", "labevents", "8867-4", ""),
            ],
        ),
    )
    _finish(root)
    return root


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cache = tmp_path / "cache"
    monkeypatch.setenv("XDG_CACHE_HOME", str(cache))
    monkeypatch.delenv("SUMEDS_ONTOLOGY_SOURCE", raising=False)
    return cache


ICD_RULES = {
    "schema": "sumeds-rules/0.1",
    "ontology": [
        {
            "id": "icd10cm",
            "pattern": r"^DIAGNOSIS//ICD//10//(?P<code>[A-Z][0-9][0-9A-Z]{1,5})$",
            "vocabulary": "ICD10CM",
            "normalize": [{"pattern": r"^(...)(.+)$", "replace": r"\1.\2"}],
            "validate": "pack",
        },
        {
            "id": "pcs",
            "pattern": r"^ICD10_PCS//(?P<code>[0-9A-Z]{1,7})$",
            "vocabulary": "ICD10PCS",
        },
    ],
}


def _enrich(frame: pl.DataFrame, packs: Path, rules: dict, **options) -> dict:
    config = EnrichmentConfig(rules=rules, ontology_source=str(packs), **options)
    result = enrich_metadata(frame.lazy(), config).collect()
    return {row["code"]: row for row in result.to_dicts()}


def test_rules_file_parses_every_rule_kind(tmp_path: Path) -> None:
    path = tmp_path / "rules.yaml"
    path.write_text(
        r"""schema: sumeds-rules/0.1
ontology:
  - id: icd9
    pattern: '^DIAGNOSIS//ICD//9//(?P<code>[0-9EV][0-9]{2,4})$'
    vocabulary: ICD9CM
    normalize:
      - {pattern: '^(E[0-9]{3})([0-9])$', replace: '\1.\2'}
      - {pattern: '^([0-9V][0-9]{2})([0-9]{1,2})$', replace: '\1.\2'}
crosswalk:
  - id: labs
    pattern: '^LAB//(?P<key>[0-9]+)//'
    table: demo/lab_loinc
    key_column: itemid
    code_column: loinc_code
    vocabulary: LOINC
    where: {source_table: labevents}
    validate: athena
hierarchy:
  - id: paths
    pattern: '^(?P<prefix>DIAGNOSIS//)(?P<path>.+)$'
    group: path
    separator: '|'
  - id: icd-prefix
    pattern: '^(?P<prefix>DIAGNOSIS//ICD10_CN//)(?P<code>.+)$'
    group: code
    mode: prefix
    ignore: '.'
    nearest_only: false
  - id: ontology-parents
    from_ontology: true
    rules: [icd9]
"""
    )
    rules = load_rules(path)
    icd9 = rules.ontology[0]
    assert icd9.validate == "pack"
    assert icd9.resolve("DIAGNOSIS//ICD//9//4019") == "401.9"
    assert icd9.resolve("DIAGNOSIS//ICD//9//E8889") == "E888.9"
    assert icd9.resolve("DIAGNOSIS//ICD//9//V5861") == "V58.61"
    assert icd9.resolve("DIAGNOSIS//ICD//9//042") == "042"
    assert icd9.resolve("DIAGNOSIS//ICD//10//I10") is None
    crosswalk = rules.crosswalk[0]
    assert crosswalk.key("LAB//51//mg") == "51"
    assert crosswalk.where == (("source_table", ("labevents",)),)
    paths, prefix, ontology = rules.hierarchy
    assert paths.mode == "separator" and paths.nearest_only
    assert paths.candidates("DIAGNOSIS//a|b|c") == ["DIAGNOSIS//a|b", "DIAGNOSIS//a"]
    assert prefix.candidates("DIAGNOSIS//ICD10_CN//I50.23") == [
        "DIAGNOSIS//ICD10_CN//I50.2",
        "DIAGNOSIS//ICD10_CN//I50",
        "DIAGNOSIS//ICD10_CN//I5",
        "DIAGNOSIS//ICD10_CN//I",
    ]
    assert ontology.mode == "ontology" and ontology.rules == ("icd9",)
    assert [rule.id for rule in rules.vocabulary_rules] == ["icd9", "labs"]


@pytest.mark.parametrize(
    ("rules", "message"),
    [
        ({"schema": "sumeds-rules/0.2"}, "schema"),
        ({"schema": "sumeds-rules/0.1", "extra": []}, "unknown rules keys"),
        (
            {
                "schema": "sumeds-rules/0.1",
                "ontology": [{"id": "a", "pattern": "^(x)$", "vocabulary": "V"}],
            },
            "named group",
        ),
        (
            {
                "schema": "sumeds-rules/0.1",
                "ontology": [{"id": "a", "pattern": "(?P<code>", "vocabulary": "V"}],
            },
            "regular expression",
        ),
        (
            {
                "schema": "sumeds-rules/0.1",
                "ontology": [
                    {
                        "id": "a",
                        "pattern": "(?P<code>x)",
                        "vocabulary": "V",
                        "validate": "maybe",
                    }
                ],
            },
            "validate",
        ),
        (
            {
                "schema": "sumeds-rules/0.1",
                "ontology": [
                    {
                        "id": "a",
                        "pattern": "(?P<code>x)",
                        "vocabulary": "V",
                        "normalize": [{"pattern": "(x)", "replace": r"\2"}],
                    }
                ],
            },
            "group reference",
        ),
        (
            {
                "schema": "sumeds-rules/0.1",
                "ontology": [
                    {"id": "a", "pattern": "(?P<code>x)", "vocabulary": "V"},
                    {"id": "a", "pattern": "(?P<code>y)", "vocabulary": "V"},
                ],
            },
            "duplicate rule id",
        ),
        (
            {
                "schema": "sumeds-rules/0.1",
                "hierarchy": [{"id": "h", "pattern": "(?P<p>.+)", "group": "p"}],
            },
            "mode: prefix",
        ),
        (
            {
                "schema": "sumeds-rules/0.1",
                "hierarchy": [
                    {
                        "id": "h",
                        "pattern": "(?P<p>.+)",
                        "group": "p",
                        "mode": "prefix",
                        "separator": "|",
                    }
                ],
            },
            "not valid with mode: prefix",
        ),
        (
            {
                "schema": "sumeds-rules/0.1",
                "hierarchy": [
                    {"id": "h", "pattern": "(?P<p>.+)", "group": "q", "separator": "|"}
                ],
            },
            "'group'",
        ),
        (
            {
                "schema": "sumeds-rules/0.1",
                "hierarchy": [{"id": "h", "from_ontology": True, "rules": ["nope"]}],
            },
            "unknown ontology rule ids",
        ),
        (
            {
                "schema": "sumeds-rules/0.1",
                "crosswalk": [
                    {
                        "id": "c",
                        "pattern": "(?P<key>x)",
                        "table": "../etc",
                        "key_column": "k",
                        "code_column": "c",
                        "vocabulary": "LOINC",
                    }
                ],
            },
            "<dataset>/<name>",
        ),
    ],
)
def test_rules_are_strict(rules: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        load_rules(rules)


def test_pack_rule_fills_description_and_ancestors(packs: Path) -> None:
    frame = pl.DataFrame(
        {
            "code": [
                "DIAGNOSIS//ICD//10//I5023",
                "DIAGNOSIS//ICD//10//A047",
                "DIAGNOSIS//ICD//10//Z999",
                "ICD10_PCS//0DTJ4",
                "OTHER",
            ],
            "description": [None, "Keep me", None, None, None],
            "parent_codes": [None, ["ICD10CM/A04.7", "LOCAL"], None, None, ["KEEP"]],
        }
    )
    rows = _enrich(frame, packs, ICD_RULES)

    heart = rows["DIAGNOSIS//ICD//10//I5023"]
    assert (
        heart["description"] == "Acute on chronic systolic (congestive) heart failure"
    )
    assert heart["parent_codes"] == [
        "ICD10CM//I50.23",
        "ICD10CM//I50.2",
        "ICD10CM//I50",
    ]
    assert heart["vocabulary_id"] == "ICD10CM" and heart["concept_code"] == "I50.23"
    assert heart["concept_id"] is None
    kept = rows["DIAGNOSIS//ICD//10//A047"]
    assert kept["description"] == "Keep me"
    assert kept["parent_codes"] == ["ICD10CM//A04.7", "LOCAL", "ICD10CM//A04"]
    unknown = rows["DIAGNOSIS//ICD//10//Z999"]
    assert unknown["description"] is None and unknown["parent_codes"] is None
    pcs = rows["ICD10_PCS//0DTJ4"]
    assert pcs["description"] == (
        "Medical and Surgical @ Gastrointestinal System @ Resection @ Appendix @ "
        "Percutaneous Endoscopic"
    )
    assert pcs["parent_codes"] == [
        "ICD10PCS//0DTJ4",
        "ICD10PCS//0DTJ",
        "ICD10PCS//0DT",
        "ICD10PCS//0D",
        "ICD10PCS//0",
    ]
    assert rows["OTHER"]["parent_codes"] == ["KEEP"]


def test_validate_none_tags_and_athena_without_source_skips(
    packs: Path, tmp_path: Path, capsys
) -> None:
    rules = {
        "schema": "sumeds-rules/0.1",
        "ontology": [
            {
                "id": "atc",
                "pattern": r"^MEDICATION//ATC//(?P<code>[A-Z0-9]+)$",
                "vocabulary": "ATC",
                "validate": "none",
            },
            {
                "id": "snomed",
                "pattern": r"^SNOMED//(?P<code>[0-9]+)$",
                "vocabulary": "SNOMED",
                "validate": "athena",
            },
        ],
    }
    frame = pl.DataFrame({"code": ["MEDICATION//ATC//B01AC06", "SNOMED//123"]})
    rows = _enrich(frame, packs, rules)
    assert rows["MEDICATION//ATC//B01AC06"]["parent_codes"] == ["ATC//B01AC06"]
    assert rows["MEDICATION//ATC//B01AC06"]["description"] is None
    assert rows["MEDICATION//ATC//B01AC06"]["vocabulary_id"] is None
    assert rows["SNOMED//123"]["parent_codes"] is None

    source = tmp_path / "codes.parquet"
    frame.write_parquet(source)
    enrich_file(
        source,
        tmp_path / "out.parquet",
        EnrichmentConfig(rules=rules, ontology_source=str(packs)),
        verbose=True,
    )
    out = capsys.readouterr().out
    assert "atc [ontology, ATC, none]: 1 matched, 0 validated" in out
    assert "1 tagged" in out
    assert "snomed [ontology, SNOMED, athena]: 1 matched, skipped" in out


def test_pack_rule_for_vocabulary_without_pack_fails(packs: Path) -> None:
    rules = {
        "schema": "sumeds-rules/0.1",
        "ontology": [
            {"id": "atc", "pattern": r"^ATC//(?P<code>.+)$", "vocabulary": "ATC"}
        ],
    }
    with pytest.raises(OntologyPackError, match="no public ontology pack"):
        _enrich(pl.DataFrame({"code": ["ATC//B01"]}), packs, rules)


def test_children_and_siblings_come_from_the_pack(packs: Path) -> None:
    rows = _enrich(
        pl.DataFrame({"code": ["DIAGNOSIS//ICD//10//I502", "ICD10_PCS//0DT"]}),
        packs,
        ICD_RULES,
        child_codes=True,
        sibling_codes=True,
        child_depth=1,
    )
    heart = rows["DIAGNOSIS//ICD//10//I502"]
    assert heart["child_codes"] == ["ICD10CM//I50.23"]
    assert heart["sibling_codes"] is None
    resection = rows["ICD10_PCS//0DT"]
    assert resection["child_codes"] == ["ICD10PCS//0DTJ"]
    # Children of every ancestor, here also of the section in another shard.
    assert resection["sibling_codes"] == ["ICD10PCS//0D", "ICD10PCS//0F"]


def test_hierarchy_rules_add_only_codes_of_the_table(packs: Path) -> None:
    rules = {
        **ICD_RULES,
        "hierarchy": [
            {
                "id": "paths",
                "pattern": r"^(?P<prefix>DX//)(?P<path>.+)$",
                "group": "path",
                "separator": "|",
            },
            {
                "id": "cn-prefix",
                "pattern": r"^(?P<prefix>CN//)(?P<code>.+)$",
                "group": "code",
                "mode": "prefix",
                "ignore": ".",
            },
            {
                "id": "cn-all",
                "pattern": r"^(?P<prefix>ALL//)(?P<code>.+)$",
                "group": "code",
                "mode": "prefix",
                "ignore": ".",
                "nearest_only": False,
            },
            {"id": "ontology", "from_ontology": True},
        ],
    }
    frame = pl.DataFrame(
        {
            "code": [
                "DX//a|b|c",
                "DX//a",
                "DX//x|y",
                "CN//I50.23",
                "CN//I50",
                "ALL//I50.23",
                "ALL//I50.2",
                "ALL//I50",
                "DIAGNOSIS//ICD//10//I5023",
                "DIAGNOSIS//ICD//10//I502",
                "DIAGNOSIS//ICD//10//I50",
                "DIAGNOSIS//ICD//10//I10",
            ],
            "parent_codes": [["EXISTING"], *([None] * 11)],
        }
    )
    rows = _enrich(frame, packs, rules)

    # Nearest existing ancestor, after existing parents; DX//a|b is not a code.
    assert rows["DX//a|b|c"]["parent_codes"] == ["EXISTING", "DX//a"]
    assert rows["DX//x|y"]["parent_codes"] is None
    assert rows["CN//I50.23"]["parent_codes"] == ["CN//I50"]
    assert rows["ALL//I50.23"]["parent_codes"] == ["ALL//I50.2", "ALL//I50"]
    # Vocabulary parents come first; the same-table parent is the nearest one.
    assert rows["DIAGNOSIS//ICD//10//I5023"]["parent_codes"] == [
        "ICD10CM//I50.23",
        "ICD10CM//I50.2",
        "ICD10CM//I50",
        "DIAGNOSIS//ICD//10//I502",
    ]
    assert rows["DIAGNOSIS//ICD//10//I502"]["parent_codes"][-1] == (
        "DIAGNOSIS//ICD//10//I50"
    )
    assert rows["DIAGNOSIS//ICD//10//I10"]["parent_codes"] == ["ICD10CM//I10"]
    codes = set(frame["code"])
    for row in rows.values():
        for parent in row["parent_codes"] or []:
            assert (
                parent in codes
                or parent.startswith("ICD10CM//")
                or parent == "EXISTING"
            )

    all_parents = {
        **rules,
        "hierarchy": [{"id": "ontology", "from_ontology": True, "nearest_only": False}],
    }
    rows = _enrich(frame, packs, all_parents)
    assert rows["DIAGNOSIS//ICD//10//I5023"]["parent_codes"][-2:] == [
        "DIAGNOSIS//ICD//10//I502",
        "DIAGNOSIS//ICD//10//I50",
    ]
    rows = _enrich(frame, packs, rules, parent_codes=False)
    assert rows["DX//a|b|c"]["parent_codes"] == ["EXISTING"]


def test_crosswalk_rules_tag_or_validate(packs: Path) -> None:
    crosswalk = {
        "id": "labs",
        "pattern": r"^LAB//(?P<key>[0-9]+)//",
        "table": "demo/lab_loinc",
        "key_column": "itemid",
        "code_column": "loinc_code",
        "vocabulary": "LOINC",
    }
    frame = pl.DataFrame({"code": ["LAB//100//mg/dL", "LAB//200//bpm", "LAB//300//x"]})
    tagged = _enrich(
        frame,
        packs,
        {
            "schema": "sumeds-rules/0.1",
            "crosswalk": [{**crosswalk, "validate": "none"}],
        },
    )
    # Without a filter, key 100 maps to two codes and both are tagged.
    assert tagged["LAB//100//mg/dL"]["parent_codes"] == [
        "LOINC//2160-0",
        "LOINC//9999-9",
    ]
    assert tagged["LAB//300//x"]["parent_codes"] is None

    validated = _enrich(
        frame,
        packs,
        {
            "schema": "sumeds-rules/0.1",
            "crosswalk": [{**crosswalk, "where": {"source_table": "labevents"}}],
        },
    )
    creatinine = validated["LAB//100//mg/dL"]
    assert creatinine["description"] == "Creatinine [Mass/volume] in Serum or Plasma"
    assert creatinine["parent_codes"] == ["LOINC//2160-0"]
    assert validated["LAB//200//bpm"]["description"] == "Heart rate"


@pytest.fixture
def athena_icd(tmp_path: Path) -> Path:
    directory = tmp_path / "athena_icd"
    directory.mkdir()
    pl.DataFrame(
        [
            (1, "Athena enterocolitis", "Condition", "ICD10CM", "A04.7", None, None),
            (2, "Athena heart failure", "Condition", "ICD10CM", "I50", None, None),
            (3, "Local concept", "Observation", "LOCAL", "X1", "S", None),
            (4, "Rule target", "Observation", "LOCAL", "X2", "S", None),
        ],
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
    ).write_csv(directory / "CONCEPT.csv", separator="\t")
    pl.DataFrame(
        [(4, 3, 1)],
        schema=[
            "ancestor_concept_id",
            "descendant_concept_id",
            "min_levels_of_separation",
        ],
        orient="row",
    ).write_csv(directory / "CONCEPT_ANCESTOR.csv", separator="\t")
    return directory


def test_athena_first_then_packs(packs: Path, athena_icd: Path) -> None:
    rules = {
        **ICD_RULES,
        "ontology": [
            *ICD_RULES["ontology"],
            {
                "id": "local",
                "pattern": r"^LOCAL//(?P<code>X1)//end$",
                "vocabulary": "LOCAL",
                "normalize": [{"pattern": "X1", "replace": "X2"}],
                "validate": "athena",
            },
        ],
    }
    frame = pl.DataFrame(
        {
            "code": [
                "DIAGNOSIS//ICD//10//A047",
                "DIAGNOSIS//ICD//10//I5023",
                "LOCAL//X1//end",
                "LOCAL//X1",
            ]
        }
    )
    rows = _enrich(frame, packs, rules, csv_dir=athena_icd)

    athena = rows["DIAGNOSIS//ICD//10//A047"]
    assert athena["description"] == "Athena enterocolitis"
    assert athena["concept_id"] == 1
    # Athena has no ICD-10-CM hierarchy; the pack adds the ancestors.
    assert athena["parent_codes"] == ["ICD10CM//A04.7", "ICD10CM//A04"]
    pack = rows["DIAGNOSIS//ICD//10//I5023"]
    assert pack["concept_id"] is None
    assert pack["description"].startswith("Acute on chronic")
    # The rule beats the heuristic VOCAB//CODE parse (which would give X1).
    assert rows["LOCAL//X1//end"]["concept_id"] == 4
    assert rows["LOCAL//X1//end"]["parent_codes"] == ["LOCAL//X2"]
    assert rows["LOCAL//X1"]["concept_id"] == 3
    assert rows["LOCAL//X1"]["parent_codes"] == ["LOCAL//X2"]


def test_parent_reference_rank_still_excludes_self(
    packs: Path, athena_icd: Path
) -> None:
    frame = pl.DataFrame(
        {"code": ["SOMETHING"], "parent_codes": [[None, "LOCAL/X1", "KEEP"]]}
    )
    rows = _enrich(frame, packs, ICD_RULES, csv_dir=athena_icd)
    assert rows["SOMETHING"]["concept_id"] == 3
    assert rows["SOMETHING"]["parent_codes"] == ["KEEP", "LOCAL//X2"]


class _FakeRemote:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.requests: list[str] = []

    def __call__(self, request, timeout=None):
        url = request.full_url
        self.requests.append(url)
        relative = url.split("/ontologies/", 1)[1]
        return io.BytesIO(
            (self.root / urllib.request.url2pathname(relative)).read_bytes()
        )


REMOTE = "https://example.org/repo/main/ontologies"


def test_remote_fetches_only_needed_shards_and_caches(
    packs: Path, monkeypatch: pytest.MonkeyPatch, isolated_cache: Path
) -> None:
    remote = _FakeRemote(packs)
    monkeypatch.setattr("sumeds.ontology.urllib.request.urlopen", remote)
    client = OntologyPacks(REMOTE)
    entries = client.lookup("ICD10CM", ["I50.23", "Z99"])
    assert set(entries) == {"I50.23"}
    assert entries["I50.23"].ancestors == ("I50.2", "I50")
    # No Z shard exists, so only the I shard is fetched.
    fetched = sorted(url.rsplit("/", 1)[1] for url in remote.requests)
    assert fetched == ["I.csv", "index.json", "manifest.json"]
    assert client.stats["ICD10CM"]["shards"] == 1
    assert (isolated_cache / "sumeds" / "ontologies").is_dir()

    # Offline: the cache serves the same lookup without any request.
    remote.requests.clear()
    offline = OntologyPacks(REMOTE, offline=True)
    assert set(offline.lookup("ICD10CM", ["I50.23"])) == {"I50.23"}
    assert remote.requests == []
    with pytest.raises(OntologyPackError, match="not in the ontology cache"):
        offline.lookup("ICD10CM", ["A04.7"])

    # A file:// URL reads in place, like a directory.
    local = OntologyPacks(packs.as_uri())
    assert local.lookup("ICD10CM", ["A04.72"])["A04.72"].ancestors == ("A04.7", "A04")


def test_default_source_is_a_pinned_release(
    packs: Path, monkeypatch: pytest.MonkeyPatch, isolated_cache: Path
) -> None:
    from sumeds import ontology

    monkeypatch.delenv(ontology.SOURCE_ENV, raising=False)
    assert re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", ontology.DEFAULT_REF)
    assert ontology.DATA_REPOSITORY in ontology.DEFAULT_SOURCE
    remote = _FakeRemote(packs)
    monkeypatch.setattr("sumeds.ontology.urllib.request.urlopen", remote)

    # The default release refuses an index whose digest is not the pinned one.
    with pytest.raises(OntologyPackError, match="sha256 mismatch for .*index.json"):
        OntologyPacks().vocabularies()

    pinned = hashlib.sha256((packs / "index.json").read_bytes()).hexdigest()
    monkeypatch.setattr(ontology, "DEFAULT_INDEX_SHA256", pinned)
    client = OntologyPacks()
    assert "ICD10CM" in client.vocabularies()
    assert client.describe() == (
        f"{ontology.DATA_REPOSITORY}@{ontology.DEFAULT_REF}, index sha256 pinned"
    )
    assert remote.requests[0].startswith(
        f"https://raw.githubusercontent.com/{ontology.DATA_REPOSITORY}/"
        f"{ontology.DEFAULT_REF}/ontologies/"
    )
    # A pinned index is immutable, so a cached copy serves offline runs.
    assert "ICD10CM" in OntologyPacks(offline=True).vocabularies()


def test_unpinned_ref_warns_and_explicit_source_does_not(
    packs: Path, monkeypatch: pytest.MonkeyPatch, recwarn
) -> None:
    from sumeds import ontology

    monkeypatch.delenv(ontology.SOURCE_ENV, raising=False)
    with pytest.warns(UserWarning, match="not a release tag"):
        branch = OntologyPacks(ref="main")
    assert branch.index_sha256 is None
    OntologyPacks(ref="0123456789abcdef0123456789abcdef01234567")
    OntologyPacks(str(packs), ref="main")
    assert not [w for w in recwarn if issubclass(w.category, UserWarning)]


def test_checksum_mismatch_is_an_error(packs: Path) -> None:
    shard = packs / "ICD10CM" / "test" / "I.csv"
    shard.write_bytes(shard.read_bytes().replace(b"Heart failure", b"Heart fail"))
    with pytest.raises(OntologyPackError, match="sha256 mismatch"):
        OntologyPacks(str(packs)).lookup("ICD10CM", ["I50"])


def test_environment_source_and_cli(
    packs: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        "schema: sumeds-rules/0.1\n"
        "ontology:\n"
        "  - id: icd10cm\n"
        "    pattern: '^DX//(?P<code>[A-Z][0-9]{2}[.0-9]*)$'\n"
        "    vocabulary: ICD10CM\n"
        "hierarchy:\n"
        "  - {id: ontology, from_ontology: true}\n"
    )
    source = tmp_path / "codes.csv"
    source.write_text("code,description\nDX//I50.2,\nDX//I50,\nDX//Q00,\n")
    output = tmp_path / "out.parquet"
    monkeypatch.setenv("SUMEDS_ONTOLOGY_SOURCE", str(packs))
    assert (
        enrich_main([str(source), "-o", str(output), "--rules", str(rules_path)]) == 0
    )
    out = capsys.readouterr().out
    assert "icd10cm [ontology, ICD10CM, pack]: 3 matched, 2 validated" in out
    assert "ontology [ontology]: 1 parents added to 1 codes" in out
    assert "Same-dataset parent links:  0 -> 1" in out
    assert "Parent codes (all entries): 0 -> 4" in out
    assert re.search(r"Descriptions +0 -> +2 ", out)
    result = {row["code"]: row for row in pl.read_parquet(output).to_dicts()}
    assert result["DX//I50.2"]["parent_codes"] == [
        "ICD10CM//I50.2",
        "ICD10CM//I50",
        "DX//I50",
    ]

    with pytest.raises(SystemExit):
        enrich_main([str(source), "-o", str(output)])
    assert "--rules is required" in capsys.readouterr().err


def test_config_accepts_rules_alone(tmp_path: Path) -> None:
    from sumeds import SummaryConfig

    rules = tmp_path / "conf" / "rules.yaml"
    rules.parent.mkdir()
    rules.write_text("schema: sumeds-rules/0.1\n")
    path = tmp_path / "conf" / "summary.yaml"
    path.write_text(
        "enrichment:\n"
        "  rules: rules.yaml\n"
        "  ontology_source: packs\n"
        "  ontology_ref: v1\n"
        "  offline: true\n"
    )
    config = SummaryConfig.from_yaml(path).enrichment
    assert config.csv_dir is None and config.postgres is None
    assert config.rules == load_rules(rules)
    assert config.ontology_source == str(tmp_path / "conf" / "packs")
    assert config.ontology_ref == "v1" and config.offline is True
    with pytest.raises(ValueError, match="exactly one"):
        EnrichmentConfig()
    with pytest.raises(ValueError, match="offline"):
        EnrichmentConfig(rules=rules, offline="yes")  # type: ignore[arg-type]


def test_summary_reports_rules(meds_dataset: Path, tmp_path: Path, capsys) -> None:
    from sumeds import SummaryConfig, summarize

    rules = {
        "schema": "sumeds-rules/0.1",
        "ontology": [
            {
                "id": "letters",
                "pattern": "^(?P<code>[A-Z])$",
                "vocabulary": "TEST",
                "validate": "none",
            }
        ],
    }
    output = summarize(
        meds_dataset,
        tmp_path / "summary.parquet",
        SummaryConfig(min_subjects=3, enrichment=EnrichmentConfig(rules=rules)),
        verbose=True,
    )
    rows = {row["code"]: row for row in pl.read_parquet(output).to_dicts()}
    assert "TEST//A" in rows["A"]["parent_codes"]
    assert rows["__RARE__"]["parent_codes"] is None
    out = capsys.readouterr().out
    assert "letters [ontology, TEST, none]: 1 matched, 0 validated" in out


# The ontologies/ folder of a sumeds-ontologies checkout at the pinned release
# (CI clones it); the test is skipped when the variable is unset.
RELEASE_PACKS = os.environ.get("SUMEDS_ONTOLOGIES_DIR")


@pytest.mark.skipif(
    not RELEASE_PACKS, reason="SUMEDS_ONTOLOGIES_DIR is not set to a release checkout"
)
def test_pinned_release_packs_are_complete_and_verified() -> None:
    from sumeds import ontology

    release_packs = Path(RELEASE_PACKS)
    index_bytes = (release_packs / "index.json").read_bytes()
    assert _sha(index_bytes) == ontology.DEFAULT_INDEX_SHA256
    index = json.loads(index_bytes)
    for entry in [*index["vocabularies"].values(), *index["crosswalks"].values()]:
        manifest_path = release_packs / entry["manifest"]
        assert _sha(manifest_path.read_bytes()) == entry["sha256"]
        manifest = json.loads(manifest_path.read_text())
        files = manifest.get("shards") or manifest["tables"]
        for item in files.values():
            data = (manifest_path.parent / item["file"]).read_bytes()
            assert _sha(data) == item["sha256"], item["file"]
    client = OntologyPacks(str(release_packs))
    assert client.lookup("ICD10CM", ["I50.23"])["I50.23"].ancestors == ("I50.2", "I50")
    pcs = client.lookup("ICD10PCS", ["0DTJ4"])["0DTJ4"]
    assert pcs.description.startswith("Medical and Surgical @ Gastrointestinal System")
    assert client.lookup("ICD9CM", ["401.9"])["401.9"].ancestors == ("401",)
    assert client.lookup("ICD9Proc", ["39.61"])["39.61"].ancestors == ("39.6", "39")
    if "LOINC" in index["vocabularies"]:
        assert client.lookup("LOINC", ["2160-0"])["2160-0"].description


def test_report_counts_only_new_same_table_parents(
    packs: Path, tmp_path: Path, capsys
) -> None:
    rules = {
        "schema": "sumeds-rules/0.1",
        "hierarchy": [
            {
                "id": "paths",
                "pattern": r"^(?P<prefix>DX//)(?P<path>.+)$",
                "group": "path",
                "separator": "|",
            }
        ],
    }
    source = tmp_path / "codes.parquet"
    pl.DataFrame(
        {
            "code": ["DX//a", "DX//a|b", "DX//a|c"],
            "parent_codes": [None, ["DX//a"], None],
        }
    ).write_parquet(source)
    enrich_file(
        source,
        tmp_path / "out.parquet",
        EnrichmentConfig(rules=rules, ontology_source=str(packs)),
        verbose=True,
    )
    out = capsys.readouterr().out
    assert "paths [separator]: 1 parents added to 1 codes" in out
    assert "Same-dataset parent links:  1 -> 2" in out
    assert "Ontology files used" not in out
    result = pl.read_parquet(tmp_path / "out.parquet")
    assert result["parent_codes"].to_list() == [None, ["DX//a"], ["DX//a"]]
