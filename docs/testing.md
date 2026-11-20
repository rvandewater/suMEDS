# Testing an ETL that uses suMEDS

An ETL package that declares suMEDS's post-stages (`sumeds_link_unit_siblings`,
`sumeds_normalize_units`, `sumeds_enrich`) and registers suMEDS rules makes the
same promises as every other such ETL. `sumeds.testing` holds the pieces its
tests need, so each ETL does not keep its own copy:

- `sumeds.testing` writes a local ontology source and reads a MEDS output. It
  needs only suMEDS's own dependencies.
- `sumeds.testing.contracts` holds the contract tests as two pytest base
  classes, `RulesContract` and `EnrichmentContract`. It needs pytest
  (`pip install "sumeds[testing]"`, or pytest in the ETL's dev dependencies).

Both are versioned with suMEDS: an ETL's `sumeds>=X,<Y` pin also pins the
contract it is tested against.

## A local ontology source

`write_ontology_source(root, packs, crosswalks=...)` writes a tiny pack
repository in the `sumeds-ontologies/0.1` layout of the
[published packs](rules.md), with sha256 checksums, so suMEDS reads and
verifies it like the real one and nothing goes over the network.

```python
from sumeds.testing import Pack, write_ontology_source

source = write_ontology_source(
    tmp_path / "ontologies",
    {
        # code -> parent (None for a top-level code); release "fy2016-fy2027"
        "ICD10CM": ("fy2016-fy2027", {"I50": None, "I50.2": "I50", "I50.23": "I50.2"}),
        # codes without parents; release "test"
        "ICD10PCS": ["5A1955Z"],
        # like the published LOINC pack: no parent column, the LOINC Table's column names
        "LOINC": Pack(
            ["2160-0"],
            release="2.83",
            parents=False,
            fields={"code": "LOINC_NUM", "description": "LONG_COMMON_NAME"},
            shard_key_length=2,
        ),
    },
    crosswalks={
        # a rule reads this as `table: nwicu/loinc_parents`
        "nwicu": {"loinc_parents": [{"itemid": "900001", "source_table": "labevents", "loinc_code": "2160-0"}]},
    },
)
```

Every code is described as `Synthetic {vocabulary} title {code}`
(`synthetic_title`), unless `Pack(titles={...})` names it. Without packs or
crosswalks, `write_ontology_source(root)` writes an empty source, which lets
string hierarchy rules run offline.

Pass the source to the stage as
`stage_overrides(ontology_source=source)` (a `meds-extract-run` argument), to
`suMEDS-enrich --ontology-source`, or to `EnrichmentConfig(ontology_source=...)`.

## Helpers

| Helper | Does |
|---|---|
| `run_cli(command, *args, env=None, ok=True)` | runs an installed console script from the environment pytest runs in, with that environment first on the child's `PATH`, so `meds-extract-run` never picks up another repository's scripts; asserts success (or failure with `ok=False`) and shows the output otherwise |
| `stage_overrides(**options)` | `overrides=['++stage_cfg.name=value',...]`, which sets stage options for one `meds-extract-run` |
| `read_codes(output_dir)` | `metadata/codes.parquet` as rows by code, with `parent_names` as a dict |
| `read_events(output_dir)` | every data shard, concatenated |
| `dataset_metadata(output_dir)` | `metadata/dataset.json`, validated against the MEDS schema |
| `enrichment_record(output_dir)` | the `sumeds_enrichment` record of `dataset.json`, or `None` |
| `names_map(value)` | a `parent_names` value as a dict, whether Polars 1, Polars 2 or pyarrow read it |
| `load_spec(name)` / `registered_resource(group, name)` | the spec or rules file an installed package registers |

## Contract tests

Subclass the contracts in the ETL's test suite and set what differs as class
attributes. Each test can still be overridden, and each fixture replaced.

```python
from sumeds.testing.contracts import EnrichmentContract, RulesContract

class TestRules(RulesContract):
    rules_name = "ALOTT"  # the name of both the spec and the rules entry point
    own_post_stages = ()  # the ETL's own post-stages, before sumeds_enrich
    # code -> (ontology rule id, vocabulary code); other example codes must match no rule
    expected_ontology = {"DIAGNOSIS//ICD//10//I50.23": ("alott-icd10cm-diagnoses", "I50.23")}
    # code -> (crosswalk rule id, key)
    expected_crosswalk_keys = None
    # string hierarchy rule id -> {code: parents}
    expected_hierarchy = None


class TestEnrichment(EnrichmentContract):
    rules_name = "ALOTT"
    packs = {"ICD10CM": ("fy2016-fy2027", {"I50": None, "I50.2": "I50", "I50.23": "I50.2"})}
    crosswalks = {}
    expected_parents = {
        "DIAGNOSIS//ICD//10//I50.23": ["ICD10CM//I50.23", "ICD10CM//I50.2", "ICD10CM//I50"],
    }
    expected_descriptions = {"DIAGNOSIS//ICD//10//I50.23": "Synthetic ICD10CM title I50.23"}
    unenriched_codes = ["DIAGNOSIS//ICD//10//I10", "FLOWSHEET//8"]
```

`EnrichmentContract` needs a `raw_dir` fixture of scope `class` or wider (the
synthetic raw input) and MEDS-Extract. `spec` (a `spec=` value other than
`rules_name`) and `etl_args` (more `meds-extract-run` arguments, such as
`--column-format` workarounds) adjust the runs.

`RulesContract` tests the examples named in its expectations. To test the codes
the ETL actually writes, or codes rendered from the spec, override the
`example_codes` fixture:

```python
class TestRules(RulesContract):
    rules_name = "MC-MED"
    expected_ontology = {...}

    @pytest.fixture(scope="class")
    @classmethod
    def example_codes(cls, codes):  # the ETL's own fixture with the plain codes.parquet
        return codes["code"].to_list()
```

### What the contracts check

`RulesContract`:

- the rules file is in the installed package and registered in `sumeds.rules`
  under `rules_name`, where `load_rules(rules_name)` finds it, and a spec is
  registered in `MEDS_extract.pipelines` under the same name;
- the spec's `etl.post_stages` are `sumeds_link_unit_siblings`,
  `sumeds_normalize_units`, `own_post_stages`, then `sumeds_enrich` with these
  rules (by `rules:` or, without it, by `etl.dataset_name`);
- the rules load in suMEDS, and every pattern is anchored (`^...$`);
- each example matches at most one ontology rule and one crosswalk rule, the
  matches are exactly `expected_ontology` and `expected_crosswalk_keys`, and
  every such rule is exercised;
- each string hierarchy rule, run alone and offline over the examples, adds
  exactly the parents of `expected_hierarchy`.

`EnrichmentContract` runs `meds-extract-run spec=NAME` with
`skip_stages=[sumeds_enrich]` (the plain ETL) and with the stage, once per
class, and checks that:

- the enriched `codes.parquet` is valid MEDS code metadata, `parent_names` is
  a `map<string, string>`, and no temporary file is left behind;
- the data equal the plain ETL's (`subject_id`, `time`, `code`);
- enrichment only adds: every code keeps its description and unit, and any
  parents it had lead the new list;
- each code of `expected_parents` has exactly those parents, every parent from
  a test pack is named with its title, `expected_descriptions` hold, and
  `unenriched_codes` keep the plain description and parents;
- `dataset.json` records the enrichment: `enriched`, the rules name, the
  `local` source, no Athena, and the coverage counts of the plain and enriched
  tables;
- the plain build ran no `sumeds_enrich` and records nothing;
- without packs (`SUMEDS_ONTOLOGY_SOURCE` at an empty directory) the run
  succeeds with the plain `codes.parquet`, a failure record, and the retry
  command in the pipeline log; running the ETL
  again does not retry, and `suMEDS-enrich OUT --ontology-source DIR` then
  gives the same table and record as a direct run;
- `strict=true` makes a failed enrichment fail the run;
- `offline=true` with an empty cache fails at once, without the network.

The contracts do not render code templates from a spec: how a code is built
from raw columns differs per ETL, so example codes stay in the ETL's own tests.
