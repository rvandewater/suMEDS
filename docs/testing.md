# Testing an ETL that uses suMEDS

An ETL package that declares suMEDS's post-stages (`sumeds_link_unit_siblings`,
`sumeds_normalize_units`, `sumeds_enrich`) and registers suMEDS rules makes the
same promises as every other such ETL. `sumeds.testing` holds the pieces its
tests need, so each ETL does not keep its own copy:

- `sumeds.testing` writes a local ontology source and reads a MEDS output. It
  needs only suMEDS's own dependencies.
- `sumeds.testing.contracts` holds the contract tests as three pytest base
  classes, `RulesContract`, `EnrichmentContract` and `UnitComponentContract`.
  It needs pytest, and `UnitComponentContract` also dftly
  (`pip install "sumeds[testing]"`, or pytest in the ETL's dev dependencies
  next to MEDS-Extract, which brings dftly).

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
from sumeds.testing.contracts import EnrichmentContract, RulesContract, UnitComponentContract

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


class TestUnitComponents(UnitComponentContract):
    spec = "ALOTT"  # the registered spec name (or a path to the spec)
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

`UnitComponentContract`: see [Unit components](#unit-components) below.

Apart from the unit component, the contracts do not render code templates from
a spec: how a code is built from raw columns differs per ETL, so example codes
stay in the ETL's own tests.

## Unit components

A code whose last component is a unit (`LAB//<item>//<unit>`) carries that unit
stripped of surrounding whitespace, and `UNK` where the row records none or a
blank one, while the event's `_metadata._self.unit` stays the raw column.
MEDS-Extract compares units stripped and drops blank ones when it writes
`codes.parquet`, so a code that kept the whitespace would split one unit over
several codes (` mg`, `mg`, `mg `), and a blank unit would give a code ending
in whitespace (or in `//`) with no unit. An ETL builds the component with a
`_table.cols` helper:

```yaml
labevents:
  _table:
    cols:
      _value_unit: '(extract /(?s)\S(?:.*\S)?/ from $valueuom) ?? "UNK"'
  lab:
    code: f"LAB//{$itemid}//{$_value_unit}"
    _metadata:
      _self:
        unit: $valueuom
```

`UnitComponentContract` checks this for every such code of a spec; the
subclass names only the spec:

```python
from sumeds.testing.contracts import UnitComponentContract

class TestUnitComponents(UnitComponentContract):
    spec = "MIMIC-IV"  # a registered name, a path to the spec, or the parsed spec
```

**Discovery.** A table's unit columns are the raw columns its events'
`_metadata` `unit` expressions read (`_self` or an external metadata table). An
event's code ends in a unit when, with that column set to a probe value and
every other column the code reads to `"1"`, the code ends in `//` and the probe,
and the code reads the column through a `_table.cols` helper, or reads it
itself while a `_self.unit` of the table passes it through unchanged. So
`f"LAB//{$itemid}//{$_value_unit}"` counts, and so does an event of the same
table without unit metadata of its own; `f"INTAKE_OUTPUT//{$cellpath}"`, whose
unit is extracted from inside `cellpath`, does not, and neither does a code
whose unit is a constant. `find_unit_components(spec)` returns what it finds.

**Checks**, one test per event (named `table/event`), each evaluating the
spec's own expressions with dftly as MEDS-Extract does (the helpers first, in
`_table.cols` order) on the units `mg`, ` mg`, `mg `, `\tmg\n`, `mL / h`,
`  `, the empty string and null:

- `test_unit_component_is_stripped_and_blank_is_unk`: the codes end in `mg`
  four times, `mL / h`, then `UNK` three times, after the same prefix;
- `test_unit_metadata_stays_the_raw_column`: for events whose `_self.unit`
  reads the column, the unit stays as recorded (` mg` stays ` mg`); a blank one
  may be left out (null), and is never `UNK`;
- `test_no_code_ends_in_a_raw_unit_column`: no code ends in the unit column as
  recorded. A new ETL that writes `f"LAB//{$item}//{$unit}"` with
  `_self.unit: $unit` fails here, with the event named;
- `test_unit_components_are_found`: every candidate could be evaluated, and
  at least one code ends in a unit (or exactly `expected_unit_events`).

**Overrides**, as class attributes, for what discovery cannot decide:

| Attribute | Does |
|---|---|
| `expected_unit_events` | the `table/event` ids discovery must find, to pin it (`()` for a spec without unit components) |
| `unit_events` | `{"table/event": "raw unit column"}` for events discovery misses |
| `ignored_events` | `table/event` ids to leave out |
| `dummy_values` | `{column: value}` where a code needs a particular value to evaluate, such as `$count > 0` (`{"count": 1}`) |
| `require_unit_in_code` | `True` to require that every row unit is in the code; see below (default `False`) |

### Every row unit in the code

Some ETLs put the unit in the code wherever a row records one, so every code
has one unit by construction: MIMIC-IV, ALOTT and HiRID. Such an ETL opts in:

```python
class TestUnitComponents(UnitComponentContract):
    spec = "MIMIC-IV"
    require_unit_in_code = True
```

`test_row_units_are_in_the_code` then checks every event whose
`_metadata._self.unit` reads a column of the row. Its code must end in that
unit; discovery finds it as a unit component, and the checks above verify its
stripping helper. The test passes over:

- a **fixed** unit, which reads no column (MIMIC-IV's
  `SUBJECT_WEIGHT_AT_INFUSION` with `'kg'`);
- a unit **derived** from what the code carries. The code reads every column
  the unit reads, and no code gets two units when each such column takes the
  probes above plus `MG` and `kg` (units compared stripped, as in
  `codes.parquet`). MIMIC-IV's `omr`, whose unit is extracted from
  `result_name`, and the code is `result_name`, passes. So does MC-MED's
  `VITAL//{$Measure}//{$Source}`, whose unit is chosen by `Measure`;
- a unit from an **external** metadata table (another `_metadata` prefix, such
  as HiRID's `hirid_variable_reference`): one row per item, so one unit per
  code already;
- an event in `ignored_events`, a list or a dict of reasons:
  `ignored_events = {"table/event": "why"}`. Such an event is also left out of
  the unit-component checks.

It fails, naming the event and why, when the code does not read a column the
unit reads (`f"LAB//{$itemid}"` with `unit: $valueuom`), or reads it but loses
the unit (only in a condition, or case-folded), so one code can carry several
units. The message says what to do: end the code in the unit through a
stripping helper, or list the event in `ignored_events` with the reason. An
event no probe gives a code for (a code condition the dummy `"1"` fails) also
fails, until `dummy_values` makes the condition hold.

Where the code carries the column matters only for the unit component: the
derived case does not ask for the unit column in a non-final component, since
MIMIC-IV's `omr` code and eICU's `INTAKE_OUTPUT//{$cellpath}` carry the
column their unit comes from as their last component.

`check_units_in_code(spec)` returns the verdict on every such event
(`in code`, `fixed`, `derived`, `ignored`, `not in code`, `undecided`), to
see which events an ETL would have to change before it opts in.
