# suMEDS

[![PyPI - Version](https://img.shields.io/pypi/v/suMEDS)](https://pypi.org/project/suMEDS/)
[![codecov](https://codecov.io/gh/rvandewater/suMEDS/graph/badge.svg?token=RW6JXHNT0W)](https://codecov.io/gh/rvandewater/suMEDS)
[![tests](https://github.com/rvandewater/suMEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/rvandewater/suMEDS/actions/workflows/tests.yml)
[![code-quality](https://github.com/rvandewater/suMEDS/actions/workflows/code-quality.yaml/badge.svg)](https://github.com/rvandewater/suMEDS/actions/workflows/code-quality-main.yaml)
[![Python Version](https://img.shields.io/pypi/pyversions/suMEDS.svg)](https://pypi.python.org/pypi/suMEDS/)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/rvandewater/suMEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/rvandewater/suMEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/rvandewater/suMEDS.svg)](https://github.com/rvandewater/suMEDS/graphs/contributors)
[![DOI](https://zenodo.org/badge/1307476724.svg)](https://doi.org/10.5281/zenodo.21647050)
[![MEDS v0.4](https://img.shields.io/badge/MEDS-0.4-blue)](https://medical-event-data-standard.github.io/)

`suMEDS` (pronounced "summed-s", past tense of "sum" + s) creates a compact code catalog for performing tasks
[Medical Event Data Standard (MEDS)](https://github.com/Medical-Event-Data-Standard/meds)
datasets. MEDS data should be "summed" in this way in a storage-efficient format, without the need to access the full dataset.


It can be seen as a form of "extended metadata," as we sometimes need more information than vanilla MEDS metadata provides, but don't want to expose our complete dataset (either because of privacy concerns, storage capacity, or token use efficiency for LLMs).
It joins canonical descriptions and configurable parent, child, and sibling
codes to event and unique-subject counts, then masks or removes rare codes
before writing Parquet, CSV, or JSON.

> [!WARNING]
> This package is in early development and is human-guided, but AI-agent-generated. It is not yet reviewed for production use. Please report issues and suggestions.
>


Polars scans, aggregations, joins, and writes stay lazy. This should mean that it could run with little resources on large MEDS datasets.

## Install and run
Normal install:

```bash
pip install sumeds
```

Datasets with more than 2³² rows require Polars' 64-bit runtime:

```bash
pip install "sumeds[rt64]"
```

### Development
```bash
uv sync
uv run suMEDS tests/resources/MIMICIV_DEMO/MEDS_cohort \
  --output code-summary.parquet \
  --min-subjects 20 \
  --split-columns
```

Or use YAML:

```yaml
summary:
  per_split: false
  split_columns: true
  partitions: 256
privacy:
  min_subjects: 20
  min_split_subjects: 1
  rare_code_action: bucket
  rare_code_label: __RARE__
  round_counts_to: 5
```

```bash
uv run suMEDS /path/to/MEDS -o summary.parquet -c examples/summary.yaml
# The suffix selects CSV or JSON instead:
uv run suMEDS /path/to/MEDS -o summary.json -c examples/summary.yaml
```

CLI flags override YAML. The defaults bucket codes seen in fewer than 20 unique
subjects and retain exact counts.

Optionally fill missing descriptions, all ancestors, OMOP concept IDs, domains,
and standard-concept markers from OHDSI Athena. Child and sibling expansion are
opt-in:

```bash
uv run suMEDS /path/to/MEDS -o summary.parquet \
  --athena-csv /path/to/athena --child-codes --child-depth 3 --sibling-codes
# Or use PostgreSQL through the installed psql client:
uv run suMEDS /path/to/MEDS -o summary.parquet \
  --athena-postgres postgresql://postgres@localhost/omop
```

Standalone enrichment avoids rescanning event data:

```bash
uv run suMEDS-enrich metadata/codes.parquet -o codes-enriched.parquet \
  --athena-csv /path/to/athena
```

The standalone command shows phase progress and reports Athena matches and
before/after metadata coverage.

The `VOCABULARY//CODE//...`, `VOCABULARY//VOCABULARY_VERSION//CODE` and
OMOP-style `VOCABULARY/CODE` layouts are resolved, and so is a code's first
existing parent reference (`ICD9CM/428.0`). Every existing parent reference
gets its name and its ancestors. See
[How enrichment works](docs/enrichment.md) for the steps and a worked example,
and [Athena enrichment](docs/athena.md) for what is read from Athena, how ICD
parents are added, and how Athena compares with the packs.

### Rules and public ontology packs

Without Athena, or in addition to it, a `sumeds-rules/0.1` file maps dataset
codes to vocabulary codes and adds parents that exist in the same table:

```yaml
schema: sumeds-rules/0.1
ontology:
  - id: icd10cm-diagnoses
    pattern: '^DIAGNOSIS//ICD//10//(?P<code>[A-Z][0-9][0-9A-Z][0-9A-Z]{0,4})$'
    vocabulary: ICD10CM
    normalize: [{pattern: '^(...)(.+)$', replace: '\1.\2'}]
    validate: pack            # pack | athena | none
crosswalk:
  - id: nwicu-lab-loinc
    pattern: '^LAB//(?P<key>[0-9]+)//'
    table: nwicu/loinc_parents
    key_column: itemid
    code_column: loinc_code
    vocabulary: LOINC
hierarchy:
  - {id: eicu-diagnosis-paths, pattern: '^(?P<prefix>DIAGNOSIS//)(?P<path>.+)$', group: path, separator: '|'}
  - {id: icd-prefix, pattern: '^(?P<p>DIAGNOSIS//ICD10_CN//)(?P<code>.+)$', group: code, mode: prefix, ignore: '.'}
  - {id: ontology-parents-in-dataset, from_ontology: true}
```

```bash
uv run suMEDS-enrich metadata/codes.parquet -o codes-enriched.parquet --rules rules.yaml
```

Codes are validated against ontology packs kept in a separately versioned
data repository, [`sumeds-ontologies`](https://github.com/rvandewater/sumeds-ontologies). suMEDS fetches them shard by shard
on demand from a pinned release tag (default `v0.2.0`, with the sha256 of its
index pinned in suMEDS), verifies every file, and caches it; `--offline` and
`--ontology-source` (a mirror, or packs you rebuilt yourself from the official
sources) are supported. Release `v0.2.0` holds ICD-10-CM and ICD-10-PCS (FY2016
to FY2027), ICD-9-CM and ICD-9 procedures (v32), which CDC/NCHS and CMS publish
without a license agreement (ICD-10-CM and ICD-9-CM adapt WHO classifications,
and WHO keeps the copyright in content taken from them), and LOINC 2.83 with
its multiaxial hierarchy under the [LOINC license](https://github.com/rvandewater/sumeds-ontologies/blob/v0.2.0/ontologies/LOINC/2.83/LoincLicense_5.8.txt).
Dataset crosswalk tables (for example lab item ids to LOINC) live there as well.
Every added parent comes with its name in `parent_names`, as the LOINC license
asks for LOINC codes; LOINC Parts as parents are opt-in (`--loinc-hierarchy`).
With an Athena download, `--no-packs` validates rule codes against Athena only.
See [Rules and ontology packs](docs/rules.md).
Vocabularies that may not be redistributed or whose terms are unknown (SNOMED
CT, CPT4, WHO ICD-10, ATC, ...) are validated only through your own Athena
download.

This material contains content from LOINC (http://loinc.org). LOINC is
copyright © Regenstrief Institute, Inc. and the Logical Observation
Identifiers Names and Codes (LOINC) Committee and is available at no cost under
the license at http://loinc.org/license. LOINC® is a registered United States
trademark of Regenstrief Institute, Inc.

## Python API

```python
from sumeds import EnrichmentConfig, SummaryConfig, summarize

path = summarize(
    "/path/to/MEDS",
    "code-summary.parquet",
    SummaryConfig(
        min_subjects=20,
        rare_code_action="bucket",
        enrichment=EnrichmentConfig(csv_dir="/path/to/athena"),
    ),
)
```

The output format is inferred from `.parquet`, `.csv`, or `.json` (with
`.jsonl`/`.ndjson` also supported). It preserves all code-metadata extension columns and adds
`event_count`, `subject_count`, and `is_masked`. `per_split=True` emits one row
per split. `split_columns=True` keeps the total row and adds columns such as
`event_count_train` and `subject_count_train` from `subject_splits.parquet`.
Set `min_split_subjects` above 1 to suppress rare split-level cells.
`numeric_stats=True` (CLI `--numeric-stats`) adds per-code `numeric_value`
statistics: count, min, max, mean, median, std, and a confidence interval, or
any subset such as `numeric_stats=["mean", "std"]`.

## Documentation

```bash
uv run mkdocs serve
uv run mkdocs build --strict
```

See the [documentation site](docs/index.md) for the data flow, complete CLI and
YAML reference, output schema, Python API, and privacy limitations.

## Development

```bash
uv run pre-commit run --all-files
uv run pytest
uv run mkdocs build --strict
```

The included integration test uses `tests/resources/MIMICIV_DEMO/MEDS_cohort` when present.
