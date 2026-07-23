# suMEDS

[![PyPI - Version](https://img.shields.io/pypi/v/suMEDS)](https://pypi.org/project/suMEDS/)
[![codecov](https://codecov.io/gh/rvandewater/suMEDS/graph/badge.svg?token=RW6JXHNT0W)](https://codecov.io/gh/rvandewater/suMEDS)
[![tests](https://github.com/rvandewater/suMEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/rvandewater/suMEDS/actions/workflows/tests.yml)
[![code-quality](https://github.com/rvandewater/suMEDS/actions/workflows/code-quality.yaml/badge.svg)](https://github.com/rvandewater/suMEDS/actions/workflows/code-quality-main.yaml)
[![Python Version](https://img.shields.io/pypi/pyversions/suMEDS.svg)](https://pypi.python.org/pypi/suMEDS/)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/rvandewater/suMEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/rvandewater/suMEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/rvandewater/suMEDS.svg)](https://github.com/rvandewater/suMEDS/graphs/contributors)
[![DOI](https://zenodo.org/badge/940565218.svg)](https://doi.org/10.5281/zenodo.19398854)
[![MEDS v0.4](https://img.shields.io/badge/MEDS-0.4-blue)](https://medical-event-data-standard.github.io/)

`suMEDS` (pronounced "summed-s", past tense of "sum" + s) creates a compact code catalog for performing tasks
[Medical Event Data Standard (MEDS)](https://github.com/Medical-Event-Data-Standard/meds)
datasets. MEDS data should be "summed" in this way in a storage-efficient format, without the need to access the full dataset.


It can be seen as a form of "extended metadata," as we sometimes need more information than vanilla MEDS metadata provides, but don't want to expose our complete dataset (either because of privacy concerns, storage capacity, or token use efficiency for LLMs).
It (currently) simply joins canonical descriptions and parent codes to the event and the unique-subject
counts, then masks or removes rare codes before writing Parquet, CSV, or JSON.

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

## Python API

```python
from sumeds import SummaryConfig, summarize

path = summarize(
    "/path/to/MEDS",
    "code-summary.parquet",
    SummaryConfig(min_subjects=20, rare_code_action="bucket"),
)
```

The output format is inferred from `.parquet`, `.csv`, or `.json` (with
`.jsonl`/`.ndjson` also supported). It preserves all code-metadata extension columns and adds
`event_count`, `subject_count`, and `is_masked`. `per_split=True` emits one row
per split. `split_columns=True` keeps the total row and adds columns such as
`event_count_train` and `subject_count_train` from `subject_splits.parquet`.
Set `min_split_subjects` above 1 to suppress rare split-level cells.

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
