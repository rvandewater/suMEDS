# meds-summary

`meds-summary` creates a compact code catalog for defining tasks on very large
[Medical Event Data Standard (MEDS)](https://github.com/Medical-Event-Data-Standard/meds)
datasets. It joins canonical descriptions and parent codes to event and unique-subject
counts, then masks or removes rare codes before writing Parquet, CSV, or JSON.

Polars scans, aggregations, joins, and writes stay lazy. Patient-level rows are never
collected into Python memory.

## Install and run

```bash
uv sync
uv run meds-summary MIMICIV_DEMO/MEDS_cohort \
  --output code-summary.parquet \
  --min-subjects 20
```

Or use YAML:

```yaml
summary:
  per_split: false
privacy:
  min_subjects: 20
  rare_code_action: bucket
  rare_code_label: __RARE__
  round_counts_to: 5
```

```bash
uv run meds-summary /path/to/MEDS -o summary.parquet -c examples/summary.yaml
# The suffix selects CSV or JSON instead:
uv run meds-summary /path/to/MEDS -o summary.json -c examples/summary.yaml
```

CLI flags override YAML. The defaults bucket codes seen in fewer than 20 unique
subjects and retain exact counts.

## Python API

```python
from meds_summary import SummaryConfig, summarize

path = summarize(
    "/path/to/MEDS",
    "code-summary.parquet",
    SummaryConfig(min_subjects=20, rare_code_action="bucket"),
)
```

The output format is inferred from `.parquet`, `.csv`, or `.json` (with
`.jsonl`/`.ndjson` also supported). It preserves all code-metadata extension columns and adds
`event_count`, `subject_count`, and `is_masked`. With `per_split=True`, it also
adds `split` using `metadata/subject_splits.parquet`.

## Documentation

```bash
uv run mkdocs serve
uv run mkdocs build --strict
```

See the [documentation site](docs/index.md) for the data flow, complete CLI and
YAML reference, output schema, Python API, and privacy limitations.

## Development

```bash
uv run pytest
uv run mkdocs build --strict
```

The included integration test uses `MIMICIV_DEMO/MEDS_cohort` when present.
