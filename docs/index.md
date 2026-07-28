# suMEDS

`suMEDS` scans a MEDS dataset and writes the code information needed to
discover and define downstream tasks:

- canonical code descriptions, parent codes, modifiers, and extension metadata;
- event occurrence counts;
- unique-subject occurrence counts;
- optional counts per canonical subject split, as rows or columns beside totals;
- configurable masking or removal of rare code cells;
- optional descriptions, parents, domains, and concept identifiers from OHDSI
  Athena CSV files or PostgreSQL.

## Data flow

```text
metadata/dataset.json ── code modifier declaration ─┐
data/**/*.parquet ── projected lazy scan ── counts ├─ privacy policy ── Parquet
metadata/codes.parquet ── descriptions/parents ─────┤
metadata/subject_splits.parquet ── optional split ──┤ Parquet / CSV / JSON
Athena CSV/PostgreSQL ── optional released-code enrichment ──┘
```

Only `subject_id`, `code`, declared code modifiers, and optionally `split` are
used. Polars performs projection pushdown and streaming Parquet writes; no
patient-level result is collected by Python. Standard JSON arrays are assembled
line by line, so JSON output is bounded-memory too.

## Compatibility

The package uses the official `meds` schema classes and targets MEDS 0.4.x. The
included MIMIC-IV demo identifies itself as MEDS 0.3.3 and remains compatible
with the fields used by this package.

Start with [Usage](usage.md), then review the [privacy model](privacy.md) before
sharing an output.
