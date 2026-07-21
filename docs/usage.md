# Usage

## Expected dataset layout

```text
MEDS_ROOT/
├── data/**/*.parquet
└── metadata/
    ├── dataset.json
    ├── codes.parquet
    └── subject_splits.parquet  # required only with --per-split
```

Every event shard is validated against the official MEDS `DataSchema` from its
Parquet footer. Code and split metadata use `CodeMetadataSchema` and
`SubjectSplitSchema`. The tool rejects duplicate metadata keys, missing declared
code modifiers, and incomplete split mappings. Event codes absent from metadata
are retained with null descriptive fields, which supports older datasets while
making the metadata gap visible. It does not run expensive whole-dataset
conformance checks such as event order.

## CLI

```bash
uv run suMEDS MEDS_ROOT -o code-summary.{parquet,csv,json} [OPTIONS]
```

| Option | Meaning |
|---|---|
| `-o`, `--output PATH` | Required `.parquet`, `.csv`, or `.json` output |
| `-c`, `--config PATH` | YAML configuration |
| `--per-split`, `--no-per-split` | Override split grouping |
| `--min-subjects N` | Minimum unique subjects for an unmasked row |
| `--rare-code-action bucket\|drop` | Combine or omit rare rows |
| `--rare-code-label TEXT` | Sentinel used for the bucket |
| `--round-counts-to N` | Round released counts to a multiple |
| `--version` | Print the installed version |

CLI options override YAML values. A successful run replaces the target
atomically and prints its path. A failed run leaves an existing output untouched.
Outputs inside the source `data/` or `metadata/` directories are rejected to
prevent dataset corruption.

## Examples

Global summary with exact common-code counts:

```bash
uv run suMEDS /data/MEDS -o summary.parquet --min-subjects 20
```

Suppress rare rows rather than combining them:

```bash
uv run suMEDS /data/MEDS -o summary.parquet \
  --min-subjects 25 --rare-code-action drop
```

Compute release cells independently per official split:

```bash
uv run suMEDS /data/MEDS -o summary-by-split.parquet \
  --per-split --min-subjects 20 --round-counts-to 5
```

## Output formats

The filename suffix selects the writer:

- `.parquet`: native typed Parquet and the recommended default;
- `.csv`: standard CSV; list columns are pipe-delimited and struct columns are JSON strings;
- `.json`: one valid JSON array assembled with bounded memory;
- `.jsonl` or `.ndjson`: newline-delimited JSON for direct streaming.

JSON preserves nested metadata values. CSV necessarily flattens them; use
Parquet when exact nested types matter.

## Output columns

| Column | Type | Description |
|---|---|---|
| `split` | string | Present only for per-split output |
| `code` | string | MEDS code or the configured rare sentinel |
| `description` | string, nullable | Canonical description; null for masked rows |
| `parent_codes` | list[string], nullable | Vocabulary parents; null for masked rows |
| code metadata extensions | source types | Preserved for common rows; null for masked rows |
| `event_count` | uint64 | Event rows in the release cell |
| `subject_count` | uint64 | Distinct subjects in the release cell |
| `is_masked` | boolean | Whether the row combines rare codes |

Code modifier columns declared by `metadata/dataset.json` are part of the
counting key. This prevents distinct modified concepts from being merged.
