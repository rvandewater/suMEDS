# Usage

## Expected dataset layout

```text
MEDS_ROOT/
├── data/**/*.parquet
└── metadata/
    ├── dataset.json
    ├── codes.parquet
    └── subject_splits.parquet  # required by either split-output mode
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
| `--per-split`, `--no-per-split` | Emit one row per split instead of totals |
| `--split-columns`, `--no-split-columns` | Add per-split columns alongside totals |
| `--partitions N` | Temporary subject partitions used to bound memory |
| `--min-subjects N` | Minimum unique subjects for an unmasked total/row |
| `--min-split-subjects N` | Minimum subjects for a visible wide split cell |
| `--rare-code-action bucket\|drop` | Combine or omit rare rows |
| `--rare-code-label TEXT` | Sentinel used for the bucket |
| `--round-counts-to N` | Round released counts to a multiple |
| `--numeric-stats [STATS]` | Summarize `numeric_value`; bare flag adds all stats, or pass e.g. `min,max,mean` or `none` |
| `--numeric-ci-level P` | Confidence level for `numeric_ci_*` (default `0.95`) |
| `--athena-csv DIR` | Enrich released codes from local Athena files |
| `--athena-postgres CONNINFO` | Enrich released codes through `psql` |
| `--parent-codes`, `--no-parent-codes` | Enable/disable all-ancestor expansion (enabled by default) |
| `--child-codes`, `--no-child-codes` | Enable/disable descendant expansion (disabled by default) |
| `--sibling-codes`, `--no-sibling-codes` | Enable/disable children-of-ancestors expansion (disabled by default) |
| `--exclude-self-parent-code`, `--no-exclude-self-parent-code` | Remove/retain the existing parent reference used as the lookup (retained by default; removing is the only option that deletes existing metadata) |
| `--child-depth N` | Maximum descendant depth (default `3`, range `1`–`100`) |
| `--rules PATH` | `sumeds-rules/0.1` file: ontology, crosswalk and same-table hierarchy rules |
| `--ontology-source URL_OR_DIR` | Pack repository URL, `file://` URL, or directory (default `$SUMEDS_ONTOLOGY_SOURCE`, else GitHub) |
| `--ontology-ref REF` | Release tag of the default pack repository, `sumeds-ontologies` (default: the pinned `v0.2.0`; other refs warn) |
| `--offline`, `--no-offline` | Use only cached or local packs (disabled by default) |
| `--packs`, `--no-packs` | Fetch packs and crosswalk tables (enabled by default); `--no-packs` needs an Athena source, validates rule codes against Athena only, and skips crosswalk rules |
| `--loinc-hierarchy`, `--no-loinc-hierarchy` | Add LOINC Parts of the LOINC multiaxial hierarchy as parents of LOINC codes resolved by a `validate: pack` rule (disabled by default) |
| `--verbose` | Print the enrichment coverage and rule report |
| `--version` | Print the installed version |

CLI options override YAML values. A successful run replaces the target
atomically and prints its path. A failed run leaves an existing output untouched.
Outputs inside the source `data/` or `metadata/` directories are rejected to
prevent dataset corruption. Runs create projected event partitions beside the
output and remove them afterward; ensure that filesystem has sufficient free
space. Install `sumeds[rt64]` for datasets exceeding 2³² rows.

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

Keep total counts and add one event/subject column pair per split:

```bash
uv run suMEDS /data/MEDS -o summary-wide.parquet \
  --split-columns --min-subjects 20

# Optional independent split-cell suppression:
uv run suMEDS /data/MEDS -o summary-wide-private.parquet \
  --split-columns --min-subjects 20 --min-split-subjects 10
```

The two split-output modes are mutually exclusive.

Optional enrichment accepts at most one local or PostgreSQL Athena source
and/or a `--rules` file validated against the public ontology packs. Use
`suMEDS-enrich INPUT -o OUTPUT` (the same source, hierarchy, and ontology
flags; `--no-verbose` hides its report) to enrich an existing metadata or
summary table without running a summary. See
[How enrichment works](enrichment.md), [Athena enrichment](athena.md), and
[Rules and ontology packs](rules.md).

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
| `parent_codes` | list[string], nullable | Existing values plus valid Athena or pack ancestors through root by default, then same-table parents from hierarchy rules |
| `child_codes` | list[string], nullable | Existing values plus optional Athena or pack descendants |
| `sibling_codes` | list[string], nullable | Existing values plus optional children of Athena or pack ancestors |
| `parent_names`, `child_names`, `sibling_names` | list[string], nullable | Name of each entry of the matching list, aligned with it; null where unknown (JSON arrays in CSV) |
| code metadata extensions | source types | Preserved for common rows; null for masked rows |
| `vocabulary_id` | string, nullable | Matched Athena or pack vocabulary when enrichment is enabled |
| `concept_id` | int64, nullable | Numeric OMOP concept identifier |
| `concept_code` | string, nullable | Vocabulary-local code (Athena or pack) |
| `domain_id` | string, nullable | OMOP domain |
| `standard_concept` | string, nullable | OMOP standard-concept marker |
| `event_count` | uint64 | Event rows in the release cell |
| `subject_count` | uint64 | Distinct subjects in the release cell or total |
| `event_count_<split>` | uint64, nullable | Split event count; null below `min_split_subjects` |
| `subject_count_<split>` | uint64, nullable | Split subject count; null below `min_split_subjects` |
| `numeric_count` | uint64, nullable | Finite `numeric_value` rows; present when `numeric_stats` is set |
| `numeric_min`, `numeric_max`, `numeric_mean`, `numeric_median`, `numeric_std` | float64, nullable | Requested statistics of finite `numeric_value` |
| `numeric_ci_lower`, `numeric_ci_upper` | float64, nullable | Normal-approximation confidence interval of the mean |
| `is_masked` | boolean | Whether the row combines rare codes |

Code modifier columns declared by `metadata/dataset.json` are part of the
counting key. This prevents distinct modified concepts from being merged.
