# Python API

All public objects can be imported directly from `sumeds`.

## High-level API

### `summarize`

```python
summarize(
    root: str | Path,
    output: str | Path,
    config: SummaryConfig | None = None,
) -> Path
```

Validates required MEDS metadata and shard schemas, computes occurrence counts,
applies the privacy policy, and atomically writes the format selected by the
`.parquet`, `.csv`, `.json`, `.jsonl`, or `.ndjson` suffix. It returns the
absolute output path. Library callers receive the original `ValueError`,
`FileNotFoundError`, MEDS schema, or Polars exception.

```python
from sumeds import SummaryConfig, summarize

output = summarize(
    "/data/MEDS",
    "task-catalog.parquet",
    SummaryConfig(
        split_columns=True,
        min_subjects=20,
        rare_code_action="bucket",
        round_counts_to=5,
    ),
)
```

### `SummaryConfig`

```python
SummaryConfig(
    per_split: bool = False,
    split_columns: bool = False,
    partitions: int = 256,
    min_subjects: int = 20,
    min_split_subjects: int = 1,
    rare_code_action: str = "bucket",
    rare_code_label: str = "__RARE__",
    round_counts_to: int | None = None,
)
```

`SummaryConfig.from_yaml(path)` loads strict YAML.
`config.with_overrides(...)` creates an updated immutable copy and ignores
`None` values, which is useful for CLI layers.

Set `enrichment=EnrichmentConfig(csv_dir=...)`,
`EnrichmentConfig(postgres=...)`, and/or `EnrichmentConfig(rules=...)` to
enrich released summary rows. `summarize(..., verbose=True)` prints the
enrichment report.

### `EnrichmentConfig`

```python
EnrichmentConfig(
    csv_dir: Path | None = None,
    postgres: str | None = None,
    parent_codes: bool = True,
    child_codes: bool = False,
    sibling_codes: bool = False,
    child_depth: int = 3,
    exclude_self_parent_code: bool = False,
    rules: OntologyRules | str | Path | Mapping | None = None,
    ontology_source: str | None = None,
    ontology_ref: str = "v0.2.0",
    offline: bool = False,
    packs: bool = True,
    loinc_hierarchy: bool = False,
)
```

Select at most one Athena source and/or `rules` (a path, mapping, or
`OntologyRules`, parsed on construction). Without an Athena source, rules use
the public ontology packs only; `packs=False` (which needs an Athena source)
uses Athena only and skips crosswalk rules. Parents expand through the root by
default; `loinc_hierarchy=True` adds the LOINC Parts of the LOINC multiaxial
hierarchy. Children and siblings are opt-in; `child_depth` accepts `1`–`100`.
Existing relationship lists are kept verbatim and new entries appended without
duplicates; every list gets an aligned names list (`parent_names` and so on).
Newly added codes must be valid in the selected source.

### `enrich_metadata`

```python
enrich_metadata(
    frame: polars.LazyFrame,
    config: EnrichmentConfig,
) -> polars.LazyFrame
```

Adds or fills Athena and rule fields on an arbitrary lazy metadata or summary
table with a `code` column; rows whose boolean `is_masked` is true are not
looked up. Existing scalar values are preserved and relationship lists are
merged without replacement. See [How enrichment works](enrichment.md).

### `load_rules(source) -> OntologyRules`

Parses and strictly validates a `sumeds-rules/0.1` YAML file or mapping into
frozen `OntologyRule`, `CrosswalkRule`, and `HierarchyRule` objects. See
[Rules and ontology packs](rules.md).

### `OntologyPacks`

```python
OntologyPacks(
    source: str | None = None,
    ref: str = "v0.2.0",
    *,
    offline: bool = False,
    cache_dir: str | Path | None = None,
)
```

Fetches, verifies, and caches packs.
`lookup(vocabulary, codes, *, child_depth=0, siblings=False, hierarchy=False)`
returns a `PackEntry`
(`description`, `ancestors`, `children`, `siblings`, and their aligned
`ancestor_names`, `child_names`, `sibling_names`) for each code present in the
pack, fetching only the shards it needs; `hierarchy=True` adds the ancestors of
a pack's separate hierarchy (LOINC Parts); `crosswalk("dataset/name")` returns the
verified rows of a crosswalk table; `stats` counts shards used and downloaded.
Integrity and fetch errors raise `OntologyPackError`.

### `enrich_file`

```python
enrich_file(
    input_path: str | Path,
    output_path: str | Path,
    config: EnrichmentConfig,
    *,
    verbose: bool = False,
) -> Path
```

Atomically enriches a supported standalone table. Input and output paths must
differ. Parquet, CSV, JSONL, and NDJSON inputs remain lazy; standard JSON arrays
are necessarily eager because Polars provides no `scan_json` equivalent.
`verbose=True` shows phase progress and prints before/after field coverage; the
`suMEDS-enrich` CLI enables it by default. See
[How enrichment works](enrichment.md) and [Athena enrichment](athena.md) for
source and parsing details.

## Lazy building blocks

### `scan_events(root, modifiers=()) -> polars.LazyFrame`

Validates every event-shard footer and returns a projection containing only
`subject_id`, `code`, and requested modifiers.

### `scan_code_metadata(root) -> polars.LazyFrame`

Returns the canonical code metadata scan. Missing optional `description` and
`parent_codes` fields are added as typed null columns.

### `scan_subject_splits(root) -> polars.LazyFrame`

Returns canonical `subject_id` and `split` columns.

### `code_occurrences(events, keys, splits=()) -> polars.LazyFrame`

Builds lazy total and optional per-split `event_count` and `subject_count`
aggregations for arbitrary code keys. Pass split names such as
`["train", "held_out"]` when the frame has a `split` column. This low-level
function does not apply privacy filtering by itself.

### Metadata helpers

- `dataset_root(path) -> Path`
- `read_dataset_metadata(root) -> dict`
- `code_modifier_columns(metadata) -> list[str]`
- `event_files(root) -> list[Path]`

These are small public helpers for consumers that need the same validated MEDS
paths and declarations without running a complete summary.
