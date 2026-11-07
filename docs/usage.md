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
| `--loinc-hierarchy`, `--no-loinc-hierarchy` | Add LOINC Parts of the LOINC multiaxial hierarchy as parents of LOINC codes looked up in the LOINC pack; needs `--rules` and packs (disabled by default) |
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
summary table without running a summary. Passing the input as `-o` enriches
it in place; the input is replaced only once the enriched table is complete.
See [How enrichment works](enrichment.md), [Athena enrichment](athena.md), and
[Rules and ontology packs](rules.md).

`suMEDS-enrich MEDS_ROOT` enriches the dataset's `metadata/codes.parquet` in
place (see [Enrich a MEDS dataset later](#enrich-a-meds-dataset-later)). It
also takes:

| Option | Meaning |
|---|---|
| `--soft-fail` | For ETL runs: if enrichment fails, leave the table as it was, print a warning with the command to retry, record the failure in the dataset's `dataset.json`, and exit 0 |
| `--strict` | Exit with an error if enrichment fails: the default for `suMEDS-enrich`, and the way to turn off the soft failure of `suMEDS-extract` |
| `--prefetch` | Instead of enriching, download the whole ontology pack release into the cache for later `--offline` runs (see [Clusters without internet access](#clusters-without-internet-access-on-compute-nodes)) |

## Enrich inside a MEDS-Extract ETL

From MEDS-Extract 0.8.1, a spec can run suMEDS enrichment as a pipeline stage.
suMEDS registers the `sumeds_enrich` stage (in the `MEDS_transforms.stages`
entry-point group), and a dataset's MESSY spec declares it after the canonical
pipeline:

```yaml
etl:
  dataset_name: eICU
  post_stages:
    - sumeds_enrich:
        rules: eICU # a registered name or a path; default: the dataset name
```

An ETL that declares the stage needs only `meds-extract-run`, with suMEDS
installed beside MEDS-Extract. It needs no custom runner and no
`suMEDS-extract`:

```bash
meds-extract-run spec=eICU output_dir=/data/eicu_meds do_download=false input_dir=/data/eicu
meds-extract-run spec=eICU output_dir=/data/eicu_meds skip_stages=[sumeds_enrich]  # without enrichment
```

After the canonical pipeline, the stage enriches the finished
`metadata/codes.parquet` in place and records the enrichment in
`metadata/dataset.json` (see
[Recorded in dataset.json](enrichment.md#recorded-in-datasetjson)). Its options
are the `suMEDS-enrich` options, spelled with underscores:

| Option | Meaning |
|---|---|
| `rules` | Registered rules name or rules file. Default: the rules registered under the dataset name, unless an Athena source is set |
| `athena_csv`, `athena_postgres` | Athena download, as `--athena-csv` and `--athena-postgres` |
| `ontology_source`, `ontology_ref`, `offline`, `packs`, `loinc_hierarchy` | Ontology pack options, as for `suMEDS-enrich` |
| `parent_codes`, `child_codes`, `sibling_codes`, `child_depth`, `exclude_self_parent_code` | Hierarchy options, as for `suMEDS-enrich` |
| `verbose` | Print the coverage and rule report to the pipeline log (default: enabled) |
| `strict` | Fail the run if enrichment fails (default: `false`) |

Unset options keep the `suMEDS-enrich` defaults. A spec that sets an option the
stage does not declare is rejected before the pipeline starts.

The stage soft-fails like `suMEDS-extract` (below). If enrichment fails,
`codes.parquet` stays as MEDS-Extract wrote it, `dataset.json` records
`"enriched": false`, and the run succeeds. The warning, with the command that
finishes the enrichment, is in `OUTPUT_DIR/.logs/pipeline.log`. That command is
`suMEDS-enrich OUTPUT_DIR` (see
[Enrich a MEDS dataset later](#enrich-a-meds-dataset-later)). MEDS-Extract
marks the stage done either way, so a later `meds-extract-run` does not try
again. To run the stage itself again, delete `OUTPUT_DIR/.logs/sumeds_enrich.done`
and run the same command; enriching the enriched cohort again gives the same
`codes.parquet`. With `strict: true`, a failed enrichment fails the run instead.

## Run an ETL and enrich it with suMEDS-extract

For a spec that does not declare the stage, or for MEDS-Extract before 0.8.1,
`suMEDS-extract` runs a [MEDS-Extract](https://github.com/mmcdermott/MEDS_extract)
ETL and then enriches the finished `metadata/codes.parquet` in place. It works
as before: ETLs that declare the `sumeds_enrich` stage need only
`meds-extract-run`, and running `suMEDS-extract` on such a spec enriches twice,
with the same result.

```bash
suMEDS-extract spec=MC-MED output_dir=/data/mc_med_meds do_download=false input_dir=/data/mc-med
# suMEDS-enrich options go after a `--`:
suMEDS-extract spec=MC-MED output_dir=/data/mc_med_meds -- --athena-csv /path/to/athena
```

Every argument before the `--` is passed to `meds-extract-run` unchanged;
`spec=` and `output_dir=` are required. Every argument after it is a
`suMEDS-enrich` option. Without `--rules`, the rules registered under the
spec's name are used (see
[Registering rules in a package](rules.md#registering-rules-in-a-package)); a
spec given as a path needs an explicit `--rules`. The arguments and the rules
are checked before the pipeline starts. If `meds-extract-run` fails, its exit
code is returned and nothing is enriched.

suMEDS does not depend on MEDS-Extract: it runs the `meds-extract-run` on
`PATH`, so install both in the same environment (an ETL package such as
`MC-MED-MEDS` brings MEDS-Extract with it). Running the command again on an
existing output reuses the stages MEDS-Extract has already finished and
enriches again. The enrichment is recorded in the output's
`metadata/dataset.json` (see
[Recorded in dataset.json](enrichment.md#recorded-in-datasetjson)), and a
second run replaces that record.

A failed enrichment never fails the ETL. A site without internet access, an
unreachable Athena database, or a problem with the rules leaves
`metadata/codes.parquet` exactly as MEDS-Extract wrote it, records the failure
in `metadata/dataset.json` (`"enriched": false`), prints a warning with the
command that enriches it later, and exits 0:

```text
suMEDS-enrich: warning: enrichment failed and /data/mc_med_meds/metadata/codes.parquet was left as it was; recorded as not enriched in /data/mc_med_meds/metadata/dataset.json: could not download https://raw.githubusercontent.com/rvandewater/sumeds-ontologies/v0.2.0/ontologies/index.json: <urlopen error [Errno 8] nodename nor servname provided, or not known>
suMEDS-enrich: warning: once the cause is fixed, enrich it with:
    suMEDS-enrich /data/mc_med_meds
```

Pass `-- --strict` to make a failed enrichment an error (exit code 1) instead.
The arguments and the rules are still checked before the pipeline starts, so a
mistake there stops before anything runs. A custom ETL runner that calls the
enrichment itself gets the same behavior from
`sumeds.cli.enrich_main([codes, "-o", codes, "--rules", NAME, "--soft-fail"])`.

## Enrich a MEDS dataset later

```bash
suMEDS-enrich /data/mc_med_meds
```

Given a MEDS dataset root, `suMEDS-enrich` enriches its
`metadata/codes.parquet` in place, with the rules and options recorded in
its `metadata/dataset.json` by an earlier run, whether that run failed or
succeeded. The result and the updated record are the same as those of a
successful ETL run. Options given on the command line override the record;
without a record, pass `--rules` or an Athena source. The record never holds
a path, so an Athena download (`--athena-csv`, `--athena-postgres`), a rules
file that no package registers (`--rules FILE`), or a local pack source
(`--ontology-source DIR`) must be given again; the warning of the failed run
lists them, and `suMEDS-enrich` says which one is missing.

A site without internet access can copy a release of the
[`sumeds-ontologies`](https://github.com/rvandewater/sumeds-ontologies) packs
over and enrich from that directory:

```bash
suMEDS-enrich /data/mc_med_meds --ontology-source /media/sumeds-ontologies/ontologies
```

## Clusters without internet access on compute nodes

On an HPC cluster whose login nodes have internet access but whose compute
nodes do not, fill a cache on shared storage once, from a login node:

```bash
export XDG_CACHE_HOME=/shared/project/cache
suMEDS-enrich --prefetch --rules eICU
```

`--prefetch` downloads every pack, hierarchy and crosswalk table of the pinned
ontology release (or of `--ontology-source` and `--ontology-ref`) into
`$XDG_CACHE_HOME/sumeds/ontologies/`, verifies each file against its checksum,
and prints where it put them. The release is a few tens of megabytes. Files
already cached are kept, so running it again downloads nothing new. With
`--rules`, it also checks that the release has every pack and crosswalk table
the rules use.

Then run on the compute nodes with the same `XDG_CACHE_HOME` and `offline`, so
nothing tries the network:

```yaml
etl:
  post_stages:
    - sumeds_enrich:
        rules: eICU
        offline: true
```

or `suMEDS-enrich ... --offline`, and `suMEDS-extract ... -- --offline`.
`suMEDS-enrich --prefetch --offline` checks that the cache is complete without
downloading anything. The pinned release makes no request once its files are
cached, even without `offline`. With `offline`, a missing file fails at once,
and the stage soft-fails instead of waiting on a network timeout. The cache
directory for a source is itself a complete pack directory, so it can also be
copied elsewhere and passed as `--ontology-source DIR`. The record then names
the source `local`.

## Output formats

The filename suffix selects the writer:

- `.parquet`: native typed Parquet and the recommended default. `code`,
  `description`, and `parent_codes` are written with the MEDS types (`string`,
  `list<string>`), so an enriched `codes.parquet` passes
  `meds.CodeMetadataSchema.validate`. Names maps are `map<string, string>`
  (`pl.Map` in Polars 2, a list of `key`/`value` structs in Polars 1);
- `.csv`: standard CSV; list columns are pipe-delimited, names maps are JSON
  objects, and struct columns are JSON strings;
- `.json`: one valid JSON array assembled with bounded memory; names maps are
  lists of `key`/`value` objects, as in NDJSON;
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
| `parent_names`, `child_names`, `sibling_names` | map[string, string], nullable | Entry of the matching list -> its name; entries with no known name are left out (JSON objects in CSV) |
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
