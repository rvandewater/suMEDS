# Metadata enrichment

suMEDS can fill missing code metadata from two kinds of sources, alone or
together. Enrichment is optional and uses no patient-level fields.

- **OHDSI Athena**, downloaded as CSV files or loaded into PostgreSQL: licensed,
  every vocabulary, obtained by each user.
- **Rules over public ontology packs**: a `sumeds-rules/0.1` YAML file maps
  dataset codes to vocabulary codes, validated against sharded packs that
  suMEDS hosts and fetches on demand (ICD-10-CM, ICD-10-PCS, ICD-9-CM, ICD-9
  procedures, LOINC), and adds parent codes that exist in the same table. See
  [Rules and ontology packs](#rules-and-ontology-packs).

With both, Athena is consulted first and the packs fill what it did not match.

## Resolved fields

For each valid Athena match, suMEDS adds or fills:

| Column | Type | Source |
|---|---|---|
| `description` | string | `concept.concept_name` |
| `parent_codes` | list[string] | all valid ancestors through the root (default) |
| `child_codes` | list[string] | valid descendants up to `child_depth` (optional) |
| `sibling_codes` | list[string] | valid direct children of every ancestor, excluding self (optional) |
| `vocabulary_id` | string | `concept.vocabulary_id` |
| `concept_id` | int64 | numeric OMOP concept identifier |
| `concept_code` | string | vocabulary-local code |
| `domain_id` | string | OMOP domain |
| `standard_concept` | string | `S`, `C`, or null |

Existing relationship lists are merged without duplicates. Parent references
using `VOCABULARY/CONCEPT_CODE` are normalized to
`VOCABULARY//CONCEPT_CODE`. By default, a parent reference used to resolve the
matched concept itself is removed; retain it with
`--no-exclude-self-parent-code` or `exclude_self_parent_code: false`. Newly
added codes must resolve to a valid row in the same Athena `CONCEPT` source.
Invalid, missing, self-referential, and cyclic hierarchy rows are ignored.
Unmatched concepts pass through unchanged.
Please check the [Athena search portal](https://athena.ohdsi.org/search-terms/start) to confirm.

Parent expansion includes every positive `min_levels_of_separation` through the
root and is enabled by default. Disable it with `--no-parent-codes` or
`parent_codes: false`. Child and sibling expansion are opt-in. Children include
transitive descendants through `child_depth` (default `3`); siblings are the
direct children of each valid ancestor, excluding the matched concept itself.

## Rules and ontology packs

### Rules file

Pass a rules file with `--rules PATH` (both `suMEDS` and `suMEDS-enrich`),
`enrichment.rules` in YAML (relative to the YAML file), or
`EnrichmentConfig(rules=...)` (a path, a mapping, or `load_rules(...)`). The
format is strict: unknown keys, a wrong `schema`, invalid regular expressions,
missing named groups, unknown group references in `replace`, and duplicate ids
are errors.

```yaml
schema: sumeds-rules/0.1            # required

ontology:                            # regex to vocabulary code; tried in order
  - id: icd10cm-diagnoses            # required, unique across all rules
    pattern: '^DIAGNOSIS//ICD//10//(?P<code>[A-Z][0-9][0-9A-Z][0-9A-Z]{0,4})$'
    vocabulary: ICD10CM              # OMOP vocabulary_id
    normalize:                       # optional ordered re.sub steps on `code`
      - {pattern: '^(...)(.+)$', replace: '\1.\2'}
    validate: pack                   # pack (default) | athena | none

crosswalk:                           # table lookup to vocabulary code; after ontology rules
  - id: nwicu-lab-loinc
    pattern: '^LAB//(?P<key>[0-9]+)//'   # named group `key`
    table: nwicu/loinc_parents           # crosswalks/<dataset>/<name>.csv at the source
    key_column: itemid
    code_column: loinc_code
    vocabulary: LOINC
    where: {source_table: labevents}     # optional: column: value or [values]
    normalize: []                        # optional, applied to code_column values
    validate: pack                       # pack (default) | athena | none

hierarchy:                           # parents that are codes of the same table
  - id: eicu-diagnosis-paths
    pattern: '^(?P<prefix>DIAGNOSIS//)(?P<path>.+)$'
    group: path                      # a named group of `pattern`
    separator: '|'                   # mode: separator (implied by `separator`)
    nearest_only: true               # default true
  - id: icd-prefix-in-dataset
    pattern: '^(?P<prefix>DIAGNOSIS//ICD10_CN//)(?P<code>.+)$'
    group: code
    mode: prefix
    ignore: '.'                      # optional characters skipped when truncating
    nearest_only: true
  - id: ontology-parents-in-dataset
    from_ontology: true
    rules: [icd10cm-diagnoses]       # optional: only codes resolved by these rules
    nearest_only: true
```

Patterns are Python regular expressions applied with `re.search`, so anchor
them with `^` and `$`.

**Ontology and crosswalk rules.** A matching code yields a vocabulary code: the
`code` group after `normalize`, or every `code_column` value of the crosswalk
rows whose `key_column` equals the `key` group (and that pass `where`). These
candidates rank before the built-in `VOCABULARY//CODE` layouts below, in file
order (ontology rules, then crosswalk rules); the first one that validates
wins. `validate` decides what may validate it:

| `validate` | Athena configured | No Athena |
|---|---|---|
| `pack` | Athena first; else the public pack | the public pack |
| `athena` | Athena only | rule skipped (reported) |
| `none` | Athena first; else tag only | tag only |

A validated code gets `description` (only where null), `vocabulary_id` and
`concept_code` (and the Athena-only fields when Athena validated it), and
`parent_codes` = the vocabulary code itself followed by its ancestors through
the root, as `VOCABULARY//CODE` (pack ancestors nearest first). `child_codes`
and `sibling_codes` follow the same options and semantics as for Athena. When
Athena validates a rule's code, Athena's scalar fields win and the pack's
hierarchy is merged in, because Athena's `CONCEPT_ANCESTOR` has no hierarchy
for non-standard vocabularies such as ICD-10-CM. A tag (`validate: none`) adds
only the vocabulary code to `parent_codes`, with no description. A `pack` rule
for a vocabulary without a pack fails with the list of available packs; codes
missing from the pack or from Athena get nothing, so no dangling code is added.
If a crosswalk key maps to several codes, Athena keeps the lowest concept id,
a pack the first code in sort order, and a tag lists them all.

**Hierarchy rules** add parents that are codes of the same (released) table,
appended after the vocabulary parents, so the first parent stays the
vocabulary reference that the parent-reference fallback relies on. They never
add a code that is not in the table, never the code itself, and only when
`parent_codes` is enabled.

- `separator`: drop the last `separator`-delimited segment of `group` (and
  then the next one) and keep the rebuilt code if it exists:
  `DIAGNOSIS//a|b|c` gets `DIAGNOSIS//a|b`, or `DIAGNOSIS//a` if only that
  exists.
- `prefix`: drop the last character of `group` repeatedly, skipping `ignore`
  characters, so `I50.23` tries `I50.2`, then `I50`, `I5`, `I`. This serves
  vocabularies without a public pack (WHO ICD-10, ICD-10-CN).
- `from_ontology`: a code whose resolved vocabulary code is an ancestor of
  this code's resolved vocabulary code (from a pack or Athena) becomes a
  parent, e.g. `DIAGNOSIS//ICD//10//I5023` gets `DIAGNOSIS//ICD//10//I502`.
- `nearest_only: true` (default) adds only the closest existing ancestor;
  `false` adds every existing ancestor, nearest first.

Existing metadata is never overwritten: descriptions and scalar fields are
filled only where null, and relationship lists are merged without duplicates.

### Packs and crosswalks

| Pack | Release | Codes | Source | License |
|---|---|---:|---|---|
| `ICD10CM` | `fy2016-fy2027` | 99,085 | CDC/NCHS order files, FY2016 to FY2027 (retired codes kept, with `first_year`/`last_year`) | public domain |
| `ICD10PCS` | `fy2016-fy2027` | 197,789 | CMS order files and code tables, FY2016 to FY2027, including every 1- to 6-character prefix described from the tables' axis labels | public domain |
| `ICD9CM` | `v32` | 17,541 | CMS v32 (final) plus NCHS FY2012 tabular headers | public domain |
| `ICD9Proc` | `v32` | 4,648 | as ICD9CM | public domain |
| `LOINC` | `2.83` | 105,050 | LOINC Table Core 2.83, original fields unchanged, all statuses, `MapTo.csv` | LOINC license |

Codes use the OMOP `concept_code` format (`I50.23`, `401.9`, `39.61`,
`0DTJ4ZZ`, `2160-0`), so rules normalize dataset codes to it. ICD-10-PCS
prefixes are described by their axis labels joined with ` @ `, e.g. `0DTJ4` is
`Medical and Surgical @ Gastrointestinal System @ Resection @ Appendix @
Percutaneous Endoscopic`. LOINC descriptions are `LONG_COMMON_NAME`.

Crosswalk tables live next to the packs (`crosswalks/eicu/lab_loinc`,
`crosswalks/hirid/observation_loinc`, `crosswalks/nwicu/loinc_parents`,
`crosswalks/zigong/nursing_vitals_loinc`), so ETL repositories no longer ship
them. A dataset that already carries a vocabulary column, such as the LOINC
codes of SICdb or PICdb, can drop ETL-side well-formedness checks of that
column and validate its codes with an ontology rule (`validate: pack`, or
`athena`). The packs, crosswalks and their builder live in the separately
versioned data repository [`sumeds-ontologies`](https://github.com/rvandewater/sumeds-ontologies); its
[pack README](https://github.com/rvandewater/sumeds-ontologies/tree/v0.1.0/ontologies) documents the format, sizes, sources,
and rebuild commands.

### Fetching, caching, offline use

At enrichment time suMEDS determines which vocabularies and shards the matched
codes need and fetches only those (plus `index.json` and the manifests). The
source is, in order of precedence:

1. `--ontology-source URL_OR_DIR` or `enrichment.ontology_source`: an
   `http(s)://` base URL, a `file://` URL, or a local directory;
2. the `SUMEDS_ONTOLOGY_SOURCE` environment variable;
3. `https://raw.githubusercontent.com/rvandewater/sumeds-ontologies/<ref>/ontologies`,
   with `<ref>` from `--ontology-ref` or `enrichment.ontology_ref`.

The default `<ref>` is a release tag of `sumeds-ontologies` (`v0.1.0` in this
suMEDS version), and suMEDS pins the sha256 of that release's `index.json`.
Because the index pins every manifest and each manifest every shard, the
default enrichment is reproducible byte for byte: if the tag were ever moved,
the run fails with a checksum error instead of silently changing results. A
`--ontology-ref` that is neither a release tag (`vX.Y.Z`) nor a full commit
sha prints a warning. Each suMEDS release states which data release it pins.

Every manifest is checked against the sha256 in `index.json` and every shard
or table against its manifest; a mismatch is an error. Remote files are cached
under `${XDG_CACHE_HOME:-~/.cache}/sumeds/ontologies/<source>/`. For the pinned
default release everything is reused from the cache; for other sources the
index and manifests are refreshed on each online run, and shards are reused
while their checksum matches. `--offline` (`enrichment.offline: true`) uses only the cache
or a local source and fails with a clear message if a needed file is missing.

```bash
uv run suMEDS-enrich metadata/codes.parquet -o codes-enriched.parquet \
  --rules rules.yaml                     # public packs only
uv run suMEDS-enrich metadata/codes.parquet -o codes-enriched.parquet \
  --rules rules.yaml --athena-csv /vocabularies/athena   # Athena first, then packs
uv run suMEDS /data/MEDS -o summary.parquet --rules rules.yaml \
  --ontology-source ../sumeds-ontologies/ontologies --offline --verbose
```

### Licenses and governance

- Only vocabularies whose license allows redistribution are hosted:
  ICD-10-CM, ICD-10-PCS, and ICD-9-CM are U.S. Government works in the public
  domain; LOINC is redistributed under the LOINC license, with its full text
  and the notice below in the pack folder and in every crosswalk folder.
- Licensed vocabularies that may not be redistributed (SNOMED CT, CPT4, WHO
  ICD-10, ICD-10-CN, ICD-O-3, ATC, and others) are never hosted. Validate them
  against your own Athena download (`validate: athena`) or tag them
  (`validate: none`).
- suMEDS is the single maintained copy of LOINC content for the MEDS ETLs. Its
  maintainers rebuild the LOINC pack from the official download for each LOINC
  release (twice a year) and record the release, date, and checksums. Users
  fetch it and never need a LOINC account. Crosswalk tables carry the LOINC
  `LONG_COMMON_NAME` of each code verbatim (`loinc_long_common_name`), filled
  from the pack at build time, never from Athena, whose names can be truncated.

> This material contains content from LOINC (http://loinc.org). LOINC is
> copyright © Regenstrief Institute, Inc. and the Logical Observation
> Identifiers Names and Codes (LOINC) Committee and is available at no cost
> under the license at http://loinc.org/license. LOINC® is a registered United
> States trademark of Regenstrief Institute, Inc.

### Coverage report

`suMEDS-enrich` prints the report by default (`--no-verbose` silences it);
`suMEDS --verbose` prints it after a summary. Besides the before/after field
coverage it lists, for each ontology and crosswalk rule, the codes matched,
validated (by Athena or a pack), and tagged; for each hierarchy rule the
same-dataset parents added; the number of same-dataset parent links before and
after; and the pack files used and downloaded. It reports counts only.

## Code parsing

The original MEDS code is never changed. After any rule candidates, these
layouts are tried:

```text
VOCABULARY//CODE//...
VOCABULARY//VOCABULARY_VERSION//CODE
```

Three-part codes are ambiguous. suMEDS first looks up the second component,
which preserves codes ending in markers such as `//start` and `//end`. It tries
the third component only when the second component has no valid match. If
neither matches, the first non-null existing `parent_codes` reference is tried
as `VOCABULARY/CODE` or `VOCABULARY//CODE`. This supports metadata such as the
MIMIC-IV demo, whose event codes are local while parent references identify
Athena concepts. Malformed and unmatched codes pass through unchanged.

## Local Athena files

Point to an Athena download directory containing at least the original
tab-delimited `CONCEPT.csv` and `CONCEPT_ANCESTOR.csv` files:

```bash
uv run suMEDS /data/MEDS -o summary.parquet \
  --athena-csv /vocabularies/athena \
  --child-codes --child-depth 3 --sibling-codes
```

Polars projects only the required columns. Header matching is case-insensitive.

## PostgreSQL

The PostgreSQL source requires `psql` on `PATH` and `concept` plus
`concept_ancestor` in the connection's search path. Requests are sent in one
CSV `COPY` batch rather than one query per code.

```bash
PGPASSWORD=... uv run suMEDS /data/MEDS -o summary.parquet \
  --athena-postgres postgresql://postgres@127.0.0.1:5432/omop \
  --child-codes --child-depth 3 --sibling-codes
```

`psql` inherits the current process environment. Supply the password as the
standard libpq variable `PGPASSWORD`, or use `PGPASSFILE`/`~/.pgpass`.
`POSTGRES_PASSWORD` configures the Docker container but is not read by `psql`.
The demo notebook safely copies an existing `POSTGRES_PASSWORD` environment
value to `PGPASSWORD` without displaying it. Restart the notebook kernel if the
environment was changed after Jupyter started.

## Standalone enrichment

Enrich an existing MEDS metadata table or suMEDS output without scanning event
data:

```bash
uv run suMEDS-enrich /data/MEDS/metadata/codes.parquet \
  -o codes-enriched.parquet --athena-csv /vocabularies/athena
```

Parquet, CSV, JSON, JSONL, and NDJSON are supported. Input and output must be
different paths, and the destination is replaced atomically. The CLI displays
a `tqdm` phase-progress bar followed by row and unique-code match counts plus
before/after coverage for descriptions, relationship codes, concept IDs,
vocabulary, domain, and standard-concept fields.

## Interactive demo

Open `examples/athena_enrichment_demo.ipynb` for a hands-on walkthrough using
individual codes and the included MIMIC-IV demo metadata. The notebook supports
either source and keeps the full summary step opt-in.

## Example output

```bash
$ uv run suMEDS-enrich summary.parquet --athena-postgres postgresql://postgres@127.0.0.1:5432/omop -o summary_enriched.parquet
Calculating coverage: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████| 4/4 [02:38<00:00, 39.59s/stage]

Enrichment report
  Rows processed:             118,088
  Unique codes:               118,088
  Athena matches added:      65,358 rows, 65,358 unique codes
  Rows with concept ID:       65,367 (55.4%)
  Rows without Athena match:  52,721 (44.6%)
  Metadata coverage (present before -> after):
    Descriptions                     9 ->     65,367 (55.4%) (+65,358 filled)
    Parent-code lists                9 ->     61,021 (51.7%) (+61,012 filled)
    Vocabulary IDs                   9 ->     65,367 (55.4%) (+65,358 filled)
    OMOP concept IDs                 9 ->     65,367 (55.4%) (+65,358 filled)
    Concept codes                    0 ->     65,366 (55.4%) (+65,366 filled)
    Domains                          0 ->     65,366 (55.4%) (+65,366 filled)
    Standard-concept flags           0 ->     61,169 (51.8%) (+61,169 filled)
```

## Privacy ordering

Integrated summary enrichment runs after rare-code masking. PostgreSQL receives
only released code strings and the configured rare label; masked source codes
are not queried or restored. Standalone enrichment has no privacy filtering and
should be applied only to an already approved table when release controls are
required.
