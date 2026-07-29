# Athena enrichment

suMEDS can fill missing code metadata from an OHDSI Athena vocabulary downloaded
as CSV files or loaded into PostgreSQL. Enrichment is optional and uses no
patient-level fields.

## Resolved fields

For each valid Athena match, suMEDS adds or fills:

| Column | Type | Source |
|---|---|---|
| `description` | string | `concept.concept_name` |
| `parent_codes` | list[string] | direct valid ancestors |
| `vocabulary_id` | string | `concept.vocabulary_id` |
| `concept_id` | int64 | numeric OMOP concept identifier |
| `concept_code` | string | vocabulary-local code |
| `domain_id` | string | OMOP domain |
| `standard_concept` | string | `S`, `C`, or null |

Existing non-null values are preserved. Invalid and unmatched concepts remain
null. Parents use `VOCABULARY//CONCEPT_CODE` and only
`min_levels_of_separation = 1`; the complete transitive ancestor set is not
copied into the output. Please check the [Athena search portal](https://athena.ohdsi.org/search-terms/start) to confirm.

## Code parsing

The original MEDS code is never changed. These layouts are supported:

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
  --athena-csv /vocabularies/athena
```

Polars projects only the required columns. Header matching is case-insensitive.

## PostgreSQL

The PostgreSQL source requires `psql` on `PATH` and `concept` plus
`concept_ancestor` in the connection's search path. Requests are sent in one
CSV `COPY` batch rather than one query per code.

```bash
PGPASSWORD=... uv run suMEDS /data/MEDS -o summary.parquet \
  --athena-postgres postgresql://postgres@127.0.0.1:5432/omop
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
before/after coverage for descriptions, parent codes, concept IDs, vocabulary,
domain, and standard-concept fields.

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
