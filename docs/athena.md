# Athena enrichment

[OHDSI Athena](https://athena.ohdsi.org) distributes the OMOP standardized
vocabularies. suMEDS can use a download of them, read directly from its CSV
files or from a PostgreSQL database that holds it, to describe codes, add OMOP
concept identifiers and domains, and add ancestors, descendants and siblings.
Athena is optional: without it, [rules](rules.md) validate codes against the
public ontology packs only. This page explains what suMEDS reads from Athena,
how it matches codes, and what it adds; [How enrichment
works](enrichment.md) covers the steps shared by every source.

## Getting the vocabularies

1. Sign in at [athena.ohdsi.org](https://athena.ohdsi.org), select the
   vocabularies your codes use (for example ICD10CM, ICD9CM, ICD9Proc,
   ICD10PCS, LOINC, SNOMED, RxNorm), and download the bundle. Athena shows the
   license terms of each vocabulary; some need a license from their owner
   before they are included.
2. Unzip the bundle into a directory. suMEDS reads two of its files, as
   downloaded (tab-delimited): `CONCEPT.csv` and `CONCEPT_ANCESTOR.csv`. It
   does not read `CONCEPT_RELATIONSHIP.csv` or any other file.
3. CPT4 arrives without names. The bundle's `readme.txt` explains how to fill
   them with `cpt.sh` and a UMLS API key.

suMEDS never ships or uploads Athena content. An enriched table, however,
contains the codes and names of your Athena vocabularies, so share it only as
their licenses allow. For example, CPT4 is licensed by the American Medical
Association.

## Choosing the source

Set at most one Athena source. It can be combined with a rules file.

### CSV directory

```bash
uv run suMEDS-enrich metadata/codes.parquet -o codes-enriched.parquet \
  --athena-csv /vocabularies/athena
uv run suMEDS /data/MEDS -o summary.parquet \
  --athena-csv /vocabularies/athena
```

In YAML, use `enrichment.csv_dir`. A relative path is resolved against the
YAML file. Polars scans the two files lazily and reads only the columns it
needs. Header matching is case-insensitive. Scanning a full download takes a
few GB of memory: about 5 GB at peak for the 6,481-code example in [How
enrichment works](enrichment.md#worked-example).

### PostgreSQL

The PostgreSQL source runs one batch through `psql`, which must be on `PATH`.
The tables `concept` and `concept_ancestor` must be in the connection's search
path. All codes are sent in one CSV `COPY`, not one query per code.

```bash
PGPASSWORD=... uv run suMEDS /data/MEDS -o summary.parquet \
  --athena-postgres postgresql://postgres@127.0.0.1:5432/omop \
  --child-codes --child-depth 3 --sibling-codes
```

In YAML, use `enrichment.postgres`. `psql` inherits the current process
environment. Supply the password as the standard libpq variable `PGPASSWORD`,
or use `PGPASSFILE` or `~/.pgpass`. `POSTGRES_PASSWORD` configures the Docker
container but is not read by `psql`.

The repository's `compose.yaml` starts PostgreSQL 17 on `127.0.0.1:5432`
(database `omop`) and loads an Athena download from `./athena` into it with
[omop-lite](https://github.com/Health-Informatics-UoN/omop-lite). Set
`POSTGRES_PASSWORD` before `docker compose up`.

The demo notebook copies an existing `POSTGRES_PASSWORD` value to
`PGPASSWORD` without displaying it. Restart the notebook kernel if the
environment changed after Jupyter started.

## How a code is matched

Athena receives the candidates that [How enrichment
works](enrichment.md#how-a-code-is-enriched) lists for each code. These are
rule codes, the built-in `VOCABULARY//CODE` layouts, and the first existing
parent reference. A candidate matches a `CONCEPT` row with the same
`vocabulary_id` and `concept_code` and an empty `invalid_reason`. Standard,
classification and non-standard concepts all match; retired and replaced
concepts do not. For example, the July 2026 download marks 1,849 ICD-10-CM
concepts invalid, so those codes are not matched. The best-ranked matching
candidate wins. If it matches several concepts, the lowest `concept_id` wins.

The comparison is exact, so the code must be in OMOP's `concept_code` format:

- ICD codes keep their dot (`I50.23`, `428.0`, `39.61`). A dataset code such
  as `DIAGNOSIS//ICD//10//I5023` needs an [ontology rule](rules.md#rules-file)
  that inserts the dot.
- The built-in layouts split codes on `//`. A code with a single slash, such as
  `LOINC/2160-0`, is not a layout. Only its first parent reference is tried, and
  if that reference is another concept (say, its LOINC Part), the code is
  matched as that concept. Give such codes an ontology rule (below).
- Only the first non-null parent reference is tried. In the eICU example, a
  diagnosis code with the parents `ICD9CM/428.40` and `ICD10CM/I50.40` is
  matched as ICD-9-CM `428.40`. Its ICD-10-CM reference is kept but not looked
  up.

A rule for OMOP-style single-slash LOINC codes:

```yaml
schema: sumeds-rules/0.1
ontology:
  - id: omop-loinc
    pattern: '^LOINC/(?P<code>.+)$'
    vocabulary: LOINC
    validate: athena
```

## What Athena adds

| Column | From |
|---|---|
| `description` | `concept.concept_name`, only where `description` is null |
| `vocabulary_id`, `concept_id`, `concept_code`, `domain_id`, `standard_concept` | the matched `concept` row, only where null |
| `parent_codes` | ancestors from `CONCEPT_ANCESTOR`; for ICD codes, also the valid shorter codes ([below](#icd-codes)) |
| `child_codes` | descendants up to `child_depth` levels (opt-in) |
| `sibling_codes` | the direct children of every ancestor, except the code itself (opt-in) |
| `parent_names`, `child_names`, `sibling_names` | `concept_name` of each related concept, aligned with its list |

Related codes are written as `VOCABULARY//CONCEPT_CODE`. A related concept must
itself be a valid row of your `CONCEPT` file. When a code is matched through a
parent reference, it takes on that concept: it gets the concept's name as its
`description` (if it has none) and the concept's identifiers. This lets
datasets whose codes are local strings, like the MIMIC-IV demo or eICU, get
Athena fields. A dataset that ships its own descriptions keeps them.

Athena's `concept_name` holds at most 255 characters. In the July 2026
download, 1,141 CPT4 names, 1,067 RxNorm names and 1 LOINC name have exactly
255 characters, so they are probably truncated. The LOINC pack uses the
official LOINC names, which are not truncated.

## Hierarchy from `CONCEPT_ANCESTOR`

`CONCEPT_ANCESTOR` is the transitive closure of OMOP's hierarchical
relationships. suMEDS keeps its rows with `min_levels_of_separation > 0`. It
drops rows that link a concept to itself and pairs that also appear in reverse
(cycles).

- **Parents** are every remaining ancestor, through the root, sorted by code.
  The closure has no single path, so this order is not nearest first.
- **Children** are the descendants with `min_levels_of_separation` up to
  `child_depth` (default `3`).
- **Siblings** are the concepts one level below any ancestor, without the
  code itself. They include children of grandparents and of higher ancestors,
  not only of the direct parent.

The closure crosses vocabularies and goes beyond "is a". In the eICU example,
the LOINC lab test `2160-0` (Creatinine [Mass/volume] in Serum or Plasma) gets
65 Athena ancestors:

- 29 LOINC Parts of the multiaxial hierarchy;
- 4 LOINC groups;
- 6 LOINC classes (`CHEM`, `PANEL.CHEM`, ...);
- 20 panels and forms that contain the test (`24320-4` Basic metabolic 1998
  panel, `96789-3` SOFA, ...);
- 6 SNOMED CT procedure concepts, among them the creatinine measurement.

With the same LOINC code, the LOINC pack and `--loinc-hierarchy` give the 7
Parts on its path in the LOINC component hierarchy instead.

Which vocabularies have ancestors in the July 2026 download (ancestor rows of
the vocabulary's valid concepts, by the ancestor's vocabulary):

| Vocabulary | Valid concepts | With ancestors | Ancestor vocabularies |
|---|---:|---:|---|
| ICD10CM | 98,186 | 0 | none |
| ICD9CM | 17,552 | 0 | none |
| ICD9Proc | 4,657 | 2,213 | SNOMED (43,442 rows), ICD9Proc (2,101) |
| LOINC | 234,951 | 147,471 | LOINC (1,065,543), SNOMED (203,974), OMOP Extension (717) |
| SNOMED | 412,033 | 348,952 | SNOMED, ICD9Proc, OMOP Extension |
| RxNorm | 213,854 | 184,906 | SPL, ATC, RxNorm, RxNorm Extension, SNOMED |
| CPT4 | 17,369 | 13,278 | SNOMED, CPT4, ICD9Proc, OMOP Extension, LOINC |

## ICD codes

ICD-10-CM and ICD-9-CM concepts are non-standard, and in the July 2026
download not one of them has a `CONCEPT_ANCESTOR` row. Their hierarchy exists
only as `Is a` rows in `CONCEPT_RELATIONSHIP`, which suMEDS does not read.

So for matches in ICD10CM, ICD9CM, ICD9Proc and ICD10PCS, suMEDS also adds
every shorter code as a parent, as long as that code is a valid concept of the
same vocabulary in your `CONCEPT` source. A shorter code drops trailing
characters and a trailing dot. For example, `I50.23` gets `I50.2` and `I50`,
and `428.40` gets `428.4` and `428`. These parents are looked up in a second
batch and named like any other concept. They are added only when
`parent_codes` is enabled.

This was compared with the transitive `Is a` hierarchy of the July 2026
download:

| Vocabulary | Codes | Result |
|---|---:|---|
| ICD10CM | 98,186 | identical for every code |
| ICD9CM | 17,552 | identical for every code |
| ICD9Proc | 4,657 | identical for 1,054; for 3,603 the shorter codes add parents that `Is a` lacks |
| ICD10PCS | | not in that download, so not compared |

ICD-9 procedure codes that are standard also keep their `CONCEPT_ANCESTOR`
ancestors, mostly SNOMED procedures. Athena gives ICD-10-CM and ICD-9-CM codes
no children or siblings. To get them, validate the codes against the packs
(`validate: pack`), whose hierarchy suMEDS merges in.

## Athena with rules and packs

With a rules file, Athena sees every rule candidate first. When Athena
validates a rule's code:

- Athena's scalar fields are used: `description`, `concept_id`, `domain_id`
  and `standard_concept`.
- `parent_codes` starts with the vocabulary code itself (`LOINC//2160-0`),
  followed by Athena's ancestors.
- For a `validate: pack` rule, the pack's entry for the same code is merged
  in. Its ancestors, children and siblings are appended where Athena lacks
  them: ICD children and siblings, and LOINC Parts with `--loinc-hierarchy`.
  Missing names are filled.

A rule's code that Athena does not know falls back to the pack, or to a tag
(see the [`validate` table](rules.md#ontology-and-crosswalk-rules)). Crosswalk
tables always come from `sumeds-ontologies`. Athena only validates the codes
they produce.

## Athena only: `--no-packs`

`--no-packs` (`packs: false`, `EnrichmentConfig(packs=False)`) needs an Athena
source. It fetches no packs and no crosswalk tables, so nothing is downloaded:

- `validate: pack` rules are validated against Athena only.
- `validate: none` rules still tag the codes Athena does not know.
- `validate: athena` rules behave as before.
- Crosswalk rules are skipped, and the report says
  `skipped (packs disabled)`.
- `--loinc-hierarchy` has no effect. Athena gives LOINC codes their ancestors
  from `CONCEPT_ANCESTOR` instead.

Rule codes then get the same ICD parents as with the packs, with Athena's
names. The results differ only where the Athena release and the pack release
differ.

## Athena or packs?

| | Athena | Packs |
|---|---|---|
| Vocabularies | every vocabulary in your download | ICD-10-CM, ICD-10-PCS, ICD-9-CM, ICD-9 procedures, LOINC |
| Releases | those of your download (July 2026: ICD-10-CM FY2026, ICD-9 v32, LOINC 2.80) | ICD-10-CM and ICD-10-PCS FY2016 to FY2027, including retired codes; ICD-9 v32; LOINC 2.83 |
| Codes matched | rule codes, built-in layouts, first parent reference | rule codes only |
| `concept_id`, `domain_id`, `standard_concept` | yes | no |
| Names | `concept_name`, at most 255 characters | official descriptions; LOINC `LONG_COMMON_NAME` |
| ICD parents | valid shorter codes | the pack's parents (identical for ICD-10-CM and ICD-9-CM) |
| ICD children and siblings | none | yes |
| LOINC parents | every `CONCEPT_ANCESTOR` ancestor: Parts, groups, classes, panels, SNOMED | none, or with `--loinc-hierarchy` the Parts on the term's component hierarchy paths |
| Parent order | sorted by code | nearest first |
| Crosswalk tables | no | yes |
| Obtained | your own download, under the Athena terms | fetched from a pinned, checksummed release, or a local copy |

## Troubleshooting

- `Athena file not found: .../CONCEPT.csv`: `--athena-csv` must point to the
  directory that holds the unzipped files.
- `CONCEPT.csv is missing columns`: the file is not an Athena `CONCEPT` table,
  or not tab-delimited.
- `psql is required for PostgreSQL Athena enrichment`: install the PostgreSQL
  client or use `--athena-csv`.
- `no password supplied`: set `PGPASSWORD` in the environment that runs
  suMEDS. For Jupyter, set it before starting the kernel.
- Few matches: check that rules normalize codes to OMOP's `concept_code`
  format, and that your download includes the vocabulary.

> This material contains content from LOINC (http://loinc.org). LOINC is
> copyright © Regenstrief Institute, Inc. and the Logical Observation
> Identifiers Names and Codes (LOINC) Committee and is available at no cost
> under the license at http://loinc.org/license. LOINC® is a registered United
> States trademark of Regenstrief Institute, Inc.
