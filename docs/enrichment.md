# How enrichment works

Enrichment fills in missing code metadata for the codes of a MEDS metadata
table or a suMEDS summary: descriptions, vocabulary codes, parent, child and
sibling codes, and OMOP concept fields. It is optional. It reads only code
strings and existing metadata, never patient-level fields, and it never
removes or rewrites what the table already has.

It runs in two places:

- **Inside `suMEDS`.** With `--athena-csv`, `--athena-postgres` or `--rules`,
  enrichment runs after rare codes are masked, so only released codes are
  looked up.
- **Standalone.** `suMEDS-enrich INPUT -o OUTPUT` enriches an existing table
  without scanning event data. It does no privacy filtering (see [Privacy
  ordering](#privacy-ordering)).

## Sources

Two kinds of sources validate codes, alone or together:

| | OHDSI Athena | Rules and ontology packs |
|---|---|---|
| What it is | your own download of the OMOP standardized vocabularies, as CSV files or in PostgreSQL | a `sumeds-rules/0.1` file mapping dataset codes to vocabulary codes, checked against public packs |
| Vocabularies | every vocabulary in your download | ICD-10-CM, ICD-10-PCS, ICD-9-CM, ICD-9 procedures, LOINC, and dataset crosswalk tables |
| Adds | names, OMOP concept ids, domains, standard flags, ancestors from `CONCEPT_ANCESTOR` | names and the pack hierarchy; parents that are codes of the same table |
| Obtained | each user downloads it under the Athena terms | fetched on demand from a pinned release of [`sumeds-ontologies`](https://github.com/rvandewater/sumeds-ontologies), or a local copy |
| Details | [Athena enrichment](athena.md) | [Rules and ontology packs](rules.md) |

These combine into four setups:

- **Athena only**: `--athena-csv DIR` or `--athena-postgres CONNINFO`.
  Resolves codes in a `VOCABULARY//CODE` layout, and codes whose first
  existing parent is a vocabulary reference.
- **Packs only**: `--rules rules.yaml`. Resolves codes that a rule maps to a
  pack vocabulary, directly or through a crosswalk table.
- **Athena and packs**: `--rules rules.yaml --athena-csv DIR`. Resolves both
  of the above. Athena's fields win, and the pack hierarchy is merged in.
- **Athena, no packs**: `--rules rules.yaml --athena-csv DIR --no-packs`.
  Validates rule codes against Athena only. Nothing is downloaded, and
  crosswalk rules are skipped.

With neither an Athena source nor a rules file, there is no enrichment.

## How a code is enriched

Enrichment works on the unique codes of the table, in eight steps.

```text
code
  │  candidates, best first: ontology rules, crosswalk rules,
  │  VOCABULARY//CODE, VOCABULARY//VERSION//CODE, first parent
  ▼
validation: Athena CONCEPT, a pack (validate: pack), or a tag
  ▼
best match ─► fields, hierarchy and names ─► same-table parents
  ▼
merge into the table, overwriting nothing
```

1. **Select codes.** Rows marked `is_masked` (the rare-code bucket of a
   summary) are skipped. Every other unique `code` is looked up once.

2. **List candidates.** Each code yields candidate vocabulary codes, as
   `(vocabulary_id, concept_code)`, ranked in this order:
    1. every matching [ontology rule](rules.md#ontology-and-crosswalk-rules),
       in file order: the rule's `code` group after `normalize`;
    2. every matching crosswalk rule, in file order: the table's codes for the
       matched key;
    3. `VOCABULARY//CODE//...`: the first two components of the code;
    4. `VOCABULARY//VERSION//CODE`: the first and third components of a
       three-part code;
    5. the first non-null existing `parent_codes` entry, read as
       `VOCABULARY/CODE` or `VOCABULARY//CODE`.

    Rules come from the rules file. The last three layouts are built in.
    Trying the second component before the third keeps codes that end in
    markers such as `//start` and `//end` matched on their second component.
    The original code is never changed.

3. **Validate.** With Athena, every candidate is looked up in `CONCEPT` in one
   batch (valid concepts only). Codes from `validate: pack` rules are looked
   up in the pack of the rule's vocabulary, unless `--no-packs` is set. A
   `validate: none` rule tags its code without validation. A
   `validate: athena` rule needs Athena.

4. **Pick one match per code.** The best-ranked candidate that validates wins.
   On equal rank, Athena wins over a pack. A tag counts only when nothing
   validates. If an Athena candidate matches several concepts, the lowest
   `concept_id` wins.

5. **Fill fields.** The match gives `description`, `vocabulary_id` and
   `concept_code`. Athena also gives `concept_id`, `domain_id` and
   `standard_concept`. Then:
    - after a rule match, `parent_codes` starts with the vocabulary code itself
      (`LOINC//2160-0`), followed by its ancestors;
    - after a built-in layout or a parent reference, the code already names
      the vocabulary code, so only its ancestors are added.

    Ancestors come from Athena (`CONCEPT_ANCESTOR`, plus [ICD shorter
    codes](athena.md#icd-codes)), from the pack, or both, merged. Children and
    siblings are added when enabled. Each added code's name goes into the
    aligned names list (`parent_names` and so on).

6. **Add same-table parents.** [Hierarchy rules](rules.md#hierarchy-rules)
   append parents that are codes of the same table.

7. **Merge.** Results are merged into the table without overwriting
   anything; see [Merge rules](#merge-rules).

8. **Report.** Counts only; see [Coverage report](#coverage-report).

## Worked example

This example enriches the public eICU-CRD demo, built with eICU-MEDS 0.1.0.
Its `metadata/codes.parquet` has 6,481 codes and no `description` column. 860
diagnosis codes carry ICD-9-CM and ICD-10-CM references in `parent_codes`. The
sources are the July 2026 Athena download and a rules file with the eICU
lab-to-LOINC crosswalk (`eicu-lab-loinc`) and `|`-path hierarchy rules.

**A lab code**, `LAB//RESULT//creatinine//mg/dL`, is not a vocabulary code
and has no parents. Only the crosswalk rule can resolve it: the key
`creatinine` maps to LOINC `2160-0`.

- **Packs:** description "Creatinine [Mass/volume] in Serum or Plasma",
  `vocabulary_id` LOINC, `concept_code` 2160-0. `parent_codes` holds
  `LOINC//2160-0`, named.
- **Packs with `--loinc-hierarchy`:** as above, plus the 7 LOINC Parts on its
  path, nearest first ([list](rules.md#loinc-parts-as-parents)).
- **Athena and packs:** as above, plus `concept_id` 3016723, `domain_id`
  Measurement, `standard_concept` S. After `LOINC//2160-0`, `parent_codes`
  holds 65 Athena ancestors sorted by code. They include LOINC Parts, groups
  and classes, panels that contain the test, and SNOMED procedures
  ([details](athena.md#hierarchy-from-concept_ancestor)).
- **Athena, no packs:** nothing. The crosswalk table is not fetched, and the
  code is not a vocabulary layout.

**A diagnosis code**,
`DIAGNOSIS//cardiovascular|ventricular disorders|congestive heart failure|combined systolic and diastolic`,
has the existing parents `ICD9CM/428.40` and `ICD10CM/I50.40`.

- **Athena:** `DIAGNOSIS` is not a vocabulary, so the first parent reference,
  `ICD9CM/428.40`, is matched. The code gets that concept's name as its
  description ("Combined systolic and diastolic heart failure, unspecified"),
  its `concept_id` 44832381, and `domain_id` Observation. After the two
  existing entries, the parents `ICD9CM//428` (Heart failure) and
  `ICD9CM//428.4` (Combined systolic and diastolic heart failure) are
  appended.
- **Rules:** the separator hierarchy rule also appends the same-table parent
  `DIAGNOSIS//cardiovascular|ventricular disorders|congestive heart failure`.
  This holds in every setup with the rules file.
- **Packs only:** only the same-table parent. Packs are queried for rule
  candidates, not for existing parent references.

All setups on the whole table, as unique codes and parent-list entries (1,712
entries before enrichment):

| Setup | Described | With `concept_id` | Parent entries | Named | Same-table parents |
|---|---:|---:|---:|---:|---:|
| Packs | 50 | 0 | 3,127 | 50 | 1,365 |
| Packs and LOINC Parts | 50 | 0 | 3,469 | 392 | 1,365 |
| Athena only (no rules) | 860 | 860 | 2,860 | 1,148 | 0 |
| Athena and packs | 910 | 910 | 5,978 | 2,901 | 1,365 |
| Athena, no packs | 860 | 860 | 4,225 | 1,148 | 1,365 |

Every entry that enrichment adds from a vocabulary is named. The unnamed
entries are the 1,712 existing references and the same-table parents (see
[Names](#names)).

## Output fields

| Column | Type | Filled from |
|---|---|---|
| `description` | string | Athena `concept_name`, or the pack description |
| `parent_codes` | list[string] | ancestors through the root (default on) |
| `child_codes` | list[string] | descendants up to `child_depth` (opt-in) |
| `sibling_codes` | list[string] | the direct children of every ancestor, except the code itself (opt-in) |
| `parent_names`, `child_names`, `sibling_names` | list[string] | the name of each entry of the list beside it, aligned entry by entry; null where no name is known |
| `vocabulary_id` | string | the matched vocabulary |
| `concept_id` | int64 | numeric OMOP concept identifier (Athena only) |
| `concept_code` | string | vocabulary-local code |
| `domain_id` | string | OMOP domain (Athena only) |
| `standard_concept` | string | `S`, `C`, or null (Athena only) |

Added codes are written as `VOCABULARY//CODE`. Columns that the table lacks
are created; relationship lists and their names only when enabled.

## Merge rules

Enrichment is non-destructive:

- Existing descriptions and other scalar values are never overwritten. Only
  null values are filled.
- Existing relationship lists are kept verbatim, in order. New entries are
  appended after them, and only when the list does not already contain them.
  For parent references, `VOCABULARY/CODE` and `VOCABULARY//CODE` count as
  the same entry, and the existing spelling is kept.
- A code is never its own parent, child or sibling.
- Newly added codes must be valid in the source that supplied them.
  Invalid, missing, self-referential and cyclic hierarchy rows are ignored.
- Unmatched codes pass through unchanged.
- With `--no-parent-codes` (and likewise for children and siblings), the list
  and its names list are left as they are.
- In CSV input, relationship lists are `|`-joined text and names lists are
  JSON arrays. Both are read back into lists.

The one opt-in exception is `--exclude-self-parent-code`
(`exclude_self_parent_code: true`). It removes the existing parent reference
that was used to match the code, so a code matched through `ICD9CM/428.40`
loses that entry and keeps the rest.

## Names

Every relationship list comes with a names list of the same length:
`parent_names[i]` names `parent_codes[i]`.

- **Codes that enrichment adds** are named by the Athena `concept_name` or the
  pack description. LOINC Parts get their Part name.
- **Existing entries** keep their existing name. If they have none, they get
  one only when enrichment adds the same code itself.
- **Null names.** These entries keep a null name:
    - same-table parents, whose description is in their own row;
    - tags;
    - existing references that enrichment did not look up as a related code.
      This includes the parent reference used to match the code, and any
      second reference.

So a dataset that keeps its own `description` still carries the official name
of every vocabulary code that enrichment links it to. The LOINC license asks
for this when such a table is shared: every LOINC code and Part number must
come with its LOINC display name (Section 10.3). CSV output stores the names
lists as JSON arrays, because names can contain `|` and can be missing.

## Hierarchy and order

- **Parents** are enabled by default (`--no-parent-codes` turns them off).
  They go through the root.
- **Children** are opt-in (`--child-codes`). They are descendants up to
  `--child-depth` levels (default `3`, range `1` to `100`).
- **Siblings** are opt-in (`--sibling-codes`). They are the direct children of
  every ancestor, so they also include children of grandparents and higher
  ancestors, without the code itself.

The order of `parent_codes` after enrichment is:

1. the existing entries, unchanged;
2. the rule's vocabulary code, if a rule matched;
3. Athena's ancestors, sorted by code (`CONCEPT_ANCESTOR` has no single path);
4. pack ancestors not already listed, nearest first, then LOINC Parts with
   `--loinc-hierarchy`;
5. same-table parents from hierarchy rules.

Pack children are ordered breadth first. Siblings are sorted by code.

## Coverage report

`suMEDS-enrich` prints a report by default (`--no-verbose` silences it).
`suMEDS --verbose` prints it after a summary. A `tqdm` progress bar precedes
it. For the eICU example with Athena and packs:

```console
$ uv run suMEDS-enrich eicu_demo/metadata/codes.parquet -o codes-enriched.parquet \
    --rules eicu_rules.yaml --athena-csv /vocabularies/athena \
    --ontology-source ../sumeds-ontologies/ontologies

Enrichment report
  Rows processed:             6,481
  Unique codes:               6,481
  Athena matches added:      910 rows, 910 unique codes
  Rows with concept ID:       910 (14.0%)
  Rows without Athena match:  5,571 (86.0%)
  Metadata coverage (present before -> after):
    Descriptions                     0 ->        910 (14.0%) (+910 filled)
    Parent-code lists              860 ->      1,747 (27.0%) (+887 filled)
    Child-code lists                 0 ->           0 (0.0%) (+0 filled)
    Sibling-code lists               0 ->           0 (0.0%) (+0 filled)
    Vocabulary IDs                   0 ->        910 (14.0%) (+910 filled)
    OMOP concept IDs                 0 ->        910 (14.0%) (+910 filled)
    Concept codes                    0 ->        910 (14.0%) (+910 filled)
    Domains                          0 ->        910 (14.0%) (+910 filled)
    Standard-concept flags           0 ->          50 (0.8%) (+50 filled)
  Parent codes (all entries): 1,712 -> 5,978 (2,901 named)
  Same-dataset parent links:  0 -> 1,365
  Vocabulary rules (unique codes):
    eicu-lab-loinc [crosswalk, LOINC, pack]: 50 matched, 50 validated (Athena 50, pack 0), 0 tagged
  Hierarchy rules (same-dataset parents):
    eicu-diagnosis-paths [separator]: 611 parents added to 611 codes
    eicu-admission-diagnosis-paths [separator]: 0 parents added to 0 codes
    eicu-treatment-paths [separator]: 754 parents added to 754 codes
  Ontology files used, from ../sumeds-ontologies/ontologies:
    crosswalk eicu/lab_loinc: 1 of 1 files (0 downloaded, 0 bytes)
    LOINC 2.83: 28 of 99 files (0 downloaded, 0 bytes)
```

- **Athena matches added**, **Rows with concept ID**: rows that gained an OMOP
  `concept_id`. The 860 diagnosis codes matched through their parent
  reference, and the 50 lab codes through the crosswalk.
- **Metadata coverage**: rows where each field is present, before and after.
  Only 50 rows gain a standard-concept flag, because ICD-9-CM and ICD-10-CM
  concepts are non-standard.
- **Parent codes (all entries)**: list entries before and after, and how many
  have a name.
- **Same-dataset parent links**: parent entries that are codes of the same
  table.
- **Vocabulary rules**: for each ontology and crosswalk rule, how many unique
  codes it matched, how many were validated by Athena or by a pack, and how
  many were tagged. A rule that could not run says why:
  `skipped (no Athena source)` or `skipped (packs disabled)`. Here Athena
  validated all 50 LOINC
  codes, so no code needed the pack. The pack was still read for its
  hierarchy.
- **Hierarchy rules**: same-table parents added per rule.
- **Ontology files used**: the pack, hierarchy and crosswalk files read, out
  of those available, and how many were downloaded. With the default source,
  the header reads
  `from rvandewater/sumeds-ontologies@v0.2.0, index sha256 pinned`.

The report contains counts only.

## Standalone enrichment

```bash
uv run suMEDS-enrich /data/MEDS/metadata/codes.parquet \
  -o codes-enriched.parquet --athena-csv /vocabularies/athena
```

`suMEDS-enrich` takes the same source, hierarchy and ontology options as
`suMEDS`. It reads and writes Parquet, CSV, JSON, JSONL and NDJSON. Input and
output must be different paths, and the destination is replaced atomically.

## Privacy ordering

Integrated summary enrichment runs after rare-code masking. PostgreSQL
receives only released code strings and the configured rare label. Masked
source codes are not queried or restored. Pack fetching requests only
vocabulary shards and crosswalk tables, never dataset codes. Standalone
enrichment has no privacy filtering. Apply it only to an already approved
table when release controls are required. See [Privacy](privacy.md).

## Interactive demo

Open `examples/athena_enrichment_demo.ipynb` for a hands-on walkthrough using
individual codes and the included MIMIC-IV demo metadata. The notebook supports
either Athena source, and the full summary step is opt-in.

> This material contains content from LOINC (http://loinc.org). LOINC is
> copyright © Regenstrief Institute, Inc. and the Logical Observation
> Identifiers Names and Codes (LOINC) Committee and is available at no cost
> under the license at http://loinc.org/license. LOINC® is a registered United
> States trademark of Regenstrief Institute, Inc.
