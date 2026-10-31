# Rules and ontology packs

A rules file tells suMEDS which dataset codes stand for which vocabulary codes.
suMEDS validates those codes against sharded ontology packs that it fetches on
demand from the separately versioned data repository
[`sumeds-ontologies`](https://github.com/rvandewater/sumeds-ontologies), or
against your own [Athena download](athena.md). The rules file can also add
parents that are codes of the same table. [How enrichment
works](enrichment.md) describes where rules fit in the overall process.

## Rules file

Pass a rules file with `--rules PATH` (to both `suMEDS` and `suMEDS-enrich`),
`enrichment.rules` in YAML (relative to the YAML file), or
`EnrichmentConfig(rules=...)` (a path, a mapping, or `load_rules(...)`). The
format is strict. These are errors:

- unknown keys and a wrong `schema`;
- invalid regular expressions and missing named groups;
- unknown group references in `replace`;
- duplicate ids;
- more than 200 ontology and crosswalk rules.

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

## Ontology and crosswalk rules

A matching code yields one or more vocabulary codes:

- an ontology rule yields its `code` group after `normalize`;
- a crosswalk rule yields every `code_column` value of the table rows whose
  `key_column` equals the `key` group (and that pass `where`).

These candidates rank before the built-in `VOCABULARY//CODE` layouts, in file
order: ontology rules first, then crosswalk rules. The first candidate that
validates wins. `validate` decides what may validate it:

| `validate` | Athena configured | No Athena |
|---|---|---|
| `pack` | Athena first; else the public pack | the public pack |
| `athena` | Athena only | rule skipped (reported) |
| `none` | Athena first; else tag only | tag only |

"Athena first" means that Athena wins if it validates this rule's code or the
code of an earlier rule. A pack match on this rule wins over an Athena match
on a later rule or a built-in layout.

A tag is a fallback. When a later rule or a built-in layout validates the same
code through a pack or Athena, that validated code wins over the tag.

A code validated through a pack gets:

- `description` (only where null), `vocabulary_id` and `concept_code`;
- `parent_codes`: the vocabulary code itself, then its pack ancestors through
  the root, nearest first, as `VOCABULARY//CODE`;
- with `--child-codes` and `--sibling-codes`, the pack's descendants (up to
  `child_depth` levels) and the children of every ancestor;
- the name of every added code, in `parent_names`, `child_names` and
  `sibling_names`.

When Athena validates a rule's code, Athena's fields are used and the pack's
hierarchy is merged in (see [Athena with rules and
packs](athena.md#athena-with-rules-and-packs)).

A tag (`validate: none`) adds only the vocabulary code to `parent_codes`. It
adds no description and no name.

Other cases:

- A `pack` rule for a vocabulary without a pack fails with the list of
  available packs.
- Codes missing from the pack or from Athena get nothing, so no dangling code
  is added.
- A code is never its own parent, child or sibling. When a dataset code
  already is a vocabulary reference (OMOP-style `ICD10PCS//0DTJ4ZZ` or
  `ICD10PCS/0DTJ4ZZ`), its rule's vocabulary code is not added as its parent.
- If a crosswalk key maps to several codes, Athena keeps the lowest concept id,
  a pack the first code in sort order, and a tag lists them all.

### LOINC Parts as parents

LOINC terms have no parents in the LOINC table itself. The LOINC pack also
carries the rows of the LOINC multiaxial hierarchy
(`ComponentHierarchyBySystem.csv`) for its terms. `--loinc-hierarchy`
(`loinc_hierarchy: true`) adds the LOINC Parts on a term's paths in that
hierarchy as its parents, nearest first. Each Part is named by its Part name in
`parent_names`. For example, `2160-0` gets:

1. `LP385359-7` Creatinine | Serum or Plasma | Chemistry - non-challenge
2. `LP14355-9` Creatinine
3. `LP31398-8` Renal function
4. `LP7786-9` Chemistry - non-challenge
5. `LP343631-0` Chemistry and Chemistry - challenge
6. `LP29693-6` Laboratory
7. `LP432695-7` {component}

The option is off by default. It applies to every LOINC code that is looked
up in the LOINC pack: rule codes, built-in layouts and parent references. It
needs a rules file and the packs; otherwise the configuration is rejected,
because the option would add nothing. It adds parents only, no children or
siblings. Athena already gives LOINC codes a broader set of ancestors through
`CONCEPT_ANCESTOR` ([Athena hierarchy](athena.md#hierarchy-from-concept_ancestor)).

## Hierarchy rules

Hierarchy rules add parents that are codes of the same (released) table. They
are appended after the vocabulary parents, so the first parent stays the
vocabulary reference that the parent-reference fallback relies on. Hierarchy
rules never add a code that is not in the table, never the code itself, and
run only when `parent_codes` is enabled. Their entries have no name in
`parent_names`, because the parent's own row carries its description.

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

## Packs and crosswalks

| Pack | Release | Codes | Source | License |
|---|---|---:|---|---|
| `ICD10CM` | `fy2016-fy2027` | 99,085 | CDC/NCHS order files, FY2016 to FY2027 (retired codes kept, with `first_year`/`last_year`) | no license agreement; adapts WHO's ICD-10 |
| `ICD10PCS` | `fy2016-fy2027` | 197,789 | CMS order files and code tables, FY2016 to FY2027, including every 1- to 6-character prefix described from the tables' axis labels | no license agreement; no WHO content |
| `ICD9CM` | `v32` | 17,541 | CMS v32 (final) plus NCHS FY2012 tabular headers | no license agreement; adapts WHO's ICD-9 |
| `ICD9Proc` | `v32` | 4,650 | as ICD9CM | no license agreement; adapts WHO's ICD-9 |
| `LOINC` | `2.83` | 105,050 | LOINC Table Core 2.83, original fields unchanged, all statuses, `MapTo.csv`; the ComponentHierarchyBySystem rows of these terms and their Parts | LOINC license |

Codes use the OMOP `concept_code` format (`I50.23`, `401.9`, `39.61`,
`0DTJ4ZZ`, `2160-0`), so rules normalize dataset codes to it. ICD-10-PCS
prefixes are described by their axis labels joined with ` @ `. For example,
`0DTJ4` is
`Medical and Surgical @ Gastrointestinal System @ Resection @ Appendix @ Percutaneous Endoscopic`.
LOINC descriptions are
`LONG_COMMON_NAME`.

Crosswalk tables live next to the packs (`crosswalks/eicu/lab_loinc`,
`crosswalks/hirid/observation_loinc`, `crosswalks/nwicu/loinc_parents`,
`crosswalks/zigong/nursing_vitals_loinc`), so ETL repositories no longer ship
them. A dataset that already carries a vocabulary column, such as the LOINC
codes of SICdb or PICdb, can drop ETL-side well-formedness checks of that
column and validate its codes with an ontology rule (`validate: pack`, or
`athena`). The packs, crosswalks and their builder live in
[`sumeds-ontologies`](https://github.com/rvandewater/sumeds-ontologies). Its
[pack README](https://github.com/rvandewater/sumeds-ontologies/tree/v0.2.0/ontologies)
documents the format, sizes, sources, and rebuild commands, and its README
cites the source of every crosswalk.

Packs are consulted for rule candidates and also, in vocabularies with a pack,
for the built-in layouts (`ICD10CM//I50.23`), the first parent reference, and
every existing parent reference (`ICD9CM/428.0`), as Athena is. So a dataset
whose codes already carry ICD references gets their names and ancestors from
the packs alone. Because of this, suMEDS reads the pack index whenever packs
are enabled and a code has a layout or a parent reference.

## Fetching, caching, offline use

At enrichment time suMEDS determines which vocabularies and shards the matched
codes need and fetches only those, plus `index.json` and the manifests. The
source is, in order of precedence:

1. `--ontology-source URL_OR_DIR` or `enrichment.ontology_source`: an
   `http(s)://` base URL, a `file://` URL, or a local directory;
2. the `SUMEDS_ONTOLOGY_SOURCE` environment variable;
3. `https://raw.githubusercontent.com/rvandewater/sumeds-ontologies/<ref>/ontologies`,
   with `<ref>` from `--ontology-ref` or `enrichment.ontology_ref`.

The default `<ref>` is a release tag of `sumeds-ontologies` (`v0.2.0` in this
suMEDS version), and suMEDS pins the sha256 of that release's `index.json`.
The index pins every manifest, and each manifest every shard, so the default
enrichment is reproducible byte for byte. If the tag were ever moved, the run
would fail with a checksum error instead of silently changing results. A
`--ontology-ref` that is neither a release tag (`vX.Y.Z`) nor a full commit
sha prints a warning. Each suMEDS release states which data release it pins.

Every manifest is checked against the sha256 in `index.json`, and every shard
or table against its manifest. A mismatch is an error. Remote files are cached
under `${XDG_CACHE_HOME:-~/.cache}/sumeds/ontologies/<source>/`. For the pinned
default release, everything is reused from the cache. For other sources, the
index and manifests are refreshed on each online run, and shards are reused
while their checksum matches. `--offline` (`enrichment.offline: true`) uses
only the cache or a local source, and fails with a clear message if a needed
file is missing.

```bash
uv run suMEDS-enrich metadata/codes.parquet -o codes-enriched.parquet \
  --rules rules.yaml                     # public packs only
uv run suMEDS-enrich metadata/codes.parquet -o codes-enriched.parquet \
  --rules rules.yaml --athena-csv /vocabularies/athena   # Athena first, then packs
uv run suMEDS /data/MEDS -o summary.parquet --rules rules.yaml \
  --ontology-source ../sumeds-ontologies/ontologies --offline --verbose
```

## Licenses and governance

- Only vocabularies that may be redistributed are hosted.
    - CDC/NCHS and CMS publish ICD-10-CM, ICD-10-PCS, and ICD-9-CM at no cost
      and without a license agreement. They are not public-domain works as a
      whole: ICD-10-CM and ICD-9-CM are U.S. clinical modifications of WHO
      classifications, and WHO keeps the copyright in content taken from them.
      Each pack manifest's `license` field says what the publisher states.
    - LOINC is redistributed under the LOINC license. Its full text and the
      notice below are in the pack folder and in every crosswalk folder.
      `MapTo.csv` keeps only rows whose codes are in the pack. The
      hierarchy rows are copied unchanged from the official
      `ComponentHierarchyBySystem.csv`, not taken from Athena.
- Vocabularies that may not be redistributed, or whose terms are unknown, are
  never hosted. These include SNOMED CT, CPT4, WHO ICD-10 itself, ICD-10-CN,
  ICD-O-3 and ATC. Validate them against your own Athena download
  (`validate: athena`) or tag them (`validate: none`).
- suMEDS is the single maintained copy of LOINC content for the MEDS ETLs.
    - Its maintainers rebuild the LOINC pack from the official download for
      each LOINC release (twice a year), and record the release, date, and
      checksums. Users fetch the pack and never need a LOINC account.
    - Crosswalk tables carry the LOINC `LONG_COMMON_NAME` of each code
      verbatim (`loinc_long_common_name`). It is filled from the pack at build
      time, never from Athena, whose names can be truncated.
- Every LOINC code and Part number that enrichment adds travels with its LOINC
  name (`parent_names` and so on), as Section 10.3 of the LOINC license asks
  for extracted content. LOINC Parts as parents are opt-in.

> This material contains content from LOINC (http://loinc.org). LOINC is
> copyright © Regenstrief Institute, Inc. and the Logical Observation
> Identifiers Names and Codes (LOINC) Committee and is available at no cost
> under the license at http://loinc.org/license. LOINC® is a registered United
> States trademark of Regenstrief Institute, Inc.
