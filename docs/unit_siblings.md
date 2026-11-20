# Unit siblings

MEDS-Extract (0.9 and later) writes `unit` as one string per code, and fails if
a code has more than one unit. An ETL whose source records an item in several
units therefore puts the unit in the code, as its last component:
`LAB//<item>//mg/dL` and `LAB//<item>//mmol/L` are two codes, each with its own
`unit`. The `sumeds_link_unit_siblings` stage makes such a family findable as
one item again: each code gets the code without its unit (the *stem*,
`LAB//<item>`) as a parent in `parent_codes`, and the stem gets a row of its own.
With `link_single_units`, so does every code that ends in its unit, also when
the item has only one unit, and with `unknown_unit_prefixes` also a lone code
ending in `UNK` in the listed code families.

Before suMEDS 0.6.6 every ETL carried its own copy of this step
(`<dataset>_unit_metadata`); the shared stage replaces them.

## The `sumeds_link_unit_siblings` stage

A MEDS-Extract spec runs it among its post-stages, before
`sumeds_normalize_units` and `sumeds_enrich`:

```yaml
etl:
  post_stages:
    - sumeds_link_unit_siblings
    - sumeds_normalize_units
    - sumeds_enrich: {rules: MC-MED}
```

It also works after `sumeds_normalize_units`: a code's last component is then
compared with `unit_source_value` (the unit as recorded), not with the UCUM
`unit` (`X//mcg` has `unit` `ug`).

Options are stage keys, as for `sumeds_enrich`; every key is optional:

```yaml
    - sumeds_link_unit_siblings:
        unknown_unit_components: [UNK]   # default
        link_unknown_units: true         # default
        link_single_units: false         # default
        unknown_unit_prefixes: []        # default; needs link_single_units: true
        stem_description: without_unit   # default; or shared
        description_unit_pattern: '\s[(\[]([^()\[\]]+)[)\]]\s*$'
        description_units: [cm, kg, mL]  # default: any unit-like spelling
        fill_units: unknown_component    # default: missing; or never
```

The same runs outside an ETL (`--help` lists the options):

```bash
suMEDS-link-unit-siblings /path/to/MEDS            # metadata/codes.parquet, in place
suMEDS-link-unit-siblings codes.parquet -o out.parquet --stem-description shared
```

```python
from sumeds import link_unit_siblings, link_unit_siblings_file

frame, counts = link_unit_siblings(frame)       # a polars DataFrame
counts = link_unit_siblings_file("metadata/codes.parquet", link_unknown_units=False)
```

## The rules

**Unit-suffixed codes.** A code is unit-suffixed when its last `//` component
is its unit (`unit_source_value` if set, else `unit`), compared exactly: case
carries meaning, a blank unit is no unit, and the stem must not be empty. A
code whose last component is in `unknown_unit_components` (`UNK`, an ETL's
`$unit ?? 'UNK'`) is unit-suffixed too, whatever its `unit`, when
`link_unknown_units` is true.

**Sibling groups.** Two or more distinct unit-suffixed codes with the same stem,
at least one of them with a real unit, are unit siblings. Two `UNK`-style codes
alone are never a group, and a unit-suffixed code without a sibling is left as
it is, unless `link_single_units` is set (next paragraph).

**Single unit codes (`link_single_units`).** With `link_single_units: true`, a
unit-suffixed code with a real unit (its last component is its `unit_source_value`
or `unit`) and no sibling is a group of one: it gets its stem as a parent and
the stem gets a row, by the same rules as below. Then every code that ends in
its unit has a unit-free stem, whether its item has one unit or several. That is
what a code family needs when its codes always end in their unit and are matched
without it: an interval start `INFUSION//START//<item>//mL/h` pairs with its end
`INFUSION//END//<item>//mL` through their stems `INFUSION//START//<item>` and
`INFUSION//END//<item>`.

- A code ending in an unknown-unit component (`UNK`) alone still gets no stem,
  as two such codes alone are no group: `UNK` says nothing about which unit, or
  whether any unit, is meant (`GENDER//UNK`, `ROUTE//UNK`). Such a code gets
  its stem only with a real-unit sibling and `link_unknown_units: true`, or in
  a family listed in `unknown_unit_prefixes` (next paragraph). With
  `link_unknown_units: false`, a real-unit code whose only sibling ends in `UNK`
  is linked on its own and the `UNK` code is not.
- A code is linked only on its own unit, as always: a last component that looks
  like a unit (`X//a//mg`) but is not the code's `unit` (none, `g`, or `MG`)
  gives no stem. A unit filled from the description (`fill_units`) is the
  code's unit, so it can link a single code; restrict the stated units with
  `description_units`, or fill only `UNK` codes (`fill_units: unknown_component`).
- The single code decides the stem's parents (its own, the stem left out). Its
  description without its unit is the stem's under `without_unit` (the span
  `description_unit_pattern` matches, else the last mention of its unit, else
  the description as it is); under `shared` the stem gets the code's description
  as it is.
- The default (`false`) is the behaviour before this option.

**Unknown units in listed families (`unknown_unit_prefixes`).** Some code
families always end in a unit, and write `UNK` where the source recorded none:
an infusion given as a bolus has no rate, so its start is
`INFUSION//START//<item>//UNK`. There `UNK` is a unit placeholder, and the
start still has to pair with `INFUSION//END//<item>//mL` by its stem. List
such families as code prefixes:

```yaml
    - sumeds_link_unit_siblings:
        link_single_units: true
        unknown_unit_prefixes: [INFUSION//START//, INFUSION//END//]
```

- A code ending in an unknown-unit component whose **stem** starts with one of
  the prefixes is linked to its stem also without a real-unit sibling.
  `INFUSION//START//` matches `INFUSION//START//7//UNK` (stem
  `INFUSION//START//7`), not `INFUSION//START//UNK` (stem `INFUSION//START`).
  Write each prefix with its trailing `//`: `MEDICATION//START` would also match
  `MEDICATION//STARTED//…`.
- Outside the prefixes nothing changes: `GENDER//UNK` or `DIAGNOSIS//UNK`
  alone still get no stem, as there `UNK` is an unknown value, not an unknown
  unit.
- Such a stem's row takes the code's own parents (if several unknown-unit
  spellings share the stem, the parents they all share) and its description
  without `UNK` under `without_unit`. With a real-unit sibling, the group is an
  ordinary sibling group, as before.
- It **needs `link_single_units: true`** (and `link_unknown_units: true`, the
  default, and some `unknown_unit_components`); otherwise the stage fails with
  an error. Its purpose is that every code of a family gets a stem; without
  single linking, a lone `//mg` start would stay without one while a lone
  `//UNK` start got one. With `link_unknown_units: false`, an `UNK` code is
  never unit-suffixed, so the prefixes could not apply.
- The default (empty) is the behaviour before this option. Codes linked this
  way are counted as `unknown_unit_prefix_codes`, apart from `sibling_codes`
  and `single_unit_codes`.

**Parents.** Each sibling gets its stem appended to `parent_codes`, after the
parents it already has (vocabulary parents stay first), and only if it is not
there yet. Existing parents are not reordered or deduplicated.
`parent_codes` keeps its Arrow type; it is added as `list<string>` if missing.

**Stem rows.** A stem that is not already a code gets a row: `unit` null, the
parents every sibling with a real unit shares (in the first such sibling's order,
deduplicated, the stem itself left out; null if none; an `UNK` sibling is linked
to the stem but does not narrow its parents), every other column null (`code_template`,
`unit_source_value`, suMEDS's columns), and a description:

- `shared`: the siblings' description if they all have the same one, else null;
- `without_unit` (default): the same, and if they differ, each sibling's
  description with its unit removed is compared instead: the span
  `description_unit_pattern` matches, else the last mention of its own unit
  (`Glucose (mg/dL)`, `Heart rate [/min]`, `Dose in mg`; never inside another
  unit, as in `mg/kg`), else the description as it is. If those agree and are
  not empty, that is the stem's description; otherwise it is null.

A stem that already has a row keeps it exactly as it is.

**Units stated in descriptions.** With `description_unit_pattern` (a regular
expression whose group `unit`, or else group 1, is the unit), a description
that states a unit fills a missing `unit`: for every code without one
(`fill_units: missing`), only for codes ending in an unknown-unit component
(`unknown_component`), or never. The stated unit must be in
`description_units` if that is given, and must otherwise look like a unit
(UCUM-like characters, not a placeholder such as `unknown`). A unit from the
data is never replaced; disagreements are counted. After
`sumeds_normalize_units`, the stated spelling fills `unit_source_value` and its
UCUM code `unit`.

**Files.** Only `parent_codes` (and `unit`/`unit_source_value`, when units are
filled) change, and stem rows are added; every column keeps its Arrow type and
position, existing rows keep their order, and stem rows are inserted in code
order if the file is sorted by code (else appended). The result is validated
against `meds.CodeMetadataSchema` and written atomically. A file without
`unit`, or without siblings, is not rewritten, and a second run changes
nothing. The stage logs the counts: codes with a unit, unit-suffixed and
unknown-unit codes, sibling groups and codes, codes linked without a sibling
(with a real unit, and under `unknown_unit_prefixes`), parents added, stem rows added or already present, and units filled from
descriptions.

## Migrating from an ETL's own stage

These options reproduce the ETL-local stages exactly on their synthetic builds
and on the public MIMIC-IV demo (`unit` keeps its Arrow type, which ALOTT's and
MIMIC-IV's stages narrowed to `string`):

| ETL stage | Options |
|---|---|
| NWICU, AUMCdb, HiRID | `link_unknown_units: false` |
| PICdb, Zigong, INSPIRE, ALOTT, MC-MED | `link_unknown_units: false`, `stem_description: shared` |
| CDSL | `link_unknown_units: false`, `description_unit_pattern: '; unit: (?P<unit>.+?)(?= \(CDSL lab_06\.item_lab, lab_06\.ud_result\)$)'` |
| MIMIC-IV | `link_unknown_units: false`, `description_unit_pattern: '\s[(\[]([^()\[\]]+)[)\]]\s*$'`, `description_units: [...]` (its label units), `fill_units: unknown_component` |
