# Unit spellings (UCUM)

MEDS ETLs copy units into the `unit` column of `metadata/codes.parquet` as their
sources record them, so the same unit arrives as `mg/dl`, `mg/dL` and `MG/DL`,
or `°C`, `degC` and `celsius`. suMEDS holds one shared table that turns a known
spelling into its [UCUM](https://ucum.org/ucum) code (case-sensitive), so that
every ETL that runs it writes units in one spelling.

Only the `unit` column changes. Codes keep their names: a code such as
`LAB//<item>//mg/dl` is still `LAB//<item>//mg/dl`, with `unit` `mg/dL`.

## The `sumeds_normalize_units` stage

A MEDS-Extract spec runs the stage after the ETL's own unit stage (which links
unit-suffixed sibling codes to a stem by comparing a code's last component with
its recorded unit) and before `sumeds_enrich`:

```yaml
etl:
  post_stages:
    - mc_med_unit_metadata
    - sumeds_normalize_units
    - sumeds_enrich: {rules: MC-MED}
```

For each row with a `unit`:

- a known spelling becomes its canonical UCUM code;
- an unknown spelling, or a placeholder for a missing unit (`unknown`, `N/A`,
  `-`), is kept as it is;
- `unit_source_value` (Arrow `large_string`) keeps the unit exactly as it was
  recorded; it is null where `unit` is null.

Every other column keeps its Arrow type and position, rows keep their order, the
result is validated against `meds.CodeMetadataSchema` and written atomically.
Without a `unit` column the file is left as it is. Running the stage twice gives
the same file: an existing `unit_source_value` is kept (only its nulls are
filled), and canonical codes map to themselves. The stage logs how many rows had
a unit, were already canonical, were rewritten, and were kept as unknown or
placeholders, and lists the unknown spellings at WARNING level.

The same runs outside an ETL:

```bash
suMEDS-normalize-units /path/to/MEDS              # metadata/codes.parquet, in place
suMEDS-normalize-units codes.parquet -o out.parquet
```

```python
from sumeds import normalize_units, normalize_units_file, to_ucum

to_ucum("mcg/kg/min")                     # 'ug/kg/min'
frame, counts = normalize_units(frame)    # a polars DataFrame with a unit column
counts = normalize_units_file("metadata/codes.parquet")
```

## How a spelling is read

`to_ucum(spelling)` returns the UCUM code of a known spelling and `None` for any
other. HTML entities are decoded (`&#176;C`) and whitespace is stripped; a
placeholder is `None`. Some spellings are known only as a whole unit (`C`, `F`,
`K/uL`, `bpm`). Any other spelling, with inner spaces removed and `µ`/`μ` as
`u`, is split on `/` and `.` and read part by part, and it is known only when
every part is. A part is:

- a UCUM atom (`mL`, `mm[Hg]`, `[degF]`, `{beats}`);
- a word, in any case: `hr`, `hrs`, `hour(s)` are `h`; `sec`, `secs`,
  `second(s)` are `s`; `min`, `minute(s)` are `min`; `day(s)` is `d`;
  `year(s)` is `a`; `grams` is `g`, `liters` is `L`, `lbs` is `[lb_av]`;
- a unit with a metric prefix: `mEq` is `meq`, `mOsm` is `mosm`, `IU` is
  `[iU]`, `mIU` is `m[iU]`, lowercase `l` and `dl` are `L` and `dL`, and `mc`
  is micro (`mcg` is `ug`, `mcL` is `uL`);
- a power of ten (`10^9` is `10*9`), a unit with an exponent (`m^2` is `m2`),
  or a count of a unit (`mg/24hr` is `mg/(24.h)`).

Equal units are written one way: thousands and millions per microlitre are
`10*9/L` and `10*12/L`, and `#/uL` (a count) is `/uL`.

### Canonical choices

| Spellings | UCUM |
|---|---|
| `°C`, `degC`, `celsius`, `Celsius`, `C` (a whole unit) | `Cel` |
| `°F`, `degF`, `F`, `fahrenheit` | `[degF]` |
| `kg`, `kilogram(s)` | `kg` |
| `lb`, `lbs`, `lbs.`, `Lbs`, `pound(s)` | `[lb_av]` |
| `inch`, `Inch`, `in` | `[in_i]` |
| `oz` | `[oz_av]` |
| `mmHg`, `mm Hg`, `mm[Hg]` | `mm[Hg]` |
| `cmH2O`, `cm H2O` | `cm[H2O]` |
| `%`, `percent` | `%` |
| `bpm`, `beats/min` | `{beats}/min` |
| `breaths/min`, `insp/min` | `{breaths}/min` |
| `/min` | `/min` |
| `ml`, `mL`, `cc` | `mL` |
| `l`, `L`, `liter(s)` | `L` |
| `fl`, `fL`, `FL` | `fL` |
| `mg/dl` | `mg/dL` |
| `mmol/l` | `mmol/L` |
| `umol/l`, `µmol/L` | `umol/L` |
| `U/l` | `U/L` |
| `IU`, `iu`, `IE` (German, Dutch), `UI` (French, Spanish) | `[iU]` |
| `mEq` | `meq` |
| `mL/hr` | `mL/h` |
| `mcg/kg/min` | `ug/kg/min` |
| `K/uL`, `K/mcL`, `10^9/L`, `G/l`, `G/L`, `Gpt/l`, `/nL` | `10*9/L` |
| `M/uL`, `M/mcL`, `10^12/L`, `T/l`, `T/L`, `Tpt/l` | `10*12/L` |
| `/hpf`, `#/lpf` | `/[HPF]`, `/[LPF]` |

### Never assert a meaning the spelling does not carry

- **Case carries meaning.** `G/l` is giga per litre (a cell count, `10*9/L`),
  `g/l` is grams per litre (`g/L`). `mL` is millilitre; `ML` (megalitre, or a
  typing slip) is left unknown, as is `CEL`.
- **Ambiguous words become annotations.** A UCUM annotation in braces has no
  magnitude, so it records the word without claiming a unit: `units`, `Units`,
  `unit` are `{units}` (enzyme units `U`, or international units `[iU]`?),
  `E` (German *Einheit*, Dutch *eenheid*) is `{units}` too, and `dose`, `bag`,
  `tube`, `each`, `ratio`, `index`, `INR`, `drop(s)`, `tablet(s)`, `puff(s)`
  and other counted things are `{dose}`, `{bag}`, `{tube}`, and so on. `U` stays
  `U`.
- **Parts must all be known.** `mg/xyz` is unknown, not `mg`; `C` is a
  temperature only on its own, never inside a compound unit.
- **Lab conventions that need context are not guessed.** `m/uL` (metre per
  microlitre, as written; millions per microlitre, as meant?), `G/24HR` (grams,
  or giga?), `dag` (Dutch *day*, or UCUM decagram?), `u` (micro, or units?) and
  `log10 IU/mL` are left unknown.

The canonical codes are checked with the [ucumvert](https://pypi.org/project/ucumvert/)
UCUM parser in the test suite (a development dependency only).
