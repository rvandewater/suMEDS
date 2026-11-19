"""Unit spellings as UCUM codes: one canonical spelling for the ``unit`` column of MEDS metadata.

MEDS ETLs copy units from their sources as recorded, so the same unit arrives spelled ``mg/dl``,
``mg/dL`` and ``MG/DL``, ``°C``, ``degC`` and ``celsius``. :func:`to_ucum` returns the case-sensitive
`UCUM <https://ucum.org/ucum>`_ code for a spelling it knows, and ``None`` for one it does not (the
caller then keeps the spelling as it is). It never asserts a meaning the spelling does not carry:

- A spelling is mapped only when every part of it is known; ``mg/xyz`` is unknown, not ``mg``.
- Case carries meaning: ``G/l`` is giga per litre (``10*9/L``, a cell count), ``g/l`` is grams per
  litre (``g/L``); ``mL`` is millilitre, while ``ML`` (megalitre, or a typo) is left unknown.
- Words that do not say which unit they mean become UCUM annotations, which have no magnitude:
  ``units`` is ``{units}`` (enzyme units ``U`` or international units ``[iU]``?), ``dose`` is
  ``{dose}``. ``U`` and ``IU`` are units and stay units (``U``, ``[iU]``).
- Missing-unit placeholders (``unknown``, ``N/A``, ``-``) are not units: ``None``.

The canonical spellings are fixed points, so normalising twice changes nothing::

    >>> to_ucum("mg/dl"), to_ucum("mg/dL")
    ('mg/dL', 'mg/dL')
    >>> to_ucum("°C"), to_ucum("celsius"), to_ucum("Cel")
    ('Cel', 'Cel', 'Cel')
    >>> to_ucum("G/l"), to_ucum("g/l")
    ('10*9/L', 'g/L')
    >>> to_ucum("mcg/kg/min"), to_ucum("mL/hr"), to_ucum("bpm")
    ('ug/kg/min', 'mL/h', '{beats}/min')
    >>> to_ucum("units"), to_ucum("U"), to_ucum("IU")
    ('{units}', 'U', '[iU]')
    >>> to_ucum("ML") is None, to_ucum("N/A") is None, to_ucum("furlongs") is None
    (True, True, True)
"""

from __future__ import annotations

import html
import logging
import os
import re
import shutil
from collections import Counter
from collections.abc import Iterator
from functools import lru_cache
from pathlib import Path
from typing import Any
from uuid import uuid4

import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

__all__ = [
    "PLACEHOLDERS",
    "SOURCE_COLUMN",
    "UNIT_COLUMN",
    "is_placeholder",
    "known_spellings",
    "normalize_units",
    "normalize_units_file",
    "to_ucum",
]

logger = logging.getLogger(__name__)

#: The code-metadata column whose spellings are normalised.
UNIT_COLUMN = "unit"
#: The column that keeps each unit as it was recorded, before normalisation.
SOURCE_COLUMN = "unit_source_value"
#: The keys of the counts :func:`normalize_units` returns, besides ``unknown_spellings``.
COUNT_KEYS = (
    "rows_with_unit",
    "already_canonical",
    "rewritten",
    "unknown_kept",
    "placeholder_kept",
)

#: Spellings of a missing unit, compared case-insensitively. They are not units.
PLACEHOLDERS = frozenset(
    {
        "",
        "-",
        "--",
        "?",
        ".",
        "na",
        "n/a",
        "n.a.",
        "nan",
        "none",
        "null",
        "unk",
        "unknown",
        "not applicable",
        "not specified",
        "unspecified",
        # Dutch (AUMCdb): "none".
        "geen",
    }
)

# --------------------------------------------------------------------------- whole spellings
# Spellings that are mapped only as a whole unit, never as part of a compound unit: ``C`` alone is a
# temperature, while ``C`` in ``C/s`` would be the coulomb. Keys are case-sensitive and are looked up
# as written and again with whitespace removed and micro signs as ``u``.
_WHOLE: dict[str, str] = {
    # Temperature.
    "C": "Cel",
    "°C": "Cel",
    "ºC": "Cel",
    "Cel": "Cel",
    "F": "[degF]",
    "°F": "[degF]",
    "ºF": "[degF]",
    # Volume: ``fl``/``FL`` as femtolitre (mean corpuscular volume), as labs write it.
    "FL": "fL",
    # Cell counts: thousands and millions per microlitre are giga and tera per litre.
    "K/uL": "10*9/L",
    "K/mcL": "10*9/L",
    "K/ul": "10*9/L",
    "k/uL": "10*9/L",
    "10^3/uL": "10*9/L",
    "10*3/uL": "10*9/L",
    "10^3/ul": "10*9/L",
    "x10^3/uL": "10*9/L",
    "10^9/L": "10*9/L",
    "10^9/l": "10*9/L",
    "10*9/l": "10*9/L",
    "x10^9/L": "10*9/L",
    "G/l": "10*9/L",
    "G/L": "10*9/L",
    "Gpt/l": "10*9/L",
    "Gpt/L": "10*9/L",
    "M/uL": "10*12/L",
    "M/mcL": "10*12/L",
    "M/ul": "10*12/L",
    "10^6/uL": "10*12/L",
    "10*6/uL": "10*12/L",
    "10^6/ul": "10*12/L",
    "x10^6/uL": "10*12/L",
    "10^12/L": "10*12/L",
    "10^12/l": "10*12/L",
    "10*12/l": "10*12/L",
    "x10^12/L": "10*12/L",
    "T/l": "10*12/L",
    "T/L": "10*12/L",
    "Tpt/l": "10*12/L",
    "Tpt/L": "10*12/L",
    # Rates of breaths; litres per minute (oxygen flow).
    "insp/min": "{breaths}/min",
    "LPM": "L/min",
    "lpm": "L/min",
    # Units qualified by what they are measured in or on.
    "ng/mLFEU": "ng/mL{FEU}",
    "mOsm/kgH2O": "mosm/kg{H2O}",
    "U/g/Hb": "U/g{Hb}",
    "U/gHb": "U/g{Hb}",
    # Vascular resistance (MIMIC-IV writes the same unit three ways).
    "dynes*sec/cm5": "dyn.s/cm5",
    "dynes.sec.cm-5": "dyn.s/cm5",
    "dynes*sec/cm5/m2": "dyn.s/cm5/m2",
    "dynes.sec.cm-5/m2": "dyn.s/cm5/m2",
}
# Whole spellings compared case-insensitively (words, whose case carries no meaning).
_WHOLE_WORDS: dict[str, str] = {
    "celsius": "Cel",
    "degc": "Cel",
    "°c": "Cel",
    "degreesc": "Cel",
    "degreecelsius": "Cel",
    "degreescelsius": "Cel",
    "fahrenheit": "[degF]",
    "degf": "[degF]",
    "°f": "[degF]",
    "degreesf": "[degF]",
    "degreefahrenheit": "[degF]",
    "degreesfahrenheit": "[degF]",
    "bpm": "{beats}/min",
    "breaths/min": "{breaths}/min",
}

# --------------------------------------------------------------------------- atoms
# The parts of a compound unit (split on ``/`` and ``.``), case-sensitive. Canonical atoms map to
# themselves, so canonical spellings are fixed points.
_ATOMS: dict[str, str] = {
    "%": "%",
    "h": "h",
    "min": "min",
    "s": "s",
    "d": "d",
    "wk": "wk",
    "mo": "mo",
    "a": "a",
    "Cel": "Cel",
    "[degF]": "[degF]",
    "kg": "kg",
    "mmHg": "mm[Hg]",
    "mm[Hg]": "mm[Hg]",
    "cmH2O": "cm[H2O]",
    "cm[H2O]": "cm[H2O]",
    "cc": "mL",
    "in": "[in_i]",
    "[in_i]": "[in_i]",
    "ft": "[ft_i]",
    "[ft_i]": "[ft_i]",
    "lb": "[lb_av]",
    "[lb_av]": "[lb_av]",
    "oz": "[oz_av]",
    "[oz_av]": "[oz_av]",
    "dyn": "dyn",
    "Ohm": "Ohm",
    "KU": "kU",  # kilo-units (IgE in KU/L); K is no other prefix
    "hPa": "hPa",
    "hpf": "[HPF]",
    "[HPF]": "[HPF]",
    "lpf": "[LPF]",
    "[LPF]": "[LPF]",
    "amol": "amol",
    # International units in German and Dutch (Internationale Einheiten), French and Spanish
    # (unités/unidades internationales); E (Einheit, eenheid) is a unit of either kind.
    "E": "{units}",
    # International normalized ratio, a ratio without a unit.
    "INR": "{INR}",
    # Anticardiolipin IgG and IgM units.
    "GPL": "[GPL'U]",
    "[GPL'U]": "[GPL'U]",
    "MPL": "[MPL'U]",
    "[MPL'U]": "[MPL'U]",
}
# Word atoms, compared case-insensitively.
_WORDS: dict[str, str] = {
    # Time.
    "hr": "h",
    "hrs": "h",
    "hour": "h",
    "hours": "h",
    "sec": "s",
    "secs": "s",
    "second": "s",
    "seconds": "s",
    "min": "min",
    "mins": "min",
    "minute": "min",
    "minutes": "min",
    "day": "d",
    "days": "d",
    "week": "wk",
    "weeks": "wk",
    "month": "mo",
    "months": "mo",
    "year": "a",
    "years": "a",
    "yr": "a",
    "yrs": "a",
    "msec": "ms",
    "sek": "s",  # German
    "uur": "h",  # Dutch
    # Mass, length and volume.
    "gram": "g",
    "grams": "g",
    "gm": "g",
    "kg": "kg",  # Kg, KG: K is no prefix, so kilogram is the only reading
    "kilogram": "kg",
    "kilograms": "kg",
    "milligram": "mg",
    "milligrams": "mg",
    "microgram": "ug",
    "micrograms": "ug",
    "liter": "L",
    "liters": "L",
    "litre": "L",
    "litres": "L",
    "milliliter": "mL",
    "milliliters": "mL",
    "millilitre": "mL",
    "millilitres": "mL",
    "mls": "mL",
    "meter": "m",
    "meters": "m",
    "metre": "m",
    "metres": "m",
    "centimeter": "cm",
    "centimeters": "cm",
    "centimetre": "cm",
    "centimetres": "cm",
    "millimeter": "mm",
    "millimeters": "mm",
    "millimetre": "mm",
    "millimetres": "mm",
    "inch": "[in_i]",
    "inches": "[in_i]",
    "foot": "[ft_i]",
    "feet": "[ft_i]",
    "lbs": "[lb_av]",
    "pound": "[lb_av]",
    "pounds": "[lb_av]",
    "ounce": "[oz_av]",
    "ounces": "[oz_av]",
    "ohm": "Ohm",
    "dyne": "dyn",
    "dynes": "dyn",
    # Pressure, ratios.
    "mmhg": "mm[Hg]",
    "cmh2o": "cm[H2O]",
    "percent": "%",
    "thousand": "10*3",
    "million": "10*6",
    # Words that do not say which unit they are: annotations, without magnitude.
    "unit": "{units}",
    "units": "{units}",
    "dose": "{dose}",
    "doses": "{dose}",
    "bag": "{bag}",
    "bags": "{bag}",
    "tube": "{tube}",
    "tubes": "{tube}",
    "each": "{each}",
    "ratio": "{ratio}",
    "index": "{index}",
    "score": "{score}",
    "times": "{times}",
    "drop": "{drop}",
    "drops": "{drop}",
    "patch": "{patch}",
    "patches": "{patch}",
    "strip": "{strip}",
    "strips": "{strip}",
    "inhalation": "{inhalation}",
    "inhalations": "{inhalation}",
    "can": "{can}",
    "cans": "{can}",
    "device": "{device}",
    "devices": "{device}",
    "cell": "{cells}",
    "cells": "{cells}",
    "titer": "{titer}",
    "titre": "{titer}",
    # Dose forms, counted.
    "tab": "{tablet}",
    "tabs": "{tablet}",
    "tablet": "{tablet}",
    "tablets": "{tablet}",
    "cap": "{capsule}",
    "caps": "{capsule}",
    "capsule": "{capsule}",
    "capsules": "{capsule}",
    "puff": "{puff}",
    "puffs": "{puff}",
    "spray": "{spray}",
    "sprays": "{spray}",
    "suppository": "{suppository}",
    "suppositories": "{suppository}",
    "lozenge": "{lozenge}",
    "lozenges": "{lozenge}",
    "packet": "{packet}",
    "packets": "{packet}",
    "kit": "{kit}",
    "kits": "{kit}",
    "vial": "{vial}",
    "vials": "{vial}",
    "syringe": "{syringe}",
    "syringes": "{syringe}",
    "bottle": "{bottle}",
    "bottles": "{bottle}",
    "piece": "{piece}",
    "pieces": "{piece}",
    "application": "{application}",
    "applications": "{application}",
    "beat": "{beat}",
    "beats": "{beats}",
    "breath": "{breath}",
    "breaths": "{breaths}",
}
# Units that take a metric prefix, by spelling (case-sensitive), and the prefixes.
_BASES: dict[str, str] = {
    "g": "g",
    "L": "L",
    "l": "L",
    "mol": "mol",
    "mole": "mol",
    "moles": "mol",
    "eq": "eq",
    "Eq": "eq",
    "osm": "osm",
    "Osm": "osm",
    "U": "U",
    "IU": "[iU]",
    "iu": "[iU]",
    "[iU]": "[iU]",
    "[IU]": "[iU]",
    "IE": "[iU]",
    "UI": "[iU]",
    "s": "s",
    "m": "m",
    "V": "V",
    "A": "A",
    "Pa": "Pa",
    "bar": "bar",
    "W": "W",
    "J": "J",
    "cal": "cal",
    "Hz": "Hz",
    "kat": "kat",
}
_PREFIXES: dict[str, str] = {
    "k": "k",
    "d": "d",
    "c": "c",
    "m": "m",
    "u": "u",
    "mc": "u",
    "n": "n",
    "p": "p",
    "f": "f",
}
# Spellings that would parse but whose meaning is not what they say: ``m/uL`` reads as metre per
# microlitre, though labs mean millions per microlitre (or milli-?). Left unknown.
_AMBIGUOUS = frozenset({"m/uL", "m/ul", "m/mcL"})
# Bases that take only some prefixes: ``pm`` and ``cs`` are more likely not picometre and centisecond.
_ONLY_PREFIXES: dict[str, frozenset[str]] = {
    "s": frozenset({"m", "u", "mc", "n"}),
    "m": frozenset({"k", "c", "m", "u", "mc", "n"}),
}
# Different but equal spellings that a mechanical rewrite can produce; the right side is canonical.
_CANONICAL: dict[str, str] = {
    "dyn.s.cm-5": "dyn.s/cm5",
    "dyn.s.cm-5/m2": "dyn.s/cm5/m2",
    "10*3/uL": "10*9/L",
    "10*6/uL": "10*12/L",
    "10*3/mm3": "10*9/L",
    "10*6/mm3": "10*12/L",
    "/nL": "10*9/L",
    "/pL": "10*12/L",
}

_ANNOTATION = re.compile(r"\{[!-z|~]*\}")  # UCUM: ASCII 33-126 except braces
_ANNOTATED = re.compile(r"(.+?)(\{[!-z|~]*\})")
_POWER_OF_TEN = re.compile(r"10\*-?\d+")
_EXPONENT = re.compile(r"(.*[A-Za-z\]])(-?\d+)")
_COUNT = re.compile(r"(\d+)([A-Za-z].*)")
_SPACE = re.compile(r"\s+")


def is_placeholder(spelling: str) -> bool:
    """Whether ``spelling`` stands for a missing unit (``unknown``, ``N/A``, ``-``, blank).

    >>> [is_placeholder(s) for s in ["UNK", " n/a ", "", "mg"]]
    [True, True, True, False]
    """

    return _SPACE.sub(" ", html.unescape(spelling).strip()).casefold() in PLACEHOLDERS


def _atom(atom: str) -> str | None:
    """The UCUM spelling of one part of a compound unit, or ``None``."""

    if atom in _ATOMS:
        return _ATOMS[atom]
    if atom.casefold() in _WORDS:
        return _WORDS[atom.casefold()]
    if _ANNOTATION.fullmatch(atom) or _POWER_OF_TEN.fullmatch(atom):
        return atom
    if atom in _BASES:
        return _BASES[atom]
    for prefix in sorted(_PREFIXES, key=len, reverse=True):
        base = _BASES.get(atom[len(prefix) :]) if atom.startswith(prefix) else None
        if base is not None and prefix in _ONLY_PREFIXES.get(base, _PREFIXES):
            return _PREFIXES[prefix] + base
    if match := _ANNOTATED.fullmatch(atom):
        base = _atom(match[1])
        return None if base is None else base + match[2]
    if match := _EXPONENT.fullmatch(atom):  # m2, cm-5
        base = _atom(match[1])
        if base is None or base.startswith("{") or base.endswith("}"):
            return None
        return base + match[2]
    if match := _COUNT.fullmatch(atom):  # 24h, as in mg/24h
        base = _atom(match[2])
        if base is None or "{" in base:
            return None
        return base if int(match[1]) == 1 else f"({int(match[1])}.{base})"
    return None


def _compound(unit: str) -> str | None:
    """Map a compound unit atom by atom; ``None`` unless every atom is known."""

    unit = re.sub(r"\.(?=/|$)", "", unit)  # mEq./hour: an abbreviation's full stop
    unit = re.sub(r"\((\d+)\.([^()./]+)\)", r"\1\2", unit)  # mg/(24.h), as written back
    unit = re.sub(r"(?<![0-9])\^(?=-?\d)", "", unit)  # m^2
    unit = re.sub(r"^[x×](?=10[*^])", "", unit)  # x10^9/L, ×10^9/L
    unit = unit.replace("10^", "10*")
    unit = re.sub(r"(?<!10)\*", ".", unit)  # dynes*sec
    if unit.startswith(("#/", "1/")):  # a count: #/uL, 1/min
        unit = unit[1:]
    parts = re.split(r"([/.])", unit)
    if parts[0] == "" and len(parts) > 1 and parts[1] == "/":
        out = [""]
    else:
        first = _atom(parts[0]) if parts[0] else None
        if first is None:
            return None
        out = [first]
    for separator, atom in zip(parts[1::2], parts[2::2]):
        mapped = _atom(atom) if atom else None
        if mapped is None:
            return None
        out += [separator, mapped]
    result = "".join(out)
    return _CANONICAL.get(result, result)


@lru_cache(maxsize=65536)
def to_ucum(spelling: str | None) -> str | None:
    """The canonical UCUM code of a unit ``spelling``, or ``None`` if it is not known.

    The rules, in order: HTML entities are decoded (``&#176;C``) and whitespace is stripped; a
    placeholder for a missing unit gives ``None``; a spelling known as a whole unit (``C``,
    ``K/uL``, ``bpm``) gives its UCUM code; otherwise, with inner whitespace removed and ``µ``/``μ``
    as ``u``, the spelling is split on ``/`` and ``.`` and mapped part by part, and is known only if
    every part is. A part is a canonical UCUM atom, a word (``hours``, ``mcg``, case-insensitive), a
    prefixed unit (``mEq`` is ``meq``, ``dl`` is ``dL``, ``mcg`` is ``ug``), an annotation
    (``{beats}``), a power of ten (``10^9`` is ``10*9``), or one of these with an exponent (``m^2``
    is ``m2``) or a count (``24hr`` is ``(24.h)``).

    >>> [to_ucum(s) for s in ["mmol/l", "µmol/L", "U/l", "mEq/L", "mm Hg", "cm H2O"]]
    ['mmol/L', 'umol/L', 'U/L', 'meq/L', 'mm[Hg]', 'cm[H2O]']
    >>> [to_ucum(s) for s in ["lbs.", "Inch", "kilograms", "ml", "cc", "Liters", "fl"]]
    ['[lb_av]', '[in_i]', 'kg', 'mL', 'mL', 'L', 'fL']
    >>> [to_ucum(s) for s in ["mg/24hr", "mL/m^2", "#/uL", "/hpf", "mOsm/kg", "&#176;C"]]
    ['mg/(24.h)', 'mL/m2', '/uL', '/[HPF]', 'mosm/kg', 'Cel']
    >>> [to_ucum(s) for s in ["CEL", "G/24HR", "m/uL", "log10 IU/mL", None]]
    [None, None, None, None, None]
    """

    if spelling is None:
        return None
    text = _SPACE.sub(" ", html.unescape(spelling).strip())
    if text.casefold() in PLACEHOLDERS:
        return None
    compact = _SPACE.sub("", text).replace("µ", "u").replace("μ", "u")
    if compact in _AMBIGUOUS:
        return None
    for candidate in dict.fromkeys((text, compact)):
        if candidate in _WHOLE:
            return _WHOLE[candidate]
        if candidate.casefold() in _WHOLE_WORDS:
            return _WHOLE_WORDS[candidate.casefold()]
    return _compound(compact)


def known_spellings() -> Iterator[str]:
    """Every spelling the tables name, for tests and documentation (not every spelling known)."""

    yield from _WHOLE
    yield from _WHOLE_WORDS
    yield from _ATOMS
    yield from _WORDS
    yield from _CANONICAL
    for base, canonical in _BASES.items():
        yield base
        for prefix in _ONLY_PREFIXES.get(canonical, _PREFIXES):
            yield prefix + base


# --------------------------------------------------------------------------- tables


def _string_values(series: pl.Series) -> pl.Series:
    """The unit values of ``series`` (a String or a list of Strings), nulls dropped."""

    if isinstance(series.dtype, pl.List):
        series = pl.Series(series.name, series.to_arrow().flatten())
    return series.cast(pl.String).drop_nulls()


def normalize_units(
    df: pl.DataFrame, column: str = UNIT_COLUMN
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Rewrite the units of ``df[column]`` as canonical UCUM codes; return the frame and counts.

    A known spelling becomes its UCUM code (:func:`to_ucum`); an unknown spelling or a placeholder
    is kept as it is. :data:`SOURCE_COLUMN` keeps every unit as it was before: it is added after the
    last column, or, if it exists (a second run), kept, with only its nulls filled. ``column`` may
    hold Strings or lists of Strings; without it, ``df`` is returned unchanged. Rows keep their
    order. Running twice gives the same frame.

    The counts are by value (a row of a list column counts once per unit): ``rows_with_unit``,
    ``already_canonical`` (its own UCUM code), ``rewritten``, ``unknown_kept``, ``placeholder_kept``,
    and ``unknown_spellings`` (each unknown spelling and its count, most frequent first).

    >>> df = pl.DataFrame({"code": ["A", "B", "C", "D", "E"],
    ...                    "unit": ["mg/dl", "mg/dL", None, "furlongs", "UNK"]})
    >>> out, counts = normalize_units(df)
    >>> out.rows()
    [('A', 'mg/dL', 'mg/dl'), ('B', 'mg/dL', 'mg/dL'), ('C', None, None), ('D', 'furlongs', 'furlongs'), ('E', 'UNK', 'UNK')]
    >>> {key: counts[key] for key in COUNT_KEYS}
    {'rows_with_unit': 4, 'already_canonical': 1, 'rewritten': 1, 'unknown_kept': 1, 'placeholder_kept': 1}
    >>> counts["unknown_spellings"]
    {'furlongs': 1}
    >>> normalize_units(out)[0].equals(out)
    True
    """

    counts: dict[str, Any] = dict.fromkeys(COUNT_KEYS, 0)
    counts["unknown_spellings"] = {}
    if column not in df.columns:
        return df, counts
    original = df[column]
    unknown: Counter[str] = Counter()
    mapping: dict[str, str] = {}
    for spelling, n in _string_values(original).value_counts().iter_rows():
        counts["rows_with_unit"] += n
        ucum = to_ucum(spelling)
        if ucum is None:
            if is_placeholder(spelling):
                counts["placeholder_kept"] += n
            else:
                counts["unknown_kept"] += n
                unknown[spelling] += n
        elif ucum == spelling:
            counts["already_canonical"] += n
        else:
            counts["rewritten"] += n
            mapping[spelling] = ucum
    counts["unknown_spellings"] = dict(
        sorted(unknown.items(), key=lambda item: (-item[1], item[0]))
    )
    old, new = list(mapping), list(mapping.values())
    if isinstance(original.dtype, pl.List):
        rewritten = pl.col(column).list.eval(
            pl.element().cast(pl.String).replace(old, new)
        )
    else:
        rewritten = pl.col(column).cast(pl.String).replace(old, new)
    if SOURCE_COLUMN in df.columns:
        source = pl.coalesce(
            pl.col(SOURCE_COLUMN), pl.col(column).cast(df[SOURCE_COLUMN].dtype)
        )
    else:
        source = pl.col(column).cast(
            original.dtype if isinstance(original.dtype, pl.List) else pl.String
        )
    return df.with_columns(rewritten.alias(column), source.alias(SOURCE_COLUMN)), counts


def _log_counts(counts: dict[str, Any], path: Path) -> None:
    logger.info(
        "Unit spellings in %s: %s with a unit, %s already canonical UCUM, %s rewritten, "
        "%s unknown kept, %s placeholders kept",
        path,
        *(f"{counts[key]:,}" for key in COUNT_KEYS),
    )
    if counts["unknown_spellings"]:
        logger.warning(
            "%d unit spellings are not known to suMEDS and were kept as recorded "
            "(spelling: rows): %s",
            len(counts["unknown_spellings"]),
            ", ".join(f"{s!r}: {n}" for s, n in counts["unknown_spellings"].items()),
        )


def _same_file(a: Path, b: Path) -> bool:
    return a.resolve() == b.resolve()


def normalize_units_file(
    input_path: str | Path, output_path: str | Path | None = None
) -> dict[str, Any]:
    """Normalise the units of a MEDS ``codes.parquet`` (:func:`normalize_units`); return the counts.

    The output (by default the input itself) is written atomically. Only ``unit`` changes and
    :data:`SOURCE_COLUMN` is added (``large_string``, or ``unit``'s type for a list column); every
    other column keeps its Arrow type and position, and rows keep their order. The result is
    validated against ``meds.CodeMetadataSchema``. Without a ``unit`` column, the file is left as
    it is (or copied, to a different output). Counts are logged, and unknown spellings at WARNING.
    """

    input_path = Path(input_path)
    output_path = input_path if output_path is None else Path(output_path)
    table = pq.read_table(input_path)
    if UNIT_COLUMN not in table.column_names:
        logger.info(
            "%s has no %r column; units left as they are", input_path, UNIT_COLUMN
        )
        if not _same_file(input_path, output_path):
            _replace_atomically(
                output_path, lambda staged: shutil.copyfile(input_path, staged)
            )
        return normalize_units(pl.DataFrame())[1]
    unit_type = table.schema.field(UNIT_COLUMN).type
    columns = [UNIT_COLUMN]
    if SOURCE_COLUMN in table.column_names:
        columns.append(SOURCE_COLUMN)
    frame = pl.from_arrow(table.select(columns))
    assert isinstance(frame, pl.DataFrame)
    frame, counts = normalize_units(frame)
    unit = frame[UNIT_COLUMN].to_arrow().cast(unit_type)
    table = table.set_column(
        table.schema.get_field_index(UNIT_COLUMN), UNIT_COLUMN, unit
    )
    if SOURCE_COLUMN in table.column_names:
        index = table.schema.get_field_index(SOURCE_COLUMN)
        source_type = table.schema.field(SOURCE_COLUMN).type
        source = frame[SOURCE_COLUMN].to_arrow().cast(source_type)
        table = table.set_column(index, SOURCE_COLUMN, source)
    else:
        source_type = (
            unit_type
            if pa.types.is_list(unit_type) or pa.types.is_large_list(unit_type)
            else pa.large_string()
        )
        table = table.append_column(
            pa.field(SOURCE_COLUMN, source_type),
            frame[SOURCE_COLUMN].to_arrow().cast(source_type),
        )
    meds.CodeMetadataSchema.validate(table)
    _replace_atomically(
        output_path, lambda staged: pq.write_table(table, staged, compression="zstd")
    )
    _log_counts(counts, output_path)
    return counts


def _replace_atomically(path: Path, write: Any) -> None:
    """Write ``path`` through a staged file beside it, which replaces it only once complete."""

    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(f".{path.name}.{uuid4().hex}.tmp{path.suffix}")
    try:
        write(staged)
        os.replace(staged, path)
    finally:
        staged.unlink(missing_ok=True)
