"""Unit siblings: codes that differ only in their unit, linked to a shared stem.

Where a MEDS ETL records an item in more than one unit, MEDS-Extract (0.9 and later) needs the unit
in the code: ``unit`` is one string per code, so ``LAB//<item>//mg/dL`` and ``LAB//<item>//mmol/L``
are two codes, each with its own ``unit``. :func:`link_unit_siblings` makes such a family findable
as one item:

- A code is **unit-suffixed** when its last ``//`` component is its recorded unit
  (``unit_source_value`` if :func:`sumeds.normalize_units` already ran, else ``unit``), compared
  case-sensitively. A code whose last component stands for an unrecorded unit
  (``unknown_unit_components``, by default ``UNK``) is unit-suffixed too, whatever its ``unit``.
  The **stem** is the code without that component.
- Two or more distinct unit-suffixed codes with the same stem, at least one of them with a real
  unit, are **unit siblings**. Each gets the stem appended to its ``parent_codes`` (after the
  parents it has; never twice).
- A stem that is not already a code gets a row of its own: no ``unit``, the parents every sibling
  with a real unit shares (siblings whose unit is unrecorded, ``UNK``, are linked but do not narrow
  the stem's parents; the first such sibling's order, the stem itself left out; null if none), and
  the description
  the siblings share (see ``stem_description``). Every other column is null. An existing row for
  the stem is left as it is.
- With ``link_single_units``, a unit-suffixed code with a real unit and no sibling is linked to its
  stem as well, as a group of one: it decides the stem's parents, and its description without its
  unit is the stem's (``stem_description="without_unit"``). A code ending in an unknown-unit
  component is never linked on its own, as two such codes alone are no group: its last component
  says nothing about which unit, or whether a unit, is meant.

A description may state its code's unit (``description_unit_pattern``, a regular expression whose
group ``unit``, or else group 1, is the unit). Such a unit fills a missing ``unit``
(``fill_units``), only if it is unit-like (or in ``description_units``, if given); a unit from the
data always wins. The same span is what ``stem_description="without_unit"`` removes from sibling
descriptions.

The ``sumeds_link_unit_siblings`` MEDS-Extract post-stage (:mod:`sumeds.unit_siblings_stage`) and
``suMEDS-link-unit-siblings`` run :func:`link_unit_siblings_file`.

>>> df = pl.DataFrame({
...     "code": ["LAB//NA//mmol/L", "LAB//NA//mEq/L", "LAB//NA//UNK", "LAB//K//mmol/L"],
...     "description": ["Sodium (mmol/L)", "Sodium (mEq/L)", "Sodium", "Potassium"],
...     "unit": ["mmol/L", "mEq/L", None, "mmol/L"],
...     "parent_codes": [["LOINC//2951-2"], ["LOINC//2951-2"], ["LOINC//2951-2"], None],
... })
>>> out, counts = link_unit_siblings(df)
>>> for row in out.rows():
...     print(row)
('LAB//NA//mmol/L', 'Sodium (mmol/L)', 'mmol/L', ['LOINC//2951-2', 'LAB//NA'])
('LAB//NA//mEq/L', 'Sodium (mEq/L)', 'mEq/L', ['LOINC//2951-2', 'LAB//NA'])
('LAB//NA//UNK', 'Sodium', None, ['LOINC//2951-2', 'LAB//NA'])
('LAB//K//mmol/L', 'Potassium', 'mmol/L', None)
('LAB//NA', 'Sodium', None, ['LOINC//2951-2'])
>>> counts["sibling_groups"], counts["sibling_codes"], counts["stem_rows_added"]
(1, 3, 1)
>>> link_unit_siblings(out)[0].equals(out)
True

With ``link_single_units``, a code with a real unit and no sibling gets its stem too:

>>> single, counts = link_unit_siblings(df, link_single_units=True)
>>> for row in single.filter(pl.col("code").str.starts_with("LAB//K")).rows():
...     print(row)
('LAB//K//mmol/L', 'Potassium', 'mmol/L', ['LAB//K'])
('LAB//K', 'Potassium', None, None)
>>> counts["sibling_groups"], counts["single_unit_codes"], counts["stem_rows_added"]
(1, 1, 2)
"""

from __future__ import annotations

import logging
import re
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Literal

import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from .units import SOURCE_COLUMN, UNIT_COLUMN, _replace_atomically, _same_file
from .units import is_placeholder, to_ucum

__all__ = [
    "COUNT_KEYS",
    "DEFAULT_UNKNOWN_UNIT_COMPONENTS",
    "is_unit_like",
    "link_unit_siblings",
    "link_unit_siblings_file",
    "unit_stem",
    "without_unit",
]

logger = logging.getLogger(__name__)

CODE_COLUMN = "code"
DESCRIPTION_COLUMN = "description"
PARENTS_COLUMN = "parent_codes"
#: The separator between code components.
SEPARATOR = "//"
#: Last code components that stand for an unrecorded unit (an ETL's ``$unit ?? 'UNK'``).
DEFAULT_UNKNOWN_UNIT_COMPONENTS: tuple[str, ...] = ("UNK",)
#: The keys of the counts :func:`link_unit_siblings` returns.
COUNT_KEYS = (
    "with_unit",
    "unit_suffixed",
    "unknown_suffixed",
    "sibling_groups",
    "sibling_codes",
    "parents_added",
    "stem_rows_added",
    "stem_rows_existing",
    "single_unit_codes",
    "description_units",
    "description_units_rejected",
    "description_units_filled",
    "description_unit_conflicts",
)
StemDescription = Literal["shared", "without_unit"]
FillUnits = Literal["never", "missing", "unknown_component"]
_STEM_DESCRIPTIONS = ("shared", "without_unit")
_FILL_UNITS = ("never", "missing", "unknown_component")
_UNIT_SHAPE = re.compile(r"^(?=.*[A-Za-z%°µμ])[A-Za-z0-9%°µμ/.*^·+\-\[\]{}'() ]{1,32}$")


def is_unit_like(text: str) -> bool:
    """Whether ``text`` has the shape of a unit: UCUM-like characters, and not a placeholder.

    >>> [is_unit_like(t) for t in ["mg/dL", "10^9/L", "%", "mmHg", "UNKNOWN", "12", "see note: x"]]
    [True, True, True, True, False, False, False]
    """

    text = text.strip()
    return not is_placeholder(text) and bool(_UNIT_SHAPE.match(text))


def unit_stem(code: str | None, unit: str | None) -> str | None:
    """``code`` without its ``//<unit>`` suffix; ``None`` if it does not end with it.

    A blank unit and an empty stem are not suffixes.

    >>> unit_stem("LAB//GLUCOSE//mg/dL", "mg/dL")
    'LAB//GLUCOSE'
    >>> unit_stem("LAB//GLUCOSE//mg/dL", "mmol/L") is None, unit_stem("//mg", "mg") is None
    (True, True)
    """

    if not code or not unit or not code.endswith(SEPARATOR + unit):
        return None
    return code[: -len(unit) - len(SEPARATOR)] or None


def without_unit(description: str | None, unit: str | None) -> str | None:
    """``description`` with its last mention of ``unit`` removed; ``None`` if it has none.

    The removed span is the unit, with its brackets if bracketed (``[mg]``, ``(mg)``), and the
    whitespace before it; the result is trimmed. An unbracketed unit must stand on its own.

    >>> without_unit("Heart rate [/min] (charted)", "/min")
    'Heart rate (charted)'
    >>> without_unit("Dose in mg", "mg"), without_unit("Dose in mg/kg", "mg") is None
    ('Dose in', True)
    """

    if not description or not unit:
        return None
    escaped = re.escape(unit)
    matches = list(
        re.finditer(
            rf"\s*(?:\[\s*{escaped}\s*\]|\(\s*{escaped}\s*\)|(?<![\w/]){escaped}(?![\w/]))",
            description,
        )
    )
    if not matches:
        return None
    span = matches[-1]
    return (description[: span.start()] + description[span.end() :]).strip()


def _compile(pattern: str | re.Pattern[str] | None) -> re.Pattern[str] | None:
    if pattern is None or isinstance(pattern, re.Pattern):
        return pattern
    compiled = re.compile(pattern)
    if "unit" not in compiled.groupindex and compiled.groups < 1:
        raise ValueError(
            f"description_unit_pattern {pattern!r} needs a group for the unit: "
            "(?P<unit>...) or a first group"
        )
    return compiled


def _stated_unit(
    description: str | None,
    pattern: re.Pattern[str] | None,
    allowed: frozenset[str] | None,
) -> tuple[re.Match[str] | None, str | None]:
    """The match of ``pattern`` in ``description`` and the unit it states, if any."""

    if pattern is None or not description:
        return None, None
    match = pattern.search(description)
    if match is None:
        return None, None
    stated = match["unit"] if "unit" in pattern.groupindex else match[1]
    if stated is None:
        return None, None
    stated = stated.strip()
    if allowed is not None and stated not in allowed:
        return None, None
    return match, stated


def _string_list(values: Iterable[Any] | None) -> list[str]:
    return [] if values is None else list(values)


def _last_component(code: str) -> str | None:
    head, separator, last = code.rpartition(SEPARATOR)
    return last if separator and head else None


def _plan(
    codes: list[str],
    units: list[Any],
    sources: list[Any] | None,
    descriptions: list[str | None] | None,
    parents: list[list[str] | None] | None,
    *,
    list_units: bool,
    unknown_unit_components: frozenset[str],
    link_unknown_units: bool,
    link_single_units: bool,
    stem_description: str,
    description_unit_pattern: re.Pattern[str] | None,
    description_units: frozenset[str] | None,
    fill_units: str,
) -> dict[str, Any]:
    """Everything :func:`link_unit_siblings` changes, as plain Python values."""

    counts = dict.fromkeys(COUNT_KEYS, 0)
    n = len(codes)
    unknown = unknown_unit_components

    # 1. Units stated in descriptions fill missing units; the data always wins.
    new_units, new_sources = list(units), None if sources is None else list(sources)
    filled = 0
    if description_unit_pattern is not None and descriptions is not None:
        for i, description in enumerate(descriptions):
            if not description or description_unit_pattern.search(description) is None:
                continue
            _, stated = _stated_unit(
                description, description_unit_pattern, description_units
            )
            if stated is None or (
                description_units is None and not is_unit_like(stated)
            ):
                counts["description_units_rejected"] += 1
                continue
            counts["description_units"] += 1
            recorded = units[i] if sources is None or sources[i] is None else sources[i]
            if recorded not in (None, []):
                in_data = recorded if isinstance(recorded, list) else [recorded]
                counts["description_unit_conflicts"] += stated not in in_data
                continue
            if fill_units == "never" or (
                fill_units == "unknown_component"
                and _last_component(codes[i]) not in unknown
            ):
                continue
            filled += 1
            ucum = stated if new_sources is None else to_ucum(stated) or stated
            new_units[i] = [ucum] if list_units else ucum
            if new_sources is not None:
                new_sources[i] = [stated] if list_units else stated
    counts["description_units_filled"] = filled

    # 2. Unit-suffixed codes and their stems.
    stems: list[str | None] = [None] * n
    suffixes: list[str | None] = [None] * n
    real: list[bool] = [False] * n
    for i, code in enumerate(codes):
        unit = new_units[i]
        source = None if new_sources is None else new_sources[i]
        if unit is not None and unit != []:
            counts["with_unit"] += 1
        if not code:
            continue
        last = _last_component(code)
        if link_unknown_units and last is not None and last in unknown:
            stems[i], suffixes[i] = code[: -len(last) - len(SEPARATOR)], last
            counts["unknown_suffixed"] += 1
            continue
        for value in (source, unit):
            for candidate in value if isinstance(value, list) else [value]:
                if (
                    stems[i] is None
                    and (stem := unit_stem(code, candidate)) is not None
                ):
                    stems[i], suffixes[i], real[i] = stem, candidate, True
        counts["unit_suffixed"] += real[i]

    groups: dict[str, list[int]] = {}
    for i, stem in enumerate(stems):
        if stem is not None:
            groups.setdefault(stem, []).append(i)
    # A group needs a code with a real unit: codes ending in `UNK` alone are never one. A single
    # code is a group only with `link_single_units`.
    min_codes = 1 if link_single_units else 2
    groups = {
        stem: members
        for stem, members in groups.items()
        if len({codes[i] for i in members}) >= min_codes
        and any(real[i] for i in members)
    }
    single = {
        stem
        for stem, members in groups.items()
        if len({codes[i] for i in members}) == 1
    }
    counts["sibling_groups"] = len(groups) - len(single)
    counts["sibling_codes"] = sum(
        len(members) for stem, members in groups.items() if stem not in single
    )
    counts["single_unit_codes"] = sum(len(groups[stem]) for stem in single)

    # 3. Each sibling gets its stem as a parent.
    new_parents = None if parents is None else list(parents)
    if groups:
        if new_parents is None:
            new_parents = [None] * n
        for stem, members in groups.items():
            for i in members:
                old = new_parents[i]
                if old is None or stem not in old:
                    new_parents[i] = [*_string_list(old), stem]
                    counts["parents_added"] += 1

    # 4. A row for each stem without one.
    present = set(codes)
    stem_rows: list[dict[str, Any]] = []
    for stem, members in groups.items():
        if stem in present:
            counts["stem_rows_existing"] += 1
            continue
        row: dict[str, Any] = {CODE_COLUMN: stem}
        if descriptions is not None:
            row[DESCRIPTION_COLUMN] = _stem_description(
                [descriptions[i] for i in members],
                [suffixes[i] for i in members],
                stem_description,
                description_unit_pattern,
                description_units,
                single=stem in single,
            )
        if new_parents is not None:
            # Only siblings with a real unit decide the stem's parents: an `UNK` sibling is linked
            # to the stem but must not drop a parent the real-unit siblings share.
            lists = [
                [p for p in _string_list(new_parents[i]) if p != stem]
                for i in members
                if real[i]
            ]
            shared = [
                p
                for p in dict.fromkeys(lists[0])
                if all(p in other for other in lists[1:])
            ]
            row[PARENTS_COLUMN] = shared or None
        stem_rows.append(row)
    counts["stem_rows_added"] = len(stem_rows)
    return {
        "units": new_units,
        "sources": new_sources,
        "units_changed": filled > 0,
        "parents": new_parents,
        "parents_changed": counts["parents_added"] > 0,
        "stem_rows": stem_rows,
        "counts": counts,
    }


def _stem_description(
    descriptions: list[str | None],
    suffixes: list[str | None],
    rule: str,
    pattern: re.Pattern[str] | None,
    allowed: frozenset[str] | None,
    *,
    single: bool = False,
) -> str | None:
    """The description of a new stem row (see :func:`link_unit_siblings`).

    A single code's description is always stripped of its unit under ``without_unit``: it is not a
    description several units share.
    """

    shared = set(descriptions)
    if len(shared) == 1 and not (single and rule == "without_unit"):
        return shared.pop()
    if rule == "shared":
        return None
    bare = set()
    for description, suffix in zip(descriptions, suffixes, strict=True):
        if description is None:
            bare.add(None)
            continue
        match, _ = _stated_unit(description, pattern, allowed)
        if match is not None:
            stripped = (
                description[: match.start()] + description[match.end() :]
            ).strip()
        else:
            stripped = without_unit(description, suffix)
            if stripped is None:
                stripped = description.strip()
        bare.add(stripped)
    if len(bare) == 1 and (description := bare.pop()):
        return description
    return None


def _resolve(
    unknown_unit_components: Iterable[str] = DEFAULT_UNKNOWN_UNIT_COMPONENTS,
    link_unknown_units: bool = True,
    link_single_units: bool = False,
    stem_description: str = "without_unit",
    description_unit_pattern: str | re.Pattern[str] | None = None,
    description_units: Iterable[str] | None = None,
    fill_units: str = "missing",
) -> dict[str, Any]:
    """The options of :func:`link_unit_siblings`, checked, in the form :func:`_plan` takes."""

    if stem_description not in _STEM_DESCRIPTIONS:
        raise ValueError(
            f"stem_description must be one of {_STEM_DESCRIPTIONS}, not {stem_description!r}"
        )
    if fill_units not in _FILL_UNITS:
        raise ValueError(f"fill_units must be one of {_FILL_UNITS}, not {fill_units!r}")
    if isinstance(unknown_unit_components, str) or isinstance(description_units, str):
        raise TypeError(
            "unknown_unit_components and description_units are lists of strings"
        )
    return {
        "unknown_unit_components": frozenset(unknown_unit_components or ()),
        "link_unknown_units": bool(link_unknown_units),
        "link_single_units": bool(link_single_units),
        "stem_description": stem_description,
        "description_unit_pattern": _compile(description_unit_pattern),
        "description_units": None
        if description_units is None
        else frozenset(description_units),
        "fill_units": fill_units,
    }


def link_unit_siblings(
    df: pl.DataFrame,
    *,
    unknown_unit_components: Iterable[str] = DEFAULT_UNKNOWN_UNIT_COMPONENTS,
    link_unknown_units: bool = True,
    link_single_units: bool = False,
    stem_description: StemDescription = "without_unit",
    description_unit_pattern: str | re.Pattern[str] | None = None,
    description_units: Iterable[str] | None = None,
    fill_units: FillUnits = "missing",
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Link the unit siblings of a code-metadata frame; return the frame and counts.

    See the module for the rules. ``df`` needs ``code`` and ``unit`` (strings, or lists of
    strings as older MEDS-Extract releases wrote them); ``unit_source_value``, ``description`` and
    ``parent_codes`` are used if present. Without ``unit``, ``df`` is returned unchanged.

    Args:
        df: The code metadata.
        unknown_unit_components: Last code components that stand for an unrecorded unit (an
            ETL's ``$unit ?? 'UNK'``).
        link_unknown_units: Whether a code ending in one of ``unknown_unit_components`` joins its
            item's unit siblings (if the item has a code with a real unit). ``False`` leaves these
            codes out, as the ETL-local stages did.
        link_single_units: Whether a unit-suffixed code with a real unit and no sibling is linked
            to its stem too (and the stem gets a row), so that every code ending in its unit has
            its unit-free stem as a parent. A code ending in one of ``unknown_unit_components``
            is still linked only with a real-unit sibling. ``False`` (default): only siblings are.
        stem_description: ``"shared"``: a stem row gets the siblings' description only if they all
            have the same one. ``"without_unit"`` (default): if they differ, each sibling's
            description without its unit (the span ``description_unit_pattern`` matches, else the
            last mention of its own unit, :func:`without_unit`) is compared instead.
        description_unit_pattern: A regular expression for a unit stated in a description, whose
            group ``unit`` (else group 1) is the unit; ``None`` (default): none.
        description_units: If given, only these stated units count; otherwise any unit-like one
            (:func:`is_unit_like`).
        fill_units: Which codes without a unit get the unit their description states:
            ``"missing"`` (default; every code without a unit), ``"unknown_component"`` (only a
            code whose last component is in ``unknown_unit_components``) or ``"never"``. A unit
            from the data is never replaced. With ``unit_source_value`` present (normalised
            units), the stated spelling fills it and its UCUM code (:func:`sumeds.to_ucum`, or the
            spelling if unknown) fills ``unit``.

    Returns:
        The frame (rows in their order; new stem rows inserted in code order if ``df`` is sorted
        by code, else appended; ``parent_codes`` added if needed) and the counts
        :data:`COUNT_KEYS`.
    """

    options = _resolve(
        unknown_unit_components,
        link_unknown_units,
        link_single_units,
        stem_description,
        description_unit_pattern,
        description_units,
        fill_units,
    )
    if UNIT_COLUMN not in df.columns:
        return df, dict.fromkeys(COUNT_KEYS, 0)
    plan = _plan(
        df[CODE_COLUMN].cast(pl.String).to_list(),
        df[UNIT_COLUMN].to_list(),
        df[SOURCE_COLUMN].to_list() if SOURCE_COLUMN in df.columns else None,
        df[DESCRIPTION_COLUMN].to_list() if DESCRIPTION_COLUMN in df.columns else None,
        df[PARENTS_COLUMN].to_list() if PARENTS_COLUMN in df.columns else None,
        list_units=isinstance(df[UNIT_COLUMN].dtype, pl.List),
        **options,
    )
    out = df
    if plan["units_changed"]:
        out = out.with_columns(
            pl.Series(UNIT_COLUMN, plan["units"], dtype=df[UNIT_COLUMN].dtype)
        )
        if plan["sources"] is not None:
            out = out.with_columns(
                pl.Series(SOURCE_COLUMN, plan["sources"], dtype=df[SOURCE_COLUMN].dtype)
            )
    if plan["parents_changed"]:
        dtype = (
            df[PARENTS_COLUMN].dtype
            if PARENTS_COLUMN in df.columns and df[PARENTS_COLUMN].dtype != pl.Null
            else pl.List(pl.String)
        )
        out = out.with_columns(pl.Series(PARENTS_COLUMN, plan["parents"], dtype=dtype))
    if plan["stem_rows"]:
        rows = pl.DataFrame(
            plan["stem_rows"],
            schema={name: out.schema[name] for name in plan["stem_rows"][0]},
        )
        out = pl.concat([out, rows], how="diagonal_relaxed")
        if df[CODE_COLUMN].is_sorted():
            out = out.sort(CODE_COLUMN, maintain_order=True)
    return out, plan["counts"]


def _validate(table: pa.Table) -> None:
    """Validate ``table`` as MEDS code metadata, its MEDS columns in their MEDS types.

    Files written by polars hold ``large_string``/``large_list``, which the MEDS schema rejects
    although the values are the same; only the check sees them cast.
    """

    expected = {field.name: field.type for field in meds.CodeMetadataSchema.schema()}
    checked = table.select(
        [name for name in table.column_names if name in expected]
    ).cast(
        pa.schema(
            [
                pa.field(name, expected[name])
                for name in table.column_names
                if name in expected
            ]
        )
    )
    meds.CodeMetadataSchema.validate(checked)


def link_unit_siblings_file(
    input_path: str | Path,
    output_path: str | Path | None = None,
    **options: Any,
) -> dict[str, int]:
    """Link the unit siblings of a MEDS ``codes.parquet`` (:func:`link_unit_siblings`).

    The output (by default the input itself) is written atomically, and an input that needs no
    change is not rewritten (or is copied, to a different output). Only ``parent_codes`` (and, if
    units are filled from descriptions, ``unit`` and ``unit_source_value``) change and stem rows
    are added: every column keeps its Arrow type and position (a new ``parent_codes`` is
    ``list<string>``), existing rows keep their order, and stem rows are inserted in code order if
    the file is sorted by code, else appended. The result is validated against
    ``meds.CodeMetadataSchema``. ``options`` are those of :func:`link_unit_siblings`.

    Returns:
        The counts :data:`COUNT_KEYS`.
    """

    input_path = Path(input_path)
    output_path = input_path if output_path is None else Path(output_path)
    resolved = _resolve(**options)
    table = pq.read_table(input_path)

    def unchanged(reason: str) -> dict[str, int]:
        logger.info("%s: %s; left as it is", input_path, reason)
        if not _same_file(input_path, output_path):
            _replace_atomically(
                output_path, lambda staged: shutil.copyfile(input_path, staged)
            )
        return counts

    counts = dict.fromkeys(COUNT_KEYS, 0)
    if UNIT_COLUMN not in table.column_names:
        return unchanged(f"no {UNIT_COLUMN!r} column")
    unit_type = table.schema.field(UNIT_COLUMN).type
    if not (
        pa.types.is_string(unit_type)
        or pa.types.is_large_string(unit_type)
        or pa.types.is_null(unit_type)
        or pa.types.is_list(unit_type)
        or pa.types.is_large_list(unit_type)
    ):
        raise TypeError(f"{UNIT_COLUMN!r} in {input_path} is {unit_type}, not a string")

    def column(name: str) -> list[Any] | None:
        return table.column(name).to_pylist() if name in table.column_names else None

    codes = column(CODE_COLUMN)
    if codes is None:
        raise ValueError(f"{input_path} has no {CODE_COLUMN!r} column")
    plan = _plan(
        codes,
        column(UNIT_COLUMN),
        column(SOURCE_COLUMN),
        column(DESCRIPTION_COLUMN),
        column(PARENTS_COLUMN),
        list_units=pa.types.is_list(unit_type) or pa.types.is_large_list(unit_type),
        **resolved,
    )
    counts = plan["counts"]
    _log_counts(counts, input_path)
    if not (plan["units_changed"] or plan["parents_changed"] or plan["stem_rows"]):
        return unchanged("no unit siblings to link")

    out = table
    if plan["units_changed"]:
        out = out.set_column(
            out.schema.get_field_index(UNIT_COLUMN),
            out.schema.field(UNIT_COLUMN),
            pa.array(plan["units"], unit_type),
        )
        if plan["sources"] is not None:
            field = out.schema.field(SOURCE_COLUMN)
            out = out.set_column(
                out.schema.get_field_index(SOURCE_COLUMN),
                field,
                pa.array(plan["sources"], field.type),
            )
    if plan["parents_changed"]:
        if PARENTS_COLUMN in out.column_names:
            field = out.schema.field(PARENTS_COLUMN)
            if pa.types.is_null(field.type):
                field = field.with_type(pa.list_(pa.string()))
            out = out.set_column(
                out.schema.get_field_index(PARENTS_COLUMN),
                field,
                pa.array(plan["parents"], field.type),
            )
        else:
            out = out.append_column(
                pa.field(PARENTS_COLUMN, pa.list_(pa.string())),
                pa.array(plan["parents"], pa.list_(pa.string())),
            )
    if plan["stem_rows"]:
        new = pa.Table.from_pylist(plan["stem_rows"], schema=out.schema)
        was_sorted = None not in codes and codes == sorted(codes)
        out = pa.concat_tables([out, new])
        if was_sorted:
            out = out.sort_by(CODE_COLUMN)
    _validate(out)
    _replace_atomically(
        output_path, lambda staged: pq.write_table(out, staged, compression="zstd")
    )
    return counts


def _log_counts(counts: dict[str, int], path: Path) -> None:
    logger.info(
        "Unit siblings in %s: %s codes with a unit; %s unit-suffixed and %s with an unknown-unit "
        "component; %s codes in %s sibling groups (%s got their stem as a parent); %s stem rows "
        "added, %s stems already had one",
        path,
        *(
            f"{counts[key]:,}"
            for key in (
                "with_unit",
                "unit_suffixed",
                "unknown_suffixed",
                "sibling_codes",
                "sibling_groups",
                "parents_added",
                "stem_rows_added",
                "stem_rows_existing",
            )
        ),
    )
    if counts["single_unit_codes"]:
        logger.info(
            "Of these, %s codes ending in their unit have no sibling and got their stem as a "
            "parent (link_single_units)",
            f"{counts['single_unit_codes']:,}",
        )
    if counts["description_units"] or counts["description_units_rejected"]:
        logger.info(
            "Units stated in descriptions: %s unit-like (%s filled a missing unit, %s disagree with "
            "the data, which wins), %s rejected",
            *(
                f"{counts[key]:,}"
                for key in (
                    "description_units",
                    "description_units_filled",
                    "description_unit_conflicts",
                    "description_units_rejected",
                )
            ),
        )
