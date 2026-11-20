"""Unit siblings, and the ``sumeds_link_unit_siblings`` stage."""

from __future__ import annotations

import logging
from importlib.metadata import entry_points
from pathlib import Path
from types import SimpleNamespace

import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sumeds.cli import link_unit_siblings_main
from sumeds.unit_siblings import (
    is_unit_like,
    link_unit_siblings,
    link_unit_siblings_file,
    unit_stem,
    without_unit,
)
from sumeds.unit_siblings_stage import DEFAULT_CONFIG, STAGE_NAME, _main, stage_options
from sumeds.units import normalize_units

# ------------------------------------------------------------------ frames


def _frame(rows: list[tuple], parents: bool = True) -> pl.DataFrame:
    """A code-metadata frame from (code, description, unit[, parent_codes]) rows."""

    data = {
        "code": [r[0] for r in rows],
        "description": [r[1] for r in rows],
        "unit": [r[2] for r in rows],
    }
    schema = {"code": pl.String, "description": pl.String, "unit": pl.String}
    if parents:
        data["parent_codes"] = [r[3] if len(r) > 3 else None for r in rows]
        schema["parent_codes"] = pl.List(pl.String)
    return pl.DataFrame(data, schema=schema)


def _rows(df: pl.DataFrame) -> dict[str, dict]:
    return {row["code"]: row for row in df.to_dicts()}


def test_siblings_get_their_stem_as_parent_and_the_stem_a_row() -> None:
    df = _frame(
        [
            ("X//a//mg", "Drug a", "mg", ["ATC//N02", "ATC//N"]),
            ("X//a//mcg", "Drug a", "mcg", ["ATC//N"]),
            ("X//b//mg", "Drug b", "mg", None),
            ("Y", None, None, None),
        ]
    )
    out, counts = link_unit_siblings(df)
    rows = _rows(out)
    assert rows["X//a//mg"]["parent_codes"] == ["ATC//N02", "ATC//N", "X//a"]
    assert rows["X//a//mcg"]["parent_codes"] == ["ATC//N", "X//a"]
    assert rows["X//b//mg"]["parent_codes"] is None  # no sibling
    assert rows["X//a"] == {
        "code": "X//a",
        "description": "Drug a",
        "unit": None,
        "parent_codes": ["ATC//N"],
    }
    assert out.height == 5 and out["code"][-1] == "X//a"  # appended: not sorted
    assert counts["sibling_groups"] == 1
    assert counts["sibling_codes"] == 2
    assert counts["parents_added"] == 2
    assert counts["stem_rows_added"] == 1
    assert counts["with_unit"] == 3 and counts["unit_suffixed"] == 3
    # Idempotent.
    again, counts = link_unit_siblings(out)
    assert again.equals(out)
    assert counts["parents_added"] == 0 and counts["stem_rows_added"] == 0
    assert counts["stem_rows_existing"] == 1


def test_a_sorted_frame_stays_sorted() -> None:
    df = _frame([("A", None, None), ("X//a//g", "x", "g"), ("X//a//mg", "x", "mg")])
    out, _ = link_unit_siblings(df)
    assert out["code"].to_list() == ["A", "X//a", "X//a//g", "X//a//mg"]


def test_an_unknown_unit_sibling_does_not_narrow_the_stem_parents() -> None:
    """An `UNK` sibling is linked to the stem, but the stem keeps the parents its real-unit siblings
    share, even when the `UNK` sibling has other ones."""
    df = _frame(
        [
            ("M//d//mg", "Drug d", "mg", ["NDC/1"]),
            ("M//d//mcg", "Drug d", "mcg", ["NDC/1"]),
            ("M//d//UNK", "Drug d", None, ["NDC/2"]),
        ]
    )
    out, counts = link_unit_siblings(df)
    rows = _rows(out)
    assert rows["M//d//UNK"]["parent_codes"] == ["NDC/2", "M//d"]
    assert rows["M//d"]["parent_codes"] == ["NDC/1"]
    assert counts["sibling_codes"] == 3


def test_parent_codes_is_created_when_needed() -> None:
    df = _frame([("X//a//g", "x", "g"), ("X//a//mg", "x", "mg")], parents=False)
    out, _ = link_unit_siblings(df)
    assert out.schema["parent_codes"] == pl.List(pl.String)
    assert _rows(out)["X//a//g"]["parent_codes"] == ["X//a"]
    assert _rows(out)["X//a"]["parent_codes"] is None
    # Without siblings, nothing is added.
    alone, _ = link_unit_siblings(df.head(1))
    assert alone.columns == ["code", "description", "unit"]


def test_existing_parents_are_kept_as_they_are() -> None:
    df = _frame(
        [
            ("X//a//g", "x", "g", ["P", "P", "Q"]),
            ("X//a//mg", "x", "mg", ["X//a", "P", "Q"]),
        ]
    )
    rows = _rows(link_unit_siblings(df)[0])
    assert rows["X//a//g"]["parent_codes"] == ["P", "P", "Q", "X//a"]
    assert rows["X//a//mg"]["parent_codes"] == ["X//a", "P", "Q"]  # already there
    assert rows["X//a"]["parent_codes"] == ["P", "Q"]  # shared, deduplicated


def test_an_existing_stem_row_is_left_as_it_is() -> None:
    df = _frame(
        [("X//a", "kept", "g", ["P"]), ("X//a//g", "x", "g"), ("X//a//mg", "x", "mg")]
    )
    out, counts = link_unit_siblings(df)
    assert out.height == 3
    assert _rows(out)["X//a"] == {
        "code": "X//a",
        "description": "kept",
        "unit": "g",
        "parent_codes": ["P"],
    }
    assert counts["stem_rows_existing"] == 1
    assert _rows(out)["X//a//g"]["parent_codes"] == ["X//a"]


@pytest.mark.parametrize(
    ("code", "unit"),
    [
        ("X//a//mg", None),  # no unit
        ("X//a//mg", "g"),  # not its unit
        ("X//a//MG", "mg"),  # case carries meaning
        ("X//a//mg", ""),  # blank
        ("//mg", "mg"),  # empty stem
        ("X//a mg", "mg"),  # not a component
    ],
)
def test_what_is_not_unit_suffixed(code: str, unit: str | None) -> None:
    df = _frame([(code, "x", unit), ("X//a//kg", "x", "kg"), ("//kg", "x", "kg")])
    out, counts = link_unit_siblings(df)
    assert counts["sibling_groups"] == 0
    assert out.equals(df)


def test_unit_stem() -> None:
    assert unit_stem("A//B//mg/dL", "mg/dL") == "A//B"
    assert unit_stem("A//B//mg/dL", "dL") is None
    assert unit_stem("A//B//x", None) is None
    assert unit_stem(None, "x") is None


# ------------------------------------------------------------------ unknown units


def test_unknown_unit_codes_join_their_items_siblings() -> None:
    df = _frame(
        [
            ("L//na//UNK", "Sodium", None),
            ("L//na//mmol/L", "Sodium", "mmol/L"),
            ("L//k//UNK", "Potassium", None),  # no real unit: no group
            ("M//x//UNK", "x", "mg"),  # a unit filled from elsewhere: still unknown
            ("M//x//g", "x", "g"),
        ]
    )
    out, counts = link_unit_siblings(df)
    rows = _rows(out)
    assert rows["L//na//UNK"]["parent_codes"] == ["L//na"]
    assert rows["L//na//mmol/L"]["parent_codes"] == ["L//na"]
    assert rows["L//k//UNK"]["parent_codes"] is None
    assert "L//k" not in rows
    assert rows["M//x//UNK"]["parent_codes"] == ["M//x"]
    assert counts["unknown_suffixed"] == 3 and counts["unit_suffixed"] == 2
    assert counts["sibling_groups"] == 2

    off, counts = link_unit_siblings(df, link_unknown_units=False)
    assert counts["sibling_groups"] == 0 and off.equals(df)
    other, counts = link_unit_siblings(df, unknown_unit_components=["NA"])
    assert counts["sibling_groups"] == 0


def test_two_unknown_unit_codes_alone_make_no_group() -> None:
    df = _frame([("X//a//UNK", "x", None), ("X//a//NA", "x", None)])
    _, counts = link_unit_siblings(df, unknown_unit_components=["UNK", "NA"])
    assert counts["sibling_groups"] == 0


# ------------------------------------------------------------------ stem descriptions


@pytest.mark.parametrize(
    ("descriptions", "units", "expected"),
    [
        (["Drug a", "Drug a"], ["mg", "g"], "Drug a"),
        ([None, None], ["mg", "g"], None),
        (["Glucose (mg)", "Glucose (g)"], ["mg", "g"], "Glucose"),
        (["Heart rate [/min]", "Heart rate [bpm]"], ["/min", "bpm"], "Heart rate"),
        (["Dose in mg", "Dose in g"], ["mg", "g"], "Dose in"),
        (["Rate", "Rate (mL/min)"], ["mL/h", "mL/min"], "Rate"),
        # A unit inside a compound unit is not the unit's own mention.
        (["Dose mg/kg", "Dose g/kg"], ["mg", "g"], None),
        (["Drug a", "Drug b"], ["mg", "g"], None),
        (["Drug a", None], ["mg", "g"], None),
        (["(mg)", "(g)"], ["mg", "g"], None),  # nothing left
    ],
)
def test_stem_description_without_unit(
    descriptions: list, units: list, expected: str | None
) -> None:
    df = _frame([(f"X//a//{u}", d, u) for d, u in zip(descriptions, units)])
    out, _ = link_unit_siblings(df)
    assert _rows(out)["X//a"]["description"] == expected


def test_stem_description_shared() -> None:
    df = _frame([("X//a//mg", "a (mg)", "mg"), ("X//a//g", "a (g)", "g")])
    out, _ = link_unit_siblings(df, stem_description="shared")
    assert _rows(out)["X//a"]["description"] is None
    same = _frame([("X//a//mg", "a", "mg"), ("X//a//g", "a", "g")])
    out, _ = link_unit_siblings(same, stem_description="shared")
    assert _rows(out)["X//a"]["description"] == "a"


def test_stem_description_with_a_pattern() -> None:
    pattern = r"; unit: (?P<unit>.+?)(?= \(src\)$)"
    df = _frame(
        [
            ("L//na//mg", "Lab: Na; unit: mg (src)", "mg"),
            ("L//na//g", "Lab: Na; unit: g (src)", "g"),
        ]
    )
    out, _ = link_unit_siblings(df, description_unit_pattern=pattern)
    assert _rows(out)["L//na"]["description"] == "Lab: Na (src)"


def test_without_unit() -> None:
    assert without_unit("Heart rate [/min] (charted)", "/min") == "Heart rate (charted)"
    assert without_unit("Dose ( mg ) given", "mg") == "Dose given"
    assert without_unit("mg of mg", "mg") == "mg of"  # the last mention
    assert without_unit("Dose", "mg") is None
    assert without_unit(None, "mg") is None
    assert without_unit("Dose", None) is None


# ------------------------------------------------------------------ units in descriptions

PATTERN = r"\s[(\[]([^()\[\]]+)[)\]]\s*$"


def test_units_stated_in_descriptions_fill_missing_units() -> None:
    df = _frame(
        [
            ("H//1", "Height (cm)", None),
            ("H//2//UNK", "Weight (kg)", None),
            ("H//3", "Width (in)", "cm"),  # the data wins
            ("H//4", "Score (see note: x)", None),  # not unit-like
            ("H//5", "Glucose (serum)", None),  # unit-like, but not a unit
            ("H//6", "Plain", None),
        ]
    )
    out, counts = link_unit_siblings(df, description_unit_pattern=PATTERN)
    assert out["unit"].to_list() == ["cm", "kg", "cm", None, "serum", None]
    assert counts["description_units"] == 4
    assert counts["description_units_filled"] == 3
    assert counts["description_unit_conflicts"] == 1
    assert counts["description_units_rejected"] == 1

    allowed, counts = link_unit_siblings(
        df, description_unit_pattern=PATTERN, description_units=["cm", "kg", "in"]
    )
    assert allowed["unit"].to_list() == ["cm", "kg", "cm", None, None, None]
    assert counts["description_units_rejected"] == 2

    unknown_only, _ = link_unit_siblings(
        df, description_unit_pattern=PATTERN, fill_units="unknown_component"
    )
    assert unknown_only["unit"].to_list() == [None, "kg", "cm", None, None, None]
    never, counts = link_unit_siblings(
        df, description_unit_pattern=PATTERN, fill_units="never"
    )
    assert never.equals(df) and counts["description_units_filled"] == 0


def test_a_filled_unit_can_make_a_code_unit_suffixed() -> None:
    df = _frame([("V//a//cm", "Height (cm)", None), ("V//a//in", "Height", "in")])
    out, counts = link_unit_siblings(df, description_unit_pattern=PATTERN)
    assert counts["sibling_groups"] == 1
    assert _rows(out)["V//a"]["description"] == "Height"


def test_fill_after_normalisation_keeps_the_stated_spelling() -> None:
    df = _frame([("H//1", "Height (cms)", None), ("H//2", "Rate (mL/hr)", None)])
    df = df.with_columns(pl.lit(None, pl.String).alias("unit_source_value"))
    out, _ = link_unit_siblings(df, description_unit_pattern=PATTERN)
    assert out["unit"].to_list() == ["cms", "mL/h"]
    assert out["unit_source_value"].to_list() == ["cms", "mL/hr"]


def test_is_unit_like() -> None:
    assert is_unit_like("mg/dL") and is_unit_like("10^9/L") and is_unit_like("%")
    assert not is_unit_like("UNK") and not is_unit_like("n/a") and not is_unit_like("7")


def test_options_are_checked() -> None:
    df = _frame([("X", None, None)])
    with pytest.raises(ValueError, match="stem_description"):
        link_unit_siblings(df, stem_description="first")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="fill_units"):
        link_unit_siblings(df, fill_units="always")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="group"):
        link_unit_siblings(df, description_unit_pattern=r"\(\w+\)")
    with pytest.raises(TypeError):
        link_unit_siblings(df, unknown_unit_components="UNK")


# ------------------------------------------------------------------ normalised units


def test_after_normalize_units_the_recorded_spelling_is_compared() -> None:
    df = _frame([("X//a//mcg", "a", "mcg"), ("X//a//mg/dl", "a", "mg/dl")])
    normalized, _ = normalize_units(df)
    assert normalized["unit"].to_list() == ["ug", "mg/dL"]
    out, counts = link_unit_siblings(normalized)
    assert counts["sibling_groups"] == 1
    before, _ = link_unit_siblings(df)
    linked_first, _ = normalize_units(before)
    assert out.equals(linked_first.select(out.columns))


def test_list_valued_units() -> None:
    df = pl.DataFrame(
        {"code": ["X//a//mg", "X//a//g", "Y"], "unit": [["mg"], ["g", "kg"], None]}
    )
    out, counts = link_unit_siblings(df)
    assert counts["sibling_groups"] == 1
    assert out["parent_codes"].to_list() == [["X//a"], ["X//a"], None, None]
    assert out.schema["unit"] == pl.List(pl.String)


def test_a_list_valued_unit_column_is_filled_with_lists() -> None:
    df = pl.DataFrame(
        {"code": ["H//1"], "description": ["Height (cm)"], "unit": [None]},
        schema={
            "code": pl.String,
            "description": pl.String,
            "unit": pl.List(pl.String),
        },
    )
    out, _ = link_unit_siblings(df, description_unit_pattern=PATTERN)
    assert out["unit"].to_list() == [["cm"]]


def test_without_a_unit_column() -> None:
    df = pl.DataFrame({"code": ["X//a//mg"]})
    out, counts = link_unit_siblings(df)
    assert out.equals(df) and counts["sibling_groups"] == 0


# ------------------------------------------------------------------ single unit codes


def test_a_code_without_a_sibling_gets_its_stem_with_link_single_units() -> None:
    df = _frame(
        [
            ("A", None, None),
            ("I//START//x//mL/h", "Start x (mL/h)", "mL/h", ["ATC//B05"]),
            ("I//START//y//g", "Start y", "g", None),
            ("I//START//y//mg", "Start y", "mg", None),
            ("V//t", "Temperature", "Cel"),  # its unit is not a component
        ]
    )
    off, counts = link_unit_siblings(df)
    assert counts["single_unit_codes"] == 0
    assert _rows(off)["I//START//x//mL/h"]["parent_codes"] == ["ATC//B05"]

    out, counts = link_unit_siblings(df, link_single_units=True)
    rows = _rows(out)
    assert rows["I//START//x//mL/h"]["parent_codes"] == ["ATC//B05", "I//START//x"]
    assert rows["I//START//x"] == {
        "code": "I//START//x",
        "description": "Start x",  # without its unit
        "unit": None,
        "parent_codes": ["ATC//B05"],  # the one code decides
    }
    assert rows["I//START//y"]["description"] == "Start y"  # siblings as before
    assert rows["V//t"]["parent_codes"] is None
    assert out["code"].to_list() == [
        "A",
        "I//START//x",
        "I//START//x//mL/h",
        "I//START//y",
        "I//START//y//g",
        "I//START//y//mg",
        "V//t",
    ]
    assert counts["sibling_groups"] == 1 and counts["sibling_codes"] == 2
    assert counts["single_unit_codes"] == 1
    assert counts["parents_added"] == 3 and counts["stem_rows_added"] == 2
    # Idempotent, and the stem row is not unit-suffixed itself.
    again, counts = link_unit_siblings(out, link_single_units=True)
    assert again.equals(out) and counts["stem_rows_existing"] == 2


def test_a_lone_unknown_unit_code_gets_no_stem() -> None:
    """`X//UNK` alone is not linked, as two `UNK`-style codes alone are no group."""
    df = _frame(
        [
            ("L//k//UNK", "Potassium", None),
            ("L//p//UNK", "p", None),
            ("L//p//NA", "p", None),
            ("L//m//UNK", "m", "mg"),  # a unit from elsewhere: still unknown
            ("L//na//UNK", "Sodium", None),
            ("L//na//mmol/L", "Sodium", "mmol/L"),
        ]
    )
    out, counts = link_unit_siblings(
        df, unknown_unit_components=["UNK", "NA"], link_single_units=True
    )
    rows = _rows(out)
    for code in ("L//k//UNK", "L//p//UNK", "L//p//NA", "L//m//UNK"):
        assert rows[code]["parent_codes"] is None
    assert not {"L//k", "L//p", "L//m"} & rows.keys()
    assert rows["L//na//UNK"]["parent_codes"] == ["L//na"]
    assert counts["sibling_groups"] == 1 and counts["single_unit_codes"] == 0

    # Without unknown-unit linking, a real-unit code whose sibling is `UNK` stands alone.
    alone, counts = link_unit_siblings(
        df, link_unknown_units=False, link_single_units=True
    )
    rows = _rows(alone)
    assert rows["L//na//mmol/L"]["parent_codes"] == ["L//na"]
    assert rows["L//na//UNK"]["parent_codes"] is None
    assert counts["single_unit_codes"] == 1 and counts["sibling_groups"] == 0


@pytest.mark.parametrize(
    ("code", "unit"),
    [
        ("X//a//mg", None),  # looks like a unit, but the code has none
        ("X//a//mg", "g"),  # not its unit
        ("X//a//MG", "mg"),  # case carries meaning
        ("X//a//mg", ""),  # blank
        ("//mg", "mg"),  # empty stem
        ("X//a mg", "mg"),  # not a component
        ("mg", "mg"),  # no component at all
    ],
)
def test_a_single_code_is_linked_only_on_its_own_unit(
    code: str, unit: str | None
) -> None:
    df = _frame([(code, "x", unit)])
    out, counts = link_unit_siblings(df, link_single_units=True)
    assert counts["single_unit_codes"] == 0
    assert out.equals(df)


def test_single_unit_stem_descriptions() -> None:
    df = _frame([("X//a//mg", "Dose a [mg] (charted)", "mg")])
    out, _ = link_unit_siblings(df, link_single_units=True)
    assert _rows(out)["X//a"]["description"] == "Dose a (charted)"
    shared, _ = link_unit_siblings(
        df, link_single_units=True, stem_description="shared"
    )
    assert _rows(shared)["X//a"]["description"] == "Dose a [mg] (charted)"
    for description, expected in [
        ("Dose a", "Dose a"),  # no unit to remove
        ("Dose mg/kg", "Dose mg/kg"),  # only inside another unit
        ("(mg)", None),  # nothing left
        (None, None),
    ]:
        out, _ = link_unit_siblings(
            _frame([("X//a//mg", description, "mg")]), link_single_units=True
        )
        assert _rows(out)["X//a"]["description"] == expected

    pattern = r"; unit: (?P<unit>.+?)(?= \(src\)$)"
    df = _frame([("L//na//mg", "Lab: Na; unit: mg (src)", "mg")])
    out, _ = link_unit_siblings(
        df, link_single_units=True, description_unit_pattern=pattern
    )
    assert _rows(out)["L//na"]["description"] == "Lab: Na (src)"


def test_single_unit_codes_with_units_filled_from_descriptions() -> None:
    """A unit filled from a description is the code's unit, so it can link a single code; the
    stated unit must pass `description_units` (or look like a unit) first."""
    df = _frame(
        [
            ("V//height//cm", "Height (cm)", None),
            ("V//glucose//serum", "Glucose (serum)", None),
            ("V//dose//UNK", "Dose (mg)", None),
        ]
    )
    out, counts = link_unit_siblings(
        df,
        link_single_units=True,
        description_unit_pattern=PATTERN,
        description_units=["cm", "mg"],
    )
    rows = _rows(out)
    assert rows["V//height//cm"]["unit"] == "cm"
    assert rows["V//height//cm"]["parent_codes"] == ["V//height"]
    assert rows["V//height"]["description"] == "Height"
    assert rows["V//glucose//serum"]["parent_codes"] is None  # not an allowed unit
    assert rows["V//dose//UNK"]["unit"] == "mg"
    assert rows["V//dose//UNK"]["parent_codes"] is None  # `UNK` alone
    assert counts["single_unit_codes"] == 1

    for fill in ("never", "unknown_component"):
        never, counts = link_unit_siblings(
            df,
            link_single_units=True,
            description_unit_pattern=PATTERN,
            description_units=["cm", "mg"],
            fill_units=fill,
        )
        assert counts["single_unit_codes"] == 0
        assert _rows(never)["V//height//cm"]["parent_codes"] is None


def test_single_unit_codes_after_normalize_units() -> None:
    df = _frame([("X//a//mcg", "a (mcg)", "mcg")])
    normalized, _ = normalize_units(df)
    out, counts = link_unit_siblings(normalized, link_single_units=True)
    assert counts["single_unit_codes"] == 1
    assert _rows(out)["X//a//mcg"]["parent_codes"] == ["X//a"]
    assert _rows(out)["X//a"]["description"] == "a"


def test_a_lone_unknown_unit_code_gets_its_stem_under_unknown_unit_prefixes() -> None:
    """Under a listed prefix, `X//UNK` alone gets its stem; elsewhere it still does not."""
    df = _frame(
        [
            ("GENDER//UNK", "Gender unknown", None),
            ("I//END//7//mL", "End 7 (mL)", "mL"),
            ("I//START//7//UNK", "Start 7", None, ["ATC//B05", "I//START//7"]),
            ("I//START//8//UNK", "Start 8 (UNK)", None, ["ATC//B05"]),
            ("I//START//9//UNK", "Start 9", None),
            ("I//START//9//mL/h", "Start 9 (mL/h)", "mL/h", ["ATC//B05"]),
            ("I//START//UNK", "Start", None),  # the stem would be the prefix's parent
            ("M//START//a//UNK", "a", None),  # not a listed prefix
        ]
    )
    prefixes = ["I//START//", "I//END//"]
    out, counts = link_unit_siblings(
        df, link_single_units=True, unknown_unit_prefixes=prefixes
    )
    rows = _rows(out)
    assert rows["I//START//7//UNK"]["parent_codes"] == ["ATC//B05", "I//START//7"]
    assert rows["I//START//7"] == {
        "code": "I//START//7",
        "description": "Start 7",
        "unit": None,
        "parent_codes": ["ATC//B05"],  # the code's own, the stem left out
    }
    assert rows["I//START//8//UNK"]["parent_codes"] == ["ATC//B05", "I//START//8"]
    assert rows["I//START//8"]["description"] == "Start 8"  # `UNK` removed
    # A real-unit sibling: a sibling group as before, and only it decides the stem's parents.
    assert rows["I//START//9//UNK"]["parent_codes"] == ["I//START//9"]
    assert rows["I//START//9"]["parent_codes"] == ["ATC//B05"]
    assert rows["I//END//7//mL"]["parent_codes"] == ["I//END//7"]
    for code in ("GENDER//UNK", "I//START//UNK", "M//START//a//UNK"):
        assert rows[code]["parent_codes"] is None
    assert not {"GENDER", "I//START", "M//START//a"} & rows.keys()
    assert counts["unknown_unit_prefix_codes"] == 2
    assert counts["single_unit_codes"] == 1
    assert counts["sibling_groups"] == 1 and counts["sibling_codes"] == 2
    assert counts["stem_rows_added"] == 4  # I//START//7, 8, 9 and I//END//7
    again, counts = link_unit_siblings(
        out, link_single_units=True, unknown_unit_prefixes=prefixes
    )
    assert again.equals(out) and counts["stem_rows_existing"] == 4

    # Without the prefixes, the lone `UNK` starts stay as they are.
    off, counts = link_unit_siblings(df, link_single_units=True)
    assert _rows(off)["I//START//8//UNK"]["parent_codes"] == ["ATC//B05"]
    assert counts["unknown_unit_prefix_codes"] == 0


def test_unknown_unit_codes_alone_under_a_prefix_share_their_stem() -> None:
    """Two unknown-unit spellings with no real-unit code: linked together, sharing parents."""
    df = _frame(
        [
            ("I//START//7//UNK", "Start 7", None, ["ATC//B05", "ATC//B"]),
            ("I//START//7//NA", "Start 7", None, ["ATC//B"]),
        ]
    )
    out, counts = link_unit_siblings(
        df,
        unknown_unit_components=["UNK", "NA"],
        link_single_units=True,
        unknown_unit_prefixes=["I//START//"],
    )
    rows = _rows(out)
    assert rows["I//START//7"]["parent_codes"] == ["ATC//B"]
    assert rows["I//START//7"]["description"] == "Start 7"
    assert counts["unknown_unit_prefix_codes"] == 2
    assert counts["sibling_groups"] == 0 and counts["single_unit_codes"] == 0


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"unknown_unit_prefixes": ["I//START//"]}, ValueError),  # no link_single_units
        (
            {
                "unknown_unit_prefixes": ["I//START//"],
                "link_single_units": True,
                "link_unknown_units": False,
            },
            ValueError,
        ),
        (
            {
                "unknown_unit_prefixes": ["I//START//"],
                "link_single_units": True,
                "unknown_unit_components": [],
            },
            ValueError,
        ),
        ({"unknown_unit_prefixes": [" "], "link_single_units": True}, ValueError),
        ({"unknown_unit_prefixes": "I//START//", "link_single_units": True}, TypeError),
    ],
)
def test_unknown_unit_prefixes_need_single_and_unknown_linking(
    options: dict, error: type[Exception]
) -> None:
    df = _frame([("I//START//7//UNK", "Start 7", None)])
    with pytest.raises(error):
        link_unit_siblings(df, **options)
    with pytest.raises(error):
        link_unit_siblings_file("unused.parquet", **options)


# ------------------------------------------------------------------ file and stage

_MAP = pa.map_(pa.string(), pa.string())


def _codes_table() -> pa.Table:
    """A table as MEDS-Extract writes it, plus a suMEDS-style map column; sorted by code."""

    return pa.table(
        {
            "code": pa.array(
                [
                    "DX//A",
                    "LAB//NA//UNK",
                    "LAB//NA//mEq/L",
                    "LAB//NA//mmol/L",
                    "VITAL//T",
                ]
            ),
            "description": pa.array(
                ["dx", "Sodium", "Sodium (mEq/L)", "Sodium (mmol/L)", "temperature"]
            ),
            "parent_codes": pa.array(
                [None, None, ["LOINC//2951-2"], ["LOINC//2951-2"], []],
                pa.list_(pa.string()),
            ),
            "code_template": pa.array(
                ["t1", "t2", "t2", "t2", "t3"], pa.large_string()
            ),
            "unit": pa.array([None, None, "mEq/L", "mmol/L", "Cel"], pa.large_string()),
            "names": pa.array([[("a", "b")], None, None, None, None], _MAP),
            "n": pa.array([1, 2, 3, 4, 5], pa.int32()),
        }
    )


def _meds_root(root: Path, table: pa.Table) -> Path:
    (root / "metadata").mkdir(parents=True)
    pq.write_table(table, root / meds.code_metadata_filepath)
    return root


def test_link_unit_siblings_file_keeps_types_and_order(tmp_path: Path) -> None:
    path = tmp_path / "codes.parquet"
    before = _codes_table()
    pq.write_table(before, path)
    counts = link_unit_siblings_file(path)
    after = pq.read_table(path)
    assert after.schema == before.schema
    assert after.column("code").to_pylist() == [
        "DX//A",
        "LAB//NA",
        "LAB//NA//UNK",
        "LAB//NA//mEq/L",
        "LAB//NA//mmol/L",
        "VITAL//T",
    ]
    stem = after.slice(1, 1).to_pylist()[0]
    assert stem == {
        "code": "LAB//NA",
        "description": "Sodium",
        # Both real-unit siblings have LOINC//2951-2; the UNK sibling does not narrow it.
        "parent_codes": ["LOINC//2951-2"],
        "code_template": None,
        "unit": None,
        "names": None,
        "n": None,
    }
    assert after.column("parent_codes").to_pylist()[2:5] == [
        ["LAB//NA"],
        ["LOINC//2951-2", "LAB//NA"],
        ["LOINC//2951-2", "LAB//NA"],
    ]
    unchanged = after.filter(pa.compute.field("code") != "LAB//NA")
    for name in ("description", "code_template", "unit", "names", "n"):
        assert unchanged.column(name).equals(before.column(name))
    assert counts["sibling_codes"] == 3 and counts["stem_rows_added"] == 1
    assert [p.name for p in tmp_path.iterdir()] == ["codes.parquet"]  # no temp files

    # A second run changes nothing, and does not rewrite the file.
    written = path.read_bytes()
    counts = link_unit_siblings_file(path)
    assert path.read_bytes() == written
    assert counts["stem_rows_existing"] == 1


def test_an_unsorted_file_gets_its_stem_rows_appended(tmp_path: Path) -> None:
    path = tmp_path / "codes.parquet"
    table = _codes_table()
    pq.write_table(table.take([4, 3, 2, 1, 0]), path)
    link_unit_siblings_file(path, link_unknown_units=False)
    codes = pq.read_table(path).column("code").to_pylist()
    assert codes[-1] == "LAB//NA" and len(codes) == 6


def test_link_unit_siblings_file_adds_parent_codes(tmp_path: Path) -> None:
    path = tmp_path / "codes.parquet"
    pq.write_table(_codes_table().drop_columns(["parent_codes"]), path)
    link_unit_siblings_file(path)
    after = pq.read_table(path)
    assert after.schema.field("parent_codes").type == pa.list_(pa.string())
    assert after.column_names[-1] == "parent_codes"
    meds.CodeMetadataSchema.validate(after)


def test_link_unit_siblings_file_without_changes(tmp_path: Path) -> None:
    path = tmp_path / "codes.parquet"
    pq.write_table(_codes_table().drop_columns(["unit"]), path)
    before = path.read_bytes()
    link_unit_siblings_file(path)
    assert path.read_bytes() == before
    link_unit_siblings_file(path, tmp_path / "out" / "codes.parquet")
    assert (tmp_path / "out" / "codes.parquet").read_bytes() == before

    pq.write_table(_codes_table().slice(0, 2), path)
    before = path.read_bytes()
    link_unit_siblings_file(path)  # a unit column, but no siblings
    assert path.read_bytes() == before


def test_link_unit_siblings_file_to_another_output(tmp_path: Path) -> None:
    path = tmp_path / "codes.parquet"
    pq.write_table(_codes_table(), path)
    before = path.read_bytes()
    link_unit_siblings_file(path, tmp_path / "out.parquet", stem_description="shared")
    assert path.read_bytes() == before
    out = pq.read_table(tmp_path / "out.parquet")
    assert out.num_rows == 6
    assert out.slice(1, 1).column("description").to_pylist() == [None]


def test_link_unit_siblings_file_rejects_bad_input(tmp_path: Path) -> None:
    path = tmp_path / "codes.parquet"
    pq.write_table(pa.table({"code": ["A//x"], "unit": [1]}), path)
    with pytest.raises(TypeError, match="not a string"):
        link_unit_siblings_file(path)
    pq.write_table(pa.table({"unit": ["mg"]}), path)
    with pytest.raises(ValueError, match="no 'code' column"):
        link_unit_siblings_file(path)
    with pytest.raises(TypeError):
        link_unit_siblings_file(path, rules="x")


def test_link_unit_siblings_file_fills_units_keeping_types(tmp_path: Path) -> None:
    path = tmp_path / "codes.parquet"
    table = pa.table(
        {
            "code": pa.array(["H//1", "H//2//cm", "H//2//in"]),
            "description": pa.array(["Height (cm)", "Height (cm)", "Height (in)"]),
            "unit": pa.array([None, None, "in"], pa.string()),
        }
    )
    pq.write_table(table, path)
    counts = link_unit_siblings_file(path, description_unit_pattern=PATTERN)
    after = pq.read_table(path)
    assert after.schema.field("unit").type == pa.string()
    assert after.column("unit").to_pylist() == ["cm", None, "cm", "in"]
    assert after.column("code").to_pylist() == ["H//1", "H//2", "H//2//cm", "H//2//in"]
    assert counts["description_units_filled"] == 2


def _cfg(root: Path, worker: int = 0, **options) -> SimpleNamespace:
    metadata = str(root / "metadata")
    return SimpleNamespace(
        worker=worker,
        stage_cfg=SimpleNamespace(
            metadata_input_dir=metadata, reducer_output_dir=metadata, **options
        ),
    )


def test_the_stage_links_in_place_and_logs(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    root = _meds_root(tmp_path / "meds", _codes_table())
    codes = root / meds.code_metadata_filepath
    before = codes.read_bytes()
    _main(_cfg(root, worker=1))  # only worker 0 works
    assert codes.read_bytes() == before

    with caplog.at_level(logging.INFO, logger="sumeds.unit_siblings"):
        _main(_cfg(root, **DEFAULT_CONFIG))
    assert pq.read_table(codes).num_rows == 6
    assert any(
        "3 codes in 1 sibling groups (3 got their stem as a parent); 1 stem rows added"
        in r.getMessage()
        for r in caplog.records
    )


def test_the_stage_passes_its_options(tmp_path: Path) -> None:
    root = _meds_root(tmp_path / "meds", _codes_table())
    _main(_cfg(root, link_unknown_units=False, stem_description="shared"))
    table = pq.read_table(root / meds.code_metadata_filepath)
    rows = {r["code"]: r for r in table.to_pylist()}
    assert rows["LAB//NA//UNK"]["parent_codes"] is None
    assert rows["LAB//NA"]["description"] is None

    root = _meds_root(tmp_path / "single", _codes_table())
    _main(_cfg(root, link_single_units=True))
    rows = {
        r["code"]: r
        for r in pq.read_table(root / meds.code_metadata_filepath).to_pylist()
    }
    assert rows["VITAL//T"]["parent_codes"] == []  # `Cel` is not a component
    assert rows["LAB//NA//mEq/L"]["parent_codes"] == ["LOINC//2951-2", "LAB//NA"]

    table = _codes_table()
    table = table.set_column(
        0,
        "code",
        pa.array(["DX//UNK", "I//START//7//UNK", *table["code"].to_pylist()[2:]]),
    )
    root = _meds_root(tmp_path / "prefixes", table)
    _main(_cfg(root, link_single_units=True, unknown_unit_prefixes=("I//START//",)))
    rows = {
        r["code"]: r
        for r in pq.read_table(root / meds.code_metadata_filepath).to_pylist()
    }
    assert rows["I//START//7//UNK"]["parent_codes"][-1] == "I//START//7"
    assert "I//START//7" in rows and "DX" not in rows


def test_stage_options_fill_defaults() -> None:
    assert stage_options(SimpleNamespace()) == DEFAULT_CONFIG
    assert stage_options(SimpleNamespace(description_units=("cm",)))[
        "description_units"
    ] == ["cm"]


def test_the_stage_is_registered() -> None:
    stages = {ep.name: ep.value for ep in entry_points(group="MEDS_transforms.stages")}
    assert stages[STAGE_NAME] == "sumeds.unit_siblings_stage:main"


def test_the_cli(tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    root = _meds_root(tmp_path / "meds", _codes_table())
    assert link_unit_siblings_main([str(root)]) == 0
    assert (
        "3 codes in 1 unit-sibling groups, 1 stem rows added" in capsys.readouterr().out
    )

    codes = root / meds.code_metadata_filepath
    pq.write_table(_codes_table(), codes)
    out = tmp_path / "out.parquet"
    assert (
        link_unit_siblings_main(
            [
                str(codes),
                "-o",
                str(out),
                "--no-unknown-unit-components",
                "--stem-description",
                "shared",
            ]
        )
        == 0
    )
    assert "2 codes in 1 unit-sibling groups" in capsys.readouterr().out
    assert pq.read_table(out).num_rows == 6
    assert (
        link_unit_siblings_main(
            [
                str(codes),
                "-o",
                str(out),
                "--unknown-unit-component",
                "NA",
                "--description-unit-pattern",
                PATTERN,
                "--description-unit",
                "cm",
                "--fill-units",
                "never",
            ]
        )
        == 0
    )
    assert "2 codes in 1 unit-sibling groups" in capsys.readouterr().out
    single = pa.table(
        {
            "code": ["X//a//mg", "X//b//g"],
            "unit": ["mg", "g"],
            "description": ["a", "b"],
        }
    )
    pq.write_table(single, codes)
    assert link_unit_siblings_main([str(codes), "-o", str(out)]) == 0
    assert "0 codes in 0 unit-sibling groups, 0 stem rows" in capsys.readouterr().out
    assert (
        link_unit_siblings_main([str(codes), "-o", str(out), "--link-single-units"])
        == 0
    )
    assert (
        "0 codes in 0 unit-sibling groups, 2 codes without a sibling, 2 stem rows added"
        in capsys.readouterr().out
    )
    assert pq.read_table(out).column("code").to_pylist() == [
        "X//a",
        "X//a//mg",
        "X//b",
        "X//b//g",
    ]
    pq.write_table(
        pa.table(
            {
                "code": ["X//a//mg", "X//b//UNK", "Y//UNK"],
                "unit": ["mg", None, None],
                "description": ["a", "b", "y"],
            }
        ),
        codes,
    )
    assert (
        link_unit_siblings_main(
            [
                str(codes),
                "-o",
                str(out),
                "--link-single-units",
                "--unknown-unit-prefix",
                "X//",
            ]
        )
        == 0
    )
    assert (
        "1 codes without a sibling, 1 unknown-unit codes under --unknown-unit-prefix, "
        "2 stem rows added" in capsys.readouterr().out
    )
    assert pq.read_table(out).column("code").to_pylist() == [
        "X//a",
        "X//a//mg",
        "X//b",
        "X//b//UNK",
        "Y//UNK",
    ]
    pq.write_table(_codes_table(), codes)
    for bad in (
        [str(root), "-o", str(out)],
        [str(tmp_path / "missing")],
        [str(codes), "--no-unknown-unit-components", "--unknown-unit-component", "X"],
        [str(codes), "-o", str(out), "--unknown-unit-prefix", "LAB//"],
    ):
        with pytest.raises(SystemExit):
            link_unit_siblings_main(bad)
    pq.write_table(pa.table({"unit": ["mg"]}), codes)
    with pytest.raises(SystemExit):
        link_unit_siblings_main([str(codes)])


# ------------------------------------------------------------------ end to end


def _extension_stages_installed() -> bool:
    try:
        import MEDS_extract.config
    except ImportError:
        return False
    registered = {ep.name for ep in entry_points(group="MEDS_transforms.stages")}
    return hasattr(MEDS_extract.config, "ExtensionStage") and STAGE_NAME in registered


SPEC = """\
etl:
  dataset_name: SYNTH
  raw_dataset_version: "1.0"
  n_subjects_per_shard: 10
  split_fracs: {train: 0.5, tuning: 0.25, held_out: 0.25}
  post_stages:
    - sumeds_link_unit_siblings:
        description_unit_pattern: '\\s[(\\[]([^()\\[\\]]+)[)\\]]\\s*$'
        description_units: [cm]
    - sumeds_normalize_units

_defaults:
  subject_id: $patient_id

patients:
  dob:
    code: MEDS_BIRTH
    time: '$dob::"%Y-%m-%dT%H:%M:%S"'

labs:
  glucose_mg:
    code: '"LAB//GLUCOSE//mg/dl"'
    time: '$timestamp::"%Y-%m-%dT%H:%M:%S"'
    numeric_value: $glucose_mg::?float64
    _metadata:
      _self:
        unit: '"mg/dl"'
        description: '"Glucose (mg/dl)"'
  glucose_mmol:
    code: '"LAB//GLUCOSE//mmol/l"'
    time: '$timestamp::"%Y-%m-%dT%H:%M:%S"'
    numeric_value: $glucose_mmol::?float64
    _metadata:
      _self:
        unit: '"mmol/l"'
        description: '"Glucose (mmol/l)"'
  height:
    code: '"VITAL//HEIGHT"'
    time: '$timestamp::"%Y-%m-%dT%H:%M:%S"'
    numeric_value: $height::?float64
    _metadata:
      _self:
        description: '"Height (cm)"'
"""


@pytest.mark.skipif(
    not _extension_stages_installed(),
    reason="needs MEDS-Extract with extension stages and an installed suMEDS",
)
def test_meds_extract_runs_the_stage(tmp_path: Path) -> None:
    import os
    import subprocess
    import sys

    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "patients.csv").write_text(
        "patient_id,dob\n1,1954-01-24T00:00:00\n2,2009-02-19T00:00:00\n"
        "3,1980-05-01T00:00:00\n4,1970-07-07T00:00:00\n"
    )
    (raw / "labs.csv").write_text(
        "patient_id,timestamp,glucose_mg,glucose_mmol,height\n"
        "1,2024-04-03T00:00:00,101,5.6,170\n2,2024-01-01T00:00:00,95,5.3,182\n"
        "3,2024-02-01T00:00:00,120,6.7,165\n4,2024-03-01T00:00:00,88,4.9,177\n"
    )
    spec = tmp_path / "spec.yaml"
    spec.write_text(SPEC)
    out = tmp_path / "meds"
    env = {
        **os.environ,
        "PATH": os.pathsep.join([str(Path(sys.executable).parent), os.environ["PATH"]]),
    }
    result = subprocess.run(
        [
            "meds-extract-run",
            f"spec={spec}",
            f"output_dir={out}",
            "do_download=false",
            f"input_dir={raw}",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=600,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    table = pq.read_table(out / meds.code_metadata_filepath)
    meds.CodeMetadataSchema.validate(table)
    rows = {r["code"]: r for r in table.to_pylist()}
    assert rows["LAB//GLUCOSE//mg/dl"]["parent_codes"] == ["LAB//GLUCOSE"]
    assert rows["LAB//GLUCOSE//mmol/l"]["parent_codes"] == ["LAB//GLUCOSE"]
    assert rows["LAB//GLUCOSE"]["description"] == "Glucose"
    assert rows["LAB//GLUCOSE"]["unit"] is None
    # The unit its description states, then normalised.
    assert rows["VITAL//HEIGHT"]["unit"] == "cm"
    assert rows["LAB//GLUCOSE//mmol/l"]["unit"] == "mmol/L"
    assert rows["LAB//GLUCOSE//mmol/l"]["unit_source_value"] == "mmol/l"
