"""Athena and public-ontology enrichment for MEDS metadata and suMEDS summaries."""

from __future__ import annotations

import csv
import io
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
from tqdm.auto import tqdm

from .config import EnrichmentConfig
from .io import output_format, scan_table, sink_table
from .ontology import CrosswalkRule, OntologyPacks, PackEntry

_RELATIONSHIPS = {
    "parent_codes": "parent",
    "child_codes": "child",
    "sibling_codes": "sibling",
}
#: Output columns naming each entry of a relationship list, aligned with it: the
#: official name of every vocabulary code enrichment adds (LOINC requires a LOINC
#: code or Part number to travel with its name), null where none is known.
_NAMES = {
    "parent_codes": "parent_names",
    "child_codes": "child_names",
    "sibling_codes": "sibling_names",
}
_FIELDS = {
    "description": pl.String,
    **{name: pl.List(pl.String) for name in _RELATIONSHIPS},
    "vocabulary_id": pl.String,
    "concept_id": pl.Int64,
    "concept_code": pl.String,
    "domain_id": pl.String,
    "standard_concept": pl.String,
}
_MATCH_RANK = "_athena_match_rank"
#: Record fields naming a code's existing parent references (``VOCABULARY//CODE``)
#: that enrichment looked up, for ``parent_names`` only; the references themselves
#: stay where they are in ``parent_codes``.
_REFERENCE_FIELDS = ("reference_codes", "reference_names")
_INTERNAL = {
    f"_athena_{name}" for name in (*_FIELDS, *_NAMES.values(), *_REFERENCE_FIELDS)
} | {_MATCH_RANK}
#: Existing parent references are looked up in the same Athena batch as the codes;
#: their lookup targets start with a tab, which no MEDS code contains.
_REFERENCE_TARGET = "\t"
_MATCH_SCHEMA = {
    "_target_code": pl.String,
    "_rank": pl.UInt8,
    **{name: dtype for name, dtype in _FIELDS.items() if name not in _RELATIONSHIPS},
    "_relationship": pl.String,
    "_related_vocabulary_id": pl.String,
    "_related_concept_code": pl.String,
    "_related_concept_name": pl.String,
}
_MATCH_KEYS = tuple(_MATCH_SCHEMA)[:-4]
_CANDIDATE_SCHEMA = {
    "_target_code": pl.String,
    "vocabulary_id": pl.String,
    "concept_code": pl.String,
    "_rank": pl.UInt8,
}
_ENRICHMENT_SCHEMA = {
    "code": pl.String,
    _MATCH_RANK: pl.UInt8,
    **{f"_athena_{name}": dtype for name, dtype in _FIELDS.items()},
    **{
        f"_athena_{name}": pl.List(pl.String)
        for name in (*_NAMES.values(), *_REFERENCE_FIELDS)
    },
}
# Heuristic candidate ranks: VOCAB//CODE, VOCAB//VERSION//CODE, parent reference.
_PARENT_REFERENCE_RANK = 2
#: Vocabularies whose hierarchy is code truncation (I50.23 -> I50.2 -> I50). OMOP
#: keeps it only as "Is a" relationships, not in CONCEPT_ANCESTOR, so Athena
#: enrichment adds the valid shorter codes of the same vocabulary as parents.
_TRUNCATION_VOCABULARIES = frozenset({"ICD9CM", "ICD9Proc", "ICD10CM", "ICD10PCS"})


def enrich_metadata(frame: pl.LazyFrame, config: EnrichmentConfig) -> pl.LazyFrame:
    """Fill scalar metadata and merge configured hierarchy fields.

    Codes are resolved by the configured rules first (ontology, then crosswalk
    rules, in file order), then as ``VOCABULARY//CODE//...`` or ``VOCABULARY/CODE``.
    For an ambiguous three-part code, the third component is tried only when the
    second has no valid match, supporting ``VOCABULARY//VERSION//CODE``. The first
    existing ``VOCABULARY/CODE`` parent reference is the final fallback. Every
    existing parent reference is also looked up: it gets its name in
    ``parent_names`` and its ancestors are added as parents.
    """

    return _enrich(frame, config)


def _enrich(
    frame: pl.LazyFrame,
    config: EnrichmentConfig,
    stats: dict[str, Any] | None = None,
) -> pl.LazyFrame:
    schema = frame.collect_schema()
    if "code" not in schema:
        raise ValueError("metadata to enrich must contain a 'code' column")
    collisions = set(schema.names()) & _INTERNAL
    if collisions:
        raise ValueError(
            f"metadata uses reserved enrichment columns: {sorted(collisions)}"
        )

    released = (
        frame.filter(~pl.col("is_masked"))
        if schema.get("is_masked") == pl.Boolean
        else frame
    )
    stats = {} if stats is None else stats
    if config.rules is None:
        enrichment = _athena_enrichment(released, config, stats)
        parent_rank = _PARENT_REFERENCE_RANK
    else:
        enrichment, parent_rank = _rule_enrichment(released, config, stats)
    joined = frame.join(enrichment, on="code", how="left", maintain_order="left")

    expressions = []
    for name, dtype in _FIELDS.items():
        incoming = pl.col(f"_athena_{name}").cast(dtype, strict=False)
        if name in _RELATIONSHIPS:
            if not getattr(config, name):
                continue
            if name not in schema:
                expressions.append(incoming.alias(name))
                continue
            existing = pl.col(name)
            if schema[name] == pl.String:
                existing = existing.str.split("|")
            existing = existing.cast(dtype, strict=False)
            expressions.append(existing.alias(f"_existing_{name}"))
            present = existing.is_not_null() | incoming.is_not_null()
            if name == "parent_codes":
                if config.exclude_self_parent_code:
                    # Drop the reference _code_candidates matched: first non-null.
                    remaining = existing.list.drop_nulls().list.slice(1)
                    matched_by_parent = pl.col(_MATCH_RANK) == parent_rank
                    present = (
                        pl.when(matched_by_parent)
                        .then((remaining.list.len() > 0) | incoming.is_not_null())
                        .otherwise(present)
                    )
                    existing = (
                        pl.when(matched_by_parent).then(remaining).otherwise(existing)
                    )
            merged = _append_new(existing, incoming, dtype, name == "parent_codes")
            expressions.append(
                pl.when(present).then(merged).otherwise(None).alias(name)
            )
        elif name not in schema:
            expressions.append(incoming.alias(name))
        else:
            expressions.append(
                pl.coalesce(pl.col(name).cast(dtype, strict=False), incoming).alias(
                    name
                )
            )
    joined = joined.with_columns(expressions)

    names = []
    for name, label in _NAMES.items():
        if not getattr(config, name):
            continue
        inputs = {
            "codes": pl.col(name),
            "incoming_codes": pl.col(f"_athena_{name}"),
            "incoming_names": pl.col(f"_athena_{label}"),
        }
        if name in schema:
            inputs["existing_codes"] = pl.col(f"_existing_{name}")
        if name == "parent_codes":
            inputs["reference_codes"] = pl.col("_athena_reference_codes")
            inputs["reference_names"] = pl.col("_athena_reference_names")
        if label in schema:
            existing = pl.col(label)
            if schema[label] == pl.String:
                existing = existing.str.json_decode(pl.List(pl.String))
            inputs["existing_names"] = existing.cast(pl.List(pl.String), strict=False)
        names.append(
            pl.struct(**inputs)
            .map_elements(_aligned_names, return_dtype=pl.List(pl.String))
            .alias(label)
        )
    temporary = [f"_existing_{name}" for name in _RELATIONSHIPS if name in schema]
    return (
        joined.with_columns(names)
        .drop(*sorted(_INTERNAL))
        .drop(temporary, strict=False)
    )


def _aligned_names(row: dict[str, Any]) -> list[str | None] | None:
    """Names for the merged list: existing names, added codes, then references."""

    codes = row["codes"]
    if codes is None:
        return None
    known: dict[str, str] = {}
    for values, names in (
        (row.get("existing_codes"), row.get("existing_names")),
        (row["incoming_codes"], row["incoming_names"]),
        (row.get("reference_codes"), row.get("reference_names")),
    ):
        for value, name in zip(values or (), names or ()):
            if value is not None and name is not None:
                known.setdefault(_reference_key(value), name)
    return [None if code is None else known.get(_reference_key(code)) for code in codes]


def _reference_key(value: str) -> str:
    """``VOCABULARY/CODE`` and ``VOCABULARY//CODE`` name the same entry."""

    return value.replace("/", "//", 1) if value.count("/") == 1 else value


def _append_new(
    existing: pl.Expr, incoming: pl.Expr, dtype: pl.DataType, references: bool
) -> pl.Expr:
    """Keep ``existing`` verbatim and append the ``incoming`` entries it lacks.

    Enrichment never removes, reorders, or rewrites existing entries. For parent
    references, ``VOCABULARY/CODE`` and ``VOCABULARY//CODE`` count as the same
    entry when deciding what is new, but the existing spelling is kept.
    """

    def key(element: pl.Expr) -> pl.Expr:
        if not references:
            return element
        return (
            pl.when(element.str.count_matches("/") == 1)
            .then(element.str.replace("/", "//", literal=True))
            .otherwise(element)
        )

    def tagged(values: pl.Expr, is_existing: bool) -> pl.Expr:
        return pl.coalesce(values, pl.lit([], dtype=dtype)).list.eval(
            pl.struct(
                value=pl.element(),
                key=key(pl.element()),
                existing=pl.lit(is_existing),
            )
        )

    entry = pl.element().struct
    return pl.concat_list(tagged(existing, True), tagged(incoming, False)).list.eval(
        pl.element()
        .filter(entry.field("existing") | entry.field("key").is_first_distinct())
        .struct.field("value")
    )


def enrich_file(
    input_path: str | Path,
    output_path: str | Path,
    config: EnrichmentConfig,
    *,
    verbose: bool = False,
) -> Path:
    """Atomically enrich a standalone table, optionally reporting coverage."""

    input_path = Path(input_path).expanduser().resolve()
    output_path = Path(output_path).expanduser().resolve()
    if input_path == output_path:
        raise ValueError("standalone enrichment output must differ from its input")
    format_ = output_format(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    staged = output_path.with_name(
        f".{output_path.name}.{uuid4().hex}.tmp{output_path.suffix}"
    )
    stats: dict[str, Any] = {}
    with tqdm(
        total=4,
        desc="Preparing enrichment",
        unit="stage",
        dynamic_ncols=True,
        disable=not verbose,
    ) as progress:
        try:
            source = scan_table(input_path)
            progress.set_description("Reading input")
            progress.update()
            progress.set_description(
                "Matching rules" if config.rules is not None else "Querying Athena"
            )
            enriched = _enrich(source, config, stats)
            progress.update()
            progress.set_description("Writing output")
            sink_table(enriched, staged, format_)
            staged.replace(output_path)
            progress.update()
            if verbose:
                progress.set_description("Calculating coverage")
                report = _coverage(source), _coverage(scan_table(output_path))
            progress.update()
        finally:
            staged.unlink(missing_ok=True)
    if verbose:
        _print_enrichment_report(report, stats)
    return output_path


def _coverage(frame: pl.LazyFrame) -> dict[str, int]:
    schema = frame.collect_schema()
    expressions = [
        pl.len().alias("rows"),
        pl.col("code").n_unique().alias("unique_codes"),
    ]
    for name in _FIELDS:
        expressions.append(
            (pl.col(name).is_not_null().sum() if name in schema else pl.lit(0)).alias(
                name
            )
        )
    expressions.append(
        (
            pl.col("code").filter(pl.col("concept_id").is_not_null()).n_unique()
            if "concept_id" in schema
            else pl.lit(0)
        ).alias("matched_codes")
    )
    coverage = {
        name: int(value or 0)
        for name, value in frame.select(expressions)
        .collect(engine="streaming")
        .row(0, named=True)
        .items()
    }
    coverage["dataset_parent_links"] = coverage["parent_links"] = 0
    coverage["named_parent_links"] = 0
    if "parent_names" in schema:
        names = pl.col("parent_names")
        if schema["parent_names"] == pl.String:
            names = names.str.json_decode(pl.List(pl.String))
        coverage["named_parent_links"] = int(
            frame.select(
                names.cast(pl.List(pl.String), strict=False)
                .list.drop_nulls()
                .list.len()
                .sum()
            )
            .collect(engine="streaming")
            .item()
            or 0
        )
    if "parent_codes" in schema:
        parents = pl.col("parent_codes")
        if schema["parent_codes"] == pl.String:
            parents = parents.str.split("|")
        data = frame.select(
            pl.col("code").cast(pl.String),
            parents.cast(pl.List(pl.String), strict=False),
        ).collect(engine="streaming")
        codes = set(data["code"].drop_nulls())
        coverage["parent_links"] = sum(
            parent is not None
            for values in data["parent_codes"].to_list()
            for parent in values or ()
        )
        coverage["dataset_parent_links"] = sum(
            parent != code and parent in codes
            for code, values in data.iter_rows()
            for parent in values or ()
        )
    return coverage


def _print_enrichment_report(
    report: tuple[dict[str, int], dict[str, int]],
    stats: dict[str, Any] | None = None,
) -> None:
    before, after = report
    rows = after["rows"]
    matched_rows = after["concept_id"]
    print("\nEnrichment report")
    print(f"  Rows processed:             {rows:,}")
    print(f"  Unique codes:               {after['unique_codes']:,}")
    print(
        "  Athena matches added:      "
        f"{max(0, matched_rows - before['concept_id']):,} rows, "
        f"{max(0, after['matched_codes'] - before['matched_codes']):,} unique codes"
    )
    print(f"  Rows with concept ID:       {_coverage_value(matched_rows, rows)}")
    print(f"  Rows without Athena match:  {_coverage_value(rows - matched_rows, rows)}")
    print("  Metadata coverage (present before -> after):")
    labels = {
        "description": "Descriptions",
        "parent_codes": "Parent-code lists",
        "child_codes": "Child-code lists",
        "sibling_codes": "Sibling-code lists",
        "vocabulary_id": "Vocabulary IDs",
        "concept_id": "OMOP concept IDs",
        "concept_code": "Concept codes",
        "domain_id": "Domains",
        "standard_concept": "Standard-concept flags",
    }
    for name, label in labels.items():
        added = max(0, after[name] - before[name])
        print(
            f"    {label:<23} {before[name]:>10,} -> "
            f"{_coverage_value(after[name], rows):>18} (+{added:,} filled)"
        )
    print(
        "  Parent codes (all entries): "
        f"{before['parent_links']:,} -> {after['parent_links']:,} "
        f"({after['named_parent_links']:,} named)"
    )
    print(
        "  Same-dataset parent links:  "
        f"{before['dataset_parent_links']:,} -> {after['dataset_parent_links']:,}"
    )
    references = (stats or {}).get("references")
    if references and references["references"]:
        print(
            "  Existing parent references: "
            f"{references['resolved']:,} of {references['references']:,} found "
            f"(Athena {references['athena']:,}, pack {references['pack']:,})"
        )
    if not stats or "rules" not in stats:
        return
    if stats["rules"]:
        print("  Vocabulary rules (unique codes):")
    for rule in stats["rules"]:
        name = (
            f"{rule['id']} [{rule['kind']}, {rule['vocabulary']}, {rule['validate']}]"
        )
        if rule["skipped"]:
            print(
                f"    {name}: {rule['matched']:,} matched, skipped ({rule['skipped']})"
            )
            continue
        validated = rule["athena"] + rule["pack"]
        print(
            f"    {name}: {rule['matched']:,} matched, {validated:,} validated "
            f"(Athena {rule['athena']:,}, pack {rule['pack']:,}), "
            f"{rule['tagged']:,} tagged"
        )
    layouts = stats.get("layouts") or {}
    if any(layouts.values()):
        print(
            "  Built-in layouts and parent reference (unique codes): "
            f"{sum(layouts.values()):,} validated (Athena {layouts['athena']:,}, "
            f"pack {layouts['pack']:,})"
        )
    if stats["hierarchy"]:
        print("  Hierarchy rules (same-dataset parents):")
    for rule in stats["hierarchy"]:
        print(
            f"    {rule['id']} [{rule['mode']}]: {rule['links']:,} parents added "
            f"to {rule['codes']:,} codes"
        )
    if stats["packs"]:
        print(f"  Ontology files used, from {stats.get('pack_source')}:")
    for name, pack in stats["packs"].items():
        release = f" {pack['release']}" if pack.get("release") else ""
        print(
            f"    {name}{release}: {pack['shards']:,} of {pack['available']:,} "
            f"files ({pack['downloaded']:,} downloaded, {pack['bytes']:,} bytes)"
        )


def _coverage_value(count: int, total: int) -> str:
    return f"{count:,} ({count / total:.1%})" if total else "0 (0.0%)"


def _athena_matches(candidates: pl.LazyFrame, config: EnrichmentConfig) -> pl.LazyFrame:
    if config.csv_dir is not None:
        matches = _csv_matches(candidates, config.csv_dir, config)
    else:
        matches = _postgres_matches(candidates, config.postgres or "", config)
    if not config.parent_codes:
        return matches
    return _with_truncation_parents(matches, config)


def _truncations(code: str) -> list[str]:
    """Shorter codes of ``code``, nearest first, without a trailing dot."""

    prefixes: list[str] = []
    for end in range(len(code) - 1, 0, -1):
        prefix = code[:end].rstrip(".")
        if prefix and prefix not in prefixes:
            prefixes.append(prefix)
    return prefixes


def _with_truncation_parents(
    matches: pl.LazyFrame, config: EnrichmentConfig
) -> pl.LazyFrame:
    """Add each valid shorter code of a truncation vocabulary as a parent."""

    found = (
        matches.collect(engine="streaming")
        .select(list(_MATCH_SCHEMA))
        .cast(_MATCH_SCHEMA)  # type: ignore[arg-type]
    )
    keys = (
        found.select(_MATCH_KEYS)
        .unique()
        .filter(pl.col("vocabulary_id").is_in(list(_TRUNCATION_VOCABULARIES)))
    )
    wanted = sorted(
        {
            (vocabulary, prefix)
            for vocabulary, code in keys.select("vocabulary_id", "concept_code").rows()
            for prefix in _truncations(code)
        }
    )
    if not wanted:
        return found.lazy()
    lookup = pl.DataFrame(
        [(f"{vocabulary}//{code}", vocabulary, code, 0) for vocabulary, code in wanted],
        schema=_CANDIDATE_SCHEMA,
        orient="row",
    ).lazy()
    plain = replace(config, parent_codes=False, child_codes=False, sibling_codes=False)
    valid = {
        (row["vocabulary_id"], row["concept_code"]): row["description"]
        for row in _athena_matches(lookup, plain).collect().iter_rows(named=True)
    }
    rows = [
        {
            **key,
            "_relationship": "parent",
            "_related_vocabulary_id": key["vocabulary_id"],
            "_related_concept_code": prefix,
            "_related_concept_name": valid[(key["vocabulary_id"], prefix)],
        }
        for key in keys.iter_rows(named=True)
        for prefix in _truncations(key["concept_code"])
        if (key["vocabulary_id"], prefix) in valid
    ]
    return pl.concat([found, pl.DataFrame(rows, schema=_MATCH_SCHEMA)]).lazy()


def _components(value: pl.Expr) -> pl.Expr:
    """``VOCABULARY//CODE//...`` parts; ``VOCABULARY/CODE`` splits at its first ``/``."""

    single = value.str.contains("/", literal=True) & ~value.str.contains(
        "//", literal=True
    )
    return (
        pl.when(single)
        .then(value.str.replace("/", "//", literal=True))
        .otherwise(value)
        .str.split("//")
    )


def _code_candidates(frame: pl.LazyFrame, offset: int = 0) -> pl.LazyFrame:
    """Heuristic layout candidates; ``offset`` shifts their ranks after rules."""

    code = pl.col("code").cast(pl.String)
    codes = frame.select(
        code.alias("_target_code"), _components(code).alias("_parts")
    ).unique()
    candidates = [
        codes.select(
            "_target_code",
            pl.col("_parts").list.get(0, null_on_oob=True).alias("vocabulary_id"),
            pl.col("_parts").list.get(1, null_on_oob=True).alias("concept_code"),
            pl.lit(offset, dtype=pl.UInt8).alias("_rank"),
        ),
        codes.filter(pl.col("_parts").list.len() == 3).select(
            "_target_code",
            pl.col("_parts").list.get(0).alias("vocabulary_id"),
            pl.col("_parts").list.get(2).alias("concept_code"),
            pl.lit(offset + 1, dtype=pl.UInt8).alias("_rank"),
        ),
    ]
    schema = frame.collect_schema()
    parent_type = schema.get("parent_codes")
    if isinstance(parent_type, pl.List) and parent_type.inner == pl.String:
        reference = pl.col("parent_codes").list.drop_nulls().list.first()
    elif parent_type == pl.String:
        reference = pl.col("parent_codes").str.split("|").list.first()
    else:
        reference = None
    if reference is not None:
        own, parts = pl.col("_own"), pl.col("_parts")

        def undotted(index: int, values: pl.Expr) -> pl.Expr:
            return values.list.get(index, null_on_oob=True).str.replace_all(
                ".", "", literal=True
            )

        # A reference to another code of the code's own vocabulary is a parent,
        # not the code (LOINC/2160-0 under LOINC/LP14355-9). The same code written
        # with its dots still is the code (ICD10CM//I5023 as ICD10CM/I50.23).
        same_vocabulary = (own.list.len() > 1) & (
            own.list.get(0) == parts.list.get(0, null_on_oob=True)
        ).fill_null(False)
        same_code = (undotted(1, parts) == undotted(1, own)).fill_null(False) | (
            undotted(1, parts) == undotted(2, own)
        ).fill_null(False)
        references = (
            frame.select(
                code.alias("_target_code"),
                _components(code).alias("_own"),
                _components(reference).alias("_parts"),
            )
            .filter(~same_vocabulary | same_code)
            .unique()
        )
        candidates.append(
            references.select(
                "_target_code",
                pl.col("_parts").list.get(0, null_on_oob=True).alias("vocabulary_id"),
                pl.col("_parts").list.get(1, null_on_oob=True).alias("concept_code"),
                pl.lit(offset + _PARENT_REFERENCE_RANK, dtype=pl.UInt8).alias("_rank"),
            )
        )
    return (
        pl.concat(candidates)
        .filter(
            pl.col("vocabulary_id").is_not_null()
            & (pl.col("vocabulary_id") != "")
            & pl.col("concept_code").is_not_null()
            & (pl.col("concept_code") != "")
        )
        .unique()
    )


def _vocabulary_reference(value: str | None) -> str | None:
    """A ``parent_codes`` entry as ``VOCABULARY//CODE``, if it names one."""

    if not value:
        return None
    if "//" not in value:
        value = value.replace("/", "//", 1)
    vocabulary, _, rest = value.partition("//")
    code = rest.split("//", 1)[0]
    return f"{vocabulary}//{code}" if vocabulary and code else None


def _parent_lists(frame: pl.LazyFrame) -> dict[str, list[str]]:
    """Existing non-null ``parent_codes`` entries per code, in order."""

    schema = frame.collect_schema()
    if "parent_codes" not in schema:
        return {}
    parents = pl.col("parent_codes")
    if schema["parent_codes"] == pl.String:
        parents = parents.str.split("|")
    data = frame.select(
        pl.col("code").cast(pl.String),
        parents.cast(pl.List(pl.String), strict=False),
    ).collect(engine="streaming")
    result: dict[str, list[str]] = {}
    for code, values in data.iter_rows():
        if values:
            result.setdefault(code, []).extend(value for value in values if value)
    return result


def _parent_references(lists: dict[str, list[str]]) -> dict[str, list[str]]:
    """The distinct ``VOCABULARY//CODE`` references among each code's parents."""

    result = {}
    for code, values in lists.items():
        keys = [key for value in values if (key := _vocabulary_reference(value))]
        if keys:
            result[code] = list(dict.fromkeys(keys))
    return result


def _reference_candidates(keys: list[str]) -> pl.LazyFrame:
    return pl.DataFrame(
        [(_REFERENCE_TARGET + key, *key.split("//", 1), 0) for key in keys],
        schema=_CANDIDATE_SCHEMA,
        orient="row",
    ).lazy()


#: A looked-up reference: its name and its ancestors, as (code, name) pairs.
Reference = tuple[str | None, list[tuple[str, str | None]]]


def _split_references(
    collapsed: pl.DataFrame,
) -> tuple[pl.DataFrame, dict[str, Reference]]:
    """Separate looked-up references from code matches: key -> (name, ancestors)."""

    is_reference = pl.col("code").str.starts_with(_REFERENCE_TARGET)
    found = {
        row["code"][len(_REFERENCE_TARGET) :]: (
            row["_athena_description"],
            list(
                zip(
                    row["_athena_parent_codes"] or (),
                    row["_athena_parent_names"] or (),
                )
            ),
        )
        for row in collapsed.filter(is_reference).iter_rows(named=True)
    }
    return collapsed.filter(~is_reference), found


def _empty_record() -> dict[str, Any]:
    return {**dict.fromkeys((*_FIELDS, *_NAMES.values())), _MATCH_RANK: None}


def _add_reference_parents(
    records: dict[str, dict[str, Any]],
    references: dict[str, list[str]],
    found: dict[str, Reference],
) -> None:
    """Name each code's existing parent references and add their ancestors.

    A parent's ancestors are ancestors of the code, so they are appended like any
    other parent. The references themselves keep their place in the existing list;
    only their names are recorded.
    """

    for code, keys in references.items():
        resolved = [key for key in keys if key in found]
        if not resolved:
            continue
        record = records.setdefault(code, _empty_record())
        codes = list(record["parent_codes"] or [])
        names = list(record["parent_names"] or [])
        names += [None] * (len(codes) - len(names))
        seen = {_reference_key(item) for item in codes}
        for key in resolved:
            for ancestor, name in found[key][1]:
                if _reference_key(ancestor) not in seen:
                    seen.add(_reference_key(ancestor))
                    codes.append(ancestor)
                    names.append(name)
        record["parent_codes"] = codes or None
        record["parent_names"] = names or None
        record["reference_codes"] = resolved
        record["reference_names"] = [found[key][0] for key in resolved]


def _enrichment_frame(records: dict[str, dict[str, Any]]) -> pl.LazyFrame:
    columns: dict[str, list[Any]] = {name: [] for name in _ENRICHMENT_SCHEMA}
    for code, record in records.items():
        columns["code"].append(code)
        columns[_MATCH_RANK].append(record[_MATCH_RANK])
        own = _reference_key(code)
        for name in _FIELDS:
            value = record[name]
            if name in _RELATIONSHIPS:
                codes = value or []
                labels = list(record[_NAMES[name]] or [])
                labels += [None] * (len(codes) - len(labels))
                # A code is never its own parent, child or sibling. Codes that already are
                # vocabulary references (OMOP-style ICD10PCS//0DTJ4ZZ or ICD10PCS/0DTJ4ZZ)
                # would otherwise list their own rule reference.
                kept = [
                    (item, label)
                    for item, label in zip(codes, labels)
                    if _reference_key(item) != own
                ]
                columns[f"_athena_{name}"].append([item for item, _ in kept] or None)
                columns[f"_athena_{_NAMES[name]}"].append(
                    [label for _, label in kept] or None
                )
                continue
            columns[f"_athena_{name}"].append(value)
        for name in _REFERENCE_FIELDS:
            columns[f"_athena_{name}"].append(record.get(name))
    return pl.DataFrame(columns, schema=_ENRICHMENT_SCHEMA).lazy()


def _athena_enrichment(
    released: pl.LazyFrame, config: EnrichmentConfig, stats: dict[str, Any]
) -> pl.LazyFrame:
    """Athena matches of the codes, with their existing parent references looked up."""

    references = (
        _parent_references(_parent_lists(released)) if config.parent_codes else {}
    )
    keys = sorted({key for values in references.values() for key in values})
    candidates = pl.concat([_code_candidates(released), _reference_candidates(keys)])
    matches, found = _split_references(
        _collapse_matches(_athena_matches(candidates, config)).collect()
    )
    records = {
        row["code"]: {
            **{name: row[f"_athena_{name}"] for name in (*_FIELDS, *_NAMES.values())},
            _MATCH_RANK: row[_MATCH_RANK],
        }
        for row in matches.iter_rows(named=True)
    }
    _add_reference_parents(records, references, found)
    stats["references"] = {
        "references": len(keys),
        "resolved": len(found),
        "athena": len(found),
        "pack": 0,
    }
    return _enrichment_frame(records)


def _rule_enrichment(
    released: pl.LazyFrame, config: EnrichmentConfig, stats: dict[str, Any]
) -> tuple[pl.LazyFrame, int]:
    """Resolve codes through rules, Athena, packs, and same-table hierarchy.

    Rule candidates rank before the heuristic layouts. Athena and the packs see
    the same candidates; for each code the best rank wins, and on a tie Athena
    wins over a pack. A tag only counts when nothing validates. When a code is
    validated by both, Athena's scalar fields are kept and the pack's hierarchy
    is merged in, since Athena's ``CONCEPT_ANCESTOR`` has no hierarchy for
    non-standard vocabularies such as ICD-10-CM. Existing parent references are
    looked up in both as well.
    """

    rules = config.rules
    assert rules is not None
    vocabulary_rules = rules.vocabulary_rules
    offset = len(vocabulary_rules)
    codes = (
        released.select(pl.col("code").cast(pl.String))
        .unique()
        .drop_nulls()
        .sort("code")
        .collect(engine="streaming")
        .get_column("code")
        .to_list()
    )
    table = set(codes)
    athena_enabled = config.csv_dir is not None or config.postgres is not None
    packs: OntologyPacks | None = None

    def ontology_packs() -> OntologyPacks:
        nonlocal packs
        if packs is None:
            packs = OntologyPacks(
                config.ontology_source, config.ontology_ref, offline=config.offline
            )
        return packs

    rule_stats = [
        {
            "id": rule.id,
            "kind": "crosswalk" if isinstance(rule, CrosswalkRule) else "ontology",
            "vocabulary": rule.vocabulary,
            "validate": rule.validate,
            "matched": 0,
            "athena": 0,
            "pack": 0,
            "tagged": 0,
            "skipped": (
                "packs disabled"
                if isinstance(rule, CrosswalkRule) and not config.packs
                else "no Athena source"
                if rule.validate == "athena" and not athena_enabled
                else None
            ),
        }
        for rule in vocabulary_rules
    ]
    hits: dict[str, list[tuple[int, tuple[str, ...]]]] = {}
    for index, rule in enumerate(vocabulary_rules):
        if isinstance(rule, CrosswalkRule):
            keys = {code: key for code in codes if (key := rule.key(code))}
            mapping = (
                rule.mapping(ontology_packs().crosswalk(rule.table))
                if keys and config.packs
                else {}
            )
            resolved_values = {code: mapping.get(key, ()) for code, key in keys.items()}
        else:
            resolved_values = {
                code: (value,) for code in codes if (value := rule.resolve(code))
            }
        for code, values in resolved_values.items():
            if values:
                hits.setdefault(code, []).append((index, values))
                rule_stats[index]["matched"] += 1

    layouts = _code_candidates(released, offset).collect(engine="streaming")
    lists = _parent_lists(released)
    parent_references = _parent_references(lists) if config.parent_codes else {}
    reference_keys = sorted(
        {key for keys in parent_references.values() for key in keys}
    )
    athena: dict[str, dict[str, Any]] = {}
    found_athena: dict[str, Reference] = {}
    if athena_enabled:
        rule_candidates = pl.DataFrame(
            [
                (code, vocabulary_rules[index].vocabulary, value, index)
                for code, matches in hits.items()
                for index, values in matches
                for value in values
            ],
            schema=_CANDIDATE_SCHEMA,
            orient="row",
        ).lazy()
        candidates = pl.concat(
            [rule_candidates, layouts.lazy(), _reference_candidates(reference_keys)]
        )
        collapsed, found_athena = _split_references(
            _collapse_matches(_athena_matches(candidates, config)).collect()
        )
        athena = {row["code"]: row for row in collapsed.iter_rows(named=True)}

    requested: dict[str, set[str]] = {}
    for matches in hits.values():
        for index, values in matches:
            rule = vocabulary_rules[index]
            if rule.validate == "pack" and config.packs:
                requested.setdefault(rule.vocabulary, set()).update(values)
    # The built-in layouts and the existing parent references are looked up in
    # every pack of their vocabulary, like rule codes.
    available = (
        set(ontology_packs().vocabularies())
        if config.packs and (layouts.height or reference_keys)
        else set()
    )
    layout_hits: dict[str, list[tuple[int, str, str]]] = {}
    for target, vocabulary, value, rank in layouts.iter_rows():
        if vocabulary in available:
            requested.setdefault(vocabulary, set()).add(value)
            layout_hits.setdefault(target, []).append((rank, vocabulary, value))
    for key in reference_keys:
        vocabulary, value = key.split("//", 1)
        if vocabulary in available:
            requested.setdefault(vocabulary, set()).add(value)
    entries: dict[tuple[str, str], PackEntry] = {}
    for vocabulary, values in sorted(requested.items()):
        found = ontology_packs().lookup(
            vocabulary,
            values,
            child_depth=config.child_depth if config.child_codes else 0,
            siblings=config.sibling_codes,
            hierarchy=config.loinc_hierarchy and vocabulary == "LOINC",
        )
        entries.update(((vocabulary, code), entry) for code, entry in found.items())

    records: dict[str, dict[str, Any]] = {}
    resolved: dict[str, tuple[int, str]] = {}
    vocabulary_parents: dict[str, list[str]] = {}
    layout_stats = {"athena": 0, "pack": 0}
    for code in codes:
        matched = athena.get(code)
        athena_rank = None if matched is None else matched[_MATCH_RANK]
        choice = None
        # A tag (validate: none) is a fallback: any later candidate that validates,
        # through a pack or Athena, wins over it.
        tag = None
        for index, values in hits.get(code, ()):
            if athena_rank is not None and athena_rank <= index:
                break
            rule = vocabulary_rules[index]
            if rule.validate == "pack":
                found = [
                    value for value in values if (rule.vocabulary, value) in entries
                ]
                if found:
                    choice = (index, (found[0],))
                    break
            elif rule.validate == "none" and tag is None:
                tag = (index, values)
        layout = None
        if choice is None:
            # A built-in layout or the parent reference that a pack validates;
            # these rank after the rules and lose ties to Athena.
            layout = next(
                (
                    (rank, vocabulary, value)
                    for rank, vocabulary, value in sorted(layout_hits.get(code, ()))
                    if (vocabulary, value) in entries
                    and (athena_rank is None or rank < athena_rank)
                ),
                None,
            )
        if choice is None and layout is None and matched is None:
            choice = tag
        if layout is not None:
            rank, vocabulary, value = layout
            record = _empty_record()
            record[_MATCH_RANK] = rank
            record["vocabulary_id"] = vocabulary
            record["concept_code"] = value
            # The code, or the reference it was matched by, already names this
            # vocabulary code, so only its hierarchy is added.
            _add_vocabulary(
                record,
                f"{vocabulary}//{value}",
                entries[(vocabulary, value)],
                vocabulary,
                include_self=False,
            )
            layout_stats["pack"] += 1
            records[code] = record
        elif choice is None and matched is not None:
            record = {
                name: matched[f"_athena_{name}"]
                for name in (*_FIELDS, *_NAMES.values())
            }
            record[_MATCH_RANK] = athena_rank
            reference = f"{record['vocabulary_id']}//{record['concept_code']}"
            if athena_rank < offset:
                rule = vocabulary_rules[athena_rank]
                entry = entries.get((rule.vocabulary, record["concept_code"]))
                _add_vocabulary(record, reference, entry, rule.vocabulary)
                resolved[code] = (athena_rank, reference)
                vocabulary_parents[code] = record["parent_codes"][1:]
                rule_stats[athena_rank]["athena"] += 1
            else:
                entry = entries.get((record["vocabulary_id"], record["concept_code"]))
                if entry is not None:
                    _add_vocabulary(
                        record,
                        reference,
                        entry,
                        record["vocabulary_id"],
                        include_self=False,
                    )
                layout_stats["athena"] += 1
            records[code] = record
        elif choice is not None:
            index, values = choice
            rule = vocabulary_rules[index]
            record = dict.fromkeys((*_FIELDS, *_NAMES.values()))
            record[_MATCH_RANK] = index
            references = [f"{rule.vocabulary}//{value}" for value in values]
            if rule.validate == "pack":
                record["vocabulary_id"] = rule.vocabulary
                record["concept_code"] = values[0]
                _add_vocabulary(
                    record,
                    references[0],
                    entries[(rule.vocabulary, values[0])],
                    rule.vocabulary,
                )
                vocabulary_parents[code] = record["parent_codes"][1:]
                rule_stats[index]["pack"] += 1
            else:
                record["parent_codes"] = references
                record["parent_names"] = [None] * len(references)
                vocabulary_parents[code] = []
                rule_stats[index]["tagged"] += 1
            resolved[code] = (index, references[0])
            records[code] = record

    found: dict[str, Reference] = {}
    for key in reference_keys:
        vocabulary, value = key.split("//", 1)
        hit, entry = found_athena.get(key), entries.get((vocabulary, value))
        if hit is None and entry is None:
            continue
        name, ancestors = hit if hit is not None else (None, [])
        if entry is not None:
            listed = {item for item, _ in ancestors}
            names = list(entry.ancestor_names)
            names += [None] * (len(entry.ancestors) - len(names))
            ancestors = [
                *ancestors,
                *(
                    (f"{vocabulary}//{item}", label)
                    for item, label in zip(entry.ancestors, names)
                    if f"{vocabulary}//{item}" not in listed
                ),
            ]
            name = name or entry.description
        found[key] = (name, ancestors)
    _add_reference_parents(records, parent_references, found)

    hierarchy_stats = []
    if config.parent_codes and rules.hierarchy:
        before = {code: set(values) for code, values in lists.items()}
        added: dict[str, list[str]] = {}
        for rule in rules.hierarchy:
            if rule.mode == "ontology":
                proposals = _ontology_parents(
                    rule, resolved, vocabulary_parents, vocabulary_rules
                )
            else:
                proposals = {}
                for code in codes:
                    parents = [
                        parent
                        for parent in rule.candidates(code)
                        if parent != code and parent in table
                    ]
                    if parents:
                        proposals[code] = parents[:1] if rule.nearest_only else parents
            links = gained = 0
            for code, parents in proposals.items():
                existing = added.setdefault(code, [])
                new = [parent for parent in parents if parent not in existing]
                existing.extend(new)
                fresh = sum(parent not in before.get(code, ()) for parent in new)
                links += fresh
                gained += fresh > 0
            hierarchy_stats.append(
                {"id": rule.id, "mode": rule.mode, "codes": gained, "links": links}
            )
        for code, parents in added.items():
            if not parents:
                continue
            record = records.setdefault(code, _empty_record())
            current = record["parent_codes"] or []
            names = record["parent_names"] or [None] * len(current)
            new = [p for p in parents if p not in current]
            record["parent_codes"] = current + new
            record["parent_names"] = names + [None] * len(new)

    stats["rules"] = rule_stats
    stats["layouts"] = layout_stats
    stats["references"] = {
        "references": len(reference_keys),
        "resolved": len(found),
        "athena": len(found_athena),
        "pack": sum(tuple(key.split("//", 1)) in entries for key in reference_keys),
    }
    stats["hierarchy"] = hierarchy_stats
    stats["packs"] = dict(packs.stats) if packs is not None else {}
    stats["pack_source"] = packs.describe() if packs is not None else None
    stats["athena"] = athena_enabled
    return _enrichment_frame(records), offset + _PARENT_REFERENCE_RANK


def _add_vocabulary(
    record: dict[str, Any],
    reference: str,
    entry: PackEntry | None,
    vocabulary: str,
    *,
    include_self: bool = True,
) -> None:
    """Put a rule's own code first in its parents and merge the pack hierarchy.

    ``include_self=False`` leaves the code out, for codes that already name it
    themselves or through their parent reference. Every relationship list keeps
    its names aligned (``parent_names`` and so on); a known name is never replaced.
    """

    Entry = tuple[str, "str | None"]

    def entries(field: str) -> list[Entry]:
        codes = record[field] or []
        names = list(record[_NAMES[field]] or [])
        return list(zip(codes, names + [None] * (len(codes) - len(names))))

    def merge(current: list[Entry], extra: list[Entry]) -> list[Entry]:
        result = list(current)
        position = {code: index for index, (code, _) in enumerate(result)}
        for code, name in extra:
            if code not in position:
                position[code] = len(result)
                result.append((code, name))
            elif result[position[code]][1] is None:
                result[position[code]] = (code, name)
        return result

    def store(field: str, values: list[Entry]) -> None:
        record[field] = [code for code, _ in values]
        record[_NAMES[field]] = [name for _, name in values]

    own = record["description"]
    if own is None and entry is not None:
        own = entry.description
    parents = [e for e in entries("parent_codes") if e[0] != reference]
    if include_self:
        parents.insert(0, (reference, own))
    if entry is not None:

        def refs(values: tuple[str, ...], names: tuple[str | None, ...]) -> list[Entry]:
            padded = list(names) + [None] * (len(values) - len(names))
            return [
                (f"{vocabulary}//{value}", name) for value, name in zip(values, padded)
            ]

        parents = merge(parents, refs(entry.ancestors, entry.ancestor_names))
        store(
            "child_codes",
            merge(entries("child_codes"), refs(entry.children, entry.child_names)),
        )
        store(
            "sibling_codes",
            merge(entries("sibling_codes"), refs(entry.siblings, entry.sibling_names)),
        )
        if record["description"] is None:
            record["description"] = entry.description
    store("parent_codes", parents)


def _ontology_parents(
    rule: Any,
    resolved: dict[str, tuple[int, str]],
    vocabulary_parents: dict[str, list[str]],
    vocabulary_rules: tuple[Any, ...],
) -> dict[str, list[str]]:
    """Same-table codes whose resolved code is an ancestor, nearest first."""

    eligible = {
        code: reference
        for code, (index, reference) in resolved.items()
        if not rule.rules or vocabulary_rules[index].id in rule.rules
    }
    by_reference: dict[str, list[str]] = {}
    for code, reference in sorted(eligible.items()):
        by_reference.setdefault(reference, []).append(code)
    proposals = {}
    for code in eligible:
        found = [
            other
            for ancestor in vocabulary_parents.get(code, ())
            for other in by_reference.get(ancestor, ())
            if other != code
        ]
        found = list(dict.fromkeys(found))
        if rule.nearest_only and len(found) > 1:
            found = [
                other
                for other in found
                if not any(
                    eligible[other] in vocabulary_parents.get(nearer, ())
                    for nearer in found
                    if nearer != other
                )
            ]
        if found:
            proposals[code] = found
    return proposals


def _csv_matches(
    candidates: pl.LazyFrame, directory: Path, config: EnrichmentConfig
) -> pl.LazyFrame:
    directory = Path(directory).expanduser().resolve()
    concepts = _scan_athena_csv(
        directory / "CONCEPT.csv",
        {
            "concept_id": pl.Int64,
            "concept_name": pl.String,
            "domain_id": pl.String,
            "vocabulary_id": pl.String,
            "concept_code": pl.String,
            "standard_concept": pl.String,
            "invalid_reason": pl.String,
        },
    ).filter(pl.col("invalid_reason").is_null())

    # If any of the hierarchy options are requested, load the CONCEPT_ANCESTOR.csv
    # and filter out any ancestor/descendant pairs that are also reversed in the file.
    if config.child_codes or config.sibling_codes or config.parent_codes:
        hierarchy = _scan_athena_csv(
            directory / "CONCEPT_ANCESTOR.csv",
            {
                "ancestor_concept_id": pl.Int64,
                "descendant_concept_id": pl.Int64,
                "min_levels_of_separation": pl.Int64,
            },
        ).filter(
            (pl.col("min_levels_of_separation") > 0)
            & (pl.col("ancestor_concept_id") != pl.col("descendant_concept_id"))
        )
        reverse = hierarchy.select(
            pl.col("descendant_concept_id").alias("ancestor_concept_id"),
            pl.col("ancestor_concept_id").alias("descendant_concept_id"),
        ).unique()
        hierarchy = hierarchy.join(
            reverse,
            on=["ancestor_concept_id", "descendant_concept_id"],
            how="anti",
        )
    matches = (
        candidates.join(
            concepts,
            on=["vocabulary_id", "concept_code"],
            how="inner",
        )
        .select(
            "_target_code",
            "_rank",
            pl.col("concept_name").alias("description"),
            "vocabulary_id",
            "concept_id",
            "concept_code",
            "domain_id",
            "standard_concept",
        )
        .unique()
    )
    related = concepts.select(
        pl.col("concept_id").alias("_related_concept_id"),
        pl.col("vocabulary_id").alias("_related_vocabulary_id"),
        pl.col("concept_code").alias("_related_concept_code"),
        pl.col("concept_name").alias("_related_concept_name"),
    )
    # Each relationship maps a matched concept_id to a _related_concept_id.
    edges = {}
    if config.parent_codes:
        edges["parent"] = hierarchy.select(
            pl.col("descendant_concept_id").alias("concept_id"),
            pl.col("ancestor_concept_id").alias("_related_concept_id"),
        )
    if config.child_codes:
        edges["child"] = hierarchy.filter(
            pl.col("min_levels_of_separation") <= config.child_depth
        ).select(
            pl.col("ancestor_concept_id").alias("concept_id"),
            pl.col("descendant_concept_id").alias("_related_concept_id"),
        )
    if config.sibling_codes:
        parents = (
            hierarchy.select(
                pl.col("descendant_concept_id").alias("concept_id"),
                pl.col("ancestor_concept_id").alias("_parent_concept_id"),
            )
            .join(
                concepts.select(pl.col("concept_id").alias("_parent_concept_id")),
                on="_parent_concept_id",
                how="inner",
            )
            .unique()
        )
        children = hierarchy.filter(pl.col("min_levels_of_separation") == 1).select(
            pl.col("ancestor_concept_id").alias("_parent_concept_id"),
            pl.col("descendant_concept_id").alias("_related_concept_id"),
        )
        edges["sibling"] = (
            parents.join(children, on="_parent_concept_id", how="inner")
            .filter(pl.col("_related_concept_id") != pl.col("concept_id"))
            .select("concept_id", "_related_concept_id")
        )
    return pl.concat(
        [
            matches.select(
                *_MATCH_KEYS,
                *(
                    pl.lit(None, dtype=pl.String).alias(name)
                    for name in tuple(_MATCH_SCHEMA)[-4:]
                ),
            ),
            *(
                matches.join(pairs, on="concept_id", how="inner")
                .join(related, on="_related_concept_id", how="inner")
                .select(
                    *_MATCH_KEYS,
                    pl.lit(relationship).alias("_relationship"),
                    "_related_vocabulary_id",
                    "_related_concept_code",
                    "_related_concept_name",
                )
                for relationship, pairs in edges.items()
            ),
        ]
    )


def _scan_athena_csv(path: Path, columns: dict[str, pl.DataType]) -> pl.LazyFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Athena file not found: {path}")
    with path.open(newline="", encoding="utf-8-sig") as file:
        header = next(csv.reader(file, delimiter="\t"), [])
    available = {name.casefold(): name for name in header}
    missing = set(columns) - set(available)
    if missing:
        raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")
    return pl.scan_csv(
        path,
        separator="\t",
        null_values=[""],
        schema_overrides={available[name]: dtype for name, dtype in columns.items()},
        quote_char=None,
    ).select(
        pl.col(available[name]).cast(dtype, strict=False).alias(name)
        for name, dtype in columns.items()
    )


def _postgres_matches(
    candidates: pl.LazyFrame, connection: str, config: EnrichmentConfig
) -> pl.LazyFrame:
    requested = candidates.collect(engine="streaming")
    if requested.is_empty():
        return pl.DataFrame(schema=_MATCH_SCHEMA).lazy()
    if any(
        "\n" in value or "\r" in value
        for row in requested.iter_rows()
        for value in row
        if isinstance(value, str)
    ):
        raise ValueError("PostgreSQL enrichment does not accept newlines in codes")

    data = io.StringIO()
    writer = csv.writer(data, lineterminator="\n")
    writer.writerow(["_target_code", "vocabulary_id", "concept_code", "_rank"])
    writer.writerows(requested.iter_rows())
    # Each relationship joins a chosen concept m to a ``related`` concept.
    joins = {}
    if config.parent_codes:
        joins["parent"] = """
JOIN hierarchy h ON h.descendant_concept_id = m.concept_id
JOIN concept related ON related.concept_id = h.ancestor_concept_id
"""
    if config.child_codes:
        joins["child"] = f"""
JOIN hierarchy h ON h.ancestor_concept_id = m.concept_id
                AND h.min_levels_of_separation <= {config.child_depth}
JOIN concept related ON related.concept_id = h.descendant_concept_id
"""
    if config.sibling_codes:
        joins["sibling"] = """
JOIN hierarchy parent_h ON parent_h.descendant_concept_id = m.concept_id
JOIN concept parent ON parent.concept_id = parent_h.ancestor_concept_id
                   AND parent.invalid_reason IS NULL
JOIN hierarchy child_h ON child_h.ancestor_concept_id = parent_h.ancestor_concept_id
                      AND child_h.min_levels_of_separation = 1
JOIN concept related ON related.concept_id = child_h.descendant_concept_id
                    AND related.concept_id <> m.concept_id
"""
    relationship_queries = [
        f"""
SELECT m._target_code, m._rank, m.concept_id,
       '{relationship}'::text AS _relationship,
       related.vocabulary_id AS _related_vocabulary_id,
       related.concept_code AS _related_concept_code,
       related.concept_name AS _related_concept_name
FROM chosen m{join}WHERE related.invalid_reason IS NULL
"""
        for relationship, join in joins.items()
    ]
    relationships = (
        "\nUNION ALL\n".join(relationship_queries)
        or """
SELECT NULL::text AS _target_code, NULL::smallint AS _rank,
       NULL::bigint AS concept_id, NULL::text AS _relationship,
       NULL::text AS _related_vocabulary_id,
       NULL::text AS _related_concept_code,
       NULL::text AS _related_concept_name
WHERE false
"""
    )
    script = f"""
CREATE TEMP TABLE requested (
    _target_code text,
    vocabulary_id text,
    concept_code text,
    _rank smallint
);
COPY requested FROM STDIN WITH (FORMAT csv, HEADER true);
{data.getvalue()}\\.
WITH matches AS (
    SELECT r.*, c.concept_id, c.concept_name, c.domain_id, c.standard_concept
    FROM requested r
    JOIN concept c USING (vocabulary_id, concept_code)
    WHERE c.invalid_reason IS NULL
), chosen AS (
    SELECT * FROM matches m
    WHERE _rank = (SELECT min(_rank) FROM matches WHERE _target_code = m._target_code)
), hierarchy AS (
    SELECT ca.*
    FROM concept_ancestor ca
    WHERE ca.min_levels_of_separation > 0
      AND ca.ancestor_concept_id <> ca.descendant_concept_id
      AND NOT EXISTS (
          SELECT 1
          FROM concept_ancestor rev
          WHERE rev.ancestor_concept_id = ca.descendant_concept_id
            AND rev.descendant_concept_id = ca.ancestor_concept_id
            AND rev.min_levels_of_separation > 0
      )
), relationships AS (
{relationships}
)
SELECT
    m._target_code,
    m._rank,
    m.concept_name AS description,
    m.vocabulary_id,
    m.concept_id,
    m.concept_code,
    m.domain_id,
    m.standard_concept,
    r._relationship,
    r._related_vocabulary_id,
    r._related_concept_code,
    r._related_concept_name
FROM chosen m
LEFT JOIN relationships r
    ON r._target_code = m._target_code
   AND r._rank = m._rank
   AND r.concept_id = m.concept_id
ORDER BY m._target_code, m.concept_id, r._relationship,
         r._related_vocabulary_id, r._related_concept_code;
"""
    try:
        result = subprocess.run(
            ["psql", connection, "-X", "-q", "--csv", "-v", "ON_ERROR_STOP=1"],
            input=script,
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError as error:
        raise FileNotFoundError(
            "psql is required for PostgreSQL Athena enrichment"
        ) from error
    if result.returncode:
        message = result.stderr.strip() or "unknown psql error"
        if "no password supplied" in message:
            message += "; set PGPASSWORD in the suMEDS/Jupyter environment"
        raise RuntimeError(f"Athena PostgreSQL query failed: {message}")
    if not result.stdout.strip():
        return pl.DataFrame(schema=_MATCH_SCHEMA).lazy()
    return pl.read_csv(
        io.StringIO(result.stdout),
        null_values=[""],
        schema_overrides=_MATCH_SCHEMA,
    ).lazy()


def _collapse_matches(matches: pl.LazyFrame) -> pl.LazyFrame:
    best = matches.with_columns(
        pl.col("_rank").min().over("_target_code").alias("_best_rank")
    ).filter(pl.col("_rank") == pl.col("_best_rank"))
    chosen = best.with_columns(
        pl.col("concept_id").min().over("_target_code").alias("_chosen_concept_id")
    ).filter(pl.col("concept_id") == pl.col("_chosen_concept_id"))
    related = pl.struct(
        code=pl.concat_str(
            "_related_vocabulary_id",
            "_related_concept_code",
            separator="//",
            ignore_nulls=False,
        ),
        name=pl.col("_related_concept_name"),
    )

    def entries(relationship: str) -> pl.Expr:
        # One entry per related code, sorted by code, with its concept name.
        values = (
            pl.when(pl.col("_relationship") == relationship)
            .then(related)
            .otherwise(None)
            .drop_nulls()
        )
        values = values.sort_by(values.struct.field("code"))
        return values.filter(values.struct.field("code").is_first_distinct())

    def field(name: str, part: str) -> pl.Expr:
        return (
            pl.when(pl.col(name).list.len() > 0)
            .then(pl.col(name).list.eval(pl.element().struct.field(part)))
            .otherwise(None)
        )

    collapsed = chosen.group_by(*_MATCH_KEYS).agg(
        entries(relationship).alias(name)
        for name, relationship in _RELATIONSHIPS.items()
    )
    return collapsed.select(
        pl.col("_target_code").alias("code"),
        pl.col("_rank").alias(_MATCH_RANK),
        *(
            field(name, "code").alias(f"_athena_{name}")
            if name in _RELATIONSHIPS
            else pl.col(name).alias(f"_athena_{name}")
            for name in _FIELDS
        ),
        *(
            field(name, "name").alias(f"_athena_{label}")
            for name, label in _NAMES.items()
        ),
    )
