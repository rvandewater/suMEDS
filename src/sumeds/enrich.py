"""Athena and public-ontology enrichment for MEDS metadata and suMEDS summaries."""

from __future__ import annotations

import csv
import io
import subprocess
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
_INTERNAL = {f"_athena_{name}" for name in _FIELDS} | {_MATCH_RANK}
_MATCH_SCHEMA = {
    "_target_code": pl.String,
    "_rank": pl.UInt8,
    **{name: dtype for name, dtype in _FIELDS.items() if name not in _RELATIONSHIPS},
    "_relationship": pl.String,
    "_related_vocabulary_id": pl.String,
    "_related_concept_code": pl.String,
}
_MATCH_KEYS = tuple(_MATCH_SCHEMA)[:-3]
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
}
# Heuristic candidate ranks: VOCAB//CODE, VOCAB//VERSION//CODE, parent reference.
_PARENT_REFERENCE_RANK = 2


def enrich_metadata(frame: pl.LazyFrame, config: EnrichmentConfig) -> pl.LazyFrame:
    """Fill scalar metadata and merge configured hierarchy fields.

    Codes are resolved by the configured rules first (ontology, then crosswalk
    rules, in file order), then as ``VOCABULARY//CODE//...``. For an ambiguous
    three-part code, the third component is tried only when the second has no
    valid Athena match, supporting ``VOCABULARY//VERSION//CODE``. An existing
    ``VOCABULARY/CODE`` parent reference is the final fallback.
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
    if config.rules is None:
        enrichment = _collapse_matches(
            _athena_matches(_code_candidates(released), config)
        )
        parent_rank = _PARENT_REFERENCE_RANK
    else:
        enrichment, parent_rank = _rule_enrichment(
            released, config, {} if stats is None else stats
        )
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
    return joined.with_columns(expressions).drop(*sorted(_INTERNAL))


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
        f"{before['parent_links']:,} -> {after['parent_links']:,}"
    )
    print(
        "  Same-dataset parent links:  "
        f"{before['dataset_parent_links']:,} -> {after['dataset_parent_links']:,}"
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
                f"    {name}: {rule['matched']:,} matched, skipped (no Athena source)"
            )
            continue
        validated = rule["athena"] + rule["pack"]
        print(
            f"    {name}: {rule['matched']:,} matched, {validated:,} validated "
            f"(Athena {rule['athena']:,}, pack {rule['pack']:,}), "
            f"{rule['tagged']:,} tagged"
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
        return _csv_matches(candidates, config.csv_dir, config)
    return _postgres_matches(candidates, config.postgres or "", config)


def _code_candidates(frame: pl.LazyFrame, offset: int = 0) -> pl.LazyFrame:
    """Heuristic layout candidates; ``offset`` shifts their ranks after rules."""

    parts = pl.col("code").cast(pl.String).str.split("//")
    codes = frame.select(
        pl.col("code").cast(pl.String).alias("_target_code"),
        parts.alias("_parts"),
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
        references = frame.select(
            pl.col("code").cast(pl.String).alias("_target_code"),
            pl.when(reference.str.contains("//", literal=True))
            .then(reference.str.split("//"))
            .otherwise(reference.str.split("/"))
            .alias("_parts"),
        ).unique()
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


def _rule_enrichment(
    released: pl.LazyFrame, config: EnrichmentConfig, stats: dict[str, Any]
) -> tuple[pl.LazyFrame, int]:
    """Resolve codes through rules, Athena, packs, and same-table hierarchy.

    Rule candidates rank before the heuristic layouts. For each code the best
    rank wins; on a tie Athena wins over a pack, and a pack over a tag. When a
    rule's code is validated by both, Athena's scalar fields are kept and the
    pack's hierarchy is merged in, since Athena's ``CONCEPT_ANCESTOR`` has no
    hierarchy for non-standard vocabularies such as ICD-10-CM.
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
            "skipped": rule.validate == "athena" and not athena_enabled,
        }
        for rule in vocabulary_rules
    ]
    hits: dict[str, list[tuple[int, tuple[str, ...]]]] = {}
    for index, rule in enumerate(vocabulary_rules):
        if isinstance(rule, CrosswalkRule):
            keys = {code: key for code in codes if (key := rule.key(code))}
            mapping = (
                rule.mapping(ontology_packs().crosswalk(rule.table)) if keys else {}
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

    athena: dict[str, dict[str, Any]] = {}
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
        candidates = pl.concat([rule_candidates, _code_candidates(released, offset)])
        collapsed = _collapse_matches(_athena_matches(candidates, config)).collect()
        athena = {row["code"]: row for row in collapsed.iter_rows(named=True)}

    requested: dict[str, set[str]] = {}
    for matches in hits.values():
        for index, values in matches:
            rule = vocabulary_rules[index]
            if rule.validate == "pack":
                requested.setdefault(rule.vocabulary, set()).update(values)
    entries: dict[tuple[str, str], PackEntry] = {}
    for vocabulary, values in sorted(requested.items()):
        found = ontology_packs().lookup(
            vocabulary,
            values,
            child_depth=config.child_depth if config.child_codes else 0,
            siblings=config.sibling_codes,
        )
        entries.update(((vocabulary, code), entry) for code, entry in found.items())

    records: dict[str, dict[str, Any]] = {}
    resolved: dict[str, tuple[int, str]] = {}
    vocabulary_parents: dict[str, list[str]] = {}
    for code in codes:
        matched = athena.get(code)
        athena_rank = None if matched is None else matched[_MATCH_RANK]
        choice = None
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
            elif rule.validate == "none":
                choice = (index, values)
                break
        if choice is None and matched is not None:
            record = {name: matched[f"_athena_{name}"] for name in _FIELDS}
            record[_MATCH_RANK] = athena_rank
            if athena_rank < offset:
                rule = vocabulary_rules[athena_rank]
                reference = f"{record['vocabulary_id']}//{record['concept_code']}"
                entry = entries.get((rule.vocabulary, record["concept_code"]))
                _add_vocabulary(record, reference, entry, rule.vocabulary)
                resolved[code] = (athena_rank, reference)
                vocabulary_parents[code] = record["parent_codes"][1:]
                rule_stats[athena_rank]["athena"] += 1
            records[code] = record
        elif choice is not None:
            index, values = choice
            rule = vocabulary_rules[index]
            record = dict.fromkeys(_FIELDS)
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
                vocabulary_parents[code] = []
                rule_stats[index]["tagged"] += 1
            resolved[code] = (index, references[0])
            records[code] = record

    hierarchy_stats = []
    if config.parent_codes and rules.hierarchy:
        before = _existing_parents(released)
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
            record = records.setdefault(
                code, {**dict.fromkeys(_FIELDS), _MATCH_RANK: None}
            )
            current = record["parent_codes"] or []
            record["parent_codes"] = current + [p for p in parents if p not in current]

    stats["rules"] = rule_stats
    stats["hierarchy"] = hierarchy_stats
    stats["packs"] = dict(packs.stats) if packs is not None else {}
    stats["pack_source"] = packs.describe() if packs is not None else None
    stats["athena"] = athena_enabled
    columns: dict[str, list[Any]] = {name: [] for name in _ENRICHMENT_SCHEMA}
    for code, record in records.items():
        columns["code"].append(code)
        columns[_MATCH_RANK].append(record[_MATCH_RANK])
        own = _reference_key(code)
        for name in _FIELDS:
            value = record[name]
            if name in _RELATIONSHIPS and value:
                # A code is never its own parent, child or sibling. Codes that already are
                # vocabulary references (OMOP-style ICD10PCS//0DTJ4ZZ or ICD10PCS/0DTJ4ZZ)
                # would otherwise list their own rule reference.
                value = [item for item in value if _reference_key(item) != own]
            if name in _RELATIONSHIPS and not value:
                value = None
            columns[f"_athena_{name}"].append(value)
    return (
        pl.DataFrame(columns, schema=_ENRICHMENT_SCHEMA).lazy(),
        offset + _PARENT_REFERENCE_RANK,
    )


def _existing_parents(frame: pl.LazyFrame) -> dict[str, set[str]]:
    """Existing ``parent_codes`` per code, to count only newly added parents."""

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
    result: dict[str, set[str]] = {}
    for code, values in data.iter_rows():
        if values:
            result.setdefault(code, set()).update(value for value in values if value)
    return result


def _add_vocabulary(
    record: dict[str, Any],
    reference: str,
    entry: PackEntry | None,
    vocabulary: str,
) -> None:
    """Put a rule's own code first in its parents and merge the pack hierarchy."""

    def merge(current: list[str] | None, extra: list[str]) -> list[str]:
        result = list(current or [])
        result.extend(value for value in extra if value not in result)
        return result

    parents = [
        reference,
        *(value for value in record["parent_codes"] or [] if value != reference),
    ]
    if entry is not None:

        def refs(values: tuple[str, ...]) -> list[str]:
            return [f"{vocabulary}//{value}" for value in values]

        parents = merge(parents, refs(entry.ancestors))
        record["child_codes"] = merge(record["child_codes"], refs(entry.children))
        record["sibling_codes"] = merge(record["sibling_codes"], refs(entry.siblings))
        if record["description"] is None:
            record["description"] = entry.description
    record["parent_codes"] = parents


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
                    for name in tuple(_MATCH_SCHEMA)[-3:]
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
       related.concept_code AS _related_concept_code
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
       NULL::text AS _related_concept_code
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
    r._related_concept_code
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
    related_code = pl.concat_str(
        "_related_vocabulary_id",
        "_related_concept_code",
        separator="//",
        ignore_nulls=False,
    )
    collapsed = chosen.group_by(*_MATCH_KEYS).agg(
        pl.when(pl.col("_relationship") == relationship)
        .then(related_code)
        .otherwise(None)
        .drop_nulls()
        .unique()
        .sort()
        .alias(name)
        for name, relationship in _RELATIONSHIPS.items()
    )
    return collapsed.select(
        pl.col("_target_code").alias("code"),
        pl.col("_rank").alias(_MATCH_RANK),
        *(
            pl.when(pl.col(name).list.len() > 0)
            .then(pl.col(name))
            .otherwise(None)
            .alias(f"_athena_{name}")
            if name in _RELATIONSHIPS
            else pl.col(name).alias(f"_athena_{name}")
            for name in _FIELDS
        ),
    )
