"""OHDSI Athena enrichment for MEDS metadata and suMEDS summaries."""

from __future__ import annotations

import csv
import io
import subprocess
from pathlib import Path
from uuid import uuid4

import polars as pl
from tqdm.auto import tqdm

from .config import EnrichmentConfig
from .io import output_format, scan_table, sink_table

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
    "description": pl.String,
    "vocabulary_id": pl.String,
    "concept_id": pl.Int64,
    "concept_code": pl.String,
    "domain_id": pl.String,
    "standard_concept": pl.String,
    "_relationship": pl.String,
    "_related_vocabulary_id": pl.String,
    "_related_concept_code": pl.String,
}
_MATCH_COLUMNS = tuple(_MATCH_SCHEMA)


def enrich_metadata(frame: pl.LazyFrame, config: EnrichmentConfig) -> pl.LazyFrame:
    """Fill scalar metadata and merge configured hierarchy fields from Athena.

    Codes are resolved as ``VOCABULARY//CODE//...`` first. For an ambiguous
    three-part code, the third component is tried only when the second has no
    valid Athena match, supporting ``VOCABULARY//VERSION//CODE``. An existing
    ``VOCABULARY/CODE`` parent reference is the final fallback.
    """

    schema = frame.collect_schema()
    if "code" not in schema:
        raise ValueError("metadata to enrich must contain a 'code' column")
    collisions = set(schema.names()) & _INTERNAL
    if collisions:
        raise ValueError(
            f"metadata uses reserved enrichment columns: {sorted(collisions)}"
        )

    candidates = _code_candidates(
        frame.filter(~pl.col("is_masked"))
        if schema.get("is_masked") == pl.Boolean
        else frame
    )
    if config.csv_dir is not None:
        matches = _csv_matches(candidates, config.csv_dir, config)
    else:
        matches = _postgres_matches(candidates, config.postgres or "", config)
    enrichment = _collapse_matches(matches)
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
            empty = pl.lit([], dtype=dtype)
            existing = existing.cast(dtype, strict=False)
            present = existing.is_not_null() | incoming.is_not_null()
            if name == "parent_codes":
                existing = existing.list.eval(
                    pl.when(pl.element().str.count_matches("/") == 1)
                    .then(pl.element().str.replace("/", "//", literal=True))
                    .otherwise(pl.element())
                )
                if config.exclude_self_parent_code:
                    matched_by_parent = pl.col(_MATCH_RANK) == 2
                    present = (
                        pl.when(matched_by_parent)
                        .then((existing.list.len() > 1) | incoming.is_not_null())
                        .otherwise(present)
                    )
                    existing = (
                        pl.when(matched_by_parent)
                        .then(existing.list.slice(1))
                        .otherwise(existing)
                    )
            merged = pl.concat_list(
                pl.coalesce(existing, empty),
                pl.coalesce(incoming, empty),
            ).list.unique(maintain_order=True)
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
            progress.set_description("Querying Athena")
            enriched = enrich_metadata(source, config)
            progress.update()
            progress.set_description("Writing output")
            sink_table(enriched, staged, format_)
            staged.replace(output_path)
            progress.update()
            if verbose:
                progress.set_description("Calculating coverage")
                report = _enrichment_report(source, scan_table(output_path))
            progress.update()
        finally:
            staged.unlink(missing_ok=True)
    if verbose:
        _print_enrichment_report(report)
    return output_path


def _enrichment_report(
    before: pl.LazyFrame, after: pl.LazyFrame
) -> tuple[dict[str, int], dict[str, int]]:
    return _coverage(before), _coverage(after)


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
    if "concept_id" in schema:
        expressions.append(
            pl.col("code")
            .filter(pl.col("concept_id").is_not_null())
            .n_unique()
            .alias("matched_codes")
        )
    else:
        expressions.append(pl.lit(0).alias("matched_codes"))
    return {
        name: int(value or 0)
        for name, value in frame.select(expressions)
        .collect(engine="streaming")
        .row(0, named=True)
        .items()
    }


def _print_enrichment_report(
    report: tuple[dict[str, int], dict[str, int]],
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


def _coverage_value(count: int, total: int) -> str:
    return f"{count:,} ({count / total:.1%})" if total else "0 (0.0%)"


def _code_candidates(frame: pl.LazyFrame) -> pl.LazyFrame:
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
            pl.lit(0, dtype=pl.UInt8).alias("_rank"),
        ),
        codes.filter(pl.col("_parts").list.len() == 3).select(
            "_target_code",
            pl.col("_parts").list.get(0).alias("vocabulary_id"),
            pl.col("_parts").list.get(2).alias("concept_code"),
            pl.lit(1, dtype=pl.UInt8).alias("_rank"),
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
                pl.lit(2, dtype=pl.UInt8).alias("_rank"),
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
    rows = [
        matches.select(
            *_MATCH_COLUMNS[:-3],
            pl.lit(None, dtype=pl.String).alias("_relationship"),
            pl.lit(None, dtype=pl.String).alias("_related_vocabulary_id"),
            pl.lit(None, dtype=pl.String).alias("_related_concept_code"),
        )
    ]
    if config.parent_codes:
        rows.append(
            _relation_rows(
                matches.join(
                    hierarchy,
                    left_on="concept_id",
                    right_on="descendant_concept_id",
                    how="inner",
                ).join(
                    related,
                    left_on="ancestor_concept_id",
                    right_on="_related_concept_id",
                    how="inner",
                ),
                "parent",
            )
        )
    if config.child_codes:
        rows.append(
            _relation_rows(
                matches.join(
                    hierarchy.filter(
                        pl.col("min_levels_of_separation") <= config.child_depth
                    ),
                    left_on="concept_id",
                    right_on="ancestor_concept_id",
                    how="inner",
                ).join(
                    related,
                    left_on="descendant_concept_id",
                    right_on="_related_concept_id",
                    how="inner",
                ),
                "child",
            )
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
        rows.append(
            _relation_rows(
                matches.join(parents, on="concept_id", how="inner")
                .join(children, on="_parent_concept_id", how="inner")
                .filter(pl.col("_related_concept_id") != pl.col("concept_id"))
                .join(related, on="_related_concept_id", how="inner"),
                "sibling",
            )
        )
    return pl.concat(rows)


def _relation_rows(frame: pl.LazyFrame, relationship: str) -> pl.LazyFrame:
    return frame.select(
        *_MATCH_COLUMNS[:-3],
        pl.lit(relationship).alias("_relationship"),
        "_related_vocabulary_id",
        "_related_concept_code",
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
    relationship_queries = []
    if config.parent_codes:
        relationship_queries.append(
            """
SELECT m._target_code, m._rank, m.concept_id,
       'parent'::text AS _relationship,
       related.vocabulary_id AS _related_vocabulary_id,
       related.concept_code AS _related_concept_code
FROM chosen m
JOIN hierarchy h ON h.descendant_concept_id = m.concept_id
JOIN concept related ON related.concept_id = h.ancestor_concept_id
                    AND related.invalid_reason IS NULL
"""
        )
    if config.child_codes:
        relationship_queries.append(
            f"""
SELECT m._target_code, m._rank, m.concept_id,
       'child'::text AS _relationship,
       related.vocabulary_id AS _related_vocabulary_id,
       related.concept_code AS _related_concept_code
FROM chosen m
JOIN hierarchy h ON h.ancestor_concept_id = m.concept_id
                AND h.min_levels_of_separation <= {config.child_depth}
JOIN concept related ON related.concept_id = h.descendant_concept_id
                    AND related.invalid_reason IS NULL
"""
        )
    if config.sibling_codes:
        relationship_queries.append(
            """
SELECT m._target_code, m._rank, m.concept_id,
       'sibling'::text AS _relationship,
       related.vocabulary_id AS _related_vocabulary_id,
       related.concept_code AS _related_concept_code
FROM chosen m
JOIN hierarchy parent_h ON parent_h.descendant_concept_id = m.concept_id
JOIN concept parent ON parent.concept_id = parent_h.ancestor_concept_id
                   AND parent.invalid_reason IS NULL
JOIN hierarchy child_h ON child_h.ancestor_concept_id = parent_h.ancestor_concept_id
                      AND child_h.min_levels_of_separation = 1
JOIN concept related ON related.concept_id = child_h.descendant_concept_id
                    AND related.invalid_reason IS NULL
WHERE related.concept_id <> m.concept_id
"""
        )
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
    collapsed = chosen.group_by(
        "_target_code",
        "_rank",
        "description",
        "vocabulary_id",
        "concept_id",
        "concept_code",
        "domain_id",
        "standard_concept",
    ).agg(
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
