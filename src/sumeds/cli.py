"""Command-line interface for :mod:`sumeds`."""

from __future__ import annotations

import argparse
from dataclasses import replace
from importlib.metadata import version
from pathlib import Path
from typing import Sequence

from .config import EnrichmentConfig, SummaryConfig
from .enrich import enrich_file
from .ontology import DEFAULT_REF
from .summary import summarize


def build_parser() -> argparse.ArgumentParser:
    """Build the public CLI parser."""

    parser = argparse.ArgumentParser(
        prog="suMEDS",
        description="Create a lazy, privacy-aware MEDS code occurrence catalog.",
    )
    parser.add_argument("dataset_root", type=Path, help="MEDS dataset root")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        help="output .parquet, .csv, .json, .jsonl, or .ndjson file",
    )
    parser.add_argument("-c", "--config", type=Path, help="YAML configuration file")
    parser.add_argument(
        "--per-split", action=argparse.BooleanOptionalAction, default=None
    )
    parser.add_argument(
        "--split-columns",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="add per-split count columns alongside totals",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        help="temporary subject partitions used to bound aggregation memory",
    )
    parser.add_argument(
        "--min-subjects", type=int, help="minimum unique subjects per released code"
    )
    parser.add_argument(
        "--min-split-subjects",
        type=int,
        help="minimum unique subjects per visible wide split cell",
    )
    parser.add_argument("--rare-code-action", choices=("bucket", "drop"))
    parser.add_argument("--rare-code-label", help="label used by bucket mode")
    parser.add_argument(
        "--round-counts-to", type=int, help="round released counts to this multiple"
    )
    parser.add_argument(
        "--numeric-stats",
        nargs="?",
        const="all",
        metavar="STATS",
        help="summarize numeric_value per code; optional comma list of "
        "min,max,mean,median,std,ci (default: all) or 'none'",
    )
    parser.add_argument(
        "--numeric-ci-level",
        type=float,
        help="confidence level for numeric_ci_* columns (default: 0.95)",
    )
    sources = parser.add_mutually_exclusive_group()
    sources.add_argument(
        "--athena-csv", type=Path, help="enrich from an Athena CSV directory"
    )
    sources.add_argument(
        "--athena-postgres", help="enrich from PostgreSQL using this psql conninfo"
    )
    _add_hierarchy_arguments(parser)
    _add_ontology_arguments(parser)
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print the enrichment coverage and rule report",
    )
    parser.add_argument("--version", action="version", version=version("suMEDS"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return a process exit code."""

    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = (
            SummaryConfig.from_yaml(args.config) if args.config else SummaryConfig()
        )
        config = config.with_overrides(
            per_split=args.per_split,
            split_columns=args.split_columns,
            partitions=args.partitions,
            min_subjects=args.min_subjects,
            min_split_subjects=args.min_split_subjects,
            rare_code_action=args.rare_code_action,
            rare_code_label=args.rare_code_label,
            round_counts_to=args.round_counts_to,
            numeric_stats=args.numeric_stats,
            numeric_ci_level=args.numeric_ci_level,
            enrichment=_enrichment_from_args(args, config.enrichment),
        )
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    try:
        # Only pass verbose when requested, keeping the 3-argument call stable.
        options = {"verbose": True} if args.verbose else {}
        output = summarize(args.dataset_root, args.output, config, **options)
    except Exception as exc:  # CLI boundary: library callers retain typed exceptions.
        parser.exit(1, f"suMEDS: error: {exc}\n")

    print(f"Wrote {output}")
    return 0


def build_enrich_parser() -> argparse.ArgumentParser:
    """Build the standalone enrichment CLI parser."""

    parser = argparse.ArgumentParser(
        prog="suMEDS-enrich",
        description="Enrich MEDS metadata or a suMEDS summary from OHDSI Athena "
        "and/or rules over public ontology packs.",
    )
    parser.add_argument("input", type=Path, help="metadata or summary table")
    parser.add_argument(
        "-o", "--output", type=Path, required=True, help="enriched output table"
    )
    sources = parser.add_mutually_exclusive_group()
    sources.add_argument("--athena-csv", type=Path, help="Athena CSV directory")
    sources.add_argument(
        "--athena-postgres", help="PostgreSQL connection string or conninfo"
    )
    _add_hierarchy_arguments(parser)
    _add_ontology_arguments(parser)
    parser.add_argument(
        "--verbose",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="print the coverage and rule report (default: enabled)",
    )
    parser.add_argument("--version", action="version", version=version("suMEDS"))
    return parser


def enrich_main(argv: Sequence[str] | None = None) -> int:
    """Run standalone Athena enrichment."""

    parser = build_enrich_parser()
    args = parser.parse_args(argv)
    if args.athena_csv is None and args.athena_postgres is None and args.rules is None:
        parser.error(
            "one of the arguments --athena-csv --athena-postgres --rules is required"
        )
    try:
        output = enrich_file(
            args.input,
            args.output,
            _enrichment_from_args(args),
            verbose=args.verbose,
        )
    except Exception as exc:
        parser.exit(1, f"suMEDS-enrich: error: {exc}\n")
    print(f"Wrote {output}")
    return 0


def _add_hierarchy_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--parent-codes",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="add all valid ancestors (default: enabled)",
    )
    parser.add_argument(
        "--child-codes",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="add valid descendants (default: disabled)",
    )
    parser.add_argument(
        "--sibling-codes",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="add valid children of ancestor codes (default: disabled)",
    )
    parser.add_argument(
        "--exclude-self-parent-code",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="remove an existing parent reference used as the Athena lookup; this is the only "
        "enrichment option that deletes existing metadata (default: disabled)",
    )
    parser.add_argument(
        "--child-depth",
        type=int,
        help="maximum descendant depth (default: 3)",
    )


def _add_ontology_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rules",
        type=Path,
        help="sumeds-rules/0.1 YAML with ontology, crosswalk and hierarchy rules",
    )
    parser.add_argument(
        "--ontology-source",
        metavar="URL_OR_DIR",
        help="ontology pack repository: http(s) or file:// URL, or a local "
        "directory (default: $SUMEDS_ONTOLOGY_SOURCE or the SumMEDS GitHub repo)",
    )
    parser.add_argument(
        "--ontology-ref",
        metavar="REF",
        help="release tag of the default GitHub pack repository "
        f"(default: {DEFAULT_REF})",
    )
    parser.add_argument(
        "--offline",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="use only cached or local ontology packs (default: disabled)",
    )
    parser.add_argument(
        "--packs",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="fetch ontology packs and crosswalk tables (default: enabled); with "
        "--no-packs, rules validate codes against Athena only and crosswalk rules "
        "are skipped",
    )
    parser.add_argument(
        "--loinc-hierarchy",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="add the LOINC Parts of the LOINC multiaxial hierarchy pack as parents "
        "of LOINC codes, named in parent_names (default: disabled)",
    )


def _enrichment_from_args(
    args: argparse.Namespace, current: EnrichmentConfig | None = None
) -> EnrichmentConfig | None:
    config = current
    source: dict[str, object] = {}
    if args.athena_csv is not None or args.athena_postgres is not None:
        source = {"csv_dir": args.athena_csv, "postgres": args.athena_postgres}
    if args.rules is not None:
        source["rules"] = args.rules
    if source:
        # A CLI source replaces the YAML source but keeps its hierarchy options.
        config = replace(current, **source) if current else EnrichmentConfig(**source)
    options = {
        "parent_codes": args.parent_codes,
        "child_codes": args.child_codes,
        "sibling_codes": args.sibling_codes,
        "exclude_self_parent_code": args.exclude_self_parent_code,
        "child_depth": args.child_depth,
        "ontology_source": args.ontology_source,
        "ontology_ref": args.ontology_ref,
        "offline": args.offline,
        "packs": args.packs,
        "loinc_hierarchy": args.loinc_hierarchy,
    }
    if config is None:
        if any(value is not None for value in options.values()):
            raise ValueError(
                "hierarchy and ontology options require an Athena enrichment "
                "source or --rules"
            )
        return None
    return config.with_overrides(**options)


if __name__ == "__main__":
    raise SystemExit(main())
