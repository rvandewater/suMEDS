"""Command-line interface for :mod:`sumeds`."""

from __future__ import annotations

import argparse
from importlib.metadata import version
from pathlib import Path
from typing import Sequence

from .config import EnrichmentConfig, SummaryConfig
from .enrich import enrich_file
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
        help="output .parquet, .csv, or .json file",
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
    sources = parser.add_mutually_exclusive_group()
    sources.add_argument(
        "--athena-csv", type=Path, help="enrich from an Athena CSV directory"
    )
    sources.add_argument(
        "--athena-postgres", help="enrich from PostgreSQL using this psql conninfo"
    )
    _add_hierarchy_arguments(parser)
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
            enrichment=_enrichment_from_args(args, config.enrichment),
        )
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    try:
        output = summarize(args.dataset_root, args.output, config)
    except Exception as exc:  # CLI boundary: library callers retain typed exceptions.
        parser.exit(1, f"suMEDS: error: {exc}\n")

    print(f"Wrote {output}")
    return 0


def build_enrich_parser() -> argparse.ArgumentParser:
    """Build the standalone enrichment CLI parser."""

    parser = argparse.ArgumentParser(
        prog="suMEDS-enrich",
        description="Enrich MEDS metadata or a suMEDS summary from OHDSI Athena.",
    )
    parser.add_argument("input", type=Path, help="metadata or summary table")
    parser.add_argument(
        "-o", "--output", type=Path, required=True, help="enriched output table"
    )
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--athena-csv", type=Path, help="Athena CSV directory")
    sources.add_argument(
        "--athena-postgres", help="PostgreSQL connection string or conninfo"
    )
    _add_hierarchy_arguments(parser)
    parser.add_argument("--version", action="version", version=version("suMEDS"))
    return parser


def enrich_main(argv: Sequence[str] | None = None) -> int:
    """Run standalone Athena enrichment."""

    parser = build_enrich_parser()
    args = parser.parse_args(argv)
    try:
        output = enrich_file(
            args.input,
            args.output,
            _enrichment_from_args(args),
            verbose=True,
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
        "--child-depth",
        type=int,
        help="maximum descendant depth (default: 3)",
    )


def _enrichment_from_args(
    args: argparse.Namespace, current: EnrichmentConfig | None = None
) -> EnrichmentConfig | None:
    if args.athena_csv is not None:
        config = EnrichmentConfig(csv_dir=args.athena_csv)
    elif args.athena_postgres is not None:
        config = EnrichmentConfig(postgres=args.athena_postgres)
    else:
        config = current
    options = {
        "parent_codes": args.parent_codes,
        "child_codes": args.child_codes,
        "sibling_codes": args.sibling_codes,
        "child_depth": args.child_depth,
    }
    if config is None:
        if any(value is not None for value in options.values()):
            raise ValueError("hierarchy options require an Athena enrichment source")
        return None
    return config.with_overrides(**options)


if __name__ == "__main__":
    raise SystemExit(main())
