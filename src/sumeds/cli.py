"""Command-line interface for :mod:`sumeds`."""

from __future__ import annotations

import argparse
from importlib.metadata import version
from pathlib import Path
from typing import Sequence

from .config import SummaryConfig
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
            min_subjects=args.min_subjects,
            min_split_subjects=args.min_split_subjects,
            rare_code_action=args.rare_code_action,
            rare_code_label=args.rare_code_label,
            round_counts_to=args.round_counts_to,
        )
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    try:
        output = summarize(args.dataset_root, args.output, config)
    except Exception as exc:  # CLI boundary: library callers retain typed exceptions.
        parser.exit(1, f"suMEDS: error: {exc}\n")

    print(f"Wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
