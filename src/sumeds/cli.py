"""Command-line interface for :mod:`sumeds`."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import sys
import urllib.parse
import urllib.request
from dataclasses import fields, replace
from importlib.metadata import version
from pathlib import Path
from typing import Any, Sequence

import meds

from .config import EnrichmentConfig, SummaryConfig
from .enrich import (
    DATASET_RECORD_KEY,
    RECORD_OPTIONS,
    enrich_file,
    provenance,
    record_failure,
)
from .ontology import (
    DATA_REPOSITORY,
    DEFAULT_REF,
    SOURCE_ENV,
    CrosswalkRule,
    OntologyPacks,
    load_rules,
    registered_rules,
    registered_rules_path,
)
from .summary import summarize

ENRICH_PROG = "suMEDS-enrich"
#: Options of the ``sumeds_enrich`` MEDS-Extract stage: the ``suMEDS-enrich`` options, with
#: underscores, by type, plus ``strict``. :func:`stage_argv` turns them into arguments.
STAGE_OPTIONS: dict[str, type] = {
    "rules": str,
    "athena_csv": str,
    "athena_postgres": str,
    "ontology_source": str,
    "ontology_ref": str,
    "offline": bool,
    "packs": bool,
    "loinc_hierarchy": bool,
    "parent_codes": bool,
    "child_codes": bool,
    "sibling_codes": bool,
    "child_depth": int,
    "exclude_self_parent_code": bool,
    "verbose": bool,
    "strict": bool,
}


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
        prog=ENRICH_PROG,
        description="Enrich MEDS metadata or a suMEDS summary from OHDSI Athena "
        "and/or rules over public ontology packs. Given a MEDS dataset root, enrich "
        "its metadata/codes.parquet in place, with the rules and options recorded in "
        "its metadata/dataset.json unless options override them.",
    )
    parser.add_argument(
        "input",
        type=Path,
        nargs="?",
        help="metadata or summary table, or a MEDS dataset root",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="enriched output table, required for a table; may be the input, to "
        "enrich it in place. A dataset root is always enriched in place",
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
    failure = parser.add_mutually_exclusive_group()
    failure.add_argument(
        "--soft-fail",
        dest="soft_fail",
        action="store_true",
        default=None,
        help="for ETL runs: if enrichment fails, leave the table as it was, warn "
        "with the command to retry, record the failure in dataset.json, and exit 0",
    )
    failure.add_argument(
        "--strict",
        dest="soft_fail",
        action="store_false",
        default=None,
        help="exit with an error if enrichment fails (the default here; "
        "suMEDS-extract soft-fails unless given this)",
    )
    parser.add_argument(
        "--prefetch",
        action="store_true",
        help="instead of enriching, download every pack, hierarchy and crosswalk table "
        "of the ontology source (--ontology-source, --ontology-ref) into the cache, for "
        "later --offline runs on machines without internet access; with --rules, also "
        "check that the source has everything the rules use",
    )
    parser.add_argument("--version", action="version", version=version("suMEDS"))
    return parser


def enrich_main(argv: Sequence[str] | None = None) -> int:
    """Run standalone enrichment; return an exit code.

    With ``--soft-fail``, as ETLs run it, a failed enrichment leaves the table as it
    was, prints a warning with the command to retry, records the failure in the
    MEDS ``dataset.json`` beside the table, and returns 0.
    """

    parser = build_enrich_parser()
    args = parser.parse_args(argv)
    if args.prefetch:
        return _prefetch(parser, args)
    if args.input is None:
        parser.error("the following arguments are required: input")
    hint = ""
    if args.input.is_dir():
        if args.output is not None:
            parser.error(
                "a MEDS dataset root is enriched in place; -o/--output is only for a table"
            )
        codes = args.input / meds.code_metadata_filepath
        if not codes.is_file():
            parser.error(
                f"{args.input} is a directory, but not a MEDS dataset root with a "
                f"{meds.code_metadata_filepath}"
            )
        args.input = args.output = codes
        try:
            if not _apply_record(args, codes):
                hint = (
                    f" (its dataset.json has no {DATASET_RECORD_KEY} record to reuse)"
                )
        except ValueError as exc:
            parser.error(str(exc))
    elif args.output is None:
        parser.error("the following arguments are required: -o/--output")
    if args.athena_csv is None and args.athena_postgres is None and args.rules is None:
        parser.error(
            "one of the arguments --athena-csv --athena-postgres --rules is required"
            + hint
        )
    try:
        output = enrich_file(
            args.input,
            args.output,
            _enrichment_from_args(args),
            verbose=args.verbose,
        )
    except Exception as exc:
        if not args.soft_fail:
            parser.exit(1, f"{ENRICH_PROG}: error: {exc}\n")
        _soft_fail(args, exc)
        return 0
    print(f"Wrote {output}")
    return 0


def _prefetch(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    """Fill the pack cache with the whole ontology source; check the rules against it."""

    if args.input is not None or args.output is not None:
        parser.error(
            "--prefetch takes no table, dataset root or -o; it only fills the cache"
        )
    if args.athena_csv is not None or args.athena_postgres is not None:
        parser.error("--prefetch fetches ontology packs only; Athena needs no prefetch")
    try:
        packs = OntologyPacks(
            args.ontology_source,
            args.ontology_ref or DEFAULT_REF,
            offline=bool(args.offline),
        )
        totals = packs.prefetch()
        if args.rules is not None:
            available = set(packs.vocabularies())
            for rule in load_rules(args.rules).vocabulary_rules:
                if isinstance(rule, CrosswalkRule):
                    # Cached by now; raises if the source has no such table.
                    packs.crosswalk(rule.table)
                if rule.validate == "pack" and rule.vocabulary not in available:
                    raise ValueError(
                        f"rule {rule.id!r} validates against the {rule.vocabulary} pack, "
                        f"which {packs.describe()} does not have"
                    )
    except Exception as exc:
        parser.exit(1, f"{ENRICH_PROG}: error: {exc}\n")
    if packs.local is not None:
        print(
            f"Checked {totals['files']:,} pack, hierarchy and crosswalk files of the "
            f"local source {packs.local}; a local source needs no cache"
        )
    else:
        print(
            f"Prefetched {packs.describe()}: {totals['files']:,} pack, hierarchy and "
            f"crosswalk files, {totals['downloaded']:,} downloaded "
            f"({totals['bytes']:,} bytes), cached in {packs.cache}"
        )
    return 0


def stage_argv(
    input_path: str | Path, output_path: str | Path, options: dict[str, Any]
) -> list[str]:
    """``suMEDS-enrich`` arguments for the ``sumeds_enrich`` stage's ``options``.

    Each option is a :data:`STAGE_OPTIONS` name; ``None`` leaves it at its default. A
    boolean ``x_y`` becomes ``--x-y`` or ``--no-x-y``. ``strict`` (default false) chooses
    ``--strict`` over ``--soft-fail``.
    """

    argv = [str(input_path), "-o", str(output_path)]
    for name, kind in STAGE_OPTIONS.items():
        value = options.get(name)
        flag = "--" + name.replace("_", "-")
        if name == "strict":
            value = False if value is None else value
        if value is None:
            continue
        if kind is bool:
            if not isinstance(value, bool):
                raise ValueError(
                    f"option {name!r} must be true or false, got {value!r}"
                )
            if name == "strict":
                argv.append("--strict" if value else "--soft-fail")
            else:
                argv.append(flag if value else "--no-" + flag[2:])
        elif kind is int:
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"option {name!r} must be an integer, got {value!r}")
            argv += [flag, str(value)]
        else:
            if not isinstance(value, (str, Path)) or not str(value).strip():
                raise ValueError(f"option {name!r} must be a non-empty string")
            argv += [flag, str(value)]
    return argv


def _apply_record(args: argparse.Namespace, codes: Path) -> bool:
    """Fill the options not given from the dataset's enrichment record, if it has one.

    Paths are never recorded, so an Athena download, a rules file that is not
    registered, or a local pack source must be given again. Returns whether a record
    was found.
    """

    path = codes.with_name(Path(meds.dataset_metadata_filepath).name)
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    record = metadata.get(DATASET_RECORD_KEY) if isinstance(metadata, dict) else None
    if not isinstance(record, dict):
        return False
    where = f"the {DATASET_RECORD_KEY} record of {path}"
    options = record.get("options") or {}
    if not isinstance(options, dict):
        raise ValueError(f"{where} has invalid options")
    for name in RECORD_OPTIONS:
        if getattr(args, name) is None and name in options:
            setattr(args, name, options[name])

    rules = record.get("rules")
    if args.rules is None and rules is not None:
        if not isinstance(rules, str) or rules not in registered_rules():
            raise ValueError(
                f"{where} names the rules {rules!r}, which no installed package "
                "registers; pass the rules file with --rules FILE"
            )
        args.rules = Path(rules)  # as the CLI passes a name
    if (
        record.get("athena") is True
        and args.athena_csv is None
        and args.athena_postgres is None
    ):
        raise ValueError(
            f"{where} used an Athena download, whose location is not recorded; pass "
            "--athena-csv DIR or --athena-postgres CONNINFO, or enrich the table "
            "itself (INPUT -o OUTPUT) to choose the sources anew"
        )
    source = record.get("ontology_source")
    if (
        isinstance(source, str)
        and args.ontology_source is None
        and not os.environ.get(SOURCE_ENV)
    ):
        if source.startswith(f"{DATA_REPOSITORY}@"):
            if args.ontology_ref is None:
                args.ontology_ref = source.partition("@")[2]
        elif source.startswith(("http://", "https://")):
            args.ontology_source = source
        elif args.packs is not False:
            raise ValueError(
                f"{where} used a local ontology pack source, whose location is not "
                "recorded; pass --ontology-source DIR"
            )
    return True


def _soft_fail(args: argparse.Namespace, error: Exception) -> None:
    """Record the failure beside the table and print a warning with the retry command."""

    output = args.output.expanduser().resolve()
    fields_ = _provenance_from_args(args)
    # What the record cannot hold: (flag, value for the warning, placeholder).
    needs = []
    if args.rules is not None and fields_["rules"] not in registered_rules():
        needs.append(("--rules", str(args.rules), "<RULES_FILE>"))
    if args.athena_csv is not None:
        needs.append(("--athena-csv", str(args.athena_csv), "<ATHENA_DIR>"))
    if args.athena_postgres is not None:
        needs.append(("--athena-postgres", "<CONNINFO>", "<CONNINFO>"))
    if fields_["ontology_source"] == "local" and not os.environ.get(SOURCE_ENV):
        needs.append(("--ontology-source", str(args.ontology_source), "<PACKS_DIR>"))
    # The error in the record names none of those paths, nor the connection.
    redact: dict[str, str] = {}

    def hide(value: str | Path, placeholder: str) -> None:
        path = Path(value).expanduser()
        for form in (str(value), str(path), str(path.resolve())):
            redact[form] = placeholder

    if needs and needs[0][0] == "--rules" and len(args.rules.parts) > 1:
        hide(args.rules, "<RULES_FILE>")  # a bare name is no path
    if args.athena_csv is not None:
        hide(args.athena_csv, "<ATHENA_DIR>")
    if args.athena_postgres is not None:
        redact[args.athena_postgres] = "<CONNINFO>"
    source = args.ontology_source or os.environ.get(SOURCE_ENV)
    if fields_["ontology_source"] == "local" and source:
        parsed = urllib.parse.urlsplit(source)
        hide(
            urllib.request.url2pathname(parsed.path)
            if parsed.scheme == "file"
            else source,
            "<PACKS_DIR>",
        )
    written = record_failure(
        output,
        fields_,
        error,
        " ".join([ENRICH_PROG, "<MEDS_ROOT>", *(f"{f} {p}" for f, _, p in needs)]),
        redact=redact,
    )
    # A MEDS dataset's codes are enriched again through its root, with the recorded options.
    root = output.parent.parent
    if (
        output == (root / meds.code_metadata_filepath)
        and (root / meds.dataset_metadata_filepath).is_file()
    ):
        retry = [ENRICH_PROG, str(root)]
    else:
        retry = [ENRICH_PROG, str(args.input), "-o", str(args.output)]
    retry += [item for flag, value, _ in needs for item in (flag, value)]
    note = (
        f"; recorded as not enriched in {root / meds.dataset_metadata_filepath}"
        if written
        else ""
    )
    print(
        f"{ENRICH_PROG}: warning: enrichment failed and {output} was left as it "
        f"was{note}: {error}\n"
        f"{ENRICH_PROG}: warning: once the cause is fixed, enrich it with:\n"
        f"    {shlex.join(retry)}",
        file=sys.stderr,
    )


def _provenance_from_args(args: argparse.Namespace) -> dict[str, Any]:
    """The record fields of the enrichment ``args`` ask for, valid or not."""

    defaults = {item.name: item.default for item in fields(EnrichmentConfig)}
    rules = sha256 = None
    if args.rules is not None:
        name = str(args.rules)
        registered = name in registered_rules()
        rules = name if registered else args.rules.name
        try:
            path = registered_rules_path(name) if registered else args.rules
            sha256 = hashlib.sha256(Path(path).expanduser().read_bytes()).hexdigest()
        except (OSError, ValueError):
            pass
    return provenance(
        rules,
        sha256,
        athena=args.athena_csv is not None or args.athena_postgres is not None,
        ontology_source=args.ontology_source,
        ontology_ref=args.ontology_ref or DEFAULT_REF,
        options={
            name: defaults[name] if getattr(args, name) is None else getattr(args, name)
            for name in RECORD_OPTIONS
        },
    )


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
