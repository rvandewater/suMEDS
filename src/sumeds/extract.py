"""``suMEDS-extract``: run a MEDS-Extract ETL, then enrich its code metadata in place.

    suMEDS-extract spec=NAME output_dir=DIR [meds-extract-run overrides ...] [-- suMEDS-enrich options ...]

1. ``meds-extract-run`` runs with every argument before a ``--``, unchanged.
2. ``suMEDS-enrich`` enriches ``DIR/metadata/codes.parquet`` in place with every argument after
   the ``--``. Without ``--rules``, it uses the rules file a package registered under the spec's
   name (the ``sumeds.rules`` entry-point group; see :func:`sumeds.registered_rules`).

suMEDS does not import MEDS-Extract: it runs the ``meds-extract-run`` found on ``PATH``, which is
the one installed alongside it. The arguments and rules are checked before the pipeline starts,
and the pipeline's exit code is returned if it fails, in which case nothing is enriched.

A failed enrichment does not fail the ETL: ``codes.parquet`` is left as the pipeline wrote it,
the failure is recorded in ``DIR/metadata/dataset.json``, a warning gives the command to enrich
it later (``suMEDS-enrich DIR``), and the exit code is 0. ``-- --strict`` makes it an error.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Sequence

from .cli import build_enrich_parser, enrich_main
from .ontology import load_rules, registered_rules

PROG = "suMEDS-extract"


def _split(argv: list[str]) -> tuple[list[str], list[str]]:
    """Split the arguments at the first ``--`` into pipeline and enrichment arguments."""

    if "--" not in argv:
        return argv, []
    index = argv.index("--")
    return argv[:index], argv[index + 1 :]


def _override(overrides: list[str], key: str) -> str | None:
    """The value of a Hydra ``key=value`` (or ``+key=`` / ``++key=``) override, unquoted."""

    values = [
        item.split("=", 1)[1]
        for item in overrides
        if item.lstrip("+").startswith(f"{key}=")
    ]
    if len(values) > 1:
        raise ValueError(f"{key}= is given {len(values)} times")
    if not values:
        return None
    value = values[0]
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        value = value[1:-1]
    return value


def _has_option(enrich_args: list[str], *names: str) -> bool:
    return any(
        arg == name or arg.startswith(f"{name}=")
        for arg in enrich_args
        for name in names
    )


def build_extract_parser() -> argparse.ArgumentParser:
    """Build the ``suMEDS-extract`` parser (for the options before the ``--``)."""

    parser = argparse.ArgumentParser(
        prog=PROG,
        usage=f"{PROG} spec=NAME output_dir=DIR [meds-extract-run overrides ...] "
        "[-- suMEDS-enrich options ...]",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=version("suMEDS"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run ``meds-extract-run``, then ``suMEDS-enrich`` in place; return an exit code."""

    parser = build_extract_parser()
    extract_args, enrich_args = _split(list(sys.argv[1:] if argv is None else argv))
    _, overrides = parser.parse_known_args(extract_args)
    if options := [item for item in overrides if item.startswith("-")]:
        parser.error(
            f"unrecognized options {options}; suMEDS-enrich options go after a `--`"
        )
    try:
        spec = _override(overrides, "spec")
        output_dir = _override(overrides, "output_dir")
    except ValueError as exc:
        parser.error(str(exc))
    if spec is None or output_dir is None:
        parser.error("spec=... and output_dir=... are required")

    # MEDS-Extract runs with the invoking working directory, so a relative output_dir is
    # relative to it here too.
    codes = Path(output_dir).expanduser().resolve() / "metadata" / "codes.parquet"
    enrich_argv = [str(codes), "-o", str(codes), *enrich_args]
    if not _has_option(enrich_args, "--soft-fail", "--strict"):
        enrich_argv.append("--soft-fail")
    if not _has_option(enrich_args, "--rules"):
        if spec not in registered_rules():
            available = ", ".join(sorted(registered_rules())) or "none"
            parser.error(
                f"no suMEDS rules are registered under spec {spec!r} (registered: "
                f"{available}); pass a rules file after the `--`: -- --rules FILE"
            )
        enrich_argv += ["--rules", spec]

    # Check the enrichment arguments and rules now, not after a long pipeline run.
    enrich_options = build_enrich_parser().parse_args(enrich_argv)
    if enrich_options.rules is not None:
        try:
            load_rules(enrich_options.rules)
        except (OSError, ValueError) as exc:
            parser.error(str(exc))

    meds_extract_run = shutil.which("meds-extract-run")
    if meds_extract_run is None:
        parser.exit(
            1,
            f"{PROG}: error: meds-extract-run is not on PATH; install MEDS-Extract "
            "(pip install MEDS-extract) in this environment\n",
        )
    returncode = subprocess.run([meds_extract_run, *overrides], check=False).returncode
    if returncode != 0:
        print(
            f"{PROG}: meds-extract-run failed with exit code {returncode}; "
            "nothing was enriched",
            file=sys.stderr,
        )
        return returncode
    if not codes.is_file():
        parser.exit(1, f"{PROG}: error: the pipeline wrote no {codes}\n")
    return enrich_main(enrich_argv)


if __name__ == "__main__":
    raise SystemExit(main())
