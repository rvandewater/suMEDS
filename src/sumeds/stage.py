"""The ``sumeds_enrich`` stage: suMEDS enrichment as a MEDS-Extract post-stage.

A MEDS-Extract spec (MEDS-Extract 0.8.1 or later) declares it after the canonical pipeline::

    etl:
      post_stages:
        - sumeds_enrich:
            rules: eICU

and ``meds-extract-run`` then enriches the finished ``metadata/codes.parquet`` in place and
records the enrichment in ``metadata/dataset.json``, as ``suMEDS-enrich --soft-fail`` does. The
stage is registered in the ``MEDS_transforms.stages`` entry-point group. suMEDS does not depend on
MEDS-Transforms: without it, this module imports, but registers nothing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import meds

from .cli import STAGE_OPTIONS, enrich_main, stage_argv
from .ontology import registered_rules

STAGE_NAME = "sumeds_enrich"
#: The stage's options and defaults. ``None`` keeps the default of ``suMEDS-enrich``.
DEFAULT_CONFIG: dict[str, Any] = {
    name: (False if name == "strict" else None) for name in STAGE_OPTIONS
}


def _main(cfg: Any) -> None:
    """Enrich the cohort's ``metadata/codes.parquet`` in place with suMEDS.

    The options mirror ``suMEDS-enrich`` (``--athena-csv`` is ``athena_csv``): ``rules`` (a
    registered name or a path; by default the rules registered under the dataset name),
    ``athena_csv``, ``athena_postgres``, ``ontology_source``, ``ontology_ref``, ``offline``,
    ``packs``, ``loinc_hierarchy``, ``parent_codes``, ``child_codes``, ``sibling_codes``,
    ``child_depth``, ``exclude_self_parent_code`` and ``verbose``. The enrichment is recorded in
    ``metadata/dataset.json``. If it fails, ``codes.parquet`` is left as it was, the failure is
    recorded, a warning gives the command to enrich it later (``suMEDS-enrich OUTPUT_DIR``), and
    the stage succeeds; ``strict: true`` makes it fail instead.
    """

    if cfg.worker != 0:
        return
    stage_cfg = cfg.stage_cfg
    options = {name: getattr(stage_cfg, name, None) for name in STAGE_OPTIONS}
    if options["rules"] is None:
        name = cfg.etl_metadata.dataset_name
        athena = (
            options["athena_csv"] is not None or options["athena_postgres"] is not None
        )
        # With an Athena source, enrichment needs no rules; without one, the dataset name
        # is the only choice, and a clear error (soft, by default) if nothing is registered.
        if name in registered_rules() or not athena:
            options["rules"] = name
    codes = Path(meds.code_metadata_filepath)
    argv = stage_argv(
        Path(stage_cfg.metadata_input_dir) / codes.name,
        Path(stage_cfg.reducer_output_dir) / codes.name,
        options,
    )
    returncode = enrich_main(argv)
    if returncode:
        raise SystemExit(returncode)


try:
    from MEDS_transforms.stages import Stage
except ModuleNotFoundError as error:  # pragma: no cover - depends on the environment
    if error.name != "MEDS_transforms":
        raise
    # Nothing loads the stage without MEDS-Transforms; keep it callable for direct use.
    main = _main
else:
    main = Stage.register(
        main_fn=_main,
        stage_name=STAGE_NAME,
        is_metadata=True,
        default_config=DEFAULT_CONFIG,
    )
