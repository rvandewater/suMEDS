"""The ``sumeds_link_unit_siblings`` stage: unit siblings linked in a MEDS ETL's code metadata.

A MEDS-Extract spec declares it among its post-stages, before ``sumeds_normalize_units`` and
``sumeds_enrich`` (it also works after ``sumeds_normalize_units``: it then compares a code's last
component with ``unit_source_value``)::

    etl:
      post_stages:
        - sumeds_link_unit_siblings
        - sumeds_normalize_units
        - sumeds_enrich: {rules: MIMIC-IV}

Options are stage keys, as for ``sumeds_enrich``::

        - sumeds_link_unit_siblings:
            unknown_unit_components: [UNK]
            link_unknown_units: true
            link_single_units: false
            stem_description: without_unit
            description_unit_pattern: '\\s[(\\[]([^()\\[\\]]+)[)\\]]\\s*$'
            description_units: [cm, kg, mL]
            fill_units: unknown_component

Codes sharing a stem with one another (``LAB//<item>//mg/dL``, ``LAB//<item>//mmol/L``) get the stem
as a parent, and the stem a row of its own (:mod:`sumeds.unit_siblings`); with
``link_single_units``, so does a code ending in its unit without such a sibling. Without a ``unit`` column
the file is left as it is, and running the stage twice changes nothing. The stage is registered in
the ``MEDS_transforms.stages`` entry-point group; without MEDS-Transforms this module imports, but
registers nothing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import meds

from .unit_siblings import DEFAULT_UNKNOWN_UNIT_COMPONENTS, link_unit_siblings_file

STAGE_NAME = "sumeds_link_unit_siblings"
#: The stage's options and defaults (those of :func:`sumeds.unit_siblings.link_unit_siblings`).
DEFAULT_CONFIG: dict[str, Any] = {
    "unknown_unit_components": list(DEFAULT_UNKNOWN_UNIT_COMPONENTS),
    "link_unknown_units": True,
    "link_single_units": False,
    "stem_description": "without_unit",
    "description_unit_pattern": None,
    "description_units": None,
    "fill_units": "missing",
}


def stage_options(stage_cfg: Any) -> dict[str, Any]:
    """The options of :data:`DEFAULT_CONFIG` from a stage config, as plain Python values.

    >>> from types import SimpleNamespace
    >>> stage_options(SimpleNamespace(unknown_unit_components=("UNK", "NA"), fill_units="never"))
    ... # doctest: +NORMALIZE_WHITESPACE
    {'unknown_unit_components': ['UNK', 'NA'], 'link_unknown_units': True,
     'link_single_units': False, 'stem_description': 'without_unit',
     'description_unit_pattern': None, 'description_units': None, 'fill_units': 'never'}
    """

    options = {}
    for name, default in DEFAULT_CONFIG.items():
        value = getattr(stage_cfg, name, default)
        if (
            name in ("unknown_unit_components", "description_units")
            and value is not None
        ):
            value = [str(v) for v in value]
        options[name] = value
    return options


def _main(cfg: Any) -> None:
    """Link the unit siblings of the cohort's ``metadata/codes.parquet`` (worker 0 only)."""

    if cfg.worker != 0:
        return
    codes = Path(meds.code_metadata_filepath).name
    link_unit_siblings_file(
        Path(cfg.stage_cfg.metadata_input_dir) / codes,
        Path(cfg.stage_cfg.reducer_output_dir) / codes,
        **stage_options(cfg.stage_cfg),
    )


try:
    from MEDS_transforms.stages import Stage
except ModuleNotFoundError as error:  # pragma: no cover - depends on the environment
    if error.name != "MEDS_transforms":
        raise
    main = _main
else:
    main = Stage.register(
        main_fn=_main,
        stage_name=STAGE_NAME,
        is_metadata=True,
        default_config=DEFAULT_CONFIG,
    )
