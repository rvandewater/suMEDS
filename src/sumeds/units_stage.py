"""The ``sumeds_normalize_units`` stage: one UCUM spelling for the ``unit`` column of a MEDS ETL.

A MEDS-Extract spec declares it after ``sumeds_link_unit_siblings`` (or the ETL's own unit stage)
and before ``sumeds_enrich``::

    etl:
      post_stages:
        - sumeds_link_unit_siblings
        - sumeds_normalize_units
        - sumeds_enrich: {rules: MC-MED}

Each known unit spelling in ``metadata/codes.parquet`` becomes its canonical UCUM code
(:func:`sumeds.units.to_ucum`), an unknown spelling is kept, and ``unit_source_value`` keeps the unit
as it was recorded. Codes are not changed. Without a ``unit`` column the file is left as it is, and
running the stage twice changes nothing. The stage is registered in the ``MEDS_transforms.stages``
entry-point group; without MEDS-Transforms this module imports, but registers nothing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import meds

from .units import normalize_units_file

STAGE_NAME = "sumeds_normalize_units"


def _main(cfg: Any) -> None:
    """Normalise the unit spellings of the cohort's ``metadata/codes.parquet`` (worker 0 only)."""

    if cfg.worker != 0:
        return
    codes = Path(meds.code_metadata_filepath).name
    normalize_units_file(
        Path(cfg.stage_cfg.metadata_input_dir) / codes,
        Path(cfg.stage_cfg.reducer_output_dir) / codes,
    )


try:
    from MEDS_transforms.stages import Stage
except ModuleNotFoundError as error:  # pragma: no cover - depends on the environment
    if error.name != "MEDS_transforms":
        raise
    main = _main
else:
    main = Stage.register(main_fn=_main, stage_name=STAGE_NAME, is_metadata=True)
