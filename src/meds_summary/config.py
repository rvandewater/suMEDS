"""Configuration for MEDS summaries."""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class SummaryConfig:
    """Options controlling grouping and privacy-aware release.

    ``min_subjects`` is evaluated on unique subjects, not event rows. A value
    of one disables useful rarity protection and should only be used on public
    or synthetic data.
    """

    per_split: bool = False
    min_subjects: int = 20
    rare_code_action: str = "bucket"
    rare_code_label: str = "__RARE__"
    round_counts_to: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.per_split, bool):
            raise ValueError("per_split must be true or false")
        if (
            not isinstance(self.min_subjects, int)
            or isinstance(self.min_subjects, bool)
            or self.min_subjects < 1
        ):
            raise ValueError("min_subjects must be a positive integer")
        if self.rare_code_action not in {"bucket", "drop"}:
            raise ValueError("rare_code_action must be 'bucket' or 'drop'")
        if not self.rare_code_label:
            raise ValueError("rare_code_label must not be empty")
        if self.round_counts_to is not None and (
            not isinstance(self.round_counts_to, int)
            or isinstance(self.round_counts_to, bool)
            or self.round_counts_to < 1
        ):
            raise ValueError("round_counts_to must be null or a positive integer")

    @classmethod
    def from_yaml(cls, path: str | Path) -> SummaryConfig:
        """Load a strict ``summary``/``privacy`` YAML configuration file."""

        source = Path(path)
        raw = yaml.safe_load(source.read_text()) or {}
        if not isinstance(raw, dict):
            raise ValueError(f"YAML root must be a mapping: {source}")
        unknown_sections = set(raw) - {"summary", "privacy"}
        if unknown_sections:
            raise ValueError(f"Unknown YAML sections: {sorted(unknown_sections)}")

        summary = _mapping(raw.get("summary"), "summary")
        privacy = _mapping(raw.get("privacy"), "privacy")
        allowed = {field.name for field in fields(cls)}
        unknown = (set(summary) - {"per_split"}) | (
            set(privacy) - (allowed - {"per_split"})
        )
        if unknown:
            raise ValueError(
                f"Unknown configuration options or wrong sections: {sorted(unknown)}"
            )
        values: dict[str, Any] = {**summary, **privacy}
        return cls(**values)

    def with_overrides(self, **values: Any) -> SummaryConfig:
        """Return a copy with non-``None`` CLI overrides applied."""

        return replace(
            self, **{key: value for key, value in values.items() if value is not None}
        )


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"YAML section '{name}' must be a mapping")
    return value
