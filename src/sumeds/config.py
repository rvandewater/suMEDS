"""Configuration for MEDS summaries."""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class EnrichmentConfig:
    """Select one OHDSI Athena source and hierarchy fields to add."""

    csv_dir: Path | None = None
    postgres: str | None = None
    parent_codes: bool = True
    child_codes: bool = False
    sibling_codes: bool = False
    child_depth: int = 3
    exclude_self_parent_code: bool = True

    def __post_init__(self) -> None:
        if (self.csv_dir is None) == (self.postgres is None):
            raise ValueError(
                "set exactly one of enrichment.csv_dir or enrichment.postgres"
            )
        if self.csv_dir is not None:
            if (
                not isinstance(self.csv_dir, (str, Path))
                or not str(self.csv_dir).strip()
            ):
                raise ValueError("enrichment.csv_dir must be a path")
            object.__setattr__(self, "csv_dir", Path(self.csv_dir))
        if self.postgres is not None and (
            not isinstance(self.postgres, str) or not self.postgres.strip()
        ):
            raise ValueError("enrichment.postgres must be a non-empty string")
        for name in (
            "parent_codes",
            "child_codes",
            "sibling_codes",
            "exclude_self_parent_code",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"enrichment.{name} must be true or false")
        if (
            not isinstance(self.child_depth, int)
            or isinstance(self.child_depth, bool)
            or not 1 <= self.child_depth <= 100
        ):
            raise ValueError("enrichment.child_depth must be an integer from 1 to 100")

    def with_overrides(self, **values: Any) -> EnrichmentConfig:
        """Return a copy with non-``None`` CLI overrides applied."""

        return replace(
            self, **{key: value for key, value in values.items() if value is not None}
        )


@dataclass(frozen=True)
class SummaryConfig:
    """Options controlling grouping and privacy-aware release.

    ``min_subjects`` is evaluated on unique subjects, not event rows. A value
    of one disables useful rarity protection and should only be used on public
    or synthetic data.
    """

    per_split: bool = False
    split_columns: bool = False
    partitions: int = 256
    min_subjects: int = 20
    min_split_subjects: int = 1
    rare_code_action: str = "bucket"
    rare_code_label: str = "__RARE__"
    round_counts_to: int | None = None
    enrichment: EnrichmentConfig | None = None

    def __post_init__(self) -> None:
        if self.enrichment is not None and not isinstance(
            self.enrichment, EnrichmentConfig
        ):
            raise ValueError("enrichment must be an EnrichmentConfig")
        if not isinstance(self.per_split, bool) or not isinstance(
            self.split_columns, bool
        ):
            raise ValueError("per_split and split_columns must be true or false")
        if self.per_split and self.split_columns:
            raise ValueError("per_split and split_columns are mutually exclusive")
        for name in ("partitions", "min_subjects", "min_split_subjects"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
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
        unknown_sections = set(raw) - {"summary", "privacy", "enrichment"}
        if unknown_sections:
            raise ValueError(f"Unknown YAML sections: {sorted(unknown_sections)}")

        summary = _mapping(raw.get("summary"), "summary")
        privacy = _mapping(raw.get("privacy"), "privacy")
        enrichment = _mapping(raw.get("enrichment"), "enrichment")
        summary_options = {"per_split", "split_columns", "partitions"}
        privacy_options = {
            field.name
            for field in fields(cls)
            if field.name not in summary_options | {"enrichment"}
        }
        unknown = (
            (set(summary) - summary_options)
            | (set(privacy) - privacy_options)
            | (
                set(enrichment)
                - {
                    "csv_dir",
                    "postgres",
                    "parent_codes",
                    "child_codes",
                    "sibling_codes",
                    "exclude_self_parent_code",
                    "child_depth",
                }
            )
        )
        if unknown:
            raise ValueError(
                f"Unknown configuration options or wrong sections: {sorted(unknown)}"
            )
        enrichment_config = None
        if enrichment:
            csv_dir = enrichment.get("csv_dir")
            if csv_dir is not None:
                if not isinstance(csv_dir, (str, Path)):
                    raise ValueError("enrichment.csv_dir must be a path")
                path = Path(csv_dir)
                enrichment["csv_dir"] = (
                    path if path.is_absolute() else source.parent / path
                )
            enrichment_config = EnrichmentConfig(**enrichment)
        values: dict[str, Any] = {
            **summary,
            **privacy,
            "enrichment": enrichment_config,
        }
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
