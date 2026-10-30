"""Configuration for MEDS summaries."""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any, Mapping, TypeVar

import yaml

from .ontology import DEFAULT_REF, OntologyRules, load_rules

NUMERIC_STATS = ("min", "max", "mean", "median", "std", "ci")
_NUMERIC_ALIASES = {"avg": "mean"}
_Config = TypeVar("_Config")


def _with_overrides(self: _Config, **values: Any) -> _Config:
    """Return a copy with non-``None`` CLI overrides applied."""

    return replace(
        self, **{key: value for key, value in values.items() if value is not None}
    )


@dataclass(frozen=True)
class EnrichmentConfig:
    """Select enrichment sources and the hierarchy fields to add.

    Set at most one OHDSI Athena source (``csv_dir`` or ``postgres``) and/or a
    ``rules`` file (path, mapping, or :class:`OntologyRules`). Without Athena,
    rules use the public ontology packs only.
    """

    csv_dir: Path | None = None
    postgres: str | None = None
    parent_codes: bool = True
    child_codes: bool = False
    sibling_codes: bool = False
    child_depth: int = 3
    exclude_self_parent_code: bool = False
    rules: OntologyRules | None = None
    ontology_source: str | None = None
    ontology_ref: str = DEFAULT_REF
    offline: bool = False

    def __post_init__(self) -> None:
        if self.csv_dir is not None and self.postgres is not None:
            raise ValueError(
                "set exactly one of enrichment.csv_dir or enrichment.postgres"
            )
        if isinstance(self.rules, (str, Path, Mapping)):
            object.__setattr__(self, "rules", load_rules(self.rules))
        elif self.rules is not None and not isinstance(self.rules, OntologyRules):
            raise ValueError("enrichment.rules must be a path to a rules file")
        if self.csv_dir is None and self.postgres is None and self.rules is None:
            raise ValueError(
                "set exactly one of enrichment.csv_dir or enrichment.postgres, "
                "or enrichment.rules to use the public ontology packs only"
            )
        if isinstance(self.ontology_source, Path):
            object.__setattr__(self, "ontology_source", str(self.ontology_source))
        if self.ontology_source is not None and (
            not isinstance(self.ontology_source, str)
            or not self.ontology_source.strip()
        ):
            raise ValueError("enrichment.ontology_source must be a URL or directory")
        if (
            not isinstance(self.ontology_ref, str)
            or not self.ontology_ref
            or any(character.isspace() for character in self.ontology_ref)
        ):
            raise ValueError("enrichment.ontology_ref must be a git ref")
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
            "offline",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"enrichment.{name} must be true or false")
        if (
            not isinstance(self.child_depth, int)
            or isinstance(self.child_depth, bool)
            or not 1 <= self.child_depth <= 100
        ):
            raise ValueError("enrichment.child_depth must be an integer from 1 to 100")

    with_overrides = _with_overrides


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
    numeric_stats: tuple[str, ...] = ()
    numeric_ci_level: float = 0.95
    enrichment: EnrichmentConfig | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "numeric_stats", _numeric_stats(self.numeric_stats))
        if (
            not isinstance(self.numeric_ci_level, (int, float))
            or isinstance(self.numeric_ci_level, bool)
            or not 0 < self.numeric_ci_level < 1
        ):
            raise ValueError("numeric_ci_level must be between 0 and 1")
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
        summary_options = {
            "per_split",
            "split_columns",
            "partitions",
            "numeric_stats",
            "numeric_ci_level",
        }
        privacy_options = {
            field.name
            for field in fields(cls)
            if field.name not in summary_options | {"enrichment"}
        }
        unknown = (
            (set(summary) - summary_options)
            | (set(privacy) - privacy_options)
            | (set(enrichment) - {field.name for field in fields(EnrichmentConfig)})
        )
        if unknown:
            raise ValueError(
                f"Unknown configuration options or wrong sections: {sorted(unknown)}"
            )
        enrichment_config = None
        if enrichment:
            # Relative paths are resolved against the YAML file; invalid values
            # are left for EnrichmentConfig to reject.
            for name in ("csv_dir", "rules", "ontology_source"):
                value = enrichment.get(name)
                if isinstance(value, str) and value.strip() and "://" not in value:
                    enrichment[name] = source.parent / Path(value).expanduser()
            enrichment_config = EnrichmentConfig(**enrichment)
        values: dict[str, Any] = {
            **summary,
            **privacy,
            "enrichment": enrichment_config,
        }
        return cls(**values)

    with_overrides = _with_overrides


def _numeric_stats(value: Any) -> tuple[str, ...]:
    """Normalize ``true``/``false``/``None``, a comma string, or a sequence."""

    if value is None or value is False:
        return ()
    if value is True:
        return NUMERIC_STATS
    if isinstance(value, str):
        value = [item for item in value.split(",") if item.strip()]
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(item, str) for item in value
    ):
        raise ValueError("numeric_stats must be true, false, or a list of names")
    names = [_NUMERIC_ALIASES.get(item.strip(), item.strip()) for item in value]
    if names in (["all"], ["default"]):
        return NUMERIC_STATS
    if names == ["none"]:
        return ()
    unknown = set(names) - set(NUMERIC_STATS)
    if unknown:
        raise ValueError(
            f"Unknown numeric_stats {sorted(unknown)}; choose from {list(NUMERIC_STATS)}"
        )
    # Canonical order keeps output columns stable regardless of input order.
    return tuple(name for name in NUMERIC_STATS if name in names)


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"YAML section '{name}' must be a mapping")
    return value
