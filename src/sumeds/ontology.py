"""Regex rules and public ontology packs for metadata enrichment.

A ``sumeds-rules/0.1`` YAML file maps dataset codes to external vocabulary codes
(``ontology`` rules by regex, ``crosswalk`` rules through a hosted table) and adds
parent codes that exist in the same table (``hierarchy`` rules). Vocabularies
that may be redistributed are validated against sharded ontology packs; packs and crosswalk
tables are fetched on demand from the separately versioned data repository
``rvandewater/sumeds-ontologies`` at a pinned release tag (or a mirror or local
copy), verified against the sha256 checksums of their manifest, and cached.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import warnings
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import yaml

RULES_SCHEMA = "sumeds-rules/0.1"
INDEX_SCHEMA = "sumeds-ontologies/0.1"
PACK_SCHEMA = "sumeds-ontology-pack/0.1"
CROSSWALK_SCHEMA = "sumeds-crosswalk/0.1"
DATA_REPOSITORY = "rvandewater/sumeds-ontologies"
DEFAULT_REF = "v0.1.0"
DEFAULT_SOURCE = (
    "https://raw.githubusercontent.com/" + DATA_REPOSITORY + "/{ref}/ontologies"
)
# sha256 of ``index.json`` at DEFAULT_REF. The index pins every manifest and each
# manifest every shard, so this digest pins all default content: a moved tag
# fails loudly instead of silently changing enrichment results.
DEFAULT_INDEX_SHA256 = (
    "d264e222ae07473fc0f60cbcea059e385e3ef61a2567060f19be4b26d139d695"
)
_RELEASE_REF = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+$|^[0-9a-f]{40}$")
SOURCE_ENV = "SUMEDS_ONTOLOGY_SOURCE"
VALIDATE_MODES = ("pack", "athena", "none")
MAX_ONTOLOGY_RULES = 200

_ONTOLOGY_KEYS = {"id", "pattern", "vocabulary", "normalize", "validate"}
_CROSSWALK_KEYS = {
    "id",
    "pattern",
    "table",
    "key_column",
    "code_column",
    "vocabulary",
    "where",
    "normalize",
    "validate",
}
_HIERARCHY_KEYS = {
    "id",
    "pattern",
    "group",
    "mode",
    "separator",
    "ignore",
    "nearest_only",
    "from_ontology",
    "rules",
}
#: Pack columns holding the code, its description, and its parent; a manifest's
#: ``fields`` overrides them (a LOINC pack keeps the LOINC Table's field names).
_DEFAULT_FIELDS = {"code": "code", "description": "description", "parent": "parent"}
_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_TEMPLATE_REFERENCE = re.compile(r"\\(?:g<([^>]*)>|(\d{1,2}))")


class OntologyPackError(RuntimeError):
    """An ontology pack could not be fetched, verified, or found."""


@dataclass(frozen=True)
class Normalization:
    """One ordered ``re.sub`` step turning a matched code into vocabulary format."""

    pattern: re.Pattern[str]
    replace: str


@dataclass(frozen=True)
class OntologyRule:
    """Tag codes matching ``pattern`` with a ``vocabulary`` code."""

    id: str
    pattern: re.Pattern[str]
    vocabulary: str
    normalize: tuple[Normalization, ...] = ()
    validate: str = "pack"

    def resolve(self, code: str) -> str | None:
        """Return the normalized vocabulary code for ``code``, or ``None``."""

        match = self.pattern.search(code)
        if match is None or match.group("code") is None:
            return None
        return _normalize(match.group("code"), self.normalize)


@dataclass(frozen=True)
class CrosswalkRule:
    """Tag codes whose ``key`` group is found in a hosted crosswalk table."""

    id: str
    pattern: re.Pattern[str]
    table: str
    key_column: str
    code_column: str
    vocabulary: str
    where: tuple[tuple[str, tuple[str, ...]], ...] = ()
    normalize: tuple[Normalization, ...] = ()
    validate: str = "pack"

    def key(self, code: str) -> str | None:
        """Return the crosswalk key of ``code``, or ``None``."""

        match = self.pattern.search(code)
        return None if match is None else match.group("key")

    def mapping(self, rows: list[dict[str, str]]) -> dict[str, tuple[str, ...]]:
        """Map each key of the table rows to its sorted vocabulary codes."""

        columns = set(rows[0]) if rows else set()
        needed = {self.key_column, self.code_column, *(name for name, _ in self.where)}
        if rows and not needed <= columns:
            raise ValueError(
                f"crosswalk rule {self.id!r}: table {self.table!r} lacks columns "
                f"{sorted(needed - columns)}"
            )
        mapping: dict[str, set[str]] = {}
        for row in rows:
            if all(row[name] in values for name, values in self.where):
                value = _normalize(row[self.code_column] or "", self.normalize)
                if row[self.key_column] and value:
                    mapping.setdefault(row[self.key_column], set()).add(value)
        return {key: tuple(sorted(values)) for key, values in mapping.items()}


def _normalize(value: str, steps: tuple[Normalization, ...]) -> str | None:
    for step in steps:
        value = step.pattern.sub(step.replace, value)
    return value or None


@dataclass(frozen=True)
class HierarchyRule:
    """Add parent codes that exist in the same table.

    ``mode`` is ``separator`` (drop trailing ``separator``-delimited segments of
    ``group``), ``prefix`` (drop trailing characters of ``group``, skipping
    ``ignore`` characters), or ``ontology`` (codes whose resolved vocabulary code
    is an ancestor of this code's vocabulary code).
    """

    id: str
    mode: str
    pattern: re.Pattern[str] | None = None
    group: str | None = None
    separator: str | None = None
    ignore: str = ""
    nearest_only: bool = True
    rules: tuple[str, ...] = ()

    def candidates(self, code: str) -> list[str]:
        """Return possible parents of ``code``, nearest first (string rules)."""

        if self.pattern is None or self.group is None:
            return []
        match = self.pattern.search(code)
        if match is None or match.group(self.group) is None:
            return []
        start, end = match.span(self.group)
        value = match.group(self.group)
        if self.mode == "separator":
            parts = value.split(self.separator)
            stems = [
                self.separator.join(parts[:length])
                for length in range(len(parts) - 1, 0, -1)
            ]
        else:
            stems = []
            stem = value.rstrip(self.ignore) if self.ignore else value
            while len(stem) > 1:
                stem = stem[:-1]
                if self.ignore:
                    stem = stem.rstrip(self.ignore)
                if stem:
                    stems.append(stem)
        return [code[:start] + stem + code[end:] for stem in stems]


@dataclass(frozen=True)
class OntologyRules:
    """Parsed ``sumeds-rules/0.1`` file."""

    ontology: tuple[OntologyRule, ...] = ()
    hierarchy: tuple[HierarchyRule, ...] = ()
    crosswalk: tuple[CrosswalkRule, ...] = ()

    def __post_init__(self) -> None:
        if len(self.ontology) + len(self.crosswalk) > MAX_ONTOLOGY_RULES:
            raise ValueError(
                f"at most {MAX_ONTOLOGY_RULES} ontology and crosswalk rules are supported"
            )

    @property
    def vocabulary_rules(self) -> tuple[OntologyRule | CrosswalkRule, ...]:
        """Ontology then crosswalk rules, in precedence order."""

        return (*self.ontology, *self.crosswalk)


def load_rules(source: str | Path | Mapping[str, Any]) -> OntologyRules:
    """Load and strictly validate a ``sumeds-rules/0.1`` YAML file or mapping."""

    if isinstance(source, Mapping):
        raw: Any = dict(source)
        where = "rules"
    else:
        path = Path(source).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"rules file not found: {path}")
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        where = str(path)
    if not isinstance(raw, dict):
        raise ValueError(f"{where}: the rules file must be a mapping")
    unknown = set(raw) - {"schema", "ontology", "crosswalk", "hierarchy"}
    if unknown:
        raise ValueError(f"{where}: unknown rules keys {sorted(unknown)}")
    if raw.get("schema") != RULES_SCHEMA:
        raise ValueError(
            f"{where}: 'schema' must be {RULES_SCHEMA!r}, got {raw.get('schema')!r}"
        )
    seen: set[str] = set()
    ontology = tuple(
        _ontology_rule(item, f"{where}: ontology[{index}]", seen)
        for index, item in enumerate(_list(raw.get("ontology"), f"{where}: ontology"))
    )
    crosswalk = tuple(
        _crosswalk_rule(item, f"{where}: crosswalk[{index}]", seen)
        for index, item in enumerate(_list(raw.get("crosswalk"), f"{where}: crosswalk"))
    )
    names = {rule.id for rule in (*ontology, *crosswalk)}
    hierarchy = tuple(
        _hierarchy_rule(item, f"{where}: hierarchy[{index}]", seen, names)
        for index, item in enumerate(_list(raw.get("hierarchy"), f"{where}: hierarchy"))
    )
    return OntologyRules(ontology, hierarchy, crosswalk)


def _list(value: Any, where: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{where} must be a list")
    return value


def _rule_id(item: dict, where: str, seen: set[str]) -> str:
    rule_id = item.get("id")
    if not isinstance(rule_id, str) or not rule_id.strip():
        raise ValueError(f"{where}: 'id' must be a non-empty string")
    if rule_id in seen:
        raise ValueError(f"{where}: duplicate rule id {rule_id!r}")
    seen.add(rule_id)
    return rule_id


def _compile(value: Any, where: str) -> re.Pattern[str]:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{where} must be a non-empty regular expression")
    try:
        return re.compile(value)
    except re.error as error:
        raise ValueError(
            f"{where} is not a valid regular expression: {error}"
        ) from None


def _ontology_rule(item: Any, where: str, seen: set[str]) -> OntologyRule:
    if not isinstance(item, dict):
        raise ValueError(f"{where} must be a mapping")
    unknown = set(item) - _ONTOLOGY_KEYS
    if unknown:
        raise ValueError(f"{where}: unknown keys {sorted(unknown)}")
    rule_id = _rule_id(item, where, seen)
    where = f"{where} ({rule_id})"
    pattern = _compile(item.get("pattern"), f"{where}: pattern")
    if "code" not in pattern.groupindex:
        raise ValueError(f"{where}: pattern needs a named group (?P<code>...)")
    vocabulary, validate, steps = _vocabulary_options(item, where)
    return OntologyRule(rule_id, pattern, vocabulary, steps, validate)


def _crosswalk_rule(item: Any, where: str, seen: set[str]) -> CrosswalkRule:
    if not isinstance(item, dict):
        raise ValueError(f"{where} must be a mapping")
    unknown = set(item) - _CROSSWALK_KEYS
    if unknown:
        raise ValueError(f"{where}: unknown keys {sorted(unknown)}")
    rule_id = _rule_id(item, where, seen)
    where = f"{where} ({rule_id})"
    pattern = _compile(item.get("pattern"), f"{where}: pattern")
    if "key" not in pattern.groupindex:
        raise ValueError(f"{where}: pattern needs a named group (?P<key>...)")
    table = item.get("table")
    parts = table.split("/") if isinstance(table, str) else []
    if len(parts) != 2 or not all(_SAFE_NAME.match(part) for part in parts):
        raise ValueError(f"{where}: 'table' must be '<dataset>/<name>'")
    columns = {}
    for name in ("key_column", "code_column"):
        value = item.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError(f"{where}: '{name}' must be a column name")
        columns[name] = value
    where_items = []
    filters = item.get("where") or {}
    if not isinstance(filters, dict):
        raise ValueError(f"{where}: 'where' must map column names to values")
    for column, value in filters.items():
        values = value if isinstance(value, list) else [value]
        if not values or not all(
            isinstance(v, (str, int)) and not isinstance(v, bool) for v in values
        ):
            raise ValueError(f"{where}: 'where.{column}' must be a string or list")
        where_items.append((str(column), tuple(str(v) for v in values)))
    vocabulary, validate, steps = _vocabulary_options(item, where)
    return CrosswalkRule(
        rule_id,
        pattern,
        table,
        columns["key_column"],
        columns["code_column"],
        vocabulary,
        tuple(where_items),
        steps,
        validate,
    )


def _vocabulary_options(
    item: dict, where: str
) -> tuple[str, str, tuple[Normalization, ...]]:
    vocabulary = item.get("vocabulary")
    if not isinstance(vocabulary, str) or not vocabulary.strip() or "/" in vocabulary:
        raise ValueError(f"{where}: 'vocabulary' must be a vocabulary_id without '/'")
    validate = item.get("validate", "pack")
    if validate not in VALIDATE_MODES:
        raise ValueError(f"{where}: 'validate' must be one of {list(VALIDATE_MODES)}")
    steps = []
    for index, step in enumerate(_list(item.get("normalize"), f"{where}: normalize")):
        step_where = f"{where}: normalize[{index}]"
        if not isinstance(step, dict) or set(step) != {"pattern", "replace"}:
            raise ValueError(f"{step_where} must have exactly 'pattern' and 'replace'")
        step_pattern = _compile(step["pattern"], f"{step_where}: pattern")
        replace = step["replace"]
        if not isinstance(replace, str):
            raise ValueError(f"{step_where}: 'replace' must be a string")
        for name, number in _TEMPLATE_REFERENCE.findall(replace):
            reference = name or number
            valid = (
                int(reference) <= step_pattern.groups
                if reference.isdigit()
                else reference in step_pattern.groupindex
            )
            if not valid:
                raise ValueError(f"{step_where}: unknown group reference {reference!r}")
        steps.append(Normalization(step_pattern, replace))
    return vocabulary, validate, tuple(steps)


def _hierarchy_rule(
    item: Any, where: str, seen: set[str], ontology_ids: set[str]
) -> HierarchyRule:
    if not isinstance(item, dict):
        raise ValueError(f"{where} must be a mapping")
    unknown = set(item) - _HIERARCHY_KEYS
    if unknown:
        raise ValueError(f"{where}: unknown keys {sorted(unknown)}")
    rule_id = _rule_id(item, where, seen)
    where = f"{where} ({rule_id})"
    nearest_only = item.get("nearest_only", True)
    if not isinstance(nearest_only, bool):
        raise ValueError(f"{where}: 'nearest_only' must be true or false")
    from_ontology = item.get("from_ontology", False)
    if not isinstance(from_ontology, bool):
        raise ValueError(f"{where}: 'from_ontology' must be true or false")
    if from_ontology:
        extra = set(item) - {"id", "from_ontology", "nearest_only", "rules"}
        if extra:
            raise ValueError(
                f"{where}: from_ontology rules do not accept {sorted(extra)}"
            )
        selected = _list(item.get("rules"), f"{where}: rules")
        missing = [name for name in selected if name not in ontology_ids]
        if missing or not all(isinstance(name, str) for name in selected):
            raise ValueError(f"{where}: unknown ontology rule ids {missing}")
        return HierarchyRule(
            rule_id, "ontology", nearest_only=nearest_only, rules=tuple(selected)
        )
    if "rules" in item:
        raise ValueError(f"{where}: 'rules' is only valid with from_ontology")
    pattern = _compile(item.get("pattern"), f"{where}: pattern")
    group = item.get("group")
    if not isinstance(group, str) or group not in pattern.groupindex:
        raise ValueError(f"{where}: 'group' must name a group of the pattern")
    separator = item.get("separator")
    mode = item.get("mode", "separator" if separator is not None else None)
    if mode not in {"separator", "prefix"}:
        raise ValueError(f"{where}: set 'mode: prefix' or a 'separator'")
    ignore = item.get("ignore", "")
    if mode == "separator":
        if not isinstance(separator, str) or not separator:
            raise ValueError(f"{where}: 'separator' must be a non-empty string")
        if ignore:
            raise ValueError(f"{where}: 'ignore' is only valid with mode: prefix")
    else:
        if separator is not None:
            raise ValueError(f"{where}: 'separator' is not valid with mode: prefix")
        if not isinstance(ignore, str):
            raise ValueError(f"{where}: 'ignore' must be a string of characters")
    return HierarchyRule(rule_id, mode, pattern, group, separator, ignore, nearest_only)


# ---------------------------------------------------------------------- packs


def default_cache_dir() -> Path:
    """``${XDG_CACHE_HOME:-~/.cache}/sumeds/ontologies``."""

    base = os.environ.get("XDG_CACHE_HOME") or "~/.cache"
    return Path(base).expanduser() / "sumeds" / "ontologies"


def resolve_source(source: str | None = None, ref: str = DEFAULT_REF) -> str:
    """Return the pack source: argument, ``SUMEDS_ONTOLOGY_SOURCE``, or GitHub.

    The GitHub default is the separately versioned data repository
    ``rvandewater/sumeds-ontologies`` at ``ref`` (a release tag by default).
    """

    return source or os.environ.get(SOURCE_ENV) or DEFAULT_SOURCE.format(ref=ref)


@dataclass(frozen=True)
class PackEntry:
    """A validated vocabulary code with its pack hierarchy (raw codes)."""

    code: str
    description: str | None
    ancestors: tuple[str, ...]
    children: tuple[str, ...] = ()
    siblings: tuple[str, ...] = ()


class OntologyPacks:
    """Fetch, verify, cache, and query sharded ontology packs.

    ``source`` is an ``http(s)://`` base URL, a ``file://`` URL, or a local
    directory containing ``index.json``. Remote files are cached under
    ``cache_dir`` (default :func:`default_cache_dir`); ``offline`` uses only
    the cache. Every shard is checked against its manifest's sha256.
    """

    def __init__(
        self,
        source: str | None = None,
        ref: str = DEFAULT_REF,
        *,
        offline: bool = False,
        cache_dir: str | Path | None = None,
    ) -> None:
        explicit = source or os.environ.get(SOURCE_ENV)
        self.source = resolve_source(source, ref)
        self.ref = None if explicit else ref
        # The default release is verified against its pinned index digest.
        self.index_sha256 = (
            DEFAULT_INDEX_SHA256 if not explicit and ref == DEFAULT_REF else None
        )
        if not explicit and not _RELEASE_REF.match(ref):
            warnings.warn(
                f"ontology ref {ref!r} is not a release tag of {DATA_REPOSITORY}; "
                "enrichment results can change whenever it moves. Pin a release "
                f"tag such as {DEFAULT_REF!r} for reproducible runs.",
                UserWarning,
                stacklevel=2,
            )
        parsed = urllib.parse.urlparse(self.source)
        if parsed.scheme in {"http", "https"}:
            self.local: Path | None = None
        elif parsed.scheme == "file":
            self.local = Path(urllib.request.url2pathname(parsed.path))
        elif parsed.scheme == "" or len(parsed.scheme) == 1:
            self.local = Path(self.source).expanduser()
        else:
            raise ValueError(f"unsupported ontology source: {self.source}")
        self.offline = offline
        slug = re.sub(r"[^A-Za-z0-9._-]+", "_", self.source.split("://", 1)[-1])
        self.cache = Path(cache_dir or default_cache_dir()).expanduser() / (
            slug.strip("_")[:120] or "default"
        )
        self.stats: dict[str, dict[str, Any]] = {}
        self._index: dict[str, Any] | None = None
        self._manifests: dict[str, tuple[str, dict[str, Any]]] = {}
        self._rows: dict[str, dict[str, tuple[str | None, str | None]]] = {}
        self._loaded: dict[str, set[str]] = {}
        self._crosswalks: dict[str, dict[str, Any]] = {}

    def describe(self) -> str:
        """Where packs come from, e.g. ``rvandewater/sumeds-ontologies@v0.1.0``."""

        if self.ref is None:
            return self.source
        pinned = ", index sha256 pinned" if self.index_sha256 else ""
        return f"{DATA_REPOSITORY}@{self.ref}{pinned}"

    def vocabularies(self) -> list[str]:
        """Vocabulary ids with a pack in the source index."""

        return sorted(self._load_index()["vocabularies"])

    def manifest(self, vocabulary: str) -> dict[str, Any]:
        """Return the verified manifest of ``vocabulary``."""

        return self._manifest(vocabulary)[1]

    def lookup(
        self,
        vocabulary: str,
        codes: Iterable[str],
        *,
        child_depth: int = 0,
        siblings: bool = False,
    ) -> dict[str, PackEntry]:
        """Return pack entries for the ``codes`` present in the pack.

        Only the shards these codes, their ancestors, and (when requested) their
        descendants or siblings live in are fetched.
        """

        directory, manifest = self._manifest(vocabulary)
        length = manifest["shard_key_length"]
        wanted = {code for code in codes if code}
        self._load(vocabulary, {code[:length] for code in wanted})
        rows = self._rows[vocabulary]
        found = {code for code in wanted if code in rows}
        self._load_ancestors(vocabulary, found, length)
        if child_depth or siblings:
            prefixes = set(found)
            if siblings:
                for code in found:
                    prefixes.update(self._ancestors(rows, code))
            keys = {
                key
                for key in manifest["shards"]
                for prefix in prefixes
                if key.startswith(prefix[:length])
            }
            self._load(vocabulary, keys)
            self._load_ancestors(vocabulary, set(rows), length)
        children: dict[str, list[str]] = {}
        if child_depth or siblings:
            for code, (_, parent) in rows.items():
                if parent:
                    children.setdefault(parent, []).append(code)
            for values in children.values():
                values.sort()
        path = manifest.get("description_path")
        entries = {}
        for code in sorted(found):
            ancestors = self._ancestors(rows, code)
            description = rows[code][0]
            if path and len(code) <= path["max_code_length"]:
                labels = [rows[item][0] for item in (*reversed(ancestors), code)]
                description = path["separator"].join(label for label in labels if label)
            entries[code] = PackEntry(
                code,
                description or None,
                tuple(ancestors),
                tuple(_descendants(children, code, child_depth)),
                tuple(
                    sorted(
                        {
                            child
                            for ancestor in ancestors
                            for child in children.get(ancestor, ())
                        }
                        - {code}
                    )
                )
                if siblings
                else (),
            )
        return entries

    def crosswalk(self, table: str) -> list[dict[str, str]]:
        """Return the verified rows of crosswalk ``table`` (``<dataset>/<name>``)."""

        dataset, _, name = table.partition("/")
        if dataset not in self._crosswalks:
            entry = self._load_index().get("crosswalks", {}).get(dataset)
            relative = f"crosswalks/{dataset}/manifest.json"
            if not isinstance(entry, dict) or entry.get("manifest") != relative:
                raise OntologyPackError(
                    f"no crosswalk dataset {dataset!r} at {self.source}"
                )
            manifest = _json(
                self._get(relative, entry.get("sha256"), refresh=True), relative
            )
            if manifest.get("schema") != CROSSWALK_SCHEMA or not isinstance(
                manifest.get("tables"), dict
            ):
                raise OntologyPackError(
                    f"{relative} is not a {CROSSWALK_SCHEMA} manifest"
                )
            self._crosswalks[dataset] = manifest
        info = self._crosswalks[dataset]["tables"].get(name)
        if not isinstance(info, dict) or not _SAFE_NAME.match(
            str(info.get("file", ""))
        ):
            raise OntologyPackError(f"no crosswalk table {table!r} at {self.source}")
        stats = self.stats.setdefault(
            f"crosswalk {table}",
            {"shards": 0, "available": 1, "downloaded": 0, "bytes": 0},
        )
        before = stats["downloaded"]
        data = self._get(
            f"crosswalks/{dataset}/{info['file']}", info.get("sha256"), stats=stats
        )
        stats["shards"] = 1
        stats["bytes"] += len(data) if stats["downloaded"] > before else 0
        return list(_csv_reader(data, info["file"]))

    # ------------------------------------------------------------------ fetch

    def _load_index(self) -> dict[str, Any]:
        if self._index is None:
            pinned = self.index_sha256 is not None
            index = _json(
                self._get("index.json", self.index_sha256, refresh=not pinned),
                "index.json",
            )
            if index.get("schema") != INDEX_SCHEMA or not isinstance(
                index.get("vocabularies"), dict
            ):
                raise OntologyPackError(
                    f"{self.source}/index.json is not a {INDEX_SCHEMA} index"
                )
            self._index = index
        return self._index

    def _manifest(self, vocabulary: str) -> tuple[str, dict[str, Any]]:
        if vocabulary not in self._manifests:
            index = self._load_index()
            entry = index["vocabularies"].get(vocabulary)
            if not isinstance(entry, dict):
                raise OntologyPackError(
                    f"no public ontology pack for vocabulary {vocabulary!r} at "
                    f"{self.source}; available: {sorted(index['vocabularies'])}. "
                    "Use 'validate: athena' or 'validate: none' for other vocabularies."
                )
            relative = str(entry.get("manifest", ""))
            parts = relative.split("/")
            if len(parts) < 2 or not all(_SAFE_NAME.match(part) for part in parts):
                raise OntologyPackError(f"unsafe manifest path in index: {relative!r}")
            manifest = _json(
                self._get(relative, entry.get("sha256"), refresh=True), relative
            )
            if (
                manifest.get("schema") != PACK_SCHEMA
                or manifest.get("vocabulary") != vocabulary
            ):
                raise OntologyPackError(f"{relative} is not a {PACK_SCHEMA} manifest")
            shards = manifest.get("shards")
            length = manifest.get("shard_key_length")
            if (
                not isinstance(shards, dict)
                or not isinstance(length, int)
                or length < 1
            ):
                raise OntologyPackError(f"{relative} has no valid shard table")
            for shard in shards.values():
                if not _SAFE_NAME.match(str(shard.get("file", ""))):
                    raise OntologyPackError(f"unsafe shard file name in {relative}")
            self._manifests[vocabulary] = (relative.rsplit("/", 1)[0], manifest)
            self.stats[vocabulary] = {
                "release": manifest.get("release"),
                "shards": 0,
                "available": len(shards),
                "downloaded": 0,
                "bytes": 0,
            }
        return self._manifests[vocabulary]

    def _load(self, vocabulary: str, keys: Iterable[str]) -> None:
        directory, manifest = self._manifest(vocabulary)
        loaded = self._loaded.setdefault(vocabulary, set())
        rows = self._rows.setdefault(vocabulary, {})
        stats = self.stats[vocabulary]
        for key in sorted(set(keys) - loaded):
            loaded.add(key)
            shard = manifest["shards"].get(key)
            if shard is None:
                continue
            relative = f"{directory}/{shard['file']}"
            before = stats["downloaded"]
            data = self._get(relative, shard.get("sha256"), stats=stats)
            stats["shards"] += 1
            stats["bytes"] += len(data) if stats["downloaded"] > before else 0
            reader = _csv_reader(data, shard["file"])
            fields = {**_DEFAULT_FIELDS, **(manifest.get("fields") or {})}
            needed = {fields["code"], fields["description"]} | (
                {fields["parent"]} if fields["parent"] else set()
            )
            if not needed <= set(reader.fieldnames or ()):
                raise OntologyPackError(f"{relative} lacks columns {sorted(needed)}")
            for row in reader:
                rows[row[fields["code"]]] = (
                    row[fields["description"]] or None,
                    (row[fields["parent"]] or None) if fields["parent"] else None,
                )

    def _load_ancestors(self, vocabulary: str, codes: set[str], length: int) -> None:
        rows = self._rows[vocabulary]
        loaded = self._loaded[vocabulary]
        pending = set(codes)
        while pending:
            missing = set()
            for code in pending:
                seen = {code}
                parent = rows[code][1] if code in rows else None
                while parent and parent not in seen:
                    seen.add(parent)
                    if parent not in rows:
                        if parent[:length] not in loaded:
                            missing.add(parent[:length])
                        break
                    parent = rows[parent][1]
            if not missing:
                return
            self._load(vocabulary, missing)

    @staticmethod
    def _ancestors(
        rows: dict[str, tuple[str | None, str | None]], code: str
    ) -> list[str]:
        ancestors: list[str] = []
        parent = rows[code][1]
        while parent and parent in rows and parent not in ancestors and parent != code:
            ancestors.append(parent)
            parent = rows[parent][1]
        return ancestors

    def _get(
        self,
        relative: str,
        sha256: str | None = None,
        *,
        refresh: bool = False,
        stats: dict[str, Any] | None = None,
    ) -> bytes:
        if self.local is not None:
            path = self.local / relative
            if not path.is_file():
                raise OntologyPackError(f"ontology file not found: {path}")
            data = path.read_bytes()
            _verify(data, sha256, str(path))
            return data
        cached = self.cache / relative
        if cached.is_file() and (self.offline or not refresh):
            data = cached.read_bytes()
            if not sha256 or hashlib.sha256(data).hexdigest() == sha256:
                return data
            if self.offline:
                _verify(data, sha256, f"cached {cached}")
        if self.offline:
            raise OntologyPackError(
                f"{relative} is not in the ontology cache {self.cache}; "
                "run once without --offline or use a local --ontology-source"
            )
        url = f"{self.source.rstrip('/')}/{urllib.parse.quote(relative)}"
        request = urllib.request.Request(url, headers={"User-Agent": "suMEDS"})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                data = response.read()
        except (urllib.error.URLError, OSError) as error:
            raise OntologyPackError(f"could not download {url}: {error}") from error
        _verify(data, sha256, url)
        if stats is not None:
            stats["downloaded"] += 1
        cached.parent.mkdir(parents=True, exist_ok=True)
        staged = cached.with_name(f".{cached.name}.{os.getpid()}.part")
        staged.write_bytes(data)
        os.replace(staged, cached)
        return data


def _descendants(children: dict[str, list[str]], code: str, depth: int) -> list[str]:
    result: list[str] = []
    seen = {code}
    queue = deque([(code, 0)])
    while queue:
        current, level = queue.popleft()
        if level >= depth:
            continue
        for child in children.get(current, ()):
            if child not in seen:
                seen.add(child)
                result.append(child)
                queue.append((child, level + 1))
    return result


def _csv_reader(data: bytes, name: str) -> csv.DictReader:
    """Read a verified CSV file; ``.gz`` files are gzip-compressed CSV."""

    if name.endswith(".gz"):
        data = gzip.decompress(data)
    return csv.DictReader(io.StringIO(data.decode("utf-8"), newline=""))


def _verify(data: bytes, sha256: str | None, where: str) -> None:
    if sha256:
        digest = hashlib.sha256(data).hexdigest()
        if digest != sha256:
            raise OntologyPackError(
                f"sha256 mismatch for {where}: expected {sha256}, got {digest}"
            )


def _json(data: bytes, where: str) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise OntologyPackError(f"{where} is not valid JSON: {error}") from None
    if not isinstance(value, dict):
        raise OntologyPackError(f"{where} must be a JSON object")
    return value
