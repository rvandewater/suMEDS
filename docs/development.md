# Development

## Environment

The project uses [uv](https://docs.astral.sh/uv/) for its environment, lockfile,
build, and commands.

```bash
uv sync
uv run pre-commit install
uv run pre-commit run --all-files
uv run pytest
uv run mkdocs build --strict
```

A plain `pre-commit run` checks only staged files, so hooks legitimately skip
when nothing matching is staged. Use `--all-files` for a full repository check.

Build the distribution with:

```bash
uv build
```

## Release

Bump and commit the project version before creating the matching tag:

```bash
uv version --bump patch
uv run pytest
uv build --clear
git add pyproject.toml uv.lock
git commit -m "Release $(uv version --short)"
VERSION=$(uv version --short)
git tag -a "$VERSION" -m "Release $VERSION"
git push origin main "$VERSION"
```

The tag workflow rejects mismatched versions, then publishes the distributions
to PyPI and creates the GitHub release.

## Test data

Unit tests create a tiny MEDS dataset with two shards, code modifiers, subject
splits, and overlapping rare-code subjects. This catches the common but
incorrect implementation that sums per-code subject counts.

When `tests/resources/MIMICIV_DEMO/MEDS_cohort` exists, the integration test validates the
real MEDS 0.3.3 demo and checks that released plus bucketed event counts cover
all source events. Tests never modify the demo dataset.

## Design constraints

- Patient-level data stays in Polars lazy plans.
- Shard validation reads Parquet footers only.
- Aggregate counts are staged as Parquet to bound memory and avoid repeating
  the first event scan.
- Bucket mode uses exactly one additional projected event pass.
- Parquet, CSV, NDJSON, and bounded-memory JSON outputs are staged beside the
  destination and atomically replaced.
- Official `meds` schema classes define compatibility; this package does not
  duplicate the standard.
- New dependencies or abstractions require a demonstrated need.

## Documentation

Preview locally:

```bash
uv run mkdocs serve
```

The website is intentionally plain MkDocs with no theme or API-generation
plugins. Public signatures and behavior are documented in `docs/api.md`; source
docstrings remain the concise in-editor reference.
