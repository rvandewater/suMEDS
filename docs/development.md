# Development

## Environment

The project uses [uv](https://docs.astral.sh/uv/) for its environment, lockfile,
build, and commands.

```bash
uv sync
uv run pytest
uv run mkdocs build --strict
```

Build the distribution with:

```bash
uv build
```

## Test data

Unit tests create a tiny MEDS dataset with two shards, code modifiers, subject
splits, and overlapping rare-code subjects. This catches the common but
incorrect implementation that sums per-code subject counts.

When `MIMICIV_DEMO/MEDS_cohort` exists, the integration test also validates the
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
