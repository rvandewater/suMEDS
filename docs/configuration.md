# Configuration

Configuration is optional and strict: misspelled sections or options fail
instead of silently weakening the privacy policy.

```yaml
summary:
  per_split: false

privacy:
  min_subjects: 20
  rare_code_action: bucket
  rare_code_label: __RARE__
  round_counts_to: null
```

## Summary options

### `per_split`

Default: `false`.

When true, event rows are joined to `metadata/subject_splits.parquet` and each
split becomes an independent release scope. Split values come from metadata,
not shard directory names. Missing or duplicate assignments fail the run.

## Privacy options

### `min_subjects`

Default: `20`. Must be a positive integer.

A code cell is common when at least this many distinct subjects contain it.
Thresholding by event count is intentionally not supported because repeated
events from one person do not provide anonymity. Setting this to `1`
effectively disables rarity masking and is suitable only for public or
synthetic data.

### `rare_code_action`

Default: `bucket`.

- `bucket` replaces all rare cells in a release scope with one sentinel row.
- `drop` omits rare cells.

Bucket mode performs a second projected data pass so its `subject_count` is the
true subject union. Per-code subject counts are never summed.

### `rare_code_label`

Default: `__RARE__`.

The sentinel for bucket mode. The run fails if the label already exists in code
metadata.

### `round_counts_to`

Default: `null` (exact counts).

A positive integer rounds released event and subject counts to the nearest
multiple after thresholding. Use `1` or `null` for exact counts.

## Precedence

```text
CLI option > YAML value > package default
```

For example, this keeps every YAML option except the threshold:

```bash
uv run suMEDS /data/MEDS -o summary.parquet \
  -c summary.yaml --min-subjects 30
```
