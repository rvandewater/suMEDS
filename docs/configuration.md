# Configuration

Configuration is optional and strict: misspelled sections or options fail
instead of silently weakening the privacy policy.

```yaml
summary:
  per_split: false
  split_columns: false

privacy:
  min_subjects: 20
  min_split_subjects: 1
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

### `split_columns`

Default: `false`.

When true, each code keeps its total `event_count` and `subject_count` and gains
pairs such as `event_count_train` and `subject_count_train`. Split names are
lowercased and non-word characters become underscores. Total code rows still
use `min_subjects`; split cells use the separate `min_split_subjects` threshold.

`per_split` and `split_columns` are mutually exclusive: the former is long
(row-per-split) output, while the latter is wide output with totals.

## Privacy options

### `min_subjects`

Default: `20`. Must be a positive integer.

A code cell is common when at least this many distinct subjects contain it.
Thresholding by event count is intentionally not supported because repeated
events from one person do not provide anonymity. Setting this to `1`
effectively disables rarity masking and is suitable only for public or
synthetic data.

### `min_split_subjects`

Default: `1`. Must be a positive integer.

Controls split-column cells independently from total-code release. The default
shows all split counts, including zeros, for globally released code rows. Raise
it to suppress split cells with fewer subjects; both event and subject values
then become null. It does not affect long `per_split` output.

### `rare_code_action`

Default: `bucket`.

- `bucket` replaces rare cells with one sentinel row when the combined bucket
  itself meets `min_subjects`.
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
