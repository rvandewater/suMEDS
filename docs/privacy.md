# Privacy model

The package provides **privacy-aware release controls**, not a formal privacy
guarantee.

## What is protected

For each output scope, codes below `min_subjects` unique subjects are either:

- omitted (`drop`), or
- combined into one row (`bucket`).

With declared code modifiers, each `code` + modifiers combination is counted and
suppressed on its own, whichever modifiers `metadata/codes.parquet` is keyed by
(see [Code modifiers and code metadata](usage.md#code-modifiers-and-code-metadata)).

A masked row contains no code description, parent relationships, modifier
values, or metadata extensions. Optional Athena and ontology-rule lookup runs
after masking, so source codes represented by a rare bucket are never sent to
PostgreSQL, matched by rules, or restored from local vocabulary files. Pack
fetching requests only vocabulary shards (named by the first one or two
characters of vocabulary codes) and crosswalk tables, never dataset codes, and
hierarchy rules only link codes that are already released. The output never contains subject
identifiers, event timestamps, or raw text values. It contains individual
numeric values only if you request `numeric_min` or `numeric_max` (see
[Numeric statistics](#numeric-statistics)).

With row-per-split summaries, `min_subjects` is applied independently within
each split. In wide `split_columns` output, total rows use `min_subjects` while
split cells use `min_split_subjects`. Its default of `1` exposes all split
counts for released total rows; raise it when small split cells must be null.

## Exact bucket semantics

Suppose rare code B occurs for subjects `{1, 2}` and rare code C occurs for
`{2, 3}`. The bucket reports three subjects, not four. This requires a second
lazy event pass after rare keys have been identified. A combined bucket that
still has fewer than `min_subjects` subjects is omitted; it is never released
merely because its code label is hidden.

## Count rounding

`round_counts_to` reduces precision after thresholding. It does not affect
whether a code is treated as rare. Rounding may make displayed counts differ
from the threshold; `is_masked` remains authoritative.

## Numeric statistics

`numeric_stats` cells are suppressed unless at least `min_subjects` distinct
subjects contribute a finite value. That stops a code that is common, but
measured in only a few people, from revealing their values through its mean or
median.

!!! warning "`numeric_min` and `numeric_max` are individual measurements"
    Each is exactly one subject's most extreme value, whatever `min_subjects`
    is: 20 or more people having a value does not hide the single highest one.
    An extreme weight, age or lab result can identify a person,
    especially with external knowledge. `numeric_stats: true` therefore leaves
    them out, and requesting them (by name or with `all`) prints a warning on
    every run. Release them only after reviewing the extremes of every
    released code.

`percentiles` (`numeric_p_lower`, `numeric_p_upper`) describe the range
instead. Each bound is released only when at least `min_subjects` distinct
subjects have a value at or beyond it, so it lies inside a group of at least
that many people rather than at one person's extreme. With linear
interpolation a bound can still equal one subject's value, as the median can;
like every released statistic, it is never attributable to fewer than
`min_subjects` people.

## Limitations

This is threshold suppression, not differential privacy. It does not prevent:

- differencing attacks across repeated runs with different configurations;
- inference from external knowledge;
- disclosure through unrestricted access to the source MEDS dataset;
- disclosure caused by choosing a threshold that is too low;
- all risks from exact aggregate counts;
- rare split-cell disclosure when `split_columns` keeps the default
  `min_split_subjects: 1`;
- standalone enrichment of a table that has not already passed release review.

Use one reviewed configuration for a release, restrict access to intermediate
and source data, and assess policy requirements with the relevant privacy or
governance team. Differential privacy and query-budget accounting are outside
the package's scope.
