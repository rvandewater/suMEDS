# Privacy model

The package provides **privacy-aware release controls**, not a formal privacy
guarantee.

## What is protected

For each output scope, codes below `min_subjects` unique subjects are either:

- omitted (`drop`), or
- combined into one row (`bucket`).

A masked row contains no code description, parent relationships, modifier
values, or metadata extensions. The output never contains subject identifiers,
event timestamps, raw text values, numeric values, or extrema.

With per-split summaries, the threshold is applied independently within each
split. A code common globally but rare in one split is therefore masked in that
split.

## Exact bucket semantics

Suppose rare code B occurs for subjects `{1, 2}` and rare code C occurs for
`{2, 3}`. The bucket reports three subjects, not four. This requires a second
lazy event pass after rare keys have been identified.

## Count rounding

`round_counts_to` reduces precision after thresholding. It does not affect
whether a code is treated as rare. Rounding may make displayed counts differ
from the threshold; `is_masked` remains authoritative.

## Limitations

This is threshold suppression, not differential privacy. It does not prevent:

- differencing attacks across repeated runs with different configurations;
- inference from external knowledge;
- disclosure through unrestricted access to the source MEDS dataset;
- disclosure caused by choosing a threshold that is too low;
- all risks from exact aggregate counts.

Use one reviewed configuration for a release, restrict access to intermediate
and source data, and assess policy requirements with the relevant privacy or
governance team. Differential privacy and query-budget accounting are outside
the package's scope.
