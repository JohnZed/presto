# Remaining TODOs for Delta Lake

## Deferred GPU TIMESTAMP WITH TIME ZONE correctness

The `delta-lake-cudf` branch reads top-level UTC timestamps into Velox's
packed representation, but does not yet implement complete zoned timestamp
semantics across GPU operators. Velox packs UTC milliseconds and a timezone
key into an INT64. SQL equality, ordering, grouping, and hashing must use the
UTC instant; the timezone key is part of the retained value, not the key used
for those operations. Ordinary BIGINT semantics must remain unchanged.

The implementation explored on 2026-10-02 spans the reader, expressions,
and several operators. It is deferred to a separate change because that scope
is too large for this branch. The following gaps remain:

| Gap | Rough implementation needed |
| --- | --- |
| Comparisons across zones | Normalize both GPU columns and typed constants to UTC milliseconds before comparing. Preserve null behavior. Ensure AST and JIT expression paths do not compare packed INT64 values directly, including comparisons nested in other expressions. Register the zoned comparison signatures after the custom type exists. |
| GROUP BY, including streaming aggregation | Group on instant-only keys. Retain an actual packed input representative for each group rather than inventing a timezone. Keep normalized buffers alive for streaming aggregation. |
| Partitioned aggregation | Hash instant-only keys at local exchanges so equal instants with different zone representatives reach the same final group. Remove any auxiliary columns from the visible output. |
| DISTINCT and mark-distinct | Use instant-only equality keys, including the persistent seen-key state across mark-distinct batches. Return the original packed values. |
| Hash joins and join filters | Retain normalized keys with the owning build/probe tables. Account for auxiliary columns in internal schemas and filter offsets, while exposing the original values and zones in output. |
| ORDER BY and TopN | Sort using normalized timestamp key views. Equal instants must tie before secondary sort keys are applied. Gather the original payload so output zones are preserved. |
| Negative fractional timestamps in Parquet, especially INT96 | Preserve each column's native timestamp precision until packing and floor pre-epoch fractions to milliseconds. Avoid forcing wide millisecond values into nanoseconds, which can overflow. An upstream cuDF reader fix for timestamp downcast rounding may be cleaner; Velox still needs correct packed-value conversion. |
| Packed timestamp overflow | Validate against `kMinMillisUtc` and `kMaxMillisUtc` before shifting milliseconds into the packed INT64. A GPU min/max reduction can validate the range without copying rows to CPU. Ignore null slots and handle all-null columns. This bound belongs to Velox's representation, not cuDF's timestamp range. |

Prefer a shared instant-key conversion at the Velox/cuDF boundary and small
operator adaptations over unrelated special cases. cuDF currently has no
Velox-style timestamp-with-timezone logical type. A longer-term cuDF logical
or comparison-key abstraction could reduce the repeated handling, but must
preserve original values and keep ordinary INT64 behavior intact.

Nested TIMESTAMP WITH TIME ZONE fields also remain unsupported by the GPU
Parquet reader and are explicitly rejected. Supporting them would require
recursive schema/metadata validation and packing of nested timestamp leaves,
with parent/child null masks and container structure preserved. This was not
implemented by the deferred patch. Local, non-UTC-normalized Parquet timestamps
must continue to be rejected when requested as zoned timestamps; their support
would need an explicit timezone interpretation.

## Regression tests retained as known failures

GoogleTest does not provide an `xfail` result. These eight cases are retained
with its standard `DISABLED_` prefix, so normal runs skip them. This records a
known gap; it does not claim that the failing case ran successfully. Run them
explicitly with `--gtest_also_run_disabled_tests --gtest_filter='*DISABLED_*'`.
The GPU fixtures disable CPU fallback, and the tests check GPU operator stats.

- `velox_cudf_delta_read_test`: `CudfDeltaReadTest.DISABLED_timestampPrecisionAndNegativeFractions` and `CudfDeltaReadTest.DISABLED_timestampPackingLimits`.
- `velox_cudf_timestamp_with_time_zone_test`: `TimestampWithTimeZoneTest.DISABLED_comparisonsAcrossTimezones`, `DISABLED_partitionedGroupingAcrossTimezones`, `DISABLED_distinctAcrossTimezones`, `DISABLED_markDistinctAcrossBatches`, `DISABLED_hashJoinAcrossTimezones`, and `DISABLED_sortingPreservesZonesAndUsesSecondaryKeys`.

An explicit opt-in run on 2026-10-02 confirmed that all eight disabled cases
fail with the production fixes stashed. The normal run passed 683 GPU tests
and 31 Java Delta tests. Existing passing timestamp read coverage remains enabled. The independent
INT96 fixture is retained because cuDF's INT96 writer truncates nanosecond
input to microseconds and would erase the regression before the reader runs.
Java/CPU correctness tests may pass, and native SQL tests permitting CPU
fallback do not establish GPU correctness for these cases.

Remove each disabled prefix when its implementation is ready, then verify
actual GPU execution, nulls, negative fractions, equal instants with different
zones, preserved output zones, and secondary sort keys. Cover both ordinary
and streaming aggregation. The shared SQL comparison test should also be
restored for integration coverage in Java and native execution.

## Saved implementation

The changes remain recoverable in local stashes on labwork, in two Git repositories:

- Velox (`presto-native-execution/velox`): `69cff1ef0c7ce89ecf39e1f54691eccf610fa1ba` — deferred Delta GPU timestamp correctness fixes (production files only).
- Presto (repository root): `729ecb6b85976f3688615a446e82aa9be624a239` — deferred Delta timestamp SQL regression test.

Use `git stash show -p <hash>` in the corresponding repository to review and
`git stash apply <hash>` to restore on a suitable branch. The C++ regression
tests and their fixture remain in the current working tree, with disabled
markers; they are not in the production stash. Re-enable them when restoring
the fixes. The stashed implementation had passed all 690 GPU test cases and
64 native Delta tests in each of CPU and GPU modes before deferral.
