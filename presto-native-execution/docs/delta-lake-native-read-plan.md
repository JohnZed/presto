# Native Delta Lake Read Support Plan

Status: **planning only — no production code changes have started**

Target branches:

- Presto: `exp-delta-lake` at `a77a6c834bb0009c883ae1148a3218d5f396ccae`
- Velox submodule: `exp-delta-lake` at `a729ad1fcdee7b3177a49d8af2060a7489513dde`

The Presto checkout already had an unrelated modified
`presto-native-execution/src/test/java/com/facebook/presto/nativeworker/PrestoNativeQueryRunnerUtils.java`
and a modified Velox submodule entry before these branches were created. Those
changes must be preserved and reviewed separately from the Delta work.

## Executive assessment

This is feasible without implementing the Delta transaction log in C++ and
without putting Java workers on the data path.

The intended division of responsibility is:

1. The existing Java Presto Delta connector and Delta Kernel remain on the
   coordinator. They resolve the table snapshot, time travel, schema, active
   Parquet files, and partition pruning, then create splits.
2. Prestissimo receives the existing Java connector handles and splits through
   the Presto worker protocol.
3. A small Prestissimo Delta adapter translates those objects into Velox Hive
   table/column handles plus a Delta-specific Parquet split.
4. CPU workers read the Parquet files through Velox.
5. GPU workers read the Parquet files through the Velox-cuDF reader, including
   synthesis of partition and missing columns on the GPU path.

Difficulty and rough single-engineer effort after review:

| Workstream | Difficulty | Estimate | Main uncertainty |
| --- | --- | ---: | --- |
| Prestissimo protocol and adapter | Medium | 4–7 days | Rebase of generated protocol code and predicate semantics |
| Velox CPU reader | Medium | 4–8 days | Column mapping/schema evolution against the current Velox reader |
| Velox-cuDF reader | Medium-hard | 8–15 days | Partition/missing-column injection and nested column mapping |
| Test porting, fixtures, labwork validation | Medium | 5–10 days | GPU feature matrix and test isolation |
| Total, sequential | Medium-hard | **4–7 weeks** | Includes integration/debug contingency |

A CPU-only first milestone should be achievable in roughly 2–3 weeks. A useful
GPU milestone for common Parquet types should add roughly 2–4 weeks. Full
coverage is governed more by the current cuDF type/operator matrix than by
Delta log semantics.

## Scope

### In scope

- Read-only Delta Lake tables whose data files are Parquet.
- Java coordinator support for Delta log discovery, checkpoints, snapshot
  selection, version/timestamp time travel, schema discovery, and split
  planning.
- CPU-native Parquet I/O and filtering through Velox.
- GPU-native Parquet I/O through Velox-cuDF, with no Java worker fallback.
- Partitioned and unpartitioned tables.
- Null partition values, empty-string partition values, special characters,
  and relative/absolute/URI file paths.
- Top-level column mapping by name/id, renamed/dropped columns, and schema
  evolution to the extent expected by the Java connector tests.
- Regular-column filter pushdown where the native reader can preserve Java
  semantics; partition pruning remains coordinator-owned.
- JSON and Thrift worker protocols.
- Explicit rejection of data files that actually carry deletion vectors.

### Out of scope for this effort

- Delta writes, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, or transaction commits.
- Deletion-vector application. A table may advertise the `deletionVectors`
  protocol feature, but any active file with a deletion vector must fail with a
  clear unsupported-feature error instead of returning incorrect rows.
- Change data feed and streaming reads.
- ORC data files.
- Moving Delta Kernel or transaction-log parsing into C++.
- The Velox-cuDF timestamp-with-time-zone conversion fix. GPU tests that require
  that fix will be excluded with a precise reason; other Delta work must not
  modify timestamp-with-time-zone implementation files.

## What can be reused

### Existing Java Delta connector — reuse directly

Keep these coordinator responsibilities in `presto-delta`:

- `DeltaClient`: load table metadata and snapshots with Delta Kernel.
- `DeltaMetadata`: resolve schemas, table layouts, and time travel.
- `DeltaSplitManager`: enumerate active files and perform partition pruning.
- `DeltaColumnHandle`, `DeltaTableHandle`, and `DeltaTableLayoutHandle`: remain
  the authoritative coordinator-side contract.
- Existing Java test resources and expected results.

Only small wire-contract changes should be made in Java, such as adding the
table location and an unambiguous representation of null partition keys to
`DeltaSplit`, plus rejecting active deletion-vector descriptors.

### Existing Hive/Parquet support — reuse for the fast path

- Translate Delta columns to `HiveColumnHandle` objects.
- Translate the table to `HiveTableHandle` with Delta data columns and safe
  subfield filters.
- Use the Velox filesystem, Parquet footer, row-group pruning, projection,
  decoding, cache, and I/O statistics infrastructure.
- Register the `delta` catalog against a Hive-derived connector factory on CPU.

### Existing Iceberg support — reuse patterns and shared mechanics

Iceberg is useful as an implementation template, not as the Delta metadata
model:

- Connector-specific generated protocol directory and `ConnectorProtocol`.
- `PrestoToVeloxConnector` adapter and registration pattern.
- Connector-specific split subclass and split-reader dispatch.
- Name-based Parquet schema adaptation, partition constants, info columns, and
  null-filling for columns absent from older files.
- On GPU, the schema inspection and constant-column injection currently in
  `CudfIcebergDataSource`/`CudfIcebergSplitReader`.

Do **not** translate Delta tables into Iceberg manifests, snapshots, partition
specs, or delete files. Delta Kernel has already planned the correct active
Parquet files; fabricating Iceberg metadata would add complexity and semantic
risk without improving the I/O path.

## Existing prototype to mine, not merge wholesale

The checkout contains `upstream/add-delta-oss`, with the following useful
reference commits:

- `9bd3511d8c` — initial Prestissimo Delta connector/protocol and native tests.
- `c0a49493da` — column mapping.
- `1fb6a4512b` — test fixes.
- `f621693c32` — adapter refactor and converter tests.
- `6a56f57432` — advances Velox to a prototype Delta reader.

The corresponding Velox lineage contains:

- `5bcf079de` — Parquet subfield rename/deletion support.
- `5f278b63b` — Delta split reader.
- `c56f87e5a` — rebase/refactor.
- `a9b36204c` — timestamp-with-time-zone conversion changes.

Do not merge these branches wholesale: they are based on a substantially older
Presto/Velox baseline and include unrelated changes. Manually port the
Delta-specific concepts and tests onto the current branches. Explicitly omit
`a9b36204c` and any equivalent GPU timestamp-with-time-zone changes.

The prototype also has two gaps to fix rather than copy:

1. It treats an empty partition string as null. The new wire contract must
   distinguish `''` from null, for example with `partitionValues` plus an
   explicit `nullPartitionKeys` set.
2. It registers a CPU `HiveConnectorFactory("delta")` even in cuDF builds.
   GPU support needs a cuDF Delta factory/data source and a test proving the
   cuDF scan path was used.

## Target architecture

```text
Java coordinator
  Delta Kernel / DeltaClient
    -> snapshot + schema + active AddFile rows
  DeltaSplitManager
    -> DeltaTableHandle / DeltaTableLayoutHandle / DeltaColumnHandle
    -> DeltaSplit (file, table location, partition values, null keys)
          |
          | JSON or Thrift task protocol
          v
Prestissimo worker
  DeltaConnectorProtocol
  DeltaPrestoToVeloxConnector
    -> HiveTableHandle / HiveColumnHandle
    -> HiveDeltaSplit
          |
          +-> CPU: Velox DeltaSplitReader -> Velox Parquet reader
          |
          +-> GPU: CudfDeltaDataSource -> CudfDeltaSplitReader
                    -> cuDF Parquet reader + GPU constant injection
```

## Implementation plan

### Phase 0 — freeze semantics and build a compatibility matrix

1. Record the current Java test baseline for `presto-delta`.
2. Inventory every inherited Delta test by feature: metadata-only, snapshot
   planning, file scan, partition synthesis, predicate pushdown, column mapping,
   timestamp, variant, and unsupported write behavior.
3. Create a CPU/GPU support matrix. A skip is allowed only for a documented
   existing GPU type/operator limitation; Delta-specific failures are not
   acceptable skips.
4. Add/identify a fixture with an **actual active deletion vector** and define
   the expected unsupported error. Do not reject a table merely because its
   protocol advertises the feature when none of its active files uses it.
5. Decide and document path normalization rules for `file:`, local absolute,
   relative, S3/S3A, and escaped paths.

Exit criterion: reviewed contract and test matrix, including the exact GPU
timestamp-with-time-zone exclusions.

### Phase 1 — Java-to-native wire contract

1. Add a generated Delta protocol under
   `presto-native-execution/presto_cpp/presto_protocol/connector/delta/` for:
   `DeltaTable`, `DeltaColumn`, `DeltaTableHandle`, `DeltaTableLayoutHandle`,
   `DeltaColumnHandle`, `DeltaSplit`, and `DeltaTransactionHandle`.
2. Add Delta to the protocol generator Makefile, umbrella headers/sources, and
   connector routing.
3. Extend `DeltaSplit` minimally:
   - carry `tableLocation` when the AddFile path is relative;
   - carry null partition keys separately from non-null string values;
   - keep empty string distinct from null;
   - preserve existing Java-worker behavior and JSON round trips.
4. In `DeltaSplitManager`, reject an active AddFile deletion-vector descriptor
   before scheduling it.
5. Add Java and C++ golden serialization tests for both JSON and Thrift.

Exit criterion: native protocol round-trips every handle/split shape produced
by the Java connector, including mapped columns and null/empty partitions.

### Phase 2 — Prestissimo Delta adapter

1. Add `DeltaPrestoToVeloxConnector` and register connector name `delta`.
2. Convert a `DeltaColumnHandle` into a `HiveColumnHandle`:
   - physical name when column mapping is enabled, logical name otherwise;
   - regular versus partition column kind;
   - Presto/Velox type;
   - required subfields when safe;
   - ISO-8601 parsing for Delta date partition values.
3. Convert `DeltaTableHandle`/layout into `HiveTableHandle`:
   - non-partition Parquet data columns;
   - filter column handles;
   - safe regular-column domain filters;
   - no remaining filter on the connector handle when the coordinator leaves
     the residual `FilterNode` above the scan.
4. Convert `DeltaSplit` into `HiveDeltaSplit`:
   - normalized full data-file URI;
   - Parquet format;
   - split byte range and file size;
   - optional partition values with null preserved;
   - `$path` and `$file_size` info columns;
   - an explicit Delta table-format marker.
5. Keep filters off raw file values when Java and native comparison semantics
   differ (notably timestamps and mapped nested paths). Correct residual
   filtering is preferable to unsafe pushdown.
6. Add focused converter tests using serialized objects captured from Java,
   including error cases for wrong handle/split types.

Exit criterion: a native worker accepts Java Delta plans and produces the exact
Velox table, column, filter, and split objects expected by unit tests.

### Phase 3 — Velox CPU Delta reader

1. Add `HiveDeltaSplit` and connector split serde.
2. Add `DeltaSplitReader` as a thin Hive/Parquet specialization and dispatch to
   it from `HiveSplitReader` when the split is Delta.
3. Reuse the current Hive/Iceberg reader mechanics to:
   - select Parquet columns by name;
   - synthesize partition and info columns as constants;
   - null-fill columns missing from older files;
   - preserve output ordering;
   - handle zero physical projected columns;
   - reconcile physical names for Delta column mapping.
4. Port only the portions of prototype commit `5bcf079de` still required on
   the current Velox baseline. Prefer a general Parquet schema-evolution fix if
   it also benefits Hive/Iceberg; guard it with existing-reader regression
   tests.
5. Do not port timestamp-with-time-zone conversion changes.
6. Add Velox tests for:
   - split serde and path handling;
   - all supported partition primitive types;
   - null versus empty-string partitions;
   - mapped, renamed, dropped, and missing columns;
   - nested projection where the Java connector exposes it;
   - regular-column filters and no-column/count scans.

Exit criterion: the CPU-native integration suite reads Delta fixtures with no
Java workers and matches Java results.

### Phase 4 — Velox-cuDF Delta reader

The generic `CudfHiveDataSource` currently converts a `HiveConnectorSplit` to a
`CudfHiveConnectorSplit` but drops partition keys and Delta-specific metadata.
Using it unchanged would make partitioned Delta reads incorrect.

1. Add a dedicated `CudfDeltaConnectorFactory("delta")` and
   `CudfDeltaConnector`, following the current cuDF Iceberg factory pattern.
2. In a cuDF build, register the Delta catalog with that factory rather than
   `HiveConnectorFactory("delta")`.
3. Add `CudfDeltaDataSource` that retains the original `HiveDeltaSplit` while
   producing the cuDF file-source split.
4. Add `CudfDeltaSplitReader`, derived from `CudfSplitReader`, for Parquet I/O.
5. Refactor the non-delete-specific schema adaptation from
   `CudfIcebergSplitReader` into a small shared helper/base where practical:
   - inspect top-level Parquet schema;
   - classify file-backed versus injected columns;
   - remove partition/info/missing columns from the Parquet projection;
   - build typed cuDF scalars for partition values/nulls;
   - interleave constant columns back into the GPU table;
   - support injected-only scans and filter-only columns.
6. Keep Iceberg delete handling in Iceberg-specific code; Delta will not use
   positional/equality deletes or deletion vectors.
7. Implement column mapping on the GPU path using physical read names while
   preserving logical output ordering. Add nested mapping only when supported
   by both the Java connector contract and cuDF schema selection; otherwise
   fail clearly rather than silently reading the wrong field.
8. Transform or defer filters that reference partition/missing/mapped fields;
   never push a filter to cuDF against a column that is injected after read.
9. Add an observable test assertion (connector/data-source type, runtime stat,
   or debug marker) that the cuDF Delta data source and cuDF Parquet reader were
   used. Successful execution alone is insufficient because a CPU fallback
   could hide missing GPU support.
10. Leave timestamp-with-time-zone code untouched. GPU tests should project
    other columns from such tables or explicitly skip only the affected cases.

Exit criterion: representative unpartitioned, partitioned, pruned, schema-
evolved, and column-mapped Delta scans perform file I/O through cuDF and match
Java/CPU results for the declared GPU type matrix.

### Phase 5 — native integration tests reused from Java

Refactor test fixtures only as needed so the same assertions can run with Java,
CPU-native, and GPU-native query-runner factories. Avoid copying expected
result literals.

CPU-native suites should subclass/reuse:

- `TestDeltaIntegration`
- `TestDeltaScanOptimizations`
- `TestIncrementalUpdateQueries`
- `TestUppercasePartitionColumns`
- `TestColumnMapping`
- `TestDeltaVariantType`, if Velox currently supports the Java connector's
  variant representation

Retain the Java connector's unit tests for config, type conversion, table name,
handles, and split JSON. Add native protocol/converter tests rather than
duplicating these Java-only unit tests.

GPU-native coverage should reuse the same fixture tables and expected results,
but use a scan-focused suite so unrelated unsupported GPU expressions do not
obscure Delta reader correctness. Minimum GPU cases:

- primitive projection and `count(*)`;
- arrays, maps, and rows supported by the current cuDF reader;
- regular-column filter and row-group pruning;
- single and multiple partition filters;
- null, empty-string, date, decimal, and case-sensitive partition columns;
- latest snapshot, version time travel, and timestamp time travel (the latter
  concerns coordinator snapshot selection, not reading a timestamp-with-time-
  zone data column);
- incremental AddFile/remove-file history;
- top-level column mapping, rename, and drop;
- relative paths and paths containing spaces/special characters;
- no projected physical columns;
- explicit deletion-vector rejection.

Tests that read a `TIMESTAMP WITH TIME ZONE` data or partition column on GPU
remain excluded from this effort and must cite the separate known GPU issue.
CPU coverage for those Java expectations should still run if current Velox CPU
semantics are correct.

Run both JSON and Thrift native integration variants for a compact smoke set;
the full data matrix can use the default protocol after protocol unit tests
cover both encodings.

### Phase 6 — labwork deployment and acceptance

1. Build the Java coordinator/plugin artifacts from the Presto branch.
2. Build a CPU Prestissimo worker with the matching Velox submodule.
3. Build the cuDF worker image with the same Presto/Velox revisions.
4. Start separate CPU and GPU-native test configurations against the same
   Delta fixture locations.
5. Capture query plans, worker logs, runtime statistics, and file-I/O metrics
   for representative scans.
6. Compare result sets with the Java Delta runner and compare CPU/GPU results
   directly.
7. Run Hive and Iceberg regression smoke tests because the implementation
   touches shared Hive/Parquet and cuDF schema-adaptation code.

Final acceptance criteria:

- No Java data worker is needed for supported Delta reads.
- Delta log/snapshot planning stays on the Java coordinator.
- CPU file reads use the Velox Parquet reader.
- GPU file reads use the cuDF Parquet reader, verified by an explicit signal.
- Java expected results match for the agreed CPU matrix and supported GPU
  matrix.
- Partition pruning occurs on the coordinator and regular-column pushdown is
  correct where enabled.
- Null and empty-string partition values are distinguishable.
- Column mapping/schema evolution never returns data from the wrong physical
  field.
- Active deletion vectors fail clearly before data is returned.
- Hive and Iceberg scan regression tests remain green.
- No timestamp-with-time-zone GPU implementation files are changed.

## Recommended implementation order and review points

1. **Review point A:** approve this architecture, scope, and support matrix.
2. Implement Phase 1 and adapter unit tests.
3. Implement CPU reader and run CPU-native Java conformance tests.
4. **Review point B:** review the CPU diff and semantics before GPU refactoring.
5. Extract/share only the cuDF Iceberg schema-adaptation mechanics needed by
   Delta, then implement the dedicated cuDF Delta reader.
6. Run the GPU matrix and shared Hive/Iceberg regressions.
7. **Review point C:** review remaining GPU exclusions and performance evidence.

This sequence keeps the Java coordinator contract stable, proves correctness on
CPU first, and prevents GPU-specific limitations from delaying the core native
Delta read path.
