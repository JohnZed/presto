# Native Delta Lake Read Support

Status: **implemented and validated on `exp-delta-lake`**

This document records the implemented design for read-only Delta Lake support
in Prestissimo CPU and Velox-cuDF workers. Delta writes, deletion-vector
application, and the separate GPU timestamp-with-time-zone fix remain out of
scope.

## Outcome

The implementation keeps transaction-log work on the Java coordinator and
moves data-file I/O to the native workers:

1. The existing Java Presto Delta connector and Delta Kernel resolve metadata,
   snapshots, time travel, active Parquet files, schema, and partition pruning.
2. Delta handles and splits cross the existing JSON or Thrift worker protocol.
3. `DeltaPrestoToVeloxConnector` adapts the Delta protocol objects to Velox
   Hive table, column, and split objects.
4. CPU workers read the planned Parquet files with the Velox Hive reader.
5. GPU workers read the same files with the existing cuDF Iceberg reader's
   delete-free scan path.

No Java worker is present on either native data path. No Delta log reader was
implemented in C++, and no production Velox source change was required.

## Implemented architecture

```text
Java coordinator
  Delta Kernel / DeltaClient
    -> snapshot, schema, active AddFile rows, partition pruning
  DeltaSplitManager
    -> Delta handles
    -> DeltaSplit(file, table location, partition values, null keys)
          |
          | JSON or Thrift task protocol
          v
Prestissimo worker
  DeltaConnectorProtocol
  DeltaPrestoToVeloxConnector
    -> HiveTableHandle / HiveColumnHandle
    -> CPU build: HiveConnectorSplit
    -> GPU build: delete-free HiveIcebergSplit
          |
          +-> CPU: HiveConnector -> Velox Parquet reader
          |
          +-> GPU: CudfIcebergConnector -> cuDF Parquet reader
```

The GPU adapter does not fabricate Iceberg snapshots, manifests, partition
specifications, or delete files. It uses `HiveIcebergSplit` only as the stable
input contract for the existing cuDF reader's schema adaptation and injected
column support. Its delete-file list is always empty.

## Scope

Implemented:

- Read-only Delta tables backed by Parquet.
- Latest snapshots and version/timestamp time travel planned by Java.
- Partitioned and unpartitioned reads.
- Coordinator partition pruning and native regular-column filtering.
- Null and empty-string partition values as distinct values.
- Current-schema partition columns missing from older files, injected as null.
- Top-level Delta name/id column mapping, rename, and drop behavior.
- Relative, absolute, and URI AddFile paths.
- `$path` and `$file_size` information columns.
- JSON and Thrift protocol routing.
- Early rejection of active deletion vectors.

Intentionally not implemented:

- Delta writes or transaction commits.
- Deletion-vector application.
- Change data feed or streaming reads.
- ORC Delta data files.
- Native Delta transaction-log parsing.
- GPU timestamp-with-time-zone conversion changes.

## Reuse of existing code

### Java Delta connector

The coordinator continues to use `DeltaClient`, `DeltaMetadata`,
`DeltaSplitManager`, Delta Kernel, and the existing Delta handle types. The
native work extends only the worker wire contract and split metadata needed to
read the already selected files.

### Velox Hive and Iceberg readers

The CPU path reuses the Hive connector directly. The GPU path reuses the cuDF
Iceberg connector because it already provides the required delete-independent
mechanics:

- Parquet schema inspection and projection;
- partition and information-column injection;
- null injection for columns absent from older files;
- output-column reordering; and
- filter handling around injected columns.

The adapter includes every logical table column in `HiveTableHandle.dataColumns`
so the cuDF data source can type filter-only and partition fields before
split-specific constants are injected. Column handles still distinguish
regular and partition columns, so partition fields are not read from Parquet.

## Split and safety contract

- Absolute paths and full URIs are preserved.
- Relative AddFile paths are resolved against the Delta table location.
- `partitionValues` contains non-null strings, including `""`.
- `nullPartitionKeys` contains SQL-null partition keys.
- A key cannot be both non-null and null.
- Partition columns present in current metadata but absent from an older
  AddFile partition map are added to `nullPartitionKeys`.
- An active AddFile with a deletion-vector descriptor fails with
  `NOT_SUPPORTED`; advertising the table protocol feature alone is allowed.
- Native splits carry an explicit `table_format=delta` marker.

## Implementation phases and commits

| Phase | Commit | Result |
| --- | --- | --- |
| Plan | `438d72aacc` | Architecture and phased test plan |
| Compatibility matrix | `8d2922fdc6` | Java/CPU/GPU expectations and exclusions |
| Native protocol | `27fc23a844` | Generated Delta protocol and routing |
| Native adapter | `a89511d375` | Delta-to-Hive handle and split conversion |
| Java split contract | `7a0487e295` | Paths, null partitions, and deletion-vector rejection |
| CPU integration | `c42273295d` | Java suites reused with a CPU-native worker |
| Partition evolution | `67283a0ac0` | Missing current partition columns injected as null |
| GPU integration | `b370117c5f` | cuDF runner plumbing and Java-compatible GPU suites |

No commit has been pushed.

## Test plan and verified results

Tests were implemented and run after each phase, not deferred until the end.
The final current-image regression results are:

| Layer | Result |
| --- | ---: |
| Full Java `presto-delta` module | 82 passed |
| Native protocol/adapter unit suite | 25 passed |
| CPU-native Java compatibility suites | 51 passed |
| GPU-native Java compatibility suites | 49 passed |
| CPU production worker build | Passed |
| GPU production worker build | Passed |

The native compatibility totals comprise:

| Java suite reused by native tests | CPU | GPU |
| --- | ---: | ---: |
| Delta integration | 20 | 18 |
| Scan optimizations | 14 | 14 |
| Incremental update reads | 6 | 6 |
| Uppercase partition columns | 4 | 4 |
| Column mapping | 7 | 7 |
| Total | 51 | 49 |

The CPU and GPU suites reuse Java fixtures, data providers, and expected
results through inheritance. Native-only unit tests cover protocol routing,
serialization, path resolution, partition nulls, deletion-vector rejection,
physical column names, and CPU/GPU split selection.

## Declared GPU limitations

The Delta-specific GPU paths pass. Two generic cuDF conversion limitations are
kept explicit in the GPU integration wrapper:

- Nested `VARBINARY` and short-decimal array elements are omitted from the
  inherited all-column array assertion. The same Delta v1/v3 fixtures still
  validate arrays of integer, bigint, tinyint, smallint, boolean, real, double,
  and varchar.
- Parquet maps are disabled because cuDF exposes them as
  `LIST<STRUCT<key,value>>`, which the current cuDF-to-Velox conversion cannot
  retag as a Velox `MAP`, even for integer keys and values.

GPU timestamp-with-time-zone tests remain disabled in the CPU wrapper inherited
by the GPU tests, as requested. No timestamp-with-time-zone implementation file
was changed.

## Build and runtime approach

Builds use the checked-in Prestissimo runtime Dockerfile and pinned
`presto/prestissimo-dependency:centos9` toolchain. Java uses the pinned Java 17
environment and Maven repository on labwork.

The checked-in slim GPU runtime omits `libnvrtc`, which cuDF loads dynamically.
Testing therefore uses a disposable Dockerfile under `/tmp` that copies the
new worker and its collected libraries into the full pinned dependency image.
This workaround is outside the repository. No Dockerfile, CMake file, Makefile,
Maven POM, or test script was changed for the build environment.

The earlier build difficulty came from two environment mismatches:

- the old successful image was CPU-only and did not link cuDF; and
- one attempted bind mount did not match the absolute source path recorded in
  the existing CMake cache.

Using the matching source mount and the pinned containers restored reproducible
builds without source-side workarounds.

## Acceptance status

- Java owns Delta log and snapshot planning: **met**.
- CPU file I/O uses Velox: **met**.
- GPU file I/O uses cuDF: **met**.
- Java expected results match the declared CPU/GPU matrices: **met**.
- Null/empty partitions and partition schema evolution are correct: **met**.
- Column mapping does not read the wrong physical field: **met**.
- Active deletion vectors fail before data is returned: **met**.
- GPU timestamp-with-time-zone work remains separate: **met**.
- No build-environment source changes: **met**.
