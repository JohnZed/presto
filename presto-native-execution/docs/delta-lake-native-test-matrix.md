# Native Delta Lake Read Compatibility Matrix

Status: **validated on `exp-delta-lake`**

This matrix defines the tested read contract shared by the Java Delta
coordinator, CPU-native workers, and cuDF-native workers.

## Java compatibility suites

| Feature | Java source of truth | CPU-native | GPU-native |
| --- | --- | --- | --- |
| Primitive projection | `TestDeltaIntegration` | Pass | Pass |
| Array projection | `TestDeltaIntegration` | Pass | Pass for integer, bigint, tinyint, smallint, boolean, real, double, and varchar elements |
| Nested binary/short-decimal arrays | `TestDeltaIntegration` | Pass | Excluded: generic cuDF nested conversion limitation |
| Map projection | `TestDeltaIntegration` | Pass | Excluded: generic cuDF `LIST<STRUCT>` to Velox `MAP` limitation |
| HMS registration and latest snapshot | `TestDeltaIntegration` | Pass | Pass |
| Version/timestamp time travel | `TestDeltaIntegration` | Pass | Pass; snapshot selection is coordinator-side |
| Primitive partition values | `TestDeltaIntegration` | Pass | Pass |
| Current partition column missing from old file | `TestDeltaIntegration` | Pass | Pass; injected as null |
| Regular and partition filters | `TestDeltaScanOptimizations` | Pass | Pass |
| Null/not-null partition filters | `TestDeltaScanOptimizations` | Pass | Pass |
| Nested filter | `TestDeltaScanOptimizations` | Pass | Pass for covered fixture |
| Add/remove file history and missing columns | `TestIncrementalUpdateQueries` | Pass | Pass |
| Case-sensitive regular/partition columns | `TestUppercasePartitionColumns` | Pass | Pass |
| Name/id mapping, rename, drop, special characters | `TestColumnMapping` | Pass | Pass |
| Timestamp-with-time-zone data columns | `TestDeltaIntegration` | Deferred in native wrapper | Deferred; separate requested GPU issue |
| Writes and DDL mutation | Java write tests | Out of scope | Out of scope |

## Verified counts

| Suite | Java | CPU-native | GPU-native |
| --- | ---: | ---: | ---: |
| Complete `presto-delta` module | 82 | — | — |
| Delta integration wrapper | — | 20 | 18 |
| Scan optimizations wrapper | — | 14 | 14 |
| Incremental update wrapper | — | 6 | 6 |
| Uppercase partition wrapper | — | 4 | 4 |
| Column mapping wrapper | — | 7 | 7 |
| Native compatibility total | — | 51 | 49 |
| Native protocol/adapter unit suite | — | 25 | Compiled with GPU-specific split assertions |

All listed executed tests completed with zero failures and zero errors.

## Path and split contract

- Full URIs and local absolute paths are preserved.
- Relative AddFile paths are resolved against the table location.
- `partitionValues` carries non-null strings, including the empty string.
- `nullPartitionKeys` carries SQL nulls.
- A partition key cannot occur in both collections.
- Current partition columns absent from an older split are treated as null.
- `$path` is the resolved file path and `$file_size` is the AddFile size.
- Active deletion vectors fail with `NOT_SUPPORTED`.
- A protocol that advertises deletion vectors is accepted when no active file
  has one.

## Native-only coverage

| Layer | Coverage |
| --- | --- |
| Java split contract | JSON round trip, table location, null versus empty partition, missing current partition, active deletion-vector rejection |
| Native protocol | Delta handle/split JSON round trips and Thrift `jsonValue` routing |
| Adapter | Physical/logical names, partition date encoding, paths, nulls, info columns, filters, wrong-handle failures |
| CPU I/O | Unpartitioned, partitioned, filtered, evolved, and mapped reads through Velox |
| GPU I/O | Same supported shapes through the cuDF Iceberg data source using delete-free Delta splits |

## Build matrix

| Artifact | Toolchain | Result |
| --- | --- | --- |
| Java coordinator and connector | Pinned Java 17 / Maven repository | Pass |
| CPU Prestissimo worker | Checked-in runtime Dockerfile, pinned dependency image | Pass |
| GPU Prestissimo worker | Checked-in runtime Dockerfile, CUDA architecture 90 | Pass |
| GPU test runtime | Disposable full dependency-based image under `/tmp` | Pass |

No repository build or test script was changed for environment setup.
