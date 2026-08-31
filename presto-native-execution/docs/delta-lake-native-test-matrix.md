# Native Delta Lake read compatibility matrix

This matrix defines the read contract shared by the Java Delta coordinator,
the CPU-native worker, and the cuDF-native worker. It is deliberately scoped
to Delta Parquet reads without deletion-vector application.

## Path and split contract

- A data-file URI (`s3://`, `s3a://`, `file:/`, or another `scheme://` URI) is
  used without modification by the Presto-to-Velox adapter. The filesystem
  layer remains responsible for any supported scheme normalization.
- A local absolute path beginning with `/` is used without modification.
- A relative AddFile path is appended to the Delta table location with exactly
  one `/` separator. Escaping in either component is preserved.
- `partitionValues` carries non-null strings, including the empty string.
  `nullPartitionKeys` carries SQL nulls. A key in both collections is invalid.
- `$path` is the resolved data-file path and `$file_size` is the AddFile size.
- An active AddFile with a deletion-vector descriptor fails with
  `NOT_SUPPORTED`. Advertising the table protocol feature alone does not fail.

## Java compatibility suites

| Feature | Java source of truth | CPU-native expectation | GPU-native expectation |
| --- | --- | --- | --- |
| Primitive, array, map and row projection | `TestDeltaIntegration` | Run inherited assertions | Run supported scan types through cuDF |
| Latest snapshot and HMS registration | `TestDeltaIntegration` | Run inherited assertions | Same coordinator fixtures and results |
| Version/timestamp time travel | `TestDeltaIntegration` | Run inherited assertions | Run; snapshot selection is coordinator-side |
| Partition values and all primitive partition types | `TestDeltaIntegration` | Run inherited assertions | Run except timestamp-with-time-zone values |
| Regular and partition filters | `TestDeltaScanOptimizations` | Run inherited assertions | Run scan-focused cases; prove cuDF data source used |
| Null/not-null partition filters | `TestDeltaScanOptimizations` | Run inherited assertions plus empty-string regression | Same, with GPU constant injection |
| Nested filter | `TestDeltaScanOptimizations.nestedColumnFilter` | Run when physical-path mapping is safe | Fail clearly or skip with cuDF nested-selection limitation |
| Add/remove file history and missing columns | `TestIncrementalUpdateQueries` | Run inherited assertions | Run projection/null-fill subset |
| Case-sensitive regular/partition columns | `TestUppercasePartitionColumns` | Run inherited assertions | Run scan-focused subset |
| Name/id mapping, rename, drop, special characters | `TestColumnMapping` | Run inherited assertions | Run top-level mapping; document nested gaps |
| Variant | `TestDeltaVariantType` | Run if current Velox variant mapping accepts the wire type | Count/projection only when cuDF type support exists |
| Timestamp with time zone data columns | `TestDeltaIntegration` timestamp cases | Run on CPU | Excluded for the separate known GPU timestamp issue |
| Writes and DDL mutation | Java write/unsupported tests | Out of scope; existing coordinator behavior remains | Out of scope |

## Native-only coverage

| Layer | Required tests |
| --- | --- |
| Java split contract | JSON round trip, relative table location, null versus empty partition, overlapping-key rejection, active deletion-vector rejection |
| Native protocol | Delta split and enum transaction JSON round trips; Thrift `jsonValue` fallback using the `hive-delta` discriminator |
| Adapter | Physical/logical names, partition date encoding, relative/absolute paths, null/empty partitions, info columns, filter safety, wrong-handle failures |
| CPU I/O | Unpartitioned, partitioned, missing/mapped columns, filter-only columns, and zero physical projected columns |
| GPU I/O | Same scan shapes, with an assertion that the cuDF Iceberg data-source/read path selected for the delete-free Delta split |
| Regressions | Focused Hive and Iceberg CPU tests plus cuDF Hive/Iceberg injection tests |

Skips must name an existing backend type/operator limitation. A failure caused
by Delta split planning, path resolution, partition synthesis, schema
adaptation, or column mapping is not an acceptable skip.
