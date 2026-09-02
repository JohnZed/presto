/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.delta;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.utils.CloseableIterator;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.delta.DeltaTable.DataFormat.PARQUET;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;

/**
 * Class to interact with Delta lake table APIs.
 */
public class DeltaClient
{
    private static final String TABLE_NOT_FOUND_ERROR_TEMPLATE = "Delta table (%s.%s) no longer exists.";
    private static final String LOG_STORE_CONFIG_PREFIX = "io.delta.kernel.logStore.";
    // Lets the stock delta-storage S3 log store use S3A's start-after listing when Presto's S3 filesystem is not in use.
    private static final String FAST_S3A_LIST_FROM = "delta.enableFastS3AListFrom";
    private final HdfsEnvironment hdfsEnvironment;
    private final DeltaQuerySnapshotCache snapshotCache;
    private final DeltaSnapshotCache sharedSnapshotCache;

    @Inject
    public DeltaClient(HdfsEnvironment hdfsEnvironment, DeltaQuerySnapshotCache snapshotCache, DeltaSnapshotCache sharedSnapshotCache)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.snapshotCache = requireNonNull(snapshotCache, "snapshotCache is null");
        this.sharedSnapshotCache = requireNonNull(sharedSnapshotCache, "sharedSnapshotCache is null");
    }

    /**
     * Load the delta table.
     *
     * @param session                     Current user session
     * @param schemaTableName             Schema and table name referred to as in the query
     * @param tableLocation               Location of the Delta table on storage
     * @param snapshotId                  Id of the snapshot to read from the Delta table
     * @param snapshotAsOfTimestampMillis Latest snapshot as of given timestamp
     * @return If the table is found return {@link DeltaTable}.
     */
    public Optional<DeltaTable> getTable(
            DeltaConfig config,
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String tableLocation,
            Optional<Long> snapshotId,
            Optional<Long> snapshotAsOfTimestampMillis)
    {
        return snapshotCache.getTable(
                session.getQueryId(),
                schemaTableName,
                tableLocation,
                snapshotId,
                snapshotAsOfTimestampMillis,
                () -> loadTable(config, session, schemaTableName, tableLocation, snapshotId, snapshotAsOfTimestampMillis));
    }

    private Optional<DeltaTable> loadTable(
            DeltaConfig config,
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String tableLocation,
            Optional<Long> snapshotId,
            Optional<Long> snapshotAsOfTimestampMillis)
    {
        Path location = new Path(tableLocation);
        Optional<Engine> deltaEngine = loadDeltaEngine(session, location, schemaTableName);
        if (!deltaEngine.isPresent()) {
            return Optional.empty();
        }

        Table deltaTable = loadDeltaTable(location.toString(), deltaEngine.get());
        Snapshot snapshot = getSnapshot(deltaTable, deltaEngine.get(), tableLocation, schemaTableName, snapshotId,
                snapshotAsOfTimestampMillis);
        snapshotCache.putSnapshot(session.getQueryId(), tableLocation, snapshot.getVersion(), deltaEngine.get(), snapshot);
        return Optional.of(new DeltaTable(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                tableLocation,
                Optional.of(snapshot.getVersion()), // lock the snapshot version
                getSchema(config, schemaTableName, deltaEngine.get(), snapshot)));
    }

    private Snapshot getSnapshot(
            Table deltaTable,
            Engine deltaEngine,
            String tableLocation,
            SchemaTableName schemaTableName,
            Optional<Long> snapshotId,
            Optional<Long> snapshotAsOfTimestampMillis)
    {
        // Fetch the snapshot info for given snapshot version. If no snapshot version is given, get the latest snapshot info.
        // Lock the snapshot version here and use it later in the rest of the query (such as fetching file list etc.).
        // If we don't lock the snapshot version here, the query may end up with schema from one version and data files from another
        // version when the underlying delta table is changing while the query is running.
        // Snapshots are immutable per version, so they are shared across queries; the latest version is revalidated
        // against the transaction log tail on every lookup.
        Snapshot snapshot;
        if (snapshotId.isPresent()) {
            snapshot = sharedSnapshotCache.getSnapshotAtVersion(
                    tableLocation,
                    snapshotId.get(),
                    () -> getSnapshotById(deltaTable, deltaEngine, snapshotId.get(), schemaTableName));
        }
        else if (snapshotAsOfTimestampMillis.isPresent()) {
            snapshot = sharedSnapshotCache.cacheSnapshot(
                    tableLocation,
                    getSnapshotAsOfTimestamp(deltaTable, deltaEngine, snapshotAsOfTimestampMillis.get(), schemaTableName));
        }
        else {
            snapshot = sharedSnapshotCache.getLatestSnapshot(
                    tableLocation,
                    deltaEngine,
                    () -> getLatestSnapshot(deltaTable, deltaEngine, schemaTableName));
        }

        if (snapshot instanceof SnapshotImpl) {
            String format = ((SnapshotImpl) snapshot).getMetadata().getFormat().getProvider();
            if (!PARQUET.name().equalsIgnoreCase(format)) {
                throw new PrestoException(DeltaErrorCode.DELTA_UNSUPPORTED_DATA_FORMAT,
                        format("Delta table %s has unsupported data format: %s. Only the Parquet data format is supported", schemaTableName, format));
            }
        }
        return snapshot;
    }

    /**
     * Get the list of files corresponding to the given Delta table.
     *
     * @return Closeable iterator of files. It is responsibility of the caller to close the iterator.
     */
    public CloseableIterator<FilteredColumnarBatch> listFiles(ConnectorSession session, DeltaTable deltaTable)
    {
        requireNonNull(deltaTable, "deltaTable is null");
        checkArgument(deltaTable.getSnapshotId().isPresent(), "Snapshot id is missing from the Delta table");
        long snapshotId = deltaTable.getSnapshotId().get();
        try {
            DeltaQuerySnapshotCache.ResolvedSnapshot resolvedSnapshot = snapshotCache.getSnapshot(
                            session.getQueryId(),
                            deltaTable.getTableLocation(),
                            snapshotId)
                    .orElseGet(() -> loadSnapshot(session, deltaTable, snapshotId));
            return resolvedSnapshot.getSnapshot().getScanBuilder().build()
                    .getScanFiles(resolvedSnapshot.getEngine());
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.NOT_FOUND,
                    format("Delta table not found in '%s'", deltaTable.getTableLocation()), e);
        }
    }

    private DeltaQuerySnapshotCache.ResolvedSnapshot loadSnapshot(ConnectorSession session, DeltaTable deltaTable, long snapshotId)
    {
        Optional<Engine> deltaEngine = loadDeltaEngine(
                session,
                new Path(deltaTable.getTableLocation()),
                new SchemaTableName(deltaTable.getSchemaName(), deltaTable.getTableName()));
        if (!deltaEngine.isPresent()) {
            throw new PrestoException(DeltaErrorCode.DELTA_ERROR_LOADING_METADATA,
                    format("Could not obtain Delta engine in '%s'", deltaTable.getTableLocation()));
        }
        Table sourceTable = loadDeltaTable(deltaTable.getTableLocation(), deltaEngine.get());
        Snapshot snapshot = sharedSnapshotCache.getSnapshotAtVersion(
                deltaTable.getTableLocation(),
                snapshotId,
                () -> sourceTable.getSnapshotAsOfVersion(deltaEngine.get(), snapshotId));
        return snapshotCache.putSnapshot(session.getQueryId(), deltaTable.getTableLocation(), snapshotId, deltaEngine.get(), snapshot);
    }

    public void cleanupQuery(String queryId)
    {
        snapshotCache.cleanupQuery(queryId);
    }

    private Optional<Engine> loadDeltaEngine(ConnectorSession session, Path tableLocation,
                                                       SchemaTableName schemaTableName)
    {
        try {
            HdfsContext hdfsContext = new HdfsContext(
                    session,
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    tableLocation.toString(),
                    false);
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, tableLocation);
            if (!fileSystem.isDirectory(tableLocation)) {
                return Optional.empty();
            }
            return Optional.of(DefaultEngine.create(configureDeltaLogStore(fileSystem.getConf())));
        }
        catch (IOException ioException) {
            throw new PrestoException(DeltaErrorCode.DELTA_ERROR_LOADING_METADATA,
                    "Failed to load Delta table: " + ioException.getMessage(), ioException);
        }
    }

    static Configuration configureDeltaLogStore(Configuration source)
    {
        Configuration configuration = new Configuration(requireNonNull(source, "source is null"));
        for (String scheme : new String[] {"s3", "s3a", "s3n"}) {
            configuration.set(LOG_STORE_CONFIG_PREFIX + scheme + ".impl", PrestoS3LogStore.class.getName());
        }
        configuration.setBoolean(FAST_S3A_LIST_FROM, true);
        return configuration;
    }

    private Table loadDeltaTable(String tableLocation, Engine deltaEngine)
    {
        return Table.forPath(deltaEngine, tableLocation);
    }

    private static Snapshot getLatestSnapshot(Table deltaTable, Engine deltaEngine, SchemaTableName schemaTableName)
    {
        try {
            return deltaTable.getLatestSnapshot(deltaEngine);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.NOT_FOUND,
                    format("Could not move to latest snapshot on table '%s.%s'", schemaTableName.getSchemaName(),
                            schemaTableName.getTableName()), e);
        }
    }

    private static Snapshot getSnapshotById(Table deltaTable, Engine deltaEngine, long snapshotId, SchemaTableName schemaTableName)
    {
        try {
            return deltaTable.getSnapshotAsOfVersion(deltaEngine, snapshotId);
        }
        catch (IllegalArgumentException exception) {
            throw new PrestoException(
                    StandardErrorCode.NOT_FOUND,
                    format("Snapshot version %d does not exist in Delta table '%s'.", snapshotId, schemaTableName),
                    exception);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.NOT_FOUND,
                    format(TABLE_NOT_FOUND_ERROR_TEMPLATE, schemaTableName.getSchemaName(),
                            schemaTableName.getTableName()));
        }
    }

    private static Snapshot getSnapshotAsOfTimestamp(Table deltaTable, Engine deltaEngine,
                                                     long snapshotAsOfTimestampMillis, SchemaTableName schemaTableName)
    {
        try {
            return deltaTable.getSnapshotAsOfTimestamp(deltaEngine, snapshotAsOfTimestampMillis);
        }
        catch (IllegalArgumentException exception) {
            throw new PrestoException(
                    StandardErrorCode.NOT_FOUND,
                    format(
                            "There is no snapshot exists in Delta table '%s' that is created on or before '%s'",
                            schemaTableName,
                            Instant.ofEpochMilli(snapshotAsOfTimestampMillis)),
                    exception);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.NOT_FOUND,
                    format(TABLE_NOT_FOUND_ERROR_TEMPLATE, schemaTableName.getSchemaName(),
                            schemaTableName.getTableName()));
        }
    }

    /**
     * Utility method that returns the columns in given Delta metadata. Returned columns include regular and partition types.
     * Data type from Delta is mapped to appropriate Presto data type.
     * <p>
     * Partition columns come from the snapshot metadata rather than from replaying the log to find an AddFile.
     * That keeps table resolution independent of the number of files and works when column mapping stores
     * partition values under physical names.
     */
    static List<DeltaColumn> getSchema(DeltaConfig config, SchemaTableName tableName, Engine deltaEngine, Snapshot snapshot)
    {
        Set<String> partitionColumns = snapshot.getPartitionColumnNames().stream()
                .map(name -> normalizeColumnName(config, name))
                .collect(toImmutableSet());
        return snapshot.getSchema().fields().stream()
                .map(field -> {
                    String columnName = normalizeColumnName(config, field.getName());
                    TypeSignature prestoType = DeltaTypeUtils.convertDeltaDataTypePrestoDataType(tableName,
                            columnName, field.getDataType());
                    return new DeltaColumn(
                            DeltaColumnMetadataUtil.getColumnIdFromMetadata(field.getMetadata()),
                            DeltaColumnMetadataUtil.getPhysicalNameFromMetadata(field.getMetadata()),
                            columnName,
                            prestoType,
                            field.isNullable(),
                            partitionColumns.contains(columnName));
                }).collect(Collectors.toList());
    }

    private static String normalizeColumnName(DeltaConfig config, String columnName)
    {
        return config.isCaseSensitivePartitionsEnabled() ? columnName : columnName.toLowerCase(US);
    }
}
