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

import com.facebook.presto.spi.SchemaTableName;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import jakarta.inject.Singleton;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Query-scoped cache for Delta table handles and immutable versioned snapshots.
 */
@Singleton
public class DeltaQuerySnapshotCache
{
    private final ConcurrentMap<String, ConcurrentMap<TableKey, Optional<DeltaTable>>> tables = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<SnapshotKey, ResolvedSnapshot>> snapshots = new ConcurrentHashMap<>();

    public Optional<DeltaTable> getTable(
            String queryId,
            SchemaTableName tableName,
            String tableLocation,
            Optional<Long> requestedVersion,
            Optional<Long> requestedTimestamp,
            Supplier<Optional<DeltaTable>> loader)
    {
        requireNonNull(loader, "loader is null");
        TableKey key = new TableKey(tableName, tableLocation, requestedVersion, requestedTimestamp);
        return tables.computeIfAbsent(requireNonNull(queryId, "queryId is null"), ignored -> new ConcurrentHashMap<>())
                .computeIfAbsent(key, ignored -> requireNonNull(loader.get(), "loader returned null"));
    }

    public ResolvedSnapshot putSnapshot(String queryId, String tableLocation, long version, Engine engine, Snapshot snapshot)
    {
        ConcurrentMap<SnapshotKey, ResolvedSnapshot> querySnapshots = snapshots.computeIfAbsent(
                requireNonNull(queryId, "queryId is null"),
                ignored -> new ConcurrentHashMap<>());
        SnapshotKey key = new SnapshotKey(tableLocation, version);
        querySnapshots.putIfAbsent(key, new ResolvedSnapshot(engine, snapshot));
        return querySnapshots.get(key);
    }

    public Optional<ResolvedSnapshot> getSnapshot(String queryId, String tableLocation, long version)
    {
        return Optional.ofNullable(snapshots.get(queryId))
                .map(querySnapshots -> querySnapshots.get(new SnapshotKey(tableLocation, version)));
    }

    public void cleanupQuery(String queryId)
    {
        tables.remove(queryId);
        snapshots.remove(queryId);
    }

    int getCachedQueryCount()
    {
        return tables.size();
    }

    public static final class ResolvedSnapshot
    {
        private final Engine engine;
        private final Snapshot snapshot;

        private ResolvedSnapshot(Engine engine, Snapshot snapshot)
        {
            this.engine = requireNonNull(engine, "engine is null");
            this.snapshot = requireNonNull(snapshot, "snapshot is null");
        }

        public Engine getEngine()
        {
            return engine;
        }

        public Snapshot getSnapshot()
        {
            return snapshot;
        }
    }

    private static final class TableKey
    {
        private final SchemaTableName tableName;
        private final String tableLocation;
        private final Optional<Long> requestedVersion;
        private final Optional<Long> requestedTimestamp;

        private TableKey(
                SchemaTableName tableName,
                String tableLocation,
                Optional<Long> requestedVersion,
                Optional<Long> requestedTimestamp)
        {
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
            this.requestedVersion = requireNonNull(requestedVersion, "requestedVersion is null");
            this.requestedTimestamp = requireNonNull(requestedTimestamp, "requestedTimestamp is null");
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            TableKey tableKey = (TableKey) object;
            return Objects.equals(tableName, tableKey.tableName) &&
                    Objects.equals(tableLocation, tableKey.tableLocation) &&
                    Objects.equals(requestedVersion, tableKey.requestedVersion) &&
                    Objects.equals(requestedTimestamp, tableKey.requestedTimestamp);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName, tableLocation, requestedVersion, requestedTimestamp);
        }
    }

    private static final class SnapshotKey
    {
        private final String tableLocation;
        private final long version;

        private SnapshotKey(String tableLocation, long version)
        {
            this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
            this.version = version;
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            SnapshotKey that = (SnapshotKey) object;
            return version == that.version && Objects.equals(tableLocation, that.tableLocation);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableLocation, version);
        }
    }
}
