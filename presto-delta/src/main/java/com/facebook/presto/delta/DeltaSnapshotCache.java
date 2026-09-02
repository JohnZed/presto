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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Cache of resolved Delta snapshots shared across queries.
 * <p>
 * A snapshot at a given version is immutable, so versioned entries are reused until they expire. The latest
 * version of a table is validated on every lookup with a single keyed listing of the transaction log tail, so
 * queries never observe a stale latest snapshot. When there are no new commits the expensive snapshot
 * construction (checkpoint discovery, protocol and metadata loading) is skipped entirely.
 */
@Singleton
public class DeltaSnapshotCache
{
    private static final Logger log = Logger.get(DeltaSnapshotCache.class);
    private static final String LOG_DIRECTORY = "_delta_log";
    private static final Pattern COMMIT_FILE = Pattern.compile("(\\d{20})\\.json");

    private final boolean enabled;
    private final Cache<SnapshotKey, Snapshot> snapshots;
    private final Cache<String, Long> latestVersions;

    @Inject
    public DeltaSnapshotCache(DeltaConfig config)
    {
        this(config.getMetadataCacheTtl(), config.getMetadataCacheMaxSize());
    }

    public DeltaSnapshotCache(Duration ttl, long maxSize)
    {
        requireNonNull(ttl, "ttl is null");
        this.enabled = ttl.toMillis() > 0 && maxSize > 0;
        this.snapshots = CacheBuilder.newBuilder()
                .expireAfterWrite(ttl.toMillis(), MILLISECONDS)
                .maximumSize(Math.max(maxSize, 0))
                .build();
        this.latestVersions = CacheBuilder.newBuilder()
                .expireAfterWrite(ttl.toMillis(), MILLISECONDS)
                .maximumSize(Math.max(maxSize, 0))
                .build();
    }

    /**
     * Returns the latest snapshot of the table, reusing the cached one when the transaction log has no commit
     * newer than the cached version.
     */
    public Snapshot getLatestSnapshot(String tableLocation, Engine engine, Supplier<Snapshot> loader)
    {
        requireNonNull(tableLocation, "tableLocation is null");
        requireNonNull(engine, "engine is null");
        if (!enabled) {
            return loader.get();
        }

        Long knownVersion = latestVersions.getIfPresent(tableLocation);
        if (knownVersion != null) {
            Snapshot cached = snapshots.getIfPresent(new SnapshotKey(tableLocation, knownVersion));
            if (cached != null && !hasNewerCommit(engine, tableLocation, knownVersion)) {
                return cached;
            }
        }

        Snapshot snapshot = cacheSnapshot(tableLocation, loader.get());
        latestVersions.put(tableLocation, snapshot.getVersion());
        return snapshot;
    }

    /**
     * Returns the snapshot at the given version, loading it at most once per cache lifetime.
     */
    public Snapshot getSnapshotAtVersion(String tableLocation, long version, Supplier<Snapshot> loader)
    {
        requireNonNull(tableLocation, "tableLocation is null");
        if (!enabled) {
            return loader.get();
        }
        try {
            return snapshots.get(new SnapshotKey(tableLocation, version), loader::get);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * Records a snapshot that was resolved by other means, such as time travel by timestamp, so that later
     * lookups of the same version are served from the cache.
     */
    public Snapshot cacheSnapshot(String tableLocation, Snapshot snapshot)
    {
        requireNonNull(snapshot, "snapshot is null");
        if (enabled) {
            snapshots.put(new SnapshotKey(tableLocation, snapshot.getVersion()), snapshot);
        }
        return snapshot;
    }

    @VisibleForTesting
    long getCachedSnapshotCount()
    {
        return snapshots.size();
    }

    /**
     * Lists the transaction log starting at the first commit after {@code version}. Any commit file found means
     * the cached snapshot is stale. Listing failures are treated as stale so the caller reloads and surfaces the
     * real error.
     */
    @VisibleForTesting
    static boolean hasNewerCommit(Engine engine, String tableLocation, long version)
    {
        Path logDirectory = new Path(tableLocation, LOG_DIRECTORY);
        String startFile = new Path(logDirectory, format("%020d.json", version + 1)).toString();
        try (CloseableIterator<FileStatus> files = engine.getFileSystemClient().listFrom(startFile)) {
            while (files.hasNext()) {
                Matcher matcher = COMMIT_FILE.matcher(new Path(files.next().getPath()).getName());
                if (matcher.matches() && Long.parseLong(matcher.group(1)) > version) {
                    return true;
                }
            }
            return false;
        }
        catch (IOException | RuntimeException e) {
            log.debug(e, "Could not check %s for commits after version %d; reloading snapshot", logDirectory, version);
            return true;
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
            return version == that.version && tableLocation.equals(that.tableLocation);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableLocation, version);
        }
    }
}
