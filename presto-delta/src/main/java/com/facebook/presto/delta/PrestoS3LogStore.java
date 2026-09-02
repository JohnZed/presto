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
import com.facebook.presto.hive.s3.PrestoS3FileSystem;
import io.delta.storage.S3SingleDriverLogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.HadoopExtendedFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Delta log store that maps listFrom to S3 ListObjectsV2 StartAfter when Presto's S3 filesystem is in use.
 * <p>
 * The stock {@link S3SingleDriverLogStore#listFrom} lists the entire {@code _delta_log} directory and filters
 * in memory, which costs one S3 LIST call per thousand log files on every snapshot load. Presto's filesystem
 * cache hands out {@link HadoopExtendedFileSystem} wrappers rather than the raw {@link PrestoS3FileSystem},
 * so the wrapper chain is unwrapped before deciding whether the efficient listing is available.
 */
public class PrestoS3LogStore
        extends S3SingleDriverLogStore
{
    private static final Logger log = Logger.get(PrestoS3LogStore.class);
    private static final Set<String> REPORTED_FALLBACK_FILE_SYSTEMS = ConcurrentHashMap.newKeySet();

    public PrestoS3LogStore(Configuration configuration)
    {
        super(configuration);
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration configuration)
            throws IOException
    {
        return listFrom(path.getFileSystem(configuration), path, configuration);
    }

    Iterator<FileStatus> listFrom(FileSystem fileSystem, Path path, Configuration configuration)
            throws IOException
    {
        Optional<PrestoS3FileSystem> s3FileSystem = unwrapPrestoS3FileSystem(fileSystem);
        if (!s3FileSystem.isPresent()) {
            reportFallback(fileSystem, path);
            return super.listFrom(path, configuration);
        }

        Path resolvedPath = fileSystem.makeQualified(path);
        Path parent = resolvedPath.getParent();
        if (parent == null || !fileSystem.exists(parent)) {
            throw new FileNotFoundException("No such file or directory: " + parent);
        }

        RemoteIterator<LocatedFileStatus> iterator = s3FileSystem.get().listFilesFrom(resolvedPath);
        List<FileStatus> statuses = new ArrayList<>();
        while (iterator.hasNext()) {
            FileStatus status = iterator.next();
            if (status.getPath().getName().compareTo(resolvedPath.getName()) >= 0) {
                statuses.add(status);
            }
        }
        statuses.sort(Comparator.comparing(status -> status.getPath().getName()));
        return statuses.iterator();
    }

    /**
     * Presto wraps every Hadoop filesystem in {@link HadoopExtendedFileSystem}, and deployments may add further
     * {@link FilterFileSystem} layers. Walk the chain to find the S3 filesystem that supports listing from a key.
     */
    static Optional<PrestoS3FileSystem> unwrapPrestoS3FileSystem(FileSystem fileSystem)
    {
        FileSystem current = fileSystem;
        while (current != null) {
            if (current instanceof PrestoS3FileSystem) {
                return Optional.of((PrestoS3FileSystem) current);
            }
            if (current instanceof HadoopExtendedFileSystem) {
                current = ((HadoopExtendedFileSystem) current).getRawFileSystem();
            }
            else if (current instanceof FilterFileSystem) {
                current = ((FilterFileSystem) current).getRawFileSystem();
            }
            else {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private static void reportFallback(FileSystem fileSystem, Path path)
    {
        String fileSystemClass = fileSystem == null ? "null" : fileSystem.getClass().getName();
        if (REPORTED_FALLBACK_FILE_SYSTEMS.add(fileSystemClass)) {
            log.warn("Delta log listing for %s uses filesystem %s, which does not support listing from a key. " +
                    "Falling back to a full directory listing, which is slow for tables with long histories.",
                    path.getParent(), fileSystemClass);
        }
    }
}
