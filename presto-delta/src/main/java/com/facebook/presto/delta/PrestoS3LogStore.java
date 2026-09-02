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

import com.facebook.presto.hive.s3.PrestoS3FileSystem;
import io.delta.storage.S3SingleDriverLogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Delta log store that maps listFrom to S3 ListObjectsV2 StartAfter when Presto's S3 filesystem is in use.
 */
public class PrestoS3LogStore
        extends S3SingleDriverLogStore
{
    public PrestoS3LogStore(Configuration configuration)
    {
        super(configuration);
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration configuration)
            throws IOException
    {
        FileSystem fileSystem = path.getFileSystem(configuration);
        if (!(fileSystem instanceof PrestoS3FileSystem)) {
            return super.listFrom(path, configuration);
        }

        Path resolvedPath = fileSystem.makeQualified(path);
        Path parent = resolvedPath.getParent();
        if (parent == null || !fileSystem.exists(parent)) {
            throw new FileNotFoundException("No such file or directory: " + parent);
        }

        RemoteIterator<LocatedFileStatus> iterator = ((PrestoS3FileSystem) fileSystem).listFilesFrom(resolvedPath);
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
}
