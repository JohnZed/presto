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
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.HadoopExtendedFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestPrestoS3LogStore
{
    private static final URI BUCKET = URI.create("s3://bucket/");
    private static final Path LOG_DIRECTORY = new Path("s3://bucket/table/_delta_log");

    @Test
    public void testUnwrapsPrestoFileSystemWrappers()
    {
        RecordingS3FileSystem s3FileSystem = new RecordingS3FileSystem(ImmutableList.of());

        assertSame(PrestoS3LogStore.unwrapPrestoS3FileSystem(s3FileSystem).get(), s3FileSystem);
        assertSame(PrestoS3LogStore.unwrapPrestoS3FileSystem(new HadoopExtendedFileSystem(s3FileSystem)).get(), s3FileSystem);
        assertSame(
                PrestoS3LogStore.unwrapPrestoS3FileSystem(new FilterFileSystem(new HadoopExtendedFileSystem(s3FileSystem))).get(),
                s3FileSystem);
        assertFalse(PrestoS3LogStore.unwrapPrestoS3FileSystem(new LocalFileSystem()).isPresent());
        assertFalse(PrestoS3LogStore.unwrapPrestoS3FileSystem(new HadoopExtendedFileSystem(new LocalFileSystem())).isPresent());
        assertFalse(PrestoS3LogStore.unwrapPrestoS3FileSystem(null).isPresent());
    }

    @Test
    public void testListFromThroughWrapperUsesKeyedListing()
            throws IOException
    {
        RecordingS3FileSystem s3FileSystem = new RecordingS3FileSystem(ImmutableList.of(
                "00000000000000000009.json",
                "00000000000000000012.json",
                "00000000000000000010.checkpoint.parquet",
                "00000000000000000010.json",
                "00000000000000000011.json",
                "_last_checkpoint"));
        Configuration configuration = new Configuration(false);
        PrestoS3LogStore logStore = new PrestoS3LogStore(configuration);
        Path start = new Path(LOG_DIRECTORY, "00000000000000000010.json");

        Iterator<FileStatus> listed = logStore.listFrom(new HadoopExtendedFileSystem(s3FileSystem), start, configuration);

        assertEquals(s3FileSystem.listFilesFromCalls, ImmutableList.of(start));
        List<String> names = new ArrayList<>();
        listed.forEachRemaining(status -> names.add(status.getPath().getName()));
        assertEquals(names, ImmutableList.of(
                "00000000000000000010.json",
                "00000000000000000011.json",
                "00000000000000000012.json",
                "_last_checkpoint"));
    }

    @Test
    public void testListFromRequiresExistingLogDirectory()
    {
        RecordingS3FileSystem s3FileSystem = new RecordingS3FileSystem(ImmutableList.of());
        s3FileSystem.directoryExists = false;
        Configuration configuration = new Configuration(false);
        PrestoS3LogStore logStore = new PrestoS3LogStore(configuration);

        try {
            logStore.listFrom(new HadoopExtendedFileSystem(s3FileSystem), new Path(LOG_DIRECTORY, "00000000000000000000.json"), configuration);
            throw new AssertionError("expected FileNotFoundException");
        }
        catch (IOException e) {
            assertTrue(e instanceof java.io.FileNotFoundException, e.toString());
            assertTrue(s3FileSystem.listFilesFromCalls.isEmpty());
        }
    }

    /**
     * Stands in for an initialized {@link PrestoS3FileSystem} without contacting S3. Records keyed listing
     * requests and returns the configured log file names in an arbitrary order so sorting is exercised.
     */
    private static class RecordingS3FileSystem
            extends PrestoS3FileSystem
    {
        private final List<String> logFileNames;
        private final List<Path> listFilesFromCalls = new ArrayList<>();
        private boolean directoryExists = true;

        RecordingS3FileSystem(List<String> logFileNames)
        {
            this.logFileNames = logFileNames;
        }

        @Override
        public URI getUri()
        {
            return BUCKET;
        }

        @Override
        public Path getWorkingDirectory()
        {
            return new Path(BUCKET);
        }

        @Override
        public FileStatus getFileStatus(Path path)
                throws IOException
        {
            if (!directoryExists) {
                throw new java.io.FileNotFoundException(path.toString());
            }
            return new FileStatus(0, true, 1, 1, 0, path);
        }

        @Override
        public RemoteIterator<LocatedFileStatus> listFilesFrom(Path path)
        {
            listFilesFromCalls.add(path);
            Iterator<LocatedFileStatus> statuses = logFileNames.stream()
                    .filter(name -> name.compareTo(path.getName()) >= 0)
                    .map(name -> new LocatedFileStatus(
                            new FileStatus(1, false, 1, 1, 0, new Path(LOG_DIRECTORY, name)),
                            null))
                    .collect(Collectors.toList())
                    .iterator();
            return new RemoteIterator<LocatedFileStatus>()
            {
                @Override
                public boolean hasNext()
                {
                    return statuses.hasNext();
                }

                @Override
                public LocatedFileStatus next()
                {
                    return statuses.next();
                }
            };
        }
    }
}
