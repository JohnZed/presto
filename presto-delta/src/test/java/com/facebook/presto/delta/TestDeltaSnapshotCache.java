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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.spi.PrestoException;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDeltaSnapshotCache
{
    private static final String FIXTURE = "delta_v3/deltatbl-partition-prune-incremental";
    private static final String LOG_DIRECTORY = "_delta_log";

    private File fixtureDirectory;
    private File tableDirectory;
    private Engine engine;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        fixtureDirectory = new File(getClass().getClassLoader().getResource(FIXTURE).toURI());
        tableDirectory = Files.createTempDirectory("presto-delta-snapshot-cache").toFile();
        // Materialize version 0 only; later commits are appended by the tests to simulate concurrent writers.
        for (File file : fixtureDirectory.listFiles()) {
            if (!file.isDirectory()) {
                Files.copy(file.toPath(), new File(tableDirectory, file.getName()).toPath());
            }
        }
        File logDirectory = new File(tableDirectory, LOG_DIRECTORY);
        assertTrue(logDirectory.mkdir());
        copyLogFile("00000000000000000000.json");
        engine = DefaultEngine.create(new Configuration());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (tableDirectory != null) {
            deleteRecursively(tableDirectory.toPath(), ALLOW_INSECURE);
        }
    }

    @Test
    public void testLatestSnapshotIsReusedUntilNewCommitAppears()
            throws IOException
    {
        DeltaSnapshotCache cache = new DeltaSnapshotCache(new Duration(10, MINUTES), 100);
        AtomicInteger loads = new AtomicInteger();

        Snapshot first = cache.getLatestSnapshot(location(), engine, () -> loadLatest(loads));
        assertEquals(first.getVersion(), 0);
        assertEquals(loads.get(), 1);

        Snapshot repeated = cache.getLatestSnapshot(location(), engine, () -> loadLatest(loads));
        assertSame(repeated, first);
        assertEquals(loads.get(), 1);

        copyLogFile("00000000000000000001.json");

        Snapshot refreshed = cache.getLatestSnapshot(location(), engine, () -> loadLatest(loads));
        assertEquals(refreshed.getVersion(), 1);
        assertNotSame(refreshed, first);
        assertEquals(loads.get(), 2);
        assertSame(cache.getLatestSnapshot(location(), engine, () -> loadLatest(loads)), refreshed);
        assertEquals(loads.get(), 2);

        // Both versions stay available for time travel without reloading.
        assertSame(cache.getSnapshotAtVersion(location(), 0, () -> loadVersion(loads, 0)), first);
        assertSame(cache.getSnapshotAtVersion(location(), 1, () -> loadVersion(loads, 1)), refreshed);
        assertEquals(loads.get(), 2);
        assertEquals(cache.getCachedSnapshotCount(), 2);
    }

    @Test
    public void testVersionedSnapshotLoadsOnce()
    {
        DeltaSnapshotCache cache = new DeltaSnapshotCache(new Duration(10, MINUTES), 100);
        AtomicInteger loads = new AtomicInteger();

        Snapshot snapshot = cache.getSnapshotAtVersion(location(), 0, () -> loadVersion(loads, 0));
        assertSame(cache.getSnapshotAtVersion(location(), 0, () -> loadVersion(loads, 0)), snapshot);
        assertEquals(loads.get(), 1);

        // A different table location with the same version is a different entry.
        assertNotSame(cache.getSnapshotAtVersion(fixtureDirectory.getPath(), 0, () -> loadFixtureVersion(loads, 0)), snapshot);
        assertEquals(loads.get(), 2);
    }

    @Test
    public void testLoaderFailuresPropagateAndAreNotCached()
    {
        DeltaSnapshotCache cache = new DeltaSnapshotCache(new Duration(10, MINUTES), 100);
        AtomicInteger loads = new AtomicInteger();

        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                cache.getSnapshotAtVersion(location(), 7, () -> {
                    loads.incrementAndGet();
                    throw new PrestoException(NOT_FOUND, "no such version");
                });
                throw new AssertionError("expected PrestoException");
            }
            catch (PrestoException e) {
                assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
            }
        }
        assertEquals(loads.get(), 2);
        assertEquals(cache.getCachedSnapshotCount(), 0);
    }

    @Test
    public void testDisabledCacheAlwaysLoads()
    {
        DeltaSnapshotCache cache = new DeltaSnapshotCache(new Duration(0, MINUTES), 100);
        AtomicInteger loads = new AtomicInteger();

        cache.getLatestSnapshot(location(), engine, () -> loadLatest(loads));
        cache.getLatestSnapshot(location(), engine, () -> loadLatest(loads));
        cache.getSnapshotAtVersion(location(), 0, () -> loadVersion(loads, 0));
        assertEquals(loads.get(), 3);
        assertEquals(cache.getCachedSnapshotCount(), 0);
    }

    @Test
    public void testHasNewerCommit()
            throws IOException
    {
        assertFalse(DeltaSnapshotCache.hasNewerCommit(engine, location(), 0));
        // Checksum and checkpoint files alone do not indicate a new commit.
        copyLogFile("00000000000000000002.crc");
        assertFalse(DeltaSnapshotCache.hasNewerCommit(engine, location(), 0));

        copyLogFile("00000000000000000001.json");
        assertTrue(DeltaSnapshotCache.hasNewerCommit(engine, location(), 0));
        assertFalse(DeltaSnapshotCache.hasNewerCommit(engine, location(), 1));

        // A missing log directory is reported as stale so the caller reloads and sees the real error.
        assertTrue(DeltaSnapshotCache.hasNewerCommit(engine, new File(tableDirectory, "missing").getPath(), 0));
    }

    private String location()
    {
        return tableDirectory.getPath();
    }

    private Snapshot loadLatest(AtomicInteger loads)
    {
        loads.incrementAndGet();
        return Table.forPath(engine, location()).getLatestSnapshot(engine);
    }

    private Snapshot loadVersion(AtomicInteger loads, long version)
    {
        loads.incrementAndGet();
        return Table.forPath(engine, location()).getSnapshotAsOfVersion(engine, version);
    }

    private Snapshot loadFixtureVersion(AtomicInteger loads, long version)
    {
        loads.incrementAndGet();
        return Table.forPath(engine, fixtureDirectory.getPath()).getSnapshotAsOfVersion(engine, version);
    }

    private void copyLogFile(String name)
            throws IOException
    {
        Files.copy(
                new File(new File(fixtureDirectory, LOG_DIRECTORY), name).toPath(),
                new File(new File(tableDirectory, LOG_DIRECTORY), name).toPath(),
                StandardCopyOption.REPLACE_EXISTING);
    }
}
