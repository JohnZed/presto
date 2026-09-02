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
import com.google.common.collect.ImmutableList;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestDeltaQuerySnapshotCache
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("schema", "table");
    private static final String TABLE_LOCATION = "s3://bucket/table";

    @Test
    public void testPinsTableWithinQueryAndRefreshesNextQuery()
    {
        DeltaQuerySnapshotCache cache = new DeltaQuerySnapshotCache();
        AtomicInteger loads = new AtomicInteger();

        DeltaTable first = cache.getTable(
                "query-1",
                TABLE_NAME,
                TABLE_LOCATION,
                Optional.empty(),
                Optional.empty(),
                () -> Optional.of(table(loads.incrementAndGet())))
                .orElseThrow(AssertionError::new);
        DeltaTable repeated = cache.getTable(
                "query-1",
                TABLE_NAME,
                TABLE_LOCATION,
                Optional.empty(),
                Optional.empty(),
                () -> Optional.of(table(loads.incrementAndGet())))
                .orElseThrow(AssertionError::new);
        DeltaTable nextQuery = cache.getTable(
                "query-2",
                TABLE_NAME,
                TABLE_LOCATION,
                Optional.empty(),
                Optional.empty(),
                () -> Optional.of(table(loads.incrementAndGet())))
                .orElseThrow(AssertionError::new);

        assertSame(repeated, first);
        assertEquals(first.getSnapshotId(), Optional.of(1L));
        assertEquals(nextQuery.getSnapshotId(), Optional.of(2L));
        assertEquals(loads.get(), 2);
    }

    @Test
    public void testSnapshotCacheIsVersionedAndQueryScoped()
    {
        DeltaQuerySnapshotCache cache = new DeltaQuerySnapshotCache();
        Engine engine = proxy(Engine.class);
        Snapshot versionOne = proxy(Snapshot.class);
        Snapshot versionTwo = proxy(Snapshot.class);

        cache.putSnapshot("query-1", TABLE_LOCATION, 1, engine, versionOne);
        cache.putSnapshot("query-1", TABLE_LOCATION, 2, engine, versionTwo);

        assertSame(cache.getSnapshot("query-1", TABLE_LOCATION, 1).orElseThrow(AssertionError::new).getSnapshot(), versionOne);
        assertSame(cache.getSnapshot("query-1", TABLE_LOCATION, 2).orElseThrow(AssertionError::new).getSnapshot(), versionTwo);
        assertFalse(cache.getSnapshot("query-2", TABLE_LOCATION, 1).isPresent());

        cache.cleanupQuery("query-1");
        assertFalse(cache.getSnapshot("query-1", TABLE_LOCATION, 1).isPresent());
    }

    @Test
    public void testConcurrentTableResolutionLoadsOnce()
            throws Exception
    {
        DeltaQuerySnapshotCache cache = new DeltaQuerySnapshotCache();
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = newFixedThreadPool(8);
        try {
            List<Future<DeltaTable>> futures = new ArrayList<>();
            for (int index = 0; index < 8; index++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    return cache.getTable(
                            "query-1",
                            TABLE_NAME,
                            TABLE_LOCATION,
                            Optional.empty(),
                            Optional.empty(),
                            () -> Optional.of(table(loads.incrementAndGet())))
                            .orElseThrow(AssertionError::new);
                }));
            }

            start.countDown();
            DeltaTable expected = futures.get(0).get();
            for (Future<DeltaTable> future : futures) {
                assertSame(future.get(), expected);
            }
            assertEquals(loads.get(), 1);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testCleanupAllowsFreshResolution()
    {
        DeltaQuerySnapshotCache cache = new DeltaQuerySnapshotCache();
        AtomicInteger loads = new AtomicInteger();

        cache.getTable("query-1", TABLE_NAME, TABLE_LOCATION, Optional.empty(), Optional.empty(),
                () -> Optional.of(table(loads.incrementAndGet())));
        assertEquals(cache.getCachedQueryCount(), 1);

        cache.cleanupQuery("query-1");
        assertEquals(cache.getCachedQueryCount(), 0);
        assertTrue(cache.getTable("query-1", TABLE_NAME, TABLE_LOCATION, Optional.empty(), Optional.empty(),
                () -> Optional.of(table(loads.incrementAndGet()))).isPresent());
        assertEquals(loads.get(), 2);
    }

    private static DeltaTable table(long version)
    {
        return new DeltaTable("schema", "table", TABLE_LOCATION, Optional.of(version), ImmutableList.of());
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type)
    {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, arguments) -> null);
    }
}
