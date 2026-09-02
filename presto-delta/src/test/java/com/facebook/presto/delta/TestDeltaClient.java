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
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDeltaClient
{
    @Test
    public void testConfigurePrestoS3LogStoreOnCopiedConfiguration()
    {
        Configuration source = new Configuration(false);
        source.set("existing", "value");

        Configuration configured = DeltaClient.configureDeltaLogStore(source);

        assertEquals(configured.get("existing"), "value");
        assertEquals(configured.get("io.delta.kernel.logStore.s3.impl"), PrestoS3LogStore.class.getName());
        assertEquals(configured.get("io.delta.kernel.logStore.s3a.impl"), PrestoS3LogStore.class.getName());
        assertEquals(configured.get("io.delta.kernel.logStore.s3n.impl"), PrestoS3LogStore.class.getName());
        assertTrue(configured.getBoolean("delta.enableFastS3AListFrom", false));
        assertNull(source.get("io.delta.kernel.logStore.s3.impl"));
        assertNull(source.get("delta.enableFastS3AListFrom"));
        assertTrue(LogStoreProvider.getLogStore(configured, "s3") instanceof PrestoS3LogStore);
    }

    @Test
    public void testPartitionColumnsComeFromSnapshotMetadata()
            throws URISyntaxException
    {
        Engine engine = DefaultEngine.create(new Configuration());
        Snapshot snapshot = latestSnapshot(engine, "delta_v3/test-partitions-uppercase");
        SchemaTableName tableName = new SchemaTableName("schema", "test_partitions_uppercase");

        List<DeltaColumn> caseInsensitive = DeltaClient.getSchema(
                new DeltaConfig().setCaseSensitivePartitionsEnabled(false), tableName, engine, snapshot);
        assertEquals(columnNames(caseInsensitive), ImmutableList.of("id", "birth_year"));
        assertEquals(partitionColumnNames(caseInsensitive), ImmutableList.of("birth_year"));

        List<DeltaColumn> caseSensitive = DeltaClient.getSchema(
                new DeltaConfig().setCaseSensitivePartitionsEnabled(true), tableName, engine, snapshot);
        assertEquals(columnNames(caseSensitive), ImmutableList.of("id", "BIRTH_YEAR"));
        assertEquals(partitionColumnNames(caseSensitive), ImmutableList.of("BIRTH_YEAR"));
    }

    @Test
    public void testUnpartitionedColumnMappedTableHasNoPartitionColumns()
            throws URISyntaxException
    {
        Engine engine = DefaultEngine.create(new Configuration());
        Snapshot snapshot = latestSnapshot(engine, "delta_v3/cm_name");

        List<DeltaColumn> columns = DeltaClient.getSchema(
                new DeltaConfig(), new SchemaTableName("schema", "cm_name"), engine, snapshot);
        assertTrue(columns.size() > 0);
        assertEquals(partitionColumnNames(columns), ImmutableList.of());
        assertTrue(columns.stream().allMatch(column -> column.getPhysicalName() != null));
    }

    private static Snapshot latestSnapshot(Engine engine, String resource)
            throws URISyntaxException
    {
        File tableDirectory = new File(TestDeltaClient.class.getClassLoader().getResource(resource).toURI());
        return Table.forPath(engine, tableDirectory.getPath()).getLatestSnapshot(engine);
    }

    private static List<String> columnNames(List<DeltaColumn> columns)
    {
        return columns.stream().map(DeltaColumn::getLogicalName).collect(toList());
    }

    private static List<String> partitionColumnNames(List<DeltaColumn> columns)
    {
        return columns.stream().filter(DeltaColumn::isPartition).map(DeltaColumn::getLogicalName).collect(toList());
    }
}
