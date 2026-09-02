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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MINUTES;

public class TestDeltaConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DeltaConfig.class)
                .setMaxSplitsBatchSize(200)
                .setParquetDereferencePushdownEnabled(true)
                .setCaseSensitivePartitionsEnabled(true)
                .setMetadataCacheTtl(new Duration(30, MINUTES))
                .setMetadataCacheMaxSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("delta.max-splits-batch-size", "400")
                .put("delta.parquet-dereference-pushdown-enabled", "false")
                .put("delta.case-sensitive-partitions-enabled", "false")
                .put("delta.metadata-cache-ttl", "5m")
                .put("delta.metadata-cache-max-size", "10")
                .build();

        DeltaConfig expected = new DeltaConfig()
                .setMaxSplitsBatchSize(400)
                .setParquetDereferencePushdownEnabled(false)
                .setCaseSensitivePartitionsEnabled(false)
                .setMetadataCacheTtl(new Duration(5, MINUTES))
                .setMetadataCacheMaxSize(10);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
