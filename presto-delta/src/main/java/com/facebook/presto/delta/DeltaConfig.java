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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class DeltaConfig
{
    private int maxSplitsBatchSize = 200;
    private boolean parquetDereferencePushdownEnabled = true;
    private boolean caseSensitivePartitionsEnabled = true;
    private Duration metadataCacheTtl = new Duration(30, MINUTES);
    private long metadataCacheMaxSize = 1000;

    @NotNull
    public boolean isParquetDereferencePushdownEnabled()
    {
        return parquetDereferencePushdownEnabled;
    }

    @Config("delta.parquet-dereference-pushdown-enabled")
    public DeltaConfig setParquetDereferencePushdownEnabled(boolean parquetDereferencePushdownEnabled)
    {
        this.parquetDereferencePushdownEnabled = parquetDereferencePushdownEnabled;
        return this;
    }

    public int getMaxSplitsBatchSize()
    {
        return maxSplitsBatchSize;
    }

    @Config("delta.max-splits-batch-size")
    public DeltaConfig setMaxSplitsBatchSize(int maxSplitsBatchSize)
    {
        this.maxSplitsBatchSize = maxSplitsBatchSize;
        return this;
    }

    public boolean isCaseSensitivePartitionsEnabled()
    {
        return this.caseSensitivePartitionsEnabled;
    }

    @Config("delta.case-sensitive-partitions-enabled")
    public DeltaConfig setCaseSensitivePartitionsEnabled(boolean caseSensitivePartitionsEnabled)
    {
        this.caseSensitivePartitionsEnabled = caseSensitivePartitionsEnabled;
        return this;
    }

    @NotNull
    public Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @Config("delta.metadata-cache-ttl")
    @ConfigDescription("How long resolved Delta snapshots stay cached across queries. Latest-snapshot lookups still " +
            "check the transaction log tail for new commits. Set to 0s to disable the cache.")
    public DeltaConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    @Min(0)
    public long getMetadataCacheMaxSize()
    {
        return metadataCacheMaxSize;
    }

    @Config("delta.metadata-cache-max-size")
    @ConfigDescription("Maximum number of Delta snapshots cached across queries")
    public DeltaConfig setMetadataCacheMaxSize(long metadataCacheMaxSize)
    {
        this.metadataCacheMaxSize = metadataCacheMaxSize;
        return this;
    }
}
