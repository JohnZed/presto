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

import io.delta.kernel.defaults.internal.logstore.LogStoreProvider;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.Test;

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
}
