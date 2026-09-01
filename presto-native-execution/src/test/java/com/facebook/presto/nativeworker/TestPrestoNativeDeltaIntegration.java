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
package com.facebook.presto.nativeworker;

import com.facebook.presto.delta.TestDeltaIntegration;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

@Test
public class TestPrestoNativeDeltaIntegration
        extends TestDeltaIntegration
{
    @Override
    protected String goldenTablePath(String tableName)
    {
        return NativeDeltaTestUtils.localResourcePath(tableName);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return NativeDeltaTestUtils.createQueryRunner(
                (queryRunner, tableName) -> registerDeltaTableInHMS(queryRunner, tableName, tableName));
    }

    @Override
    @Test(dataProvider = "deltaReaderVersions")
    public void readPartitionedTableAllDataTypes(String version)
    {
        String testQuery = "SELECT as_int, as_long, as_byte, as_short, as_boolean, as_float, as_double, " +
                "as_string, as_date, as_big_decimal, value FROM \"" + getVersionPrefix(version) +
                "data-reader-partition-values\"";
        String expectedQuery = "SELECT * FROM VALUES " +
                "(0, cast(0 AS bigint), cast(0 AS smallint), cast(0 AS tinyint), true, 0.0, " +
                "cast(0.0 AS double), '0', DATE '2021-09-08', cast(0 AS decimal), '0'), " +
                "(1, cast(1 AS bigint), cast(1 AS smallint), cast(1 AS tinyint), false, 1.0, " +
                "cast(1.0 AS double), '1', DATE '2021-09-08', cast(1 AS decimal), '1'), " +
                "(null, null, null, null, null, null, null, null, null, null, '2')";
        assertQuery(testQuery, expectedQuery);
    }

    @Override
    @Test(enabled = false, dataProvider = "deltaReaderVersions")
    public void testDeltaTimezoneTypeSupportINT96(String version)
    {
        // Timestamp-with-time-zone support is tracked separately from native Delta reads.
    }

    @Override
    @Test(enabled = false, dataProvider = "deltaReaderVersions")
    public void testDeltaTimezoneTypeSupportINT64(String version)
    {
        // Timestamp-with-time-zone support is tracked separately from native Delta reads.
    }
}
