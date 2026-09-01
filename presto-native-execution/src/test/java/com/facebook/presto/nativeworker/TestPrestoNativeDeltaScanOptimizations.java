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

import com.facebook.presto.Session;
import com.facebook.presto.delta.TestDeltaScanOptimizations;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.delta.DeltaSessionProperties.PARQUET_DEREFERENCE_PUSHDOWN_ENABLED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static java.lang.String.format;

@Test
public class TestPrestoNativeDeltaScanOptimizations
        extends TestDeltaScanOptimizations
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
    public void nestedColumnFilter(String version)
    {
        String tableName = getVersionPrefix(version) + "data-reader-nested-struct";
        String testQuery = format("SELECT a.aa, a.ac.aca FROM \"%s\" WHERE a.aa in ('8', '9') AND a.ac.aca > 6",
                tableName);
        assertQuery(testQuery, "SELECT * FROM VALUES('8', 8),('9', 9)");

        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setCatalogSessionProperty(DELTA_CATALOG, PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, "true")
                .build();
        // Native execution fuses Project, Filter and TableScan into one
        // ScanFilterProject operator. The Java-only TupleDomain matcher in the
        // parent test cannot see nested filters represented in this fused
        // operator. The result assertion verifies the nested filter, while
        // this assertion verifies that the fused operator scans the Delta
        // table rather than falling back to another connector path.
        assertPlan(session, testQuery, anyTree(tableScan(tableName)));
    }
}
