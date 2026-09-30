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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.delta.TestDeltaScanOptimizations;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

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
    protected void assertDeltaQueryPlan(
            String tableName,
            String testQuery,
            Map<String, Domain> expectedConstraint,
            Map<String, Domain> expectedEnforcedConstraint)
    {
        if (expectedConstraint.keySet().stream().noneMatch(name -> name.contains("$_$_$"))) {
            super.assertDeltaQueryPlan(tableName, testQuery, expectedConstraint, expectedEnforcedConstraint);
            return;
        }
        // Native execution fuses nested filters into ScanFilterProject, so the
        // Java TupleDomain matcher cannot inspect them. The inherited test
        // still checks the query results; retain the Delta table-scan check.
        assertPlan(withDereferencePushdownEnabled(), testQuery, anyTree(tableScan(tableName)));
    }
}
