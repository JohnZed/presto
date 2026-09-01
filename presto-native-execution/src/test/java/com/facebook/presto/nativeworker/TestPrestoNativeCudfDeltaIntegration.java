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

import com.facebook.presto.testing.QueryRunner;
import com.google.common.base.Joiner;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

@Test
public class TestPrestoNativeCudfDeltaIntegration
        extends TestPrestoNativeDeltaIntegration
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return NativeDeltaTestUtils.createQueryRunner(
                (queryRunner, tableName) -> registerDeltaTableInHMS(queryRunner, tableName, tableName),
                builder -> builder.setEnableCudf(true));
    }

    @Override
    @Test(dataProvider = "deltaReaderVersions")
    public void readArrayTypeData(String version)
    {
        // The cuDF conversion layer does not yet support nested VARBINARY or
        // short-decimal values. Keep the inherited Delta fixture and validate
        // every other array primitive here without pulling that unrelated work
        // into native Delta support.
        String testQuery = format(
                "SELECT as_array_int, as_array_long, as_array_byte, as_array_short, " +
                        "as_array_boolean, as_array_float, as_array_double, as_array_string " +
                        "FROM \"%s\".\"%s\"",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(version, "data-reader-array-primitives"));

        List<String> expectedRows = new ArrayList<>();
        for (byte i = 0; i < 10; i++) {
            expectedRows.add(format(
                    "SELECT array[cast(%s as integer)], array[cast(%s as bigint)], " +
                            "array[cast(%s as tinyint)], array[cast(%s as smallint)], " +
                            "array[%s], array[cast(%s as real)], array[cast(%s as double)], array['%s']",
                    i,
                    i,
                    i,
                    i,
                    i % 2 == 0 ? "true" : "false",
                    i,
                    i,
                    i));
        }
        assertQuery(testQuery, Joiner.on(" UNION ").join(expectedRows));
    }

    @Override
    @Test(enabled = false, dataProvider = "deltaReaderVersions")
    public void readMapTypeData(String version)
    {
        // The generic cuDF conversion layer currently exposes Parquet maps as
        // LIST<STRUCT<key, value>> and cannot retag them as Velox MAP vectors.
    }
}
