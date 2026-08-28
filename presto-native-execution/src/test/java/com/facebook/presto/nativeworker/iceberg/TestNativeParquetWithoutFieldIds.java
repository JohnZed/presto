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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.iceberg.AbstractTestParquetColumnNames;
import com.facebook.presto.testing.QueryRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeQueryRunnerParameters;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;

public class TestNativeParquetWithoutFieldIds
        extends AbstractTestParquetColumnNames
{
    @Override
    protected String parquetFixture()
    {
        // Use the exact same Delta Lake file as the Java control.
        return "iceberg/delta-lake-no-field-ids.parquet";
    }

    @Override
    protected Path createTemporaryDirectory()
            throws IOException
    {
        // The container launcher bind-mounts DATA_DIR at the same absolute path.
        return Files.createTempDirectory(getNativeQueryRunnerParameters().dataDirectory, "iceberg-parquet-column-names");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .setAddStorageFormatToPath(false)
                .setSchemaName(SCHEMANAME)
                .build();
    }
}
