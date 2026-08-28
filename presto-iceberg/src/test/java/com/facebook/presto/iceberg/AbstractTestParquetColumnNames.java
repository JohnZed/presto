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
package com.facebook.presto.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.format.Util.readFileMetaData;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestParquetColumnNames
        extends IcebergImportedTableTestBase
{
    private static final String TABLE_NAME = "parquet_column_names";
    private static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "ucolumnname", Types.StringType.get()),
            Types.NestedField.optional(3, "lcolumnname", Types.StringType.get()));

    private Path temporaryDirectory;
    private Path dataFile;

    protected abstract String parquetFixture();

    protected Path createTemporaryDirectory()
            throws IOException
    {
        return Files.createTempDirectory("iceberg-parquet-column-names");
    }

    @BeforeMethod
    public void setupTable()
            throws Exception
    {
        temporaryDirectory = createTemporaryDirectory();
        Path tableDirectory = temporaryDirectory.resolve(TABLE_NAME);
        Table table = new HadoopTables(new Configuration()).create(TABLE_SCHEMA, tableDirectory.toUri().toString());

        Path dataDirectory = tableDirectory.resolve("data");
        Files.createDirectories(dataDirectory);
        dataFile = dataDirectory.resolve(Path.of(parquetFixture()).getFileName().toString());
        try (InputStream input = requireNonNull(
                getClass().getClassLoader().getResourceAsStream(parquetFixture()),
                "Missing test resource: " + parquetFixture())) {
            Files.copy(input, dataFile);
        }

        DataFile icebergDataFile = DataFiles.builder(table.spec())
                .withPath(dataFile.toUri().toString())
                .withFormat("PARQUET")
                .withFileSizeInBytes(Files.size(dataFile))
                .withRecordCount(1)
                .build();
        table.newAppend().appendFile(icebergDataFile).commit();

        assertQuerySucceeds(format(
                "CALL %s.system.register_table(schema => '%s', table_name => '%s', metadata_location => '%s')",
                CATALOGNAME,
                SCHEMANAME,
                TABLE_NAME,
                tableDirectory.toUri()));
    }

    @AfterMethod(alwaysRun = true)
    public void teardownTable()
    {
        if (temporaryDirectory != null) {
            dropAndCleanupTable(TABLE_NAME, temporaryDirectory.toString());
        }
    }

    @Test
    public void testReadParquetUsingColumnNames()
            throws Exception
    {
        assertFooterFieldIdsAreAbsent(dataFile);
        assertQuery(
                format("SELECT id, ucolumnname, lcolumnname FROM %s.%s.%s", CATALOGNAME, SCHEMANAME, TABLE_NAME),
                "VALUES (CAST(1 AS BIGINT), 'column1', 'column1')");
    }

    private static void assertFooterFieldIdsAreAbsent(Path parquetFile)
            throws IOException
    {
        byte[] fileBytes = Files.readAllBytes(parquetFile);
        int footerLength = ByteBuffer.wrap(fileBytes, fileBytes.length - 8, Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
        try (InputStream footer = new ByteArrayInputStream(fileBytes, fileBytes.length - 8 - footerLength, footerLength)) {
            FileMetaData metadata = readFileMetaData(footer);
            List<SchemaElement> fields = metadata.getSchema().subList(1, metadata.getSchemaSize());
            assertEquals(fields.size(), 3);
            for (SchemaElement field : fields) {
                assertEquals(field.isSetField_id(), false, "Unexpected field_id presence for " + field.getName());
            }
        }
    }
}
